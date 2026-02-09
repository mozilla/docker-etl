import argparse
import csv
import logging
from datetime import datetime, UTC
from typing import Any, Iterator, cast

import httpx
from google.cloud import bigquery

from .base import Context, EtlJob, dataset_arg
from .bqhelpers import BigQuery, Json, TableSchema
from .httphelpers import get_json
from .projectdata import Project


def get_latest_yyyymm(client: BigQuery, table: TableSchema) -> int:
    query = f"""SELECT yyyymm
FROM `{table}`
ORDER BY yyyymm DESC
LIMIT 1
"""

    try:
        result = list(client.query(query))
    except Exception:
        # Table doesn't exist
        return 0

    if len(result) != 1:
        return 0

    return result[0].yyyymm


def get_latest_crux_dataset(client: BigQuery) -> int:
    query = r"""SELECT
  cast(tables.table_name as int) AS crux_date
FROM
  `chrome-ux-report.all.INFORMATION_SCHEMA.TABLES` AS tables
WHERE
  tables.table_schema = "all"
  AND REGEXP_CONTAINS(tables.table_name, r"20\d\d\d\d")
ORDER BY crux_date DESC
LIMIT 1
"""

    result = list(client.query(query))
    if len(result) != 1:
        raise ValueError("Failed to get latest CrUX import")

    return result[0]["crux_date"]


def update_crux_data(project: Project, client: BigQuery, yyyymm: int) -> None:
    table = project["crux_imported"]["origin_ranks"].table()
    query = """
SELECT yyyymm, origin, country_code, experimental.popularity.rank as rank from `chrome-ux-report.experimental.country` WHERE yyyymm = @yyyymm
UNION ALL
SELECT yyyymm, origin, "global" as country_code, experimental.popularity.rank as rank from `chrome-ux-report.experimental.global` WHERE yyyymm = @yyyymm
"""
    logging.info("Updating CrUX data")
    client.insert_query(
        table,
        ["yyyymm", "origin", "country_code", "rank"],
        query,
        parameters=[bigquery.ScalarQueryParameter("yyyymm", "INTEGER", yyyymm)],
    )


def get_tranco_data() -> Iterator[tuple[int, str]]:
    id_resp = get_json("https://tranco-list.eu/api/lists/date/latest?subdomains=true")
    assert isinstance(id_resp, dict)
    id_resp = cast(dict[str, Any], id_resp)
    list_id = id_resp["list_id"]

    data_resp = httpx.get(
        f"https://tranco-list.eu/download/{list_id}/1000000", follow_redirects=True
    )
    data_resp.raise_for_status()
    for row in csv.reader(data_resp.iter_lines()):
        yield int(row[0]), row[1]


def update_tranco_data(client: BigQuery, table: TableSchema, yyyymm: int) -> None:
    rows: list[dict[str, Json]] = [
        {"yyyymm": yyyymm, "rank": rank, "host": host}
        for rank, host in get_tranco_data()
    ]
    client.delete_query(
        table,
        condition="yyyymm = @yyyymm",
        parameters=[bigquery.ScalarQueryParameter("yyyymm", "INTEGER", yyyymm)],
    )
    client.write_table(table, table.schema, rows, overwrite=False)


def update_sightline_data(project: Project, client: BigQuery, yyyymm: int) -> None:
    origin_ranks_table = project["crux_imported"]["origin_ranks"].table()
    sightline_top_1000_table = project["crux_imported"]["sightline_top_1000"].table()
    query = f"""
SELECT
  DISTINCT yyyymm,
  NET.HOST(origin) AS host
FROM
  `{origin_ranks_table}` AS crux_ranks
JOIN UNNEST(["global", "us", "fr", "de", "es", "it", "mx"]) as country_code USING(country_code)
WHERE
  crux_ranks.rank = 1000
  AND crux_ranks.yyyymm = {yyyymm}"""

    logging.info("Updating sightline data")
    client.insert_query(sightline_top_1000_table, ["yyyymm", "host"], query)


def update_min_rank_data(project: Project, client: BigQuery, yyyymm: int) -> None:
    rank_columns = project.data.rank_dfns
    column_names = ["yyyymm", "host"] + [f"{item.name}" for item in rank_columns]

    crux_columns = ",\n    ".join(
        f"MIN(IF({column.crux_condition}, crux_ranks.rank, NULL)) as {column.name}"
        for column in rank_columns
        if column.crux_condition
    )
    host_min_rank_columns = ",\n  ".join(
        f"{column.rank} as {column.name}" if column.rank else column.name
        for column in rank_columns
    )
    query = f"""
WITH
  crux_ranks AS (
  SELECT
    NET.HOST(origin) AS host,
    {crux_columns}
  FROM
    `moz-fx-dev-dschubert-wckb.crux_imported.origin_ranks` AS crux_ranks
  WHERE
    yyyymm = @yyyymm
  GROUP BY
    host),
  tranco_ranks AS (
  SELECT
    host,
    rank,
    CASE
      WHEN rank <= 1000 THEN 1000
      WHEN rank <= 5000 THEN 5000
      WHEN rank <= 10000 THEN 10000
      WHEN rank <= 50000 THEN 50000
      WHEN rank <= 100000 THEN 100000
      WHEN rank <= 500000 THEN 500000
      ELSE 1000000
  END
    AS rank_bucket
  FROM
    `{project["tranco_imported"]["top_1M_subdomains"]}`
  WHERE
    yyyymm = @yyyymm)

SELECT
  @yyyymm as yyyymm,
  host,
  {host_min_rank_columns}
FROM
  crux_ranks
FULL OUTER JOIN
  tranco_ranks
USING
  (host)
"""
    host_min_ranks_table = project["crux_imported"]["host_min_ranks"].table()
    parameters = [bigquery.ScalarQueryParameter("yyyymm", "INTEGER", yyyymm)]
    logging.info("Updating host_min_ranks data")
    client.delete_query(
        host_min_ranks_table,
        condition="yyyymm = @yyyymm",
        parameters=parameters,
    )
    client.insert_query(
        host_min_ranks_table,
        column_names,
        query,
        parameters=parameters,
    )


def record_update(client: BigQuery, table: TableSchema, yyyymm: int) -> None:
    client.insert_query(
        table,
        columns=[item.name for item in table.fields],
        query="SELECT @yyyymm, CURRENT_TIMESTAMP()",
        parameters=[bigquery.ScalarQueryParameter("yyyymm", "INTEGER", yyyymm)],
    )


def get_previous_month_yyyymm(date: datetime) -> int:
    year = date.year
    month = date.month - 1

    if month == 0:
        year -= 1
        month = 12

    return year * 100 + month


def update_crux(
    project: Project, client: BigQuery, last_import_yyyymm: int
) -> tuple[int, bool]:
    latest_yyyymm = get_latest_crux_dataset(client)
    logging.debug(f"Latest CrUX data is {latest_yyyymm}")

    if last_import_yyyymm >= latest_yyyymm:
        logging.info("No new CrUX data available")
        return latest_yyyymm, False

    update_crux_data(project, client, latest_yyyymm)
    return latest_yyyymm, True


def update_tranco(
    project: Project, client: BigQuery, latest_crux_yyyymm: int, force_update: bool
) -> bool:
    subdomains_table = project["tranco_imported"]["top_1M_subdomains"].table()
    last_import_yyyymm = get_latest_yyyymm(client, subdomains_table)

    if last_import_yyyymm >= latest_crux_yyyymm and not force_update:
        return False

    update_tranco_data(client, subdomains_table, latest_crux_yyyymm)
    return True


class SiteRanksJob(EtlJob):
    name = "site-ranks"

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(
            title="Site-Ranks", description="site-ranks update arguments"
        )
        group.add_argument(
            "--bq-crux-dataset",
            default="crux_imported",
            type=dataset_arg,
            help="BigQuery CrUX import dataset",
        )
        group.add_argument(
            "--bq-tranco-dataset",
            default="tranco_imported",
            type=dataset_arg,
            help="BigQuery Tranco import dataset",
        )
        group.add_argument(
            "--site-ranks-force-tranco-update",
            action="store_true",
            help="Update tranco data even if there isn't any new CrUX data",
        )
        group.add_argument(
            "--site-ranks-force-host-min-ranks-update",
            action="store_true",
            help="Update hosts-min-rank data even if there isn't any new CrUX data",
        )

    def default_dataset(self, context: Context) -> str:
        return context.args.bq_crux_dataset

    def main(self, context: Context) -> None:
        project = context.project
        client = context.bq_client
        run_at = datetime.now(UTC)

        last_month_yyyymm = get_previous_month_yyyymm(run_at)
        logging.debug(f"Last month was {last_month_yyyymm}")

        import_runs_table = project["crux_imported"]["import_runs"].table()

        last_import_yyyymm = get_latest_yyyymm(client, import_runs_table)
        logging.debug(f"Last site-ranks import was {last_import_yyyymm}")

        if (
            not context.args.site_ranks_force_tranco_update
            and last_import_yyyymm >= last_month_yyyymm
        ):
            logging.info("Site-ranks data is up to date")
            return

        latest_yyyymm, have_new_crux = update_crux(project, client, last_import_yyyymm)
        if have_new_crux or context.args.site_ranks_force_tranco_update:
            update_tranco(
                project,
                client,
                latest_yyyymm,
                context.args.site_ranks_force_tranco_update,
            )

        if have_new_crux:
            update_sightline_data(project, client, latest_yyyymm)
        if have_new_crux or context.args.site_ranks_force_host_min_ranks_update:
            update_min_rank_data(project, client, latest_yyyymm)
        if have_new_crux:
            record_update(client, import_runs_table, latest_yyyymm)
