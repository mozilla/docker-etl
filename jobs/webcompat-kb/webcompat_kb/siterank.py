import argparse
import csv
import logging
from dataclasses import dataclass
from datetime import datetime, UTC
from typing import Iterator, Self

import httpx
from google.cloud import bigquery

from .base import Context, EtlJob, dataset_arg
from .bqhelpers import BigQuery, Json, RangePartition, get_client
from .httphelpers import get_json
from .metrics import ranks


@dataclass
class Config:
    bq_project: str
    bq_crux_dataset: str
    bq_tranco_dataset: str
    write: bool

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> Self:
        return cls(
            bq_project=args.bq_project_id,
            bq_crux_dataset=args.bq_crux_dataset,
            bq_tranco_dataset=args.bq_tranco_dataset,
            write=args.write,
        )


def get_last_import(client: BigQuery, config: Config) -> int:
    query = f"""
SELECT
  yyyymm
FROM
  `{config.bq_crux_dataset}.import_runs`
ORDER BY yyyymm DESC
LIMIT 1
"""

    try:
        result = list(client.query(query))
    except Exception:
        return 0

    if not result:
        return 0

    return result[0]["yyyymm"]


def get_latest_tranco_dataset(client: BigQuery, table: bigquery.Table) -> int:
    query = rf"""SELECT yyyymm
FROM `{table.table_id}`
ORDER BY yyyymm DESC
LIMIT 1
"""

    result = list(client.query(query))
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


def update_crux_data(client: BigQuery, config: Config, date: int) -> None:
    query = f"""
SELECT yyyymm, origin, country_code, experimental.popularity.rank as rank from `chrome-ux-report.experimental.country` WHERE yyyymm = {date}
UNION ALL
SELECT yyyymm, origin, "global" as country_code, experimental.popularity.rank as rank from `chrome-ux-report.experimental.global` WHERE yyyymm = {date}
"""
    schema = [
        bigquery.SchemaField("yyyymm", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("origin", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("country_code", "STRING", mode="REQUIRED", max_length=8),
        bigquery.SchemaField("rank", "INTEGER", mode="REQUIRED"),
    ]

    logging.info("Updating CrUX data")
    client.ensure_table(
        "origin_ranks", schema, partition=RangePartition("yyyymm", 201701, 202501)
    )
    client.insert_query(
        "origin_ranks",
        ["yyyymm", "origin", "country_code", "rank"],
        query,
        dataset_id=config.bq_crux_dataset,
    )


def get_tranco_data() -> Iterator[tuple[int, str]]:
    id_resp = get_json("https://tranco-list.eu/api/lists/date/latest?subdomains=true")
    assert isinstance(id_resp, dict)
    list_id = id_resp["list_id"]

    data_resp = httpx.get(
        f"https://tranco-list.eu/download/{list_id}/1000000", follow_redirects=True
    )
    data_resp.raise_for_status()
    for row in csv.reader(data_resp.iter_lines()):
        yield int(row[0]), row[1]


def update_tranco_data(
    client: BigQuery, config: Config, table: bigquery.Table, yyyymm: int
) -> None:
    rows: list[dict[str, Json]] = [
        {"yyyymm": yyyymm, "rank": rank, "host": host}
        for rank, host in get_tranco_data()
    ]
    if config.write:
        client.query(
            f"DELETE FROM `{table.table_id}` WHERE yyyymm = @yyyymm",
            parameters=[bigquery.ScalarQueryParameter("yyyymm", "INTEGER", yyyymm)],
        )
    client.write_table(table, table.schema, rows, overwrite=False)


def update_sightline_data(client: BigQuery, config: Config, yyyymm: int) -> None:
    query = f"""
SELECT
  DISTINCT yyyymm,
  NET.HOST(origin) AS host
FROM
  `{config.bq_crux_dataset}.origin_ranks` AS crux_ranks
JOIN UNNEST(["global", "us", "fr", "de", "es", "it", "mx"]) as country_code USING(country_code)
WHERE
  crux_ranks.rank = 1000
  AND crux_ranks.yyyymm = {yyyymm}"""

    schema = [
        bigquery.SchemaField("yyyymm", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("host", "STRING"),
    ]

    logging.info("Updating sightline data")
    client.ensure_table(
        "sightline_top_1000", schema, partition=RangePartition("yyyymm", 201701, 202501)
    )
    client.insert_query(
        "sightline_top_1000",
        ["yyyymm", "host"],
        query,
        dataset_id=config.bq_crux_dataset,
    )


def update_min_rank_data(client: BigQuery, config: Config, yyyymm: int) -> None:
    rank_columns = ranks.load()
    column_names = [f"{item.name}" for item in rank_columns]

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
    `moz-fx-dev-dschubert-wckb.tranco_imported.top_1M_subdomains`
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
    schema = [
        bigquery.SchemaField("yyyymm", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("host", "STRING", mode="REQUIRED"),
    ]
    schema.extend(
        bigquery.SchemaField(column.name, "INTEGER") for column in rank_columns
    )
    parameters = [bigquery.ScalarQueryParameter("yyyymm", "INTEGER", yyyymm)]
    logging.info("Updating host_min_ranks data")
    table = client.ensure_table(
        "host_min_ranks",
        schema,
        partition=RangePartition("yyyymm", 201701, 202501),
        update_fields=True,
    )
    if config.write:
        client.query(
            f"DELETE FROM `{table.table_id}` WHERE yyyymm = @yyyymm",
            parameters=parameters,
        )
    client.insert_query(
        "host_min_ranks",
        column_names,
        query,
        dataset_id=config.bq_crux_dataset,
        parameters=parameters,
    )


def update_import_date(
    client: BigQuery, config: Config, run_at: datetime, data_yyyymm: int
) -> None:
    if not config.write:
        return

    formatted_time = run_at.strftime("%Y-%m-%dT%H:%M:%SZ")

    rows_to_insert = [
        {
            "run_at": formatted_time,
            "yyyymm": data_yyyymm,
        },
    ]
    logging.info("Updating last run date")

    schema = [
        bigquery.SchemaField("run_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("yyyymm", "INTEGER", mode="REQUIRED"),
    ]
    runs_table = client.ensure_table("import_runs", schema)
    client.insert_rows(runs_table, rows_to_insert)


def get_previous_month_yyyymm(date: datetime) -> int:
    year = date.year
    month = date.month - 1

    if month == 0:
        year -= 1
        month = 12

    return year * 100 + month


def update_crux(
    client: BigQuery, config: Config, last_import_yyyymm: int
) -> tuple[int, bool]:
    latest_yyyymm = get_latest_crux_dataset(client)
    logging.debug(f"Latest CrUX data is {latest_yyyymm}")

    if last_import_yyyymm >= latest_yyyymm:
        logging.info("No new CrUX data available")
        return latest_yyyymm, False

    update_crux_data(client, config, latest_yyyymm)
    return latest_yyyymm, True


def update_tranco(
    client: BigQuery, config: Config, latest_crux_yyyymm: int, force_update: bool
) -> bool:
    schema = [
        bigquery.SchemaField("yyyymm", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("rank", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("host", "STRING", mode="REQUIRED"),
    ]
    subdomains_table = client.ensure_table(
        "top_1M_subdomains", schema, partition=RangePartition("yyyymm", 201701, 202501)
    )
    last_import_yyyymm = get_latest_tranco_dataset(client, subdomains_table)

    if last_import_yyyymm >= latest_crux_yyyymm and not force_update:
        return False

    update_tranco_data(client, config, subdomains_table, latest_crux_yyyymm)
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
        run_at = datetime.now(UTC)
        config = Config.from_args(context.args)
        client_crux = context.bq_client
        client_tranco = BigQuery(
            get_client(context.args.bq_project_id),
            context.args.bq_tranco_dataset,
            write=context.config.write,
        )

        last_month_yyyymm = get_previous_month_yyyymm(run_at)
        logging.debug(f"Last month was {last_month_yyyymm}")

        last_import_yyyymm = get_last_import(client_crux, config)
        logging.debug(f"Last site-ranks import was {last_import_yyyymm}")

        if (
            not context.args.site_ranks_force_tranco_update
            and last_import_yyyymm >= last_month_yyyymm
        ):
            logging.info("Site-ranks data is up to date")
            return

        latest_yyyymm, have_new_crux = update_crux(
            client_crux, config, last_import_yyyymm
        )
        if have_new_crux or context.args.site_ranks_force_tranco_update:
            update_tranco(
                client_tranco,
                config,
                latest_yyyymm,
                context.args.site_ranks_force_tranco_update,
            )

        if have_new_crux:
            update_sightline_data(client_crux, config, latest_yyyymm)
        if have_new_crux or context.args.site_ranks_force_host_min_ranks_update:
            update_min_rank_data(client_crux, config, latest_yyyymm)
        if have_new_crux:
            update_import_date(client_crux, config, run_at, latest_yyyymm)


def check_yyyymm(client: BigQuery, config: Config, yyyymm: int) -> bool:
    query = f"""
SELECT EXISTS (
  SELECT 1 FROM `{config.bq_crux_dataset}.host_min_ranks` WHERE yyyymm = @yyyymm) as has_yyyymm
"""
    parameters = [bigquery.ScalarQueryParameter("yyyymm", "INTEGER", yyyymm)]
    result = list(client.query(query, parameters=parameters))[0]
    return result.has_yyyymm


def create_new_routine(client: BigQuery, config: Config, yyyymm: int) -> str:
    new_name = f"{client.project_id}.{client.default_dataset_id}.WEBCOMPAT_METRIC_YYYYMM_{yyyymm}"
    query = f"CREATE OR REPLACE FUNCTION `{new_name}`() RETURNS INT64 AS ({yyyymm});"
    if config.write:
        logging.info(f"Creating function {new_name}")
        client.query(query)
    else:
        logging.info(f"Would create function {new_name}")
    return new_name


def create_new_scored_site_reports(
    client: BigQuery, config: Config, yyyymm: int, new_fn_name: str
) -> str:
    current_table = client.get_table("scored_site_reports")
    current_query = current_table.view_query

    fn_name = f"{client.project_id}.{client.default_dataset_id}.WEBCOMPAT_METRIC_YYYYMM"

    if fn_name not in current_query:
        raise ValueError(f"Failed to find {fn_name} in {current_table}")

    new_query = current_query.replace(fn_name, new_fn_name)
    new_table_id = f"{current_table.reference}_{yyyymm}"
    new_table = bigquery.Table(new_table_id)
    new_table.view_query = new_query

    if config.write:
        logging.info(f"Creating view {new_table_id}")
        client.client.create_table(new_table, exists_ok=True)
    else:
        logging.info(f"Would create view {new_table_id} with query:\n{new_query}")
    return new_table_id


def update_site_ranks(client: BigQuery, config: Config, yyyymm: int) -> None:
    from . import metric_rescore

    if not check_yyyymm(client, config, yyyymm):
        raise ValueError(f"No site rank data found for {yyyymm}")

    new_fn_name = create_new_routine(client, config, yyyymm)
    new_site_reports = create_new_scored_site_reports(
        client, config, yyyymm, new_fn_name
    )

    _, new_routine_id = new_fn_name.split(".", 1)
    logging.info(new_routine_id)

    metric_rescore.rescore(
        client,
        new_site_reports.rsplit(".", 1)[1],
        f"Update site rank data to {yyyymm}",
        [f"{client.default_dataset_id}.WEBCOMPAT_METRIC_YYYYMM:{new_routine_id}"],
    )


class SiteRanksUpdateList(EtlJob):
    name = "site-ranks-update"
    default = False

    def default_dataset(self, context: Context) -> str:
        return context.args.bq_kb_dataset

    def required_args(self) -> set[str | tuple[str, str]]:
        return {"bq_kb_dataset", "site_ranks_update_yyyymm"}

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(
            title="Site-Ranks", description="site-ranks update arguments"
        )
        group.add_argument(
            "--site-ranks-update-yyyymm",
            action="store",
            type=int,
            help="New site rank data to use in the format YYYYMM",
        )

    def main(self, context: Context) -> None:
        config = Config.from_args(context.args)
        update_site_ranks(
            context.bq_client,
            config,
            context.args.site_ranks_update_yyyymm,
        )
