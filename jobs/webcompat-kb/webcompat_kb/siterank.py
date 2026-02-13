import argparse
import csv
import logging
from collections.abc import Collection, Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, cast
import httpx
from google.cloud import bigquery

from .base import Context, EtlJob
from .bqhelpers import BigQuery, Json, TableSchema
from .httphelpers import get_json
from .projectdata import Project


@dataclass
class ImportData:
    yyyymm: int
    run_at: datetime
    crux_rows: int
    is_complete: bool


@dataclass
class CruxUpdateResult:
    yyyymm: int
    crux_rows: int
    is_complete: bool


def get_latest_import(
    client: BigQuery, table: TableSchema
) -> tuple[Optional[ImportData], Optional[ImportData]]:
    """Return import data for latest complete import and latest incomplete import"""

    complete_import = None
    incomplete_import = None
    for complete in [True, False]:
        query = f"""SELECT yyyymm, run_at, IFNULL(is_complete, FALSE) AS is_complete, IFNULL(crux_rows, 0) AS crux_rows
FROM `{table}`
WHERE is_complete = @complete OR (NOT @complete AND is_complete IS NULL)
ORDER BY yyyymm DESC
LIMIT 1
"""

        try:
            result = list(
                client.query(
                    query,
                    parameters=[
                        bigquery.ScalarQueryParameter("complete", "BOOL", complete)
                    ],
                )
            )
        except Exception:
            continue

        if len(result) != 1:
            continue

        row = result[0]
        data = ImportData(
            yyyymm=row.yyyymm,
            run_at=row.run_at,
            is_complete=row.is_complete,
            crux_rows=row.crux_rows,
        )
        if complete:
            complete_import = data
        else:
            incomplete_import = data

    return complete_import, incomplete_import


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


def count_crux_rows(client: BigQuery, yyyymm: int) -> int:
    query = """
SELECT count(*) AS count FROM `chrome-ux-report.experimental.country` WHERE yyyymm = @yyyymm
UNION ALL
SELECT count(*) AS count FROM `chrome-ux-report.experimental.global` WHERE yyyymm = @yyyymm
"""
    result = list(
        client.query(
            query,
            parameters=[bigquery.ScalarQueryParameter("yyyymm", "INTEGER", yyyymm)],
        )
    )
    if len(result) != 2:
        return 0

    return sum(item.count for item in result)


def delete_existing_data(client: BigQuery, table: TableSchema, yyyymm: int) -> None:
    parameters = [bigquery.ScalarQueryParameter("yyyymm", "INTEGER", yyyymm)]

    client.delete_query(table, condition="yyyymm = @yyyymm", parameters=parameters)


def update_crux_data(project: Project, client: BigQuery, yyyymm: int) -> None:
    logging.info(f"Importing CrUX data for {yyyymm}")
    table = project["crux_imported"]["origin_ranks"].table()
    delete_existing_data(client, table, yyyymm)
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
    logging.info(f"Importing Tranco data for {yyyymm}")
    rows: list[dict[str, Json]] = [
        {"yyyymm": yyyymm, "rank": rank, "host": host}
        for rank, host in get_tranco_data()
    ]
    delete_existing_data(client, table, yyyymm)
    client.write_table(table, table.schema, rows, overwrite=False)


def update_sightline_data(project: Project, client: BigQuery, yyyymm: int) -> None:
    logging.info(f"Importing sightline data for {yyyymm}")
    origin_ranks_table = project["crux_imported"]["origin_ranks"].table()
    sightline_top_1000_table = project["crux_imported"]["sightline_top_1000"].table()
    parameters = [bigquery.ScalarQueryParameter("yyyymm", "INTEGER", yyyymm)]

    delete_existing_data(client, sightline_top_1000_table, yyyymm)

    query = f"""
SELECT
  DISTINCT yyyymm,
  NET.HOST(origin) AS host
FROM
  `{origin_ranks_table}` AS crux_ranks
JOIN UNNEST(["global", "us", "fr", "de", "es", "it", "mx"]) as country_code USING(country_code)
WHERE
  crux_ranks.rank = 1000
  AND crux_ranks.yyyymm = @yyyymm"""

    logging.info("Updating sightline data")
    client.insert_query(
        sightline_top_1000_table, ["yyyymm", "host"], query, parameters=parameters
    )


def host_min_ranks_query(
    project: Project,
    filter_yyyymm: bool = True,
    ranks: Optional[Collection[str]] = None,
) -> str:
    rank_columns = project.data.rank_dfns
    if ranks:
        rank_columns = [item for item in rank_columns if item.name in ranks]

    host_min_rank_columns = ",\n  ".join(
        f"{column.rank} as {column.name}" if column.rank else column.name
        for column in rank_columns
    )
    crux_columns = ",\n    ".join(
        f"MIN(IF({column.crux_condition}, crux_ranks.rank, NULL)) as {column.name}"
        for column in rank_columns
        if column.crux_condition
    )

    filter_str = "WHERE yyyymm = @yyyymm" if filter_yyyymm else ""

    crux_query = f"""SELECT
  yyyymm,
  NET.HOST(origin) AS host,
    {crux_columns}
  FROM
    `{project["crux_imported"]["origin_ranks"]}` AS crux_ranks
  {filter_str}
  GROUP BY yyyymm, host"""

    tranco_query = f"""SELECT
  yyyymm,
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
{filter_str}
"""

    query = f"""
SELECT
  yyyymm,
  host,
  {host_min_rank_columns}
FROM ({crux_query}) as crux_ranks
FULL OUTER JOIN ({tranco_query}) AS tranco_ranks USING(yyyymm, host)
"""
    return query


def update_min_rank_data(project: Project, client: BigQuery, yyyymm: int) -> None:
    query = host_min_ranks_query(project)
    host_min_ranks_table = project["crux_imported"]["host_min_ranks"].table()
    parameters = [bigquery.ScalarQueryParameter("yyyymm", "INTEGER", yyyymm)]
    logging.info(f"Updating host_min_ranks data for {yyyymm}")

    delete_existing_data(client, host_min_ranks_table, yyyymm)
    column_names = ["yyyymm", "host"] + [
        f"{item.name}" for item in project.data.rank_dfns
    ]
    client.insert_query(
        host_min_ranks_table,
        column_names,
        query,
        parameters=parameters,
    )


def record_update(
    client: BigQuery, table: TableSchema, update_result: CruxUpdateResult
) -> None:
    delete_existing_data(client, table, update_result.yyyymm)
    client.insert_query(
        table,
        columns=[item.name for item in table.fields],
        query="SELECT @yyyymm, CURRENT_TIMESTAMP(), @crux_rows, @is_complete",
        parameters=[
            bigquery.ScalarQueryParameter("yyyymm", "INTEGER", update_result.yyyymm),
            bigquery.ScalarQueryParameter(
                "crux_rows", "INTEGER", update_result.crux_rows
            ),
            bigquery.ScalarQueryParameter(
                "is_complete", "BOOL", update_result.is_complete
            ),
        ],
    )


def get_previous_month_yyyymm(date: datetime) -> int:
    year = date.year
    month = date.month - 1

    if month == 0:
        year -= 1
        month = 12

    return year * 100 + month


def update_crux_latest(
    project: Project, client: BigQuery, import_runs_table: TableSchema
) -> Optional[CruxUpdateResult]:
    """Update to specified target CrUX version, if available.

    CrUX imports are not atomic upstream, so we run them in multiple stages.
    Initially we count the number of rows available in the data we will import,
    and store this in the import_runs table. Only when the number of rows matches
    between ETL runs do we actually perform the import."""

    last_month_yyyymm = get_previous_month_yyyymm(datetime.now())
    logging.debug(f"Last month was {last_month_yyyymm}")

    complete_import, incomplete_import = get_latest_import(client, import_runs_table)

    if (
        complete_import
        and incomplete_import
        and complete_import.yyyymm > incomplete_import.yyyymm
    ):
        logging.debug(
            f"There are incomplete imports (latest {incomplete_import.yyyymm},"
            "but later complete imports, skipping the incomplete imports"
        )
        incomplete_import = None

    if incomplete_import is None:
        # We want to start a new import, if there's data available
        if complete_import:
            logging.debug(f"Last site-ranks import was {complete_import.yyyymm}")
            if complete_import.yyyymm >= last_month_yyyymm:
                logging.info("Site-ranks data is up to date")
                return None

        latest_crux_yyyymm = get_latest_crux_dataset(client)
        if complete_import and latest_crux_yyyymm <= complete_import.yyyymm:
            logging.info("Site-ranks data is up to date")
            return None

        target_yyyymm = latest_crux_yyyymm
        crux_rows_prev = 0
    else:
        # We want to complete the existing import
        target_yyyymm = incomplete_import.yyyymm
        crux_rows_prev = incomplete_import.crux_rows or 0
        logging.info(f"Found incomplete CrUX import for {target_yyyymm}")

    crux_rows = count_crux_rows(client, target_yyyymm)

    if crux_rows == 0:
        logging.warning(f"No CrUX data available for {target_yyyymm}")
        return None

    logging.debug(f"Found {crux_rows}, previously {crux_rows_prev}")
    if crux_rows != crux_rows_prev:
        # The row count has not yet stabilized, return the new row count
        # but don't actually import anything
        return CruxUpdateResult(
            yyyymm=target_yyyymm, crux_rows=crux_rows, is_complete=False
        )

    update_crux_data(project, client, target_yyyymm)
    return CruxUpdateResult(yyyymm=target_yyyymm, crux_rows=crux_rows, is_complete=True)


def update_tranco(project: Project, client: BigQuery, latest_crux_yyyymm: int) -> bool:
    subdomains_table = project["tranco_imported"]["top_1M_subdomains"].table()
    query = f"SELECT MAX(yyyymm) as yyyymm FROM {subdomains_table}"
    rows = list(client.query(query))
    if len(rows) == 0:
        last_import_yyyymm = 0
    else:
        assert len(rows) == 1
        last_import_yyyymm = rows[0].yyyymm

    if last_import_yyyymm >= latest_crux_yyyymm:
        return False

    update_tranco_data(client, subdomains_table, latest_crux_yyyymm)
    return True


class SiteRanksJob(EtlJob):
    name = "site-ranks"

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(
            title="site-ranks", description="site-ranks arguments"
        )
        group.add_argument(
            "--site-ranks-recreate",
            action="store",
            type=int,
            help="Recreate all imported data for specified yyyymm",
        )

    def default_dataset(self, context: Context) -> str:
        return "crux_imported"

    def main(self, context: Context) -> None:
        project = context.project
        client = context.bq_client

        recreate_yyyymm = context.args.site_ranks_recreate
        import_runs_table = project["crux_imported"]["import_runs"].table()

        if recreate_yyyymm is None:
            update_result = update_crux_latest(project, client, import_runs_table)
            if update_result is None:
                return
        else:
            update_crux_data(project, client, recreate_yyyymm)
            crux_rows = count_crux_rows(client, recreate_yyyymm)
            update_result = CruxUpdateResult(
                yyyymm=recreate_yyyymm, crux_rows=crux_rows, is_complete=True
            )

        if update_result.is_complete is False and recreate_yyyymm is None:
            # Record the partial update
            logging.info(
                f"Recording CrUX data row count for {update_result.yyyymm}, but not updating"
            )
            record_update(client, import_runs_table, update_result)
            return

        if recreate_yyyymm is None:
            update_tranco(project, client, update_result.yyyymm)
        else:
            logging.warning("Updating tranco data to previous months is not supported")
        update_sightline_data(project, client, update_result.yyyymm)
        update_min_rank_data(project, client, update_result.yyyymm)

        if recreate_yyyymm is None:
            record_update(client, import_runs_table, update_result)
