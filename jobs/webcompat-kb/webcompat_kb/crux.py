import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, UTC
from typing import Self

from google.cloud import bigquery

from .base import EtlJob, VALID_DATASET_ID
from .bqhelpers import BigQuery, RangePartition


@dataclass
class Config:
    bq_project: str
    bq_crux_dataset: str
    write: bool

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> Self:
        return cls(
            bq_project=args.bq_project_id,
            bq_crux_dataset=args.bq_crux_dataset,
            write=args.write,
        )


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


def get_imported_datasets(client: BigQuery, config: Config) -> int:
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
    if config.write:
        query = f"INSERT `{config.bq_project}.{config.bq_crux_dataset}.origin_ranks` (yyyymm, origin, country_code, rank)\n({query})"

    logging.info("Updating CrUX data")
    client.ensure_table(
        "origin_ranks", schema, partition=RangePartition("yyyymm", 201701, 202501)
    )
    client.query(query)


def update_sightline_data(client: BigQuery, config: Config, date: int) -> None:
    if config.write:
        insert_str = f"INSERT `{config.bq_project}.{config.bq_crux_dataset}.sightline_top_1000` (yyyymm, host)"
    else:
        insert_str = ""

    query = f"""
{insert_str}
SELECT
  DISTINCT yyyymm,
  NET.HOST(origin) AS host,
FROM
  `{config.bq_crux_dataset}.origin_ranks` AS crux_ranks
JOIN (
  SELECT
    country_code
  FROM
    UNNEST(JSON_VALUE_ARRAY('["global", "us", "fr", "de", "es", "it", "mx"]')) AS country_code) AS countries
ON
  crux_ranks.country_code = countries.country_code
WHERE
  crux_ranks.rank = 1000
  AND crux_ranks.yyyymm = {date}"""

    schema = [
        bigquery.SchemaField("yyyymm", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("host", "STRING"),
    ]

    logging.info("Updating sightline data")
    client.ensure_table(
        "sightline_top_1000", schema, partition=RangePartition("yyyymm", 201701, 202501)
    )
    client.query(query)


def update_min_rank_data(client: BigQuery, config: Config, date: int) -> None:
    if config.write:
        insert_str = f"INSERT `{config.bq_project}.{config.bq_crux_dataset}.host_min_ranks` (yyyymm, host, global_rank, local_rank, sightline_rank, japan_rank)"
    else:
        insert_str = ""

    query = f"""
{insert_str}
SELECT
  yyyymm,
  NET.HOST(origin) AS host,
  MIN(
  IF
    (origin_ranks.country_code = "global", origin_ranks.rank, NULL)) AS global_rank,
  MIN(
  IF
    (origin_ranks.country_code != "global", origin_ranks.rank, NULL)) AS local_rank,
  MIN(
  IF
    (country_code In UNNEST(["global", "us", "fr", "de", "es", "it", "mx"]), origin_ranks.rank, NULL)) as sightline_rank,
  MIN(
  IF
    (country_code = "jp", origin_ranks.rank, NULL)) as japan_rank
FROM
  `moz-fx-dev-dschubert-wckb.crux_imported.origin_ranks` AS origin_ranks
WHERE
  origin_ranks.yyyymm = {date}
GROUP BY
  host
"""
    schema = [
        bigquery.SchemaField("yyyymm", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("host", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("global_rank", "INTEGER"),
        bigquery.SchemaField("local_rank", "INTEGER"),
        bigquery.SchemaField("sightline_rank", "INTEGER"),
        bigquery.SchemaField("japan_rank", "INTEGER"),
    ]

    logging.info("Updating host_min_ranks data")
    client.ensure_table(
        "host_min_ranks", schema, partition=RangePartition("yyyymm", 201701, 202501)
    )
    client.query(query)


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


class CruxJob(EtlJob):
    name = "crux"

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(
            title="CrUX", description="CrUX update arguments"
        )
        group.add_argument(
            "--bq-crux-dataset",
            default="crux_imported",
            help="BigQuery CrUX import dataset",
        )

    def set_default_args(
        self, parser: argparse.ArgumentParser, args: argparse.Namespace
    ) -> None:
        if not VALID_DATASET_ID.match(args.bq_crux_dataset):
            parser.print_usage()
            logging.error(f"Invalid crux dataset id {args.bq_crux_dataset}")
            sys.exit(1)

    def default_dataset(self, args: argparse.Namespace) -> str:
        return args.bq_crux_dataset

    def main(self, client: BigQuery, args: argparse.Namespace) -> None:
        run_at = datetime.now(UTC)
        config = Config.from_args(args)

        last_import_yyyymm = get_imported_datasets(client, config)
        last_yyyymm = get_previous_month_yyyymm(run_at)

        logging.debug(f"Last CrUX import was {last_import_yyyymm}")
        logging.debug(f"Last month was {last_yyyymm}")

        if last_import_yyyymm >= last_yyyymm:
            logging.info(f"Already have a CrUX import for {last_yyyymm}")
            return

        latest_yyyymm = get_latest_crux_dataset(client)
        logging.debug(f"Latest CrUX data is {latest_yyyymm}")

        if last_import_yyyymm >= latest_yyyymm:
            logging.info("No new CrUX data available")
            return

        update_crux_data(client, config, latest_yyyymm)
        update_sightline_data(client, config, latest_yyyymm)
        update_min_rank_data(client, config, latest_yyyymm)
        update_import_date(client, config, run_at, latest_yyyymm)
