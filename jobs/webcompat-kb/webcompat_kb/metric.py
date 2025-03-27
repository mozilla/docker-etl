import argparse
import logging
from datetime import date

from google.cloud import bigquery

from .base import EtlJob


def update_metric_history(
    client: bigquery.Client, bq_dataset_id: str, write: bool
) -> None:
    for suffix in ["global_1000", "sightline", "all"]:
        metrics_table = f"{bq_dataset_id}.webcompat_topline_metric_{suffix}"
        history_table = f"{bq_dataset_id}.webcompat_topline_metric_{suffix}_history"

        history_schema = [
            bigquery.SchemaField("recorded_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("bug_count", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("needs_diagnosis_score", "NUMERIC", mode="REQUIRED"),
            bigquery.SchemaField("platform_score", "NUMERIC", mode="REQUIRED"),
            bigquery.SchemaField("not_supported_score", "NUMERIC", mode="REQUIRED"),
            bigquery.SchemaField("total_score", "NUMERIC", mode="REQUIRED"),
        ]

        client.create_table(
            bigquery.Table(f"{client.project}.{history_table}", history_schema),
            exists_ok=True,
        )

        query = f"""
                SELECT recorded_date
                FROM `{history_table}`
                ORDER BY recorded_date DESC
                LIMIT 1
            """

        rows = list(client.query(query).result())

        today = date.today()

        if rows and rows[0]["recorded_date"] >= today:
            # We've already recorded historic data today
            logging.info(
                f"Already recorded historic data in {history_table} today, skipping"
            )
            continue

        query = f"""
                SELECT *
                FROM `{metrics_table}`
            """
        rows = list(dict(row.items()) for row in client.query(query).result())
        for row in rows:
            row["recorded_date"] = today

        if write:
            logging.info(f"Writing to {history_table} table")

            table = client.get_table(history_table)
            errors = client.insert_rows(table, rows)

            if errors:
                logging.error(errors)
            else:
                logging.info("Metrics history recorded")
                logging.info(f"Loaded {len(rows)} rows into {table}")
        else:
            logging.info(f"Skipping writes, would have written:\n{rows}")


def update_metric_daily(
    client: bigquery.Client, bq_dataset_id: str, write: bool
) -> None:
    history_table = f"{bq_dataset_id}.webcompat_topline_metric_daily"
    query = f"""
            SELECT date
            FROM `{history_table}`
            ORDER BY date DESC
            LIMIT 1"""

    rows = list(client.query(query).result())

    today = date.today()

    if rows and rows[0]["date"] >= today:
        # We've already recorded historic data today
        logging.info(
            f"Already recorded historic data in {history_table} today, skipping"
        )
        return

    metrics_query = f"""
SELECT
  current_date() as date,
  count(bugs.number) as bug_count_all,
  SUM(if(bugs.metric_type_needs_diagnosis, bugs.score, 0)) as needs_diagnosis_score_all,
  SUM(if(bugs.metric_type_firefox_not_supported, bugs.score, 0)) as not_supported_score_all,
  SUM(bugs.score) AS total_score_all,
  COUNTIF(bugs.is_sightline) as bug_count_sightline,
  SUM(if(bugs.is_sightline and bugs.metric_type_needs_diagnosis, bugs.score, 0)) as needs_diagnosis_score_sightline,
  SUM(if(bugs.is_sightline and bugs.metric_type_firefox_not_supported, bugs.score, 0)) as not_supported_score_sightline,
  SUM(if(bugs.is_sightline, bugs.score, 0)) AS total_score_sightline,
  COUNTIF(bugs.is_global_1000) as bug_count_global_1000,
  SUM(if(bugs.is_global_1000 and bugs.metric_type_needs_diagnosis, bugs.score, 0)) as needs_diagnosis_score_global_1000,
  SUM(if(bugs.is_global_1000 and bugs.metric_type_firefox_not_supported, bugs.score, 0)) as not_supported_score_global_1000,
  SUM(if(bugs.is_global_1000, bugs.score, 0)) AS total_score_global_1000
FROM
  `{bq_dataset_id}.scored_site_reports` AS bugs
WHERE bugs.resolution = ""
"""

    if write:
        insert_query = f"""INSERT `{bq_dataset_id}.webcompat_topline_metric_daily`
        (date,
        bug_count_all,
        needs_diagnosis_score_all,
        not_supported_score_all,
        total_score_all,
        bug_count_sightline,
        needs_diagnosis_score_sightline,
        not_supported_score_sightline,
        total_score_sightline,
        bug_count_global_1000,
        needs_diagnosis_score_global_1000,
        not_supported_score_global_1000,
        total_score_global_1000)
        ({metrics_query})"""
        logging.debug(insert_query)
        client.query(insert_query).result()
        logging.info("Updated daily metric")
    else:
        result = client.query(metrics_query).result()
        logging.info(f"Would insert {list(result)[0]}")


class MetricJob(EtlJob):
    name = "metric"

    def main(self, client: bigquery.Client, args: argparse.Namespace) -> None:
        update_metric_history(client, args.bq_kb_dataset, args.write)
        update_metric_daily(client, args.bq_kb_dataset, args.write)
