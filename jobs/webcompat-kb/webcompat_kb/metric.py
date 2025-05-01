import argparse
import logging
from datetime import date

from google.cloud import bigquery

from .base import EtlJob
from .bqhelpers import BigQuery


def update_metric_history(client: BigQuery, bq_dataset_id: str, write: bool) -> None:
    for suffix in ["global_1000", "sightline", "all"]:
        metrics_table_name = f"webcompat_topline_metric_{suffix}"
        history_table_name = f"webcompat_topline_metric_{suffix}_history"

        history_schema = [
            bigquery.SchemaField("recorded_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("bug_count", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("needs_diagnosis_score", "NUMERIC", mode="REQUIRED"),
            bigquery.SchemaField("platform_score", "NUMERIC", mode="REQUIRED"),
            bigquery.SchemaField("not_supported_score", "NUMERIC", mode="REQUIRED"),
            bigquery.SchemaField("total_score", "NUMERIC", mode="REQUIRED"),
        ]

        history_table = client.ensure_table(
            history_table_name, history_schema, recreate=False
        )

        query = f"""
                SELECT recorded_date
                FROM `{bq_dataset_id}.{history_table_name}`
                ORDER BY recorded_date DESC
                LIMIT 1
            """

        rows = list(client.query(query))

        today = date.today()

        if rows and rows[0]["recorded_date"] >= today:
            # We've already recorded historic data today
            logging.info(
                f"Already recorded historic data in {history_table} today, skipping"
            )
            continue

        query = f"""
                SELECT *
                FROM `{bq_dataset_id}.{metrics_table_name}`
            """
        rows = [
            {
                "recorded_date": today,
                "date": row.date,
                "bug_count": row.bug_count,
                "needs_diagnosis_score": row.needs_diagnosis_score,
                "platform_score": row.platform_score,
                "not_supported_score": row.not_supported_score,
                "total_score": row.total_score,
            }
            for row in client.query(query)
        ]

        client.insert_rows(history_table, rows)


def update_metric_daily(client: BigQuery, bq_dataset_id: str, write: bool) -> None:
    history_table = f"{bq_dataset_id}.webcompat_topline_metric_daily"
    query = f"""
            SELECT date
            FROM `{history_table}`
            ORDER BY date DESC
            LIMIT 1"""

    rows = list(client.query(query))

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
        client.query(insert_query)
        logging.info("Updated daily metric")
    else:
        result = client.query(metrics_query)
        logging.info(f"Would insert {list(result)[0]}")


class MetricJob(EtlJob):
    name = "metric"

    def default_dataset(self, args: argparse.Namespace) -> str:
        return args.bq_kb_dataset

    def main(self, client: BigQuery, args: argparse.Namespace) -> None:
        update_metric_history(client, args.bq_kb_dataset, args.write)
        update_metric_daily(client, args.bq_kb_dataset, args.write)
