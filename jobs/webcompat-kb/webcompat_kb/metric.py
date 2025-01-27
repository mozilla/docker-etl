import argparse
import logging
from datetime import date

from google.cloud import bigquery

from .base import EtlJob


def update_metric_history(
    client: bigquery.Client, bq_dataset_id: str, write: bool
) -> None:
    metrics_table = f"{bq_dataset_id}.webcompat_topline_metric"
    history_table = f"{bq_dataset_id}.webcompat_topline_metric_history"

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
        logging.info("Already recorded historic data today, skipping")
        return

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


class MetricJob(EtlJob):
    name = "metric"

    def main(self, client: bigquery.Client, args: argparse.Namespace) -> None:
        update_metric_history(client, args.bq_kb_dataset, args.write)
