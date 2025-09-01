import argparse
import logging
from abc import ABC, abstractmethod
from datetime import date
from typing import Optional

from google.cloud import bigquery

from .base import EtlJob
from .bqhelpers import BigQuery


class Metric:
    def __init__(self, name: str):
        self.name = name
        self.conditional = name != "all"

    @property
    def site_reports_field(self) -> str:
        return f"is_{self.name}"


class MetricType(ABC):
    field_type: str

    def __init__(self, name: str, metric_type_field: Optional[str] = None):
        self.name = name
        self.metric_type_field = metric_type_field

    @abstractmethod
    def agg_function(self, table: str, metric: Metric) -> str: ...

    def condition(self, table: str, metric: Metric) -> str:
        """Condition applied to the scored_site_reports table to decide if the entry contributes to the metric score.

        :param str table: Alias for scored_site_reports.
        :param Metric metric: Metric for which the condition applies
        :returns str: SQL condition that is TRUE when the scored_site_reports row is included in the metric.conditional
        """
        conds = []
        if self.metric_type_field is not None:
            conds.append(f"{table}.{self.metric_type_field}")
        if metric.conditional:
            conds.append(f"{table}.{metric.site_reports_field}")
        if not conds:
            return "TRUE"
        return " AND ".join(conds)


class CountMetricType(MetricType):
    field_type = "INTEGER"

    def agg_function(self, table: str, metric: Metric) -> str:
        if not metric.conditional:
            return f"COUNT({table}.number)"
        return f"COUNTIF({self.condition(table, metric)})"


class SumMetricType(MetricType):
    field_type = "NUMERIC"

    def agg_function(self, table: str, metric: Metric) -> str:
        return f"SUM(IF({self.condition(table, metric)}, {table}.score, 0))"


metrics = [
    Metric("all"),
    Metric("sightline"),
    Metric("japan_1000"),
    Metric("japan_1000_mobile"),
    Metric("global_1000"),
]


metric_types = [
    CountMetricType("bug_count", None),
    SumMetricType("needs_diagnosis", "metric_type_needs_diagnosis"),
    SumMetricType("not_supported", "metric_type_firefox_not_supported"),
    SumMetricType("total_score", None),
]


def update_metric_history(client: BigQuery, bq_dataset_id: str, write: bool) -> None:
    for metric in metrics:
        metrics_table_name = f"webcompat_topline_metric_{metric.name}"
        history_table_name = f"webcompat_topline_metric_{metric.name}_history"

        history_schema = [
            bigquery.SchemaField("recorded_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("platform_score", "NUMERIC", mode="REQUIRED"),
        ]
        for metric_type in metric_types:
            history_schema.append(
                bigquery.SchemaField(
                    metric_type.name, metric_type.field_type, mode="REQUIRED"
                )
            )

        history_table = client.ensure_table(history_table_name, history_schema)

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

    query_fields = ["CURRENT_DATE() AS date"]
    insert_fields = ["date"]

    for metric in metrics:
        for metric_type in metric_types:
            field_name = f"{metric_type.name}_{metric.name}"
            agg_function = metric_type.agg_function("bugs", metric)

            query_fields.append(f"{agg_function} AS {field_name}")
            insert_fields.append(field_name)

    metrics_query = f"""
SELECT
  {",\n  ".join(query_fields)}
FROM
  `{bq_dataset_id}.scored_site_reports` AS bugs
WHERE bugs.resolution = ""
"""

    if write:
        insert_query = f"""
INSERT `{bq_dataset_id}.webcompat_topline_metric_daily`
  ({",\n  ".join(insert_fields)})
  ({metrics_query})"""
        client.query(insert_query)
        logging.info("Updated daily metric")
    else:
        result = client.query(metrics_query)
        logging.info(f"Would insert {list(result)[0]}")


class MetricJob(EtlJob):
    name = "metric"

    def required_args(self) -> set[str | tuple[str, str]]:
        return {"bq_kb_dataset"}

    def default_dataset(self, args: argparse.Namespace) -> str:
        return args.bq_kb_dataset

    def main(self, client: BigQuery, args: argparse.Namespace) -> None:
        update_metric_history(client, args.bq_kb_dataset, args.write)
        update_metric_daily(client, args.bq_kb_dataset, args.write)
