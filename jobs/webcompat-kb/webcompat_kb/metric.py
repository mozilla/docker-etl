import argparse
import logging
from datetime import date

from google.cloud import bigquery

from .base import Context, EtlJob
from .bqhelpers import BigQuery
from .projectdata import Project


def update_metric_history(project: Project, client: BigQuery) -> None:
    bq_dataset_id = project["webcompat_knowledge_base"].id.dataset
    metric_dfns, metric_types = project.data.metric_dfns, project.data.metric_types
    history_metric_types = [
        metric_type for metric_type in metric_types if "history" in metric_type.contexts
    ]

    for metric in metric_dfns:
        metrics_table_name = f"webcompat_topline_metric_{metric.name}"
        history_table_name = f"webcompat_topline_metric_{metric.name}_history"

        history_schema = [
            bigquery.SchemaField("recorded_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        ]
        for metric_type in history_metric_types:
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
        rows = []
        for row in client.query(query):
            row_data = {
                "recorded_date": today,
                "date": row.date,
            }
            row_data.update(
                {
                    metric_type.name: row[metric_type.name]
                    for metric_type in history_metric_types
                }
            )
            rows.append(row_data)

        client.insert_rows(history_table, rows)


def update_metric_daily(project: Project, client: BigQuery) -> None:
    bq_dataset_id = project["webcompat_knowledge_base"].id.dataset
    metric_dfns, metric_types = project.data.metric_dfns, project.data.metric_types
    daily_metric_types = [
        metric_type for metric_type in metric_types if "daily" in metric_type.contexts
    ]

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

    for metric in metric_dfns:
        for metric_type in daily_metric_types:
            field_name = f"{metric_type.name}_{metric.name}"
            agg_function = metric_type.agg_function("bugs", metric)

            query_fields.append(f"{agg_function} AS {field_name}")
            insert_fields.append(field_name)

    metrics_query = f"""
SELECT
  {",\n  ".join(query_fields)}
FROM
  `{project["webcompat_knowledge_base"]["scored_site_reports"]}` AS bugs
WHERE bugs.resolution = ""
"""

    client.insert_query(
        history_table,
        insert_fields,
        metrics_query,
    )


def backfill_metric_daily(
    project: Project,
    client: BigQuery,
    write: bool,
    metric_name: str,
) -> None:
    bq_dataset_id = project["webcompat_knowledge_base"].id.dataset
    metric_dfns, metric_types = project.data.metric_dfns, project.data.metric_types
    daily_metric_types = [
        metric_type for metric_type in metric_types if "daily" in metric_type.contexts
    ]

    metric = None
    for metric in metric_dfns:
        if metric.name == metric_name:
            break
    else:
        raise ValueError(f"Metric named {metric_name} not found")

    select_fields = []
    field_names = []
    conditions = []
    for metric_type in daily_metric_types:
        field_name = f"{metric_type.name}_{metric.name}"
        field_names.append(field_name)
        select_fields.append(
            f"{metric_type.agg_function('bugs', metric)} AS {field_name}"
        )
        conditions.append(f"metric_daily.{field_name} IS NULL")
    select_query = f"""
SELECT
  date,
  {",\n  ".join(select_fields)}
FROM
  `{bq_dataset_id}.scored_site_reports` AS bugs
  JOIN `{bq_dataset_id}.webcompat_topline_metric_daily` as metric_daily
ON
  DATE(bugs.creation_time) <= metric_daily.date
  AND IF (bugs.resolved_time IS NOT NULL, DATE(bugs.resolved_time) >= date, TRUE)
WHERE
  {metric.condition("bugs")} AND {" AND ".join(conditions)}
GROUP BY
  date
ORDER BY date"""

    update_query = f"""
UPDATE `{bq_dataset_id}.webcompat_topline_metric_daily` AS metric_daily
SET
  {",\n  ".join(f"metric_daily.{field_name}=new_data.{field_name}" for field_name in field_names)}
FROM ({select_query}) AS new_data
WHERE new_data.date = metric_daily.date
"""

    if write:
        result = client.query(update_query)
    else:
        logging.info(f"Would run query:\n{update_query}")
        result = client.query(select_query)
        logging.info(f"Would set {list(result)}")


class MetricJob(EtlJob):
    name = "metric"

    def default_dataset(self, context: Context) -> str:
        return "webcompat_knowledge_base"

    def main(self, context: Context) -> None:
        update_metric_history(
            context.project,
            context.bq_client,
        )
        update_metric_daily(
            context.project,
            context.bq_client,
        )


class MetricBackfillJob(EtlJob):
    name = "metric-backfill"
    default = False

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(
            title="Metric Backfill", description="metric-backfill arguments"
        )
        group.add_argument(
            "--metric-backfill-metric", help="Name of the metric to backfill"
        )

    def required_args(self) -> set[str | tuple[str, str]]:
        return {"metric_backfill_metric"}

    def default_dataset(self, context: Context) -> str:
        return "webcompat_knowledge_base"

    def main(self, context: Context) -> None:
        backfill_metric_daily(
            context.project,
            context.bq_client,
            context.config.write,
            context.args.metric_backfill_metric,
        )
