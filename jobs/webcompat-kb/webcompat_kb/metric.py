import logging
from datetime import date

from .base import Context, EtlJob
from .bqhelpers import BigQuery
from .projectdata import Project


def update_metric_history(project: Project, client: BigQuery) -> None:
    metric_dfns, metric_types = project.data.metric_dfns, project.data.metric_types
    history_metric_types = [
        metric_type for metric_type in metric_types if "history" in metric_type.contexts
    ]

    for metric in metric_dfns:
        metrics_table = project["webcompat_knowledge_base"][
            f"webcompat_topline_metric_{metric.name}"
        ].view()
        history_table = project["webcompat_knowledge_base"][
            f"webcompat_topline_metric_{metric.name}_history"
        ].table()

        query = f"""
                SELECT recorded_date
                FROM `{history_table}`
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

        query = f"SELECT * FROM `{metrics_table}`"
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
    metric_dfns, metric_types = project.data.metric_dfns, project.data.metric_types
    daily_metric_types = [
        metric_type for metric_type in metric_types if "daily" in metric_type.contexts
    ]

    history_table = project["webcompat_knowledge_base"][
        "webcompat_topline_metric_daily"
    ].table()
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
