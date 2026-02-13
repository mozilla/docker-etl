import logging

from google.cloud import bigquery

from .base import Context, EtlJob
from .bqhelpers import BigQuery
from .projectdata import Project


def update_user_report_aggregate(project: Project, client: BigQuery) -> None:
    user_reports_view = project["webcompat_user_reports"][
        "user_reports_dedupe"
    ].view()
    aggregate_table = project["webcompat_user_reports"][
        "user_reports_aggregate"
    ].table()

    rows = list(
        client.query(f"""
SELECT
  DATE(MAX(day)) AS latest_stored,
  DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AS store_to
FROM `{aggregate_table}`
""")
    )

    if rows:
        latest_stored = rows[0].latest_stored
        store_to = rows[0].store_to

    if latest_stored and latest_stored >= store_to:
        logging.info("User report aggregates already up to date")
        return

    logging.info(f"Latest stored date: {latest_stored}, updating to {store_to}")

    new_rows_query = f"""
SELECT day, host, app_name, app_version, breakage_category, count(*) as count
FROM (
  SELECT DATE(DATE_TRUNC(reported_at, DAY)) as day, net.host(url) as host, app_name, app_version, breakage_category
  FROM `{user_reports_view}`
)
WHERE (day > @latest_stored OR @latest_stored IS NULL) AND day <= @store_to
GROUP BY day, host, app_name, app_version, breakage_category
"""

    client.insert_query(
        aggregate_table,
        ["day", "host", "app_name", "app_version", "breakage_category", "count"],
        new_rows_query,
        parameters=[
            bigquery.ScalarQueryParameter("latest_stored", "DATE", latest_stored),
            bigquery.ScalarQueryParameter("store_to", "DATE", store_to),
        ],
    )


class UserReportAggregateJob(EtlJob):
    name = "user-reports-aggregate"

    def default_dataset(self, context: Context) -> str:
        return "webcompat_user_reports"

    def main(self, context: Context) -> None:
        update_user_report_aggregate(
            context.project,
            context.bq_client,
        )
