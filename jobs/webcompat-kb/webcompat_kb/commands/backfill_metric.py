import argparse
import logging
import os
from typing import Optional

from .. import projectdata
from ..base import Command
from ..bqhelpers import BigQuery, DatasetId, get_client
from ..config import Config
from ..projectdata import Project


def backfill_metric_daily(
    project: Project,
    client: BigQuery,
    write: bool,
    metric_name: str,
) -> None:
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
  `{project["webcompat_knowledge_base"]["scored_site_reports"]}` AS bugs
  JOIN `{project["webcompat_knowledge_base"]["webcompat_topline_metric_daily"]}` as metric_daily
ON
  DATE(bugs.creation_time) <= metric_daily.date
  AND IF (bugs.resolved_time IS NOT NULL, DATE(bugs.resolved_time) >= date, TRUE)
WHERE
  {metric.condition("bugs")} AND {" AND ".join(conditions)}
GROUP BY
  date
ORDER BY date"""

    update_query = f"""
UPDATE `{project["webcompat_knowledge_base"]["webcompat_topline_metric_daily"]}` AS metric_daily
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


class BackfillMetric(Command):
    def argument_parser(self) -> argparse.ArgumentParser:
        parser = super().argument_parser()
        parser.add_argument("metric", action="store", help="Metric name to update")
        return parser

    def main(self, args: argparse.Namespace) -> Optional[int]:
        client = get_client(args.bq_project_id)
        config = Config(write=args.write, stage=args.stage)
        project = projectdata.load(
            client, args.bq_project_id, os.path.normpath(args.data_path), set(), config
        )
        if args.metric not in {item.name for item in project.data.metric_dfns}:
            raise ValueError(f"Unknown metric {args.metric}")

        bq_client = BigQuery(
            client,
            DatasetId(args.bq_project_id, "webcompat_knowledge_base"),
            args.write,
            None,
        )

        backfill_metric_daily(
            project,
            bq_client,
            config.write,
            args.metric,
        )
        return None


main = BackfillMetric()
