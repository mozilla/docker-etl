import argparse
import logging
import os
from typing import Sequence, Optional

from .. import projectdata
from ..base import Command
from ..bqhelpers import BigQuery, DatasetId, get_client
from ..config import Config
from ..projectdata import Project
from ..siterank import host_min_ranks_query


def backfill_host_min_ranks(
    project: Project, client: BigQuery, ranks: Sequence[str]
) -> None:
    host_min_ranks_table = project["crux_imported"]["host_min_ranks"].table()

    query = host_min_ranks_query(project, filter_yyyymm=False, ranks=ranks)
    condition = "source.yyyymm = target.yyyymm AND source.host = target.host"

    client.update_query(host_min_ranks_table, ranks, query, condition)


def backfill_metric_daily(
    project: Project,
    client: BigQuery,
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

    scored_site_reports = project["webcompat_knowledge_base"][
        "scored_site_reports"
    ].view()
    metric_daily = project["webcompat_knowledge_base"][
        "webcompat_topline_metric_daily"
    ].table()

    select_query = f"""
SELECT
  date,
  {",\n  ".join(select_fields)}
FROM
  `{scored_site_reports}` AS bugs
  JOIN `{metric_daily}` as metric_daily
ON
  DATE(bugs.creation_time) <= metric_daily.date
  AND IF (bugs.resolved_time IS NOT NULL, DATE(bugs.resolved_time) >= date, TRUE)
WHERE
  {metric.condition("bugs")} AND {" AND ".join(conditions)}
GROUP BY
  date
ORDER BY date"""

    client.update_query(
        metric_daily, field_names, select_query, "source.date = target.date"
    )


class BackfillMetric(Command):
    def argument_parser(self) -> argparse.ArgumentParser:
        parser = super().argument_parser()
        parser.add_argument(
            "--rank",
            action="append",
            dest="ranks",
            help="Rank name (from host_min_ranks) to backfill",
        )
        parser.add_argument(
            "--metric", action="append", dest="metrics", help="Metric name to backfill"
        )
        return parser

    def main(self, args: argparse.Namespace) -> Optional[int]:
        client = get_client(args.bq_project_id)
        config = Config(write=args.write, stage=args.stage)
        project = projectdata.load(
            client,
            args.bq_project_id,
            os.path.normpath(args.data_path),
            {"metric"},
            config,
        )

        for name, valid_values, arg_values in [
            ("metrics", {item.name for item in project.data.metric_dfns}, args.metrics),
            ("ranks", {item.name for item in project.data.rank_dfns}, args.ranks),
        ]:
            if arg_values:
                unknown_values = set(arg_values) - valid_values
                if unknown_values:
                    logging.error(
                        f"Unknown {name}: {' '.join(unknown_values)}, options are {' '.join(valid_values)}"
                    )
                    return 1

        bq_client = BigQuery(
            client,
            DatasetId(args.bq_project_id, "webcompat_knowledge_base"),
            args.write,
            None,
        )

        if args.ranks:
            backfill_host_min_ranks(project, bq_client, args.ranks)

        if args.metrics:
            for metric in args.metrics:
                backfill_metric_daily(
                    project,
                    bq_client,
                    metric,
                )
        return None


main = BackfillMetric()
