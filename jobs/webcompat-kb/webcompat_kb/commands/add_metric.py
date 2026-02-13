import argparse
import logging
import os
from collections import defaultdict
from typing import Mapping, Optional, Sequence

from .. import projectdata
from ..base import Command
from ..bqhelpers import BigQuery, DatasetId, SchemaId, get_client
from ..config import Config
from ..projectdata import (
    Project,
    SchemaMetadata,
    TableMetadata,
)
from ..metrics.metrics import Metric, MetricTable, metric_tables


def all_metric_tables(project: Project) -> Mapping[Metric, Sequence[MetricTable]]:
    to_add = defaultdict(list)
    for metric in project.data.metric_dfns:
        for metric_table in metric_tables:
            to_add[metric].append(metric_table)
    return to_add


def find_new_metrics(
    project: Project, bq_client: BigQuery
) -> Mapping[Metric, Sequence[MetricTable]]:
    expected = {}
    for metric in project.data.metric_dfns:
        for metric_table in metric_tables:
            expected[metric_table.name(metric)] = (metric, metric_table)

    logging.info("Looking up existing tables")
    dataset = project["webcompat_knowledge_base"]
    for table in bq_client.get_tables(dataset.id):
        schema_id = bq_client.get_table_id(dataset, table)
        if schema_id.name in expected:
            del expected[schema_id.name]

    by_metric = defaultdict(list)
    for metric, metric_table in expected.values():
        by_metric[metric].append(metric_table)

    return by_metric


def add_metrics(
    project: Project, bq_client: BigQuery, recreate: bool, write: bool
) -> None:
    if not recreate:
        to_add = find_new_metrics(project, bq_client)
    else:
        to_add = all_metric_tables(project)

    if not to_add:
        logging.info("No new metric schemas to add")

    dataset = project["webcompat_knowledge_base"].id

    for metric, tables in to_add.items():
        logging.info(f"Adding schemas for {metric.name}")
        for table in tables:
            schema_id = SchemaId(dataset.project, dataset.dataset, table.name(metric))
            logging.info(f"Adding schema {schema_id}")
            if table.type == "view":
                metadata = SchemaMetadata(name=table.name(metric))
                project.data.add_view(
                    schema_id, metadata, table.template(metric), write
                )
            elif table.type == "table":
                metadata = TableMetadata(name=table.name(metric))
                project.data.add_table(
                    schema_id, metadata, table.template(metric), write
                )


class AddMetricCommand(Command):
    def argument_parser(self) -> argparse.ArgumentParser:
        parser = super().argument_parser()
        parser.add_argument(
            "--recreate", action="store_true", help="Recreate all metric tables"
        )
        return parser

    def main(self, args: argparse.Namespace) -> Optional[int]:
        client = get_client(args.bq_project_id)
        config = Config(write=args.write, stage=args.stage)
        project = projectdata.load(
            client, args.bq_project_id, os.path.normpath(args.data_path), set(), config
        )
        bq_client = BigQuery(
            client, DatasetId(args.bq_project_id, ""), args.write, set()
        )

        add_metrics(project, bq_client, args.recreate, config.write)
        return None


main = AddMetricCommand()
