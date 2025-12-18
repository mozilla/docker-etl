import argparse
import logging
from typing import Optional

from google.cloud import bigquery

from . import metric_rescore
from .. import projectdata
from ..base import Command
from ..bqhelpers import BigQuery, DatasetId, SchemaId, SchemaType, get_client
from ..config import Config
from ..projectdata import Project, RoutineTemplate, SchemaMetadata


def check_yyyymm(project: Project, client: BigQuery, yyyymm: int) -> bool:
    query = f"""
SELECT EXISTS (
  SELECT 1 FROM `{project["crux_imported"]["host_min_ranks"]}` WHERE yyyymm = @yyyymm) as has_yyyymm
"""
    parameters = [bigquery.ScalarQueryParameter("yyyymm", "INTEGER", yyyymm)]
    result = list(client.query(query, parameters=parameters))[0]
    return result.has_yyyymm


def update_crux_routine(
    project: Project, yyyymm: int, schema_id: SchemaId, write: bool
) -> SchemaId:
    template_data = f"""CREATE OR REPLACE FUNCTION `{{{{ ref(name) }}}}`() RETURNS INT64 AS (
{yyyymm}
);"""

    schema_path = project.data.get_schema_path(SchemaType.routine, schema_id)
    if write and not schema_path.exists():
        raise ValueError(f"Expected {schema_path} to exist")
    metadata = SchemaMetadata(name=schema_id.name)
    template = RoutineTemplate(schema_path, metadata, template_data)
    template_path = schema_path / template.filename
    if write and not schema_path.exists():
        raise ValueError(f"Expected {template_path} to exist")
    if write:
        with open(template_path, "w") as f:
            f.write(template.template)
    else:
        logging.info(f"Would write template {template_path}:\n{template.template}")
    return schema_id


class SiteRankUpdate(Command):
    def argument_parser(self) -> argparse.ArgumentParser:
        parser = super().argument_parser()
        parser.add_argument(
            "phase",
            action="store",
            choices=["create-schemas", "prepare-deploy"],
            help="",
        )
        parser.add_argument(
            "yyyymm",
            action="store",
            type=int,
            help="New site rank data to use in the format YYYYMM",
        )

        return parser

    def main(self, args: argparse.Namespace) -> Optional[int]:
        config = Config(write=args.write, stage=args.stage)

        client = get_client(args.bq_project_id)
        project = projectdata.load(
            client, args.bq_project_id, args.data_path, set(), config
        )

        kb_dataset = DatasetId(args.bq_project_id, "webcompat_knowledge_base")
        bq_client = BigQuery(client, kb_dataset, args.write, set())

        rescore = None
        if args.phase == "create-schemas":
            if not check_yyyymm(project, bq_client, args.yyyymm):
                raise ValueError(f"No site rank data found for {args.yyyymm}")

            rescore = metric_rescore.create_schemas(
                project,
                bq_client,
                args.data_path,
                kb_dataset,
                f"crux_{args.yyyymm}",
                f"Update CrUX data to {args.yyyymm}",
                ["WEBCOMPAT_METRIC_YYYYMM"],
                args.write,
            )
            if rescore is not None:
                staging_schema_id = rescore.staging_routine_ids()[
                    SchemaId(
                        kb_dataset.project,
                        kb_dataset.dataset,
                        "WEBCOMPAT_METRIC_YYYYMM",
                    )
                ]
                update_crux_routine(project, args.yyyymm, staging_schema_id, args.write)
        elif args.phase == "prepare-deploy":
            rescore = metric_rescore.prepare_deploy(
                project,
                bq_client,
                args.data_path,
                kb_dataset,
                f"crux_{args.yyyymm}",
                args.write,
            )

        return 0 if rescore is not None else 1


main = SiteRankUpdate()
