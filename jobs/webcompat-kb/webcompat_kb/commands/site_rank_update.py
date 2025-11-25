import argparse
import logging

from google.cloud import bigquery

from . import metric_rescore
from .. import projectdata
from ..base import Command
from ..bqhelpers import BigQuery, DatasetId, SchemaId, get_client
from ..config import Config
from ..projectdata import Project


def check_yyyymm(project: Project, client: BigQuery, yyyymm: int) -> bool:
    query = f"""
SELECT EXISTS (
  SELECT 1 FROM `{project["crux_imported"]["host_min_ranks"]}` WHERE yyyymm = @yyyymm) as has_yyyymm
"""
    parameters = [bigquery.ScalarQueryParameter("yyyymm", "INTEGER", yyyymm)]
    result = list(client.query(query, parameters=parameters))[0]
    return result.has_yyyymm


def create_new_routine(client: BigQuery, yyyymm: int) -> str:
    new_name = f"{client.project_id}.{client.default_dataset_id}.WEBCOMPAT_METRIC_YYYYMM_{yyyymm}"
    query = f"CREATE OR REPLACE FUNCTION `{new_name}`() RETURNS INT64 AS ({yyyymm});"
    if client.write:
        logging.info(f"Creating function {new_name}")
        client.query(query)
    else:
        logging.info(f"Would create function {new_name}")
    return new_name


def create_new_scored_site_reports(
    client: BigQuery, yyyymm: int, new_fn_name: str
) -> str:
    current_table = client.get_table("scored_site_reports")
    current_query = current_table.view_query

    fn_name = f"{client.project_id}.{client.default_dataset_id}.WEBCOMPAT_METRIC_YYYYMM"

    if fn_name not in current_query:
        raise ValueError(f"Failed to find {fn_name} in {current_table}")

    new_query = current_query.replace(fn_name, new_fn_name)
    new_table_id = f"{current_table.reference}_{yyyymm}"
    new_table = bigquery.Table(new_table_id)
    new_table.view_query = new_query

    if client.write:
        logging.info(f"Creating view {new_table_id}")
        client.client.create_table(new_table, exists_ok=True)
    else:
        logging.info(f"Would create view {new_table_id} with query:\n{new_query}")
    return new_table_id


def update_site_ranks(project: Project, client: BigQuery, yyyymm: int) -> None:
    if not check_yyyymm(project, client, yyyymm):
        raise ValueError(f"No site rank data found for {yyyymm}")

    new_fn_name = create_new_routine(client, yyyymm)
    new_site_reports = create_new_scored_site_reports(client, yyyymm, new_fn_name)

    _, new_routine_id = new_fn_name.split(".", 1)
    logging.info(new_routine_id)

    metric_rescore.rescore(
        project,
        client,
        new_site_reports.rsplit(".", 1)[1],
        f"Update site rank data to {yyyymm}",
        [f"{client.default_dataset_id}.WEBCOMPAT_METRIC_YYYYMM:{new_routine_id}"],
    )


class SiteRankUpdate(Command):
    def argument_parser(self) -> argparse.ArgumentParser:
        parser = super().argument_parser()
        parser.add_argument(
            "--yyyymm",
            action="store",
            type=int,
            help="New site rank data to use in the format YYYYMM",
        )
        return parser

    def main(self, args: argparse.Namespace) -> None:
        config = Config(write=args.write, stage=args.stage)

        client = get_client(args.bq_project_id)
        project = projectdata.load(
            client, args.bq_project_id, args.data_path, set(), config
        )

        rescores_table = SchemaId(
            args.bq_project_id,
            "webcompat_knowledge_base",
            "webcompat_topline_metric_rescores",
        )

        bq_client = BigQuery(
            client, DatasetId(args.bq_project_id, ""), args.write, {rescores_table}
        )

        update_site_ranks(
            project,
            bq_client,
            args.yyyymm,
        )


main = SiteRankUpdate()
