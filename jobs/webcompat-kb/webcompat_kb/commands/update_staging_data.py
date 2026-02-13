import argparse
import logging
from typing import Optional

from .. import projectdata
from ..base import Command
from ..bqhelpers import BigQuery, get_client
from ..config import Config
from ..projectdata import SchemaId, ReferenceType
from ..update_schema import update_schema_if_needed


class UpdateStagingData(Command):
    def argument_parser(self) -> argparse.ArgumentParser:
        parser = super().argument_parser()
        parser.add_argument(
            "--update-views",
            action="store_true",
            default=False,
            help="Redeploy views to staging",
        )
        parser.add_argument(
            dest="datasets", nargs="*", help="Datasets to update from prod"
        )
        return parser

    def main(self, args: argparse.Namespace) -> Optional[int]:
        client = get_client(args.bq_project_id)
        config = Config(write=args.write, stage=args.stage)
        project = projectdata.load(
            client, args.bq_project_id, args.data_path, set(), config
        )

        if args.datasets:
            dataset_names = args.datasets
        else:
            logging.info(
                "No dataset names specified, defaulting to webcompat_knowledge_base"
            )
            dataset_names = ["webcompat_knowledge_base"]

        invalid = []
        datasets = []
        for dataset_name in dataset_names:
            if dataset_name not in project.datasets:
                invalid.append(dataset_name)
            else:
                datasets.append(project[dataset_name].id)

        if invalid:
            logging.error(f"Unknown datasets {' '.join(invalid)}")
            import pdb

            pdb.set_trace()
            return 1

        dataset_mapping = {
            dataset: projectdata.stage_dataset(dataset) for dataset in datasets
        }

        for dataset in datasets:
            bq_client = BigQuery(
                client,
                dataset,
                config.write,
                None,
            )

            bq_client.ensure_dataset(dataset, None)
            tables = list(
                SchemaId(dataset.project, dataset.dataset, item.table_id)
                for item in bq_client.client.list_tables(dataset.dataset)
                if item.table_type != "VIEW"
            )
            schema_id_mapper = projectdata.SchemaIdMapper(dataset_mapping, set(tables))
            for src_table in tables:
                dest_table = schema_id_mapper(ReferenceType.table, src_table)
                assert dest_table != src_table
                logging.info(f"Creating {dest_table} from {src_table}")
                bq_client.delete_table(str(dest_table), not_found_ok=True)

                query = f"""
CREATE TABLE `{dest_table}`
CLONE `{src_table}`
"""
                if config.write:
                    logging.info(f"Creating table {dest_table} from {src_table}")
                    try:
                        bq_client.query(query)
                    except Exception:
                        logging.error(f"Creating table {dest_table} failed")
                else:
                    logging.info(f"Would run query:{query}")

        if args.update_views:
            logging.info("Updating stage views")
            update_schema_if_needed(
                project,
                bq_client,
                etl_jobs_enabled=set(),
                stage=True,
                recreate=True,
                delete_extra=False,
            )

        return None


main = UpdateStagingData()
