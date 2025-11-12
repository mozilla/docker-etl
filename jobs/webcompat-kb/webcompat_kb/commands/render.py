import argparse
import os
import pathlib

from .. import projectdata
from ..base import DEFAULT_DATA_DIR
from ..bqhelpers import SchemaId, get_client
from ..config import Config
from ..update_schema import render_schemas


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bq-project-id", action="store", help="BigQuery project ID")
    parser.add_argument(
        "--default-dataset",
        action="store",
        default="webcompat_knowledge_base",
        help="Default dataset name",
    )
    parser.add_argument("--pdb", action="store_true", help="Run debugger on failure")
    parser.add_argument(
        "--data-path",
        action="store",
        type=pathlib.Path,
        default=DEFAULT_DATA_DIR,
        help="Path to directory containing data",
    )
    parser.add_argument(
        "schema_ids",
        action="store",
        nargs="+",
        help="Schemas to render e.g. dataset.view_name",
    )
    try:
        # This should be unused
        client = get_client("test")
        args = parser.parse_args()

        project = projectdata.load(
            client,
            args.bq_project_id,
            os.path.normpath(args.data_path),
            set(),
            Config(stage=False, write=False),
        )

        schema_ids = [
            SchemaId.from_str(schema_str, args.bq_project_id, args.default_dataset)
            for schema_str in args.schema_ids
        ]

        outputs = render_schemas(project, schema_ids)

        for schema_id in schema_ids:
            print(f"== {schema_id} ==")
            print(outputs[schema_id][1])
    except Exception:
        if args.pdb:
            import pdb

            pdb.post_mortem()
        raise
