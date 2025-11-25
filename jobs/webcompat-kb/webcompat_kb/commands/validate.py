import argparse
import os
from typing import Optional

from google.auth.exceptions import RefreshError


from .. import projectdata
from ..base import Command
from ..config import Config
from ..bqhelpers import BigQuery, DatasetId, SchemaId, SchemaType, get_client
from ..update_schema import render_schemas


class Validate(Command):
    def argument_parser(self) -> argparse.ArgumentParser:
        parser = super().argument_parser()
        parser.add_argument(
            "--default-dataset",
            action="store",
            default="webcompat_knowledge_base",
            help="Default dataset name",
        )
        parser.add_argument(
            "schema_ids",
            action="store",
            nargs="*",
            help="Schemas to render e.g. dataset.view_name",
        )
        return parser

    def main(self, args: argparse.Namespace) -> Optional[int]:
        client = get_client(args.bq_project_id)
        project = projectdata.load(
            client,
            args.bq_project_id,
            os.path.normpath(args.data_path),
            set(),
            Config(stage=False, write=False),
        )

        if args.schema_ids:
            schema_ids = [
                SchemaId.from_str(schema_str, args.bq_project_id, args.default_dataset)
                for schema_str in args.schema_ids
            ]
        else:
            schema_ids = [
                schema.id
                for dataset in project
                for schema in dataset
                if schema.type != SchemaType.table
            ]

        outputs = render_schemas(project, schema_ids)
        bq_client = BigQuery(
            client, DatasetId(args.bq_project_id, args.default_dataset), False
        )

        success = True
        for schema_id in schema_ids:
            schema_type, sql = outputs[schema_id]
            print(f"== {schema_id} ==")
            if schema_type == SchemaType.table:
                print("  Can't validate table schemas")
                success = False
            else:
                try:
                    bq_client.validate_query(sql, schema_id.dataset)
                except RefreshError:
                    raise
                except Exception as e:
                    messages = []
                    if hasattr(e, "errors"):
                        for error in getattr(e, "errors"):
                            messages.append(error.get("message"))
                    if messages:
                        msg = "\n    ".join(messages)
                    else:
                        msg = str(e)
                    print(f"  Validation failed:\n    {msg}")
                    success = False
                else:
                    print("  Validation succeeded")
        if not success:
            return 1

        return None


main = Validate()
