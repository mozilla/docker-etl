import argparse
import logging
import os
import sys

from ..update_schema import (
    SchemaIdMapper,
    create_schemas,
    load_templates,
    lint_templates,
)


here = os.path.dirname(__file__)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bq-project-id", action="store", help="BigQuery project ID")
    parser.add_argument(
        "--path",
        action="store",
        default=os.path.join(here, os.pardir, os.pardir, "data", "sql"),
        help="Path to directory containing sql",
    )
    args = parser.parse_args()

    templates_by_dataset = load_templates(args.bq_project_id, args.path)
    if not lint_templates(templates_by_dataset):
        logging.error("Lint failed")
        sys.exit(1)

    schema_id_mapper = SchemaIdMapper({}, set())
    try:
        create_schemas(args.bq_project_id, schema_id_mapper, templates_by_dataset)
    except Exception:
        logging.error("Creating schemas failed")
        raise
