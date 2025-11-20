import argparse
import logging
import os
import sys

from .. import projectdata
from ..base import ALL_JOBS
from ..bqhelpers import get_client
from ..config import Config
from ..update_schema import (
    SchemaCreator,
    lint_templates,
)


here = os.path.dirname(__file__)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bq-project-id", action="store", help="BigQuery project ID")
    parser.add_argument("--pdb", action="store_true", help="Run debugger on failure")
    parser.add_argument(
        "--path",
        action="store",
        default=os.path.join(here, os.pardir, os.pardir, "data"),
        help="Path to directory containing data",
    )
    try:
        # This should be unused
        client = get_client("test")
        args = parser.parse_args()

        project = projectdata.load(
            client,
            args.bq_project_id,
            os.path.normpath(args.path),
            set(),
            Config(write=False, stage=False),
        )
        if not lint_templates(
            {item.name for item in ALL_JOBS.values()},
            project.data.templates_by_dataset.values(),
        ):
            logging.error("Lint failed")
            sys.exit(1)

        try:
            creator = SchemaCreator(project)
            creator.create()
        except Exception:
            logging.error("Creating schemas failed")
            raise
    except Exception:
        if args.pdb:
            import pdb

            pdb.post_mortem()
        raise
