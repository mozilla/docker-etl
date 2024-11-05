import argparse
import logging
import re
import sys

import google.auth
from google.cloud import bigquery

from . import bugzilla, crux

ALL_JOBS = {"bugzilla": bugzilla, "crux": crux}

# In the following we assume ascii-only characters for now. That's perhaps wrong,
# but it covers everything we're currently using.

# See https://cloud.google.com/resource-manager/docs/creating-managing-projects#before_you_begin
VALID_PROJECT_ID = re.compile(r"^[a-z](?:[a-z0-9\-]){4,28}[a-z0-9]$")
# See https://cloud.google.com/bigquery/docs/datasets#dataset-naming
VALID_DATASET_ID = re.compile(r"^[a-zA-Z_0-9]{1,1024}$")


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--log-level",
        choices=["debug", "info", "warn", "error"],
        default="info",
        help="Log level",
    )

    # Legacy argument names
    parser.add_argument("--bq_project_id", help=argparse.SUPPRESS)
    parser.add_argument("--bq_dataset_id", help=argparse.SUPPRESS)

    parser.add_argument(
        "--bq-project", dest="bq_project_id", help="BigQuery project id"
    )
    parser.add_argument("--bq-kb-dataset", help="BigQuery knowledge base dataset id")

    parser.add_argument(
        "--no-write",
        dest="write",
        action="store_false",
        default=True,
        help="Don't write updates to BigQuery",
    )

    for job_module in ALL_JOBS.values():
        job_module.add_arguments(parser)

    parser.add_argument(
        "jobs",
        nargs="*",
        help="Jobs to run (defaults to all)",
    )

    return parser


def set_default_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.bq_project_id is None:
        parser.print_usage()
        logging.error("The following arguments are required --bq-project")
        sys.exit(1)

    if not VALID_PROJECT_ID.match(args.bq_project_id):
        parser.print_usage()
        logging.error(f"Invalid project id {args.bq_project_id}")
        sys.exit(1)

    if args.bq_kb_dataset is None:
        # Default to a test dataset
        args.bq_kb_dataset = "webcompat_knowledge_base_test"

    if not VALID_DATASET_ID.match(args.bq_kb_dataset):
        parser.print_usage()
        logging.error(f"Invalid dataset id {args.bq_kb_dataset}")
        sys.exit(1)

    if not args.jobs:
        args.jobs = list(ALL_JOBS.keys())
    elif any(job not in ALL_JOBS for job in args.jobs):
        invalid = [job for job in args.jobs if job not in ALL_JOBS]
        parser.print_usage()
        logging.error(f"Invalid jobs {', '.join(invalid)}")
        sys.exit(1)


def get_client(bq_project_id: str) -> bigquery.Client:
    credentials, _ = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/bigquery",
        ]
    )

    return bigquery.Client(credentials=credentials, project=bq_project_id)


def main() -> None:
    logging.basicConfig()

    parser = get_parser()
    args = parser.parse_args()
    logging.getLogger().setLevel(logging.getLevelNamesMapping()[args.log_level.upper()])
    set_default_args(parser, args)

    client = get_client(args.bq_project_id)

    for job in args.jobs:
        logging.info(f"Running job {job}")
        ALL_JOBS[job].main(client, args)


if __name__ == "__main__":
    main()
