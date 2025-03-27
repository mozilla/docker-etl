import argparse
import logging
import sys

# These imports are required to populate ALL_JOBS
from . import bugzilla, crux, metric  # noqa: F401
from .base import ALL_JOBS, VALID_PROJECT_ID, VALID_DATASET_ID, get_client


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

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Drop into debugger if there's an exception",
    )

    for job_cls in ALL_JOBS.values():
        job_cls.add_arguments(parser)

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
        logging.error(f"Invalid kb dataset id {args.bq_kb_dataset}")
        sys.exit(1)

    if not args.jobs:
        args.jobs = list(ALL_JOBS.keys())
    elif any(job not in ALL_JOBS for job in args.jobs):
        invalid = [job for job in args.jobs if job not in ALL_JOBS]
        parser.print_usage()
        logging.error(f"Invalid jobs {', '.join(invalid)}")
        sys.exit(1)


def run(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    set_default_args(parser, args)

    jobs = {job_name: ALL_JOBS[job_name]() for job_name in args.jobs}

    for job_name, job in jobs.items():
        job.set_default_args(parser, args)

    client = get_client(args.bq_project_id)

    for job_name, job in jobs.items():
        logging.info(f"Running job {job_name}")
        job.main(client, args)


def main() -> None:
    logging.basicConfig()

    parser = get_parser()
    args = parser.parse_args()
    logging.getLogger().setLevel(logging.getLevelNamesMapping()[args.log_level.upper()])

    try:
        run(parser, args)
    except Exception:
        if args.debug:
            import traceback

            traceback.print_exc()
            import pdb

            pdb.post_mortem()
        else:
            raise


if __name__ == "__main__":
    main()
