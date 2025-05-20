import argparse
import logging
import sys

# These imports are required to populate ALL_JOBS
from . import bugzilla, crux, metric, metric_changes, web_features, metric_rescore  # noqa: F401
from .base import ALL_JOBS, VALID_PROJECT_ID, VALID_DATASET_ID
from .bqhelpers import get_client, BigQuery


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
        "--pdb", action="store_true", help="Drop into debugger on execption"
    )
    parser.add_argument(
        "--fail-on-error", action="store_true", help="Fail immediately if any job fails"
    )

    for job_cls in ALL_JOBS.values():
        job_cls.add_arguments(parser)

    parser.add_argument(
        "jobs",
        nargs="*",
        choices=list(ALL_JOBS.keys()),
        help=f"Jobs to run (defaults to {' '.join(name for name, cls in ALL_JOBS.items() if cls.default)})",
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
        args.jobs = list(name for name, cls in ALL_JOBS.items() if cls.default)
    elif any(job not in ALL_JOBS for job in args.jobs):
        invalid = [job for job in args.jobs if job not in ALL_JOBS]
        parser.print_usage()
        logging.error(f"Invalid jobs {', '.join(invalid)}")
        sys.exit(1)


def main() -> None:
    logging.basicConfig()

    parser = get_parser()
    args = parser.parse_args()
    failed = []
    try:
        logging.getLogger().setLevel(
            logging.getLevelNamesMapping()[args.log_level.upper()]
        )
        set_default_args(parser, args)

        jobs = {job_name: ALL_JOBS[job_name]() for job_name in args.jobs}

        for job_name, job in jobs.items():
            job.set_default_args(parser, args)

        client = get_client(args.bq_project_id)

        for job_name, job in jobs.items():
            logging.info(f"Running job {job_name}")
            bq_client = BigQuery(client, job.default_dataset(args), args.write)

            try:
                job.main(bq_client, args)
            except Exception as e:
                if args.pdb or args.fail_on_error:
                    raise
                failed.append(job_name)
                logging.error(e)
    except Exception:
        if args.pdb:
            import pdb
            import traceback

            traceback.print_exc()
            pdb.post_mortem()
        else:
            raise

    if failed:
        logging.error(f"{len(failed)} jobs failed: {', '.join(failed)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
