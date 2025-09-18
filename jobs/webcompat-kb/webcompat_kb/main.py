import argparse
import logging
import sys
from typing import Iterable

# These imports are required to populate ALL_JOBS
from . import (
    bugzilla,  # noqa: F401
    siterank,  # noqa: F401
    metric,  # noqa: F401
    metric_changes,  # noqa: F401
    web_features,  # noqa: F401
    standards_positions,  # noqa: F401
    metric_rescore,  # noqa: F401
    chrome_use_counters,  # noqa: F401
    interop,  # noqa: F401
)
from .base import ALL_JOBS, EtlJob, dataset_arg, project_arg
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
        "--bq-project",
        dest="bq_project_id",
        type=project_arg,
        help="BigQuery project id",
    )
    parser.add_argument(
        "--bq-kb-dataset", type=dataset_arg, help="BigQuery knowledge base dataset id"
    )

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


def validate_args(
    parser: argparse.ArgumentParser, args: argparse.Namespace, jobs: Iterable[EtlJob]
) -> None:
    required_args: set[str | tuple[str, str]] = {("bq_project_id", "--bq-project")}
    for job in jobs:
        required_args |= job.required_args()

    missing = []
    for arg in required_args:
        if isinstance(arg, tuple):
            prop_name, arg_name = arg
        else:
            prop_name = arg
            arg_name = f"--{arg.replace('_', '-')}"

        if getattr(args, prop_name) is None:
            missing.append(arg_name)

    if missing:
        parser.print_usage()
        logging.error(f"The following arguments are required {' '.join(missing)}")
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
        jobs = (
            {job_name: ALL_JOBS[job_name]() for job_name in args.jobs}
            if args.jobs
            else {name: cls() for name, cls in ALL_JOBS.items() if cls.default}
        )

        validate_args(parser, args, jobs.values())

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
