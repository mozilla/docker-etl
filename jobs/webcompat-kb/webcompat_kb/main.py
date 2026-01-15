import argparse
import logging
import os
from typing import Iterable, Optional

# These imports are required to populate ALL_JOBS
# Unhappily the ordering here is significant
from . import (
    update_schema,  # noqa: F401
    bugzilla,  # noqa: F401
    siterank,  # noqa: F401
    metric,  # noqa: F401
    metric_changes,  # noqa: F401
    web_features,  # noqa: F401
    standards_positions,  # noqa: F401
    chrome_use_counters,  # noqa: F401
    interop,  # noqa: F401
)
from .base import (
    ALL_JOBS,
    Command,
    Context,
    Config,
    EtlJob,
    dataset_arg,
)
from .bqhelpers import get_client, BigQuery, DatasetId
from . import projectdata


here = os.path.dirname(__file__)


class EtlCommand(Command):
    def argument_parser(self) -> argparse.ArgumentParser:
        parser = super().argument_parser()
        parser.add_argument(
            "--fail-on-error",
            action="store_true",
            help="Fail immediately if any job fails",
        )

        # Legacy: BigQuery knowledge base dataset id
        parser.add_argument("--bq-kb-dataset", type=dataset_arg, help=argparse.SUPPRESS)

        # Legacy argument names
        parser.add_argument("--bq_project_id", help=argparse.SUPPRESS)
        parser.add_argument("--bq_dataset_id", help=argparse.SUPPRESS)

        for job_cls in ALL_JOBS.values():
            job_cls.add_arguments(parser)

        parser.add_argument(
            "jobs",
            nargs="*",
            choices=list(ALL_JOBS.keys()),
            help=f"Jobs to run (defaults to {' '.join(name for name, cls in ALL_JOBS.items() if cls.default)})",
        )

        return parser

    def validate_args(self, args: argparse.Namespace, jobs: Iterable[EtlJob]) -> bool:
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
            self.argument_parser().print_usage()
            logging.error(f"The following arguments are required {' '.join(missing)}")
            return False

        return True

    def main(self, args: argparse.Namespace) -> Optional[int]:
        failed = []
        jobs = (
            {job_name: ALL_JOBS[job_name]() for job_name in args.jobs}
            if args.jobs
            else {name: cls() for name, cls in ALL_JOBS.items() if cls.default}
        )

        if not self.validate_args(args, jobs.values()):
            return 1

        config = Config(write=args.write, stage=args.stage)

        client = get_client(args.bq_project_id)
        project = projectdata.load(
            client, args.bq_project_id, args.data_path, set(jobs.keys()), config
        )

        context = Context(
            args=args,
            bq_client=BigQuery(
                client, DatasetId(args.bq_project_id, ""), args.write, set()
            ),
            config=config,
            jobs=list(jobs.values()),
            project=project,
        )

        for job_name, job in jobs.items():
            logging.info(f"Running job {job_name}")
            bq_client = BigQuery(
                client,
                DatasetId(args.bq_project_id, job.default_dataset(context)),
                args.write,
                job.write_targets(project),
            )
            context.bq_client = bq_client
            try:
                job.main(context)
            except Exception as e:
                if args.pdb or args.fail_on_error:
                    raise
                failed.append(job_name)
                logging.error(e)

        if failed:
            logging.error(f"{len(failed)} jobs failed: {', '.join(failed)}")
            return 1

        return 0


main = EtlCommand()


if __name__ == "__main__":
    main()
