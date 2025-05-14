import argparse
import logging
import sys
import time
import traceback
from types import TracebackType
from typing import Optional

from google.cloud import bigquery

# These imports are required to populate ALL_JOBS
from . import bugzilla, crux, metric, metric_changes  # noqa: F401
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
        "--wait-lock",
        action="store_true",
        help="Wait to acquire the ETL lock (not intended for prod)",
    )

    parser.add_argument(
        "--pdb", action="store_true", help="Drop into debugger on execption"
    )

    for job_cls in ALL_JOBS.values():
        job_cls.add_arguments(parser)

    parser.add_argument(
        "jobs",
        nargs="*",
        choices=list(ALL_JOBS.keys()),
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


class LockError(Exception):
    pass


class EtlLock:
    def __init__(
        self, client: bigquery.Client, kb_dataset_id: str, write: bool, wait: bool
    ):
        self.client = BigQuery(client, kb_dataset_id, write)
        self.write = write
        self.wait = wait
        # This acts as an early check that we have auth credentials for GH, even if we won't take a lock
        self.table = self.client.ensure_table(
            "etl_lock",
            schema=[
                bigquery.SchemaField("lock_time", "DATETIME"),
            ],
        )
        self.lock_time = None

    def __enter__(self) -> None:
        if not self.write:
            logging.debug("Not writing, so ETL lock is not required")
            return
        first = True
        while True:
            # Updates are serialized by transactions whereas inserts are not
            # We use an insert to check we have at least one row, but then
            # use the (serialized) updates to acquire the lock
            try:
                result = self.client.query(f"""
DECLARE update_count INT64 DEFAULT 0;
BEGIN TRANSACTION;

INSERT {self.table.table_id} (lock_time)
SELECT NULL
FROM UNNEST([1])
WHERE NOT EXISTS (SELECT 1 FROM {self.table.table_id});

UPDATE {self.table.table_id}
SET lock_time = CURRENT_DATETIME()
WHERE lock_time IS NULL;
SET update_count = @@row_count;

COMMIT TRANSACTION;

SELECT update_count, lock_time FROM {self.table.table_id}
""")
            except Exception:
                # This can happen if there's a simultaneous modification in the transaction
                pass
            else:
                row = list(result)[0]
                if row.update_count == 1:
                    self.lock_time = row.lock_time
                    logging.info(f"Acquired lock with timestamp {self.lock_time}")
                    return

            if not self.wait:
                raise LockError(
                    f"Can't run, another ETL process is running; started at time {row.lock_time}"
                )
            if first:
                logging.info(
                    f"Waiting for lock, current lock was acquired at {row.lock_time}"
                )
                first = False
            # This doesn't guarantee we ever get the lock; if another process happens to acquire the lock whilst we're
            # sleeping we'll lose out. But the main case we care about is being able to run local jobs when the airflow
            # task is not running, so as long as this is shorter than the setup time of airflow we're in a good place
            time.sleep(5)

    def __exit__(
        self,
        type_: Optional[type[BaseException]],
        value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        failed = False
        if self.lock_time is not None:
            try:
                result = self.client.query(
                    f"""
DECLARE update_count INT64 DEFAULT 0;
BEGIN TRANSACTION;
UPDATE {self.table.table_id}
SET lock_time = NULL
WHERE lock_time = @lock_time;
SET update_count = @@row_count;
COMMIT TRANSACTION;

SELECT update_count
""",
                    parameters=[
                        bigquery.ScalarQueryParameter(
                            "lock_time", "DATETIME", self.lock_time
                        )
                    ],
                )
            except Exception:
                # This can happen if there are multiple concurrent transactions
                failed = True
            else:
                failed = list(result)[0].update_count != 1
            if failed:
                logging.warning(
                    f"Expected to clear lock with lock_time {self.lock_time}, but failed"
                )
            self.lock_time = None


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

        with EtlLock(client, args.bq_kb_dataset, args.write, args.wait_lock):
            for job_name, job in jobs.items():
                logging.info(f"Running job {job_name}")
                bq_client = BigQuery(client, job.default_dataset(args), args.write)

                try:
                    job.main(bq_client, args)
                except Exception as e:
                    if args.pdb:
                        raise
                    failed.append(job_name)
                    traceback.print_exc()
                    logging.error(e)
    except LockError as e:
        logging.info(str(e))
        logging.error("Failed to acquire ETL lock, exiting")
        sys.exit(1)
    except Exception:
        if args.pdb:
            import pdb

            traceback.print_exc()
            pdb.post_mortem()
        else:
            raise

    if failed:
        logging.error(f"{len(failed)} jobs failed: {', '.join(failed)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
