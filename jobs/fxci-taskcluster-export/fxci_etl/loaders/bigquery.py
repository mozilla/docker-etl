import base64
import json
from dataclasses import asdict
from itertools import batched
from pprint import pprint
from textwrap import dedent

from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.cloud.bigquery import (
    Client,
    Table,
    TimePartitioning,
    TimePartitioningType,
)
from loguru import logger

from fxci_etl.schemas import Record, get_record_cls, generate_schema
from fxci_etl.config import Config


class BigQueryLoader:
    CHUNK_SIZE = 5000

    def __init__(self, config: Config, table_type: str):
        self.config = config

        if config.bigquery.credentials:
            self.client = Client.from_service_account_info(
                json.loads(base64.b64decode(config.bigquery.credentials).decode("utf8"))
            )
        else:
            self.client = Client(project=config.bigquery.project)

        if config.storage.credentials:
            self.storage_client = storage.Client.from_service_account_info(
                json.loads(base64.b64decode(config.storage.credentials).decode("utf8"))
            )
        else:
            self.storage_client = storage.Client(project=config.storage.project)

        self.table = self.ensure_table(table_type)
        self.bucket = self.storage_client.bucket(config.storage.bucket)
        self._record_backup = self.bucket.blob(f"failed-bq-records.{table_type}.json")

    def ensure_table(self, table_type: str) -> Table:
        """Ensures the specified table exists and returns it.

        Checks if the table exists in BQ and creates it otherwise. Fails if the table
        exists but has the wrong schema.

        Args:
            table_type (str): Table type to check and return.

        Returns:
            Table: A BigQuery Table instance for the specified table type.
        """
        bq = self.config.bigquery
        self.table_name = f"{bq.project}.{bq.dataset}.{getattr(bq.tables, table_type)}"

        logger.debug(f"Ensuring table {self.table_name} exists.")

        schema_cls = get_record_cls(table_type)
        schema = generate_schema(schema_cls)

        partition = TimePartitioning(
            type_=TimePartitioningType.DAY,
            field="submission_date",
            require_partition_filter=True,
        )
        table = Table(self.table_name, schema=schema)
        table.time_partitioning = partition
        self.client.create_table(table, exists_ok=True)
        return table

    def replace(self, submission_date: str, records: list[Record] | Record):
        """Replace all records in the partition designated by 'submission_date' with these new ones.

        Args:
            submission_date (str): The date partition to replace.
            records (list[Record]): The records to overwrite existing records with.
        """
        if not records:
            return

        logger.info(
            f"Deleting records in '{self.table_name}' for partition '{submission_date}'"
        )
        job = self.client.query(
            dedent(
                f"""
                DELETE FROM `{self.table_name}`
                WHERE submission_date = "{submission_date}"
                """
            )
        )
        job.result()

        if isinstance(records, Record):
            records = [records]

        for record in records:
            record.submission_date = submission_date
        self.insert(records, use_backup=False)

    def insert(self, records: list[Record] | Record, use_backup=True):
        """Insert records into the table.

        Args:
            records (list[Record]): List of records to insert into the table.
        """
        if isinstance(records, Record):
            records = [records]

        if use_backup:
            try:
                # Load previously failed records from storage, maybe the issue is fixed.
                for obj in json.loads(self._record_backup.download_as_string()):
                    records.append(Record.from_dict(obj))
            except NotFound:
                pass

        logger.info(f"{len(records)} records to insert into table '{self.table_name}'")

        # There's a 10MB limit on the `insert_rows` request, submit rows in
        # batches to avoid exceeding it.
        errors = []
        for batch in batched(records, self.CHUNK_SIZE):
            logger.debug(f"Inserting batch of {len(batch)} records")
            errors.extend(
                self.client.insert_rows(
                    self.table, [asdict(row) for row in batch], retry=False
                )
            )

        if errors:
            logger.error("The following records failed:")
            pprint(errors)

        num_inserted = len(records) - len(errors)
        logger.info(
            f"Successfully inserted {num_inserted} records in table '{self.table_name}'"
        )

        if use_backup:
            self._record_backup.upload_from_string(json.dumps(errors))
