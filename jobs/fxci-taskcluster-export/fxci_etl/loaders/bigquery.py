import base64
import json
from dataclasses import asdict
from itertools import batched
from pprint import pprint

from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.cloud.bigquery import (
    Client,
    Table,
    TimePartitioning,
    TimePartitioningType,
)

from fxci_etl.schemas import Record, get_record_cls, generate_schema
from fxci_etl.config import Config


class BigQueryLoader:
    CHUNK_SIZE = 25000

    def __init__(self, config: Config, table_type: str):
        self.config = config
        self.table_type = table_type
        self.table = self.ensure_table(table_type)

        if config.bigquery.credentials:
            self.client = Client.from_service_account_info(
                json.loads(base64.b64decode(config.bigquery.credentials).decode("utf8"))
            )
        else:
            self.client = Client()

        if config.storage.credentials:
            self.storage_client = storage.Client.from_service_account_info(
                json.loads(base64.b64decode(config.storage.credentials).decode("utf8"))
            )
        else:
            self.storage_client = storage.Client()

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
        table_name = getattr(bq.tables, table_type)

        print(f"Ensuring table {table_name} exists.")

        schema_cls = get_record_cls(table_type)
        schema = generate_schema(schema_cls)

        partition = TimePartitioning(
            type_=TimePartitioningType.DAY,
            field="submission_date",
            require_partition_filter=True,
        )
        table = Table(f"{bq.project}.{bq.dataset}.{table_name}", schema=schema)
        table.time_partitioning = partition
        self.client.create_table(table, exists_ok=True)
        return table

    def insert(self, records: list[Record] | Record):
        """Insert records into the table.

        Args:
            records (list[Record]): List of records to insert into the table.
        """
        if isinstance(records, Record):
            records = [records]

        try:
            # Load previously failed records from storage, maybe the issue is fixed.
            for obj in json.loads(self._record_backup.download_as_string()):
                records.append(Record.from_dict(obj))
        except NotFound:
            pass

        print(f"Attempting to insert {len(records)} records into table '{self.table_type}'")

        # There's a 10MB limit on the `insert_rows` request, submit rows in
        # batches to avoid exceeding it.
        errors = []
        for batch in batched(records, self.CHUNK_SIZE):
            errors.extend(
                self.client.insert_rows(
                    self.table, [asdict(row) for row in batch], retry=False
                )
            )

        if errors:
            print("The following records failed:")
            pprint(errors)

        num_inserted = len(records) - len(errors)
        print(f"Inserted {num_inserted} records in table '{self.table_type}'")

        self._record_backup.upload_from_string(json.dumps(errors))
