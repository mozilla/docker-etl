from abc import ABC, abstractmethod
import base64
from collections import defaultdict
from dataclasses import asdict, dataclass
import json
import os
from pprint import pprint
from typing import Any

import dacite
from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.cloud.bigquery import Client, Table

from fxci_etl.config import Config


@dataclass
class Record(ABC):
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Record":
        return dacite.from_dict(data_class=cls, data=data)

    @classmethod
    @abstractmethod
    def table_name(cls) -> str: ...

    @abstractmethod
    def __str__(self) -> str: ...


class BigQueryLoader:
    def __init__(self, config: Config):
        self.config = config
        self._tables = {}

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
        self._record_backup = self.bucket.blob("failed-bq-records.json")

    def get_table(self, name: str) -> Table:
        if name not in self._tables:
            bq = self.config.bigquery
            self._tables[name] = self.client.get_table(
                f"{bq.project}.{bq.dataset}.{name}"
            )
        return self._tables[name]

    def insert(self, records: list[Record] | Record):
        if isinstance(records, Record):
            records = [records]

        try:
            # Load previously failed records from storage, maybe the issue is fixed.
            for obj in json.loads(self._record_backup.download_as_string()):
                records.append(Record.from_dict(obj))
        except NotFound:
            pass

        tables = defaultdict(list)
        for record in records:
            tables[record.table_name()].append(record)

        failed_records = []
        for name, rows in tables.items():
            table = self.get_table(name)
            errors = self.client.insert_rows(table, [asdict(row) for row in rows])

            for error in errors:
                pprint(error, indent=2)
                failed_records.append(rows[error["index"]])

            num_inserted = len(rows) - len(errors)
            print(f"Inserted {num_inserted} records in table '{table}'")

        self._record_backup.upload_from_string(json.dumps(failed_records))
