import base64
import json
from abc import ABC, abstractmethod
from dataclasses import InitVar, asdict, dataclass, fields, is_dataclass
from datetime import datetime, timezone
from itertools import batched
from pprint import pprint
from typing import Any, Type, TypeAlias, Union, get_args, get_origin

import dacite
from aenum import Enum, NoAlias
from dacite.config import Config as DaciteConfig
from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.cloud.bigquery import (
    Client,
    SchemaField,
    Table,
    TimePartitioning,
    TimePartitioningType,
)

from fxci_etl.config import Config


class BigQueryTypes(Enum, settings=NoAlias):  # type: ignore
    DATE: TypeAlias = str
    FLOAT: TypeAlias = float
    INTEGER: TypeAlias = int
    STRING: TypeAlias = str
    TIMESTAMP: TypeAlias = int


def generate_schema(cls):
    assert is_dataclass(cls)
    schema = []
    for field in fields(cls):
        _type = field.type
        origin = get_origin(_type)
        args = get_args(_type)

        if origin is Union and type(None) in args:
            mode = "NULLABLE"
            _type = [arg for arg in args if arg is not type(None)][0]

        elif origin is list:
            mode = "REPEATED"
            _type = args[0]

        else:
            mode = "REQUIRED"

        if is_dataclass(_type):
            nested_schema = generate_schema(_type)
            schema.append(
                SchemaField(field.name, "RECORD", mode=mode, fields=nested_schema)
            )
        else:
            schema.append(SchemaField(field.name, _type.name, mode=mode))
    return schema


@dataclass
class Record(ABC):
    submission_date: BigQueryTypes.DATE
    table_name: InitVar[str]

    def __post_init__(self, table_name):
        self.table = table_name

    @classmethod
    def from_dict(cls, table_name: str, data: dict[str, Any]) -> "Record":
        current_date = datetime.now(timezone.utc).date()

        data["submission_date"] = current_date.strftime("%Y-%m-%d")
        data["table_name"] = table_name
        return dacite.from_dict(
            data_class=cls, data=data, config=DaciteConfig(check_types=False)
        )

    @abstractmethod
    def __str__(self) -> str: ...


class BigQueryLoader:
    CHUNK_SIZE = 25000

    def __init__(self, config: Config):
        self.config = config
        self._tables = {}

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

        self.bucket = self.storage_client.bucket(config.storage.bucket)
        self._record_backup = self.bucket.blob("failed-bq-records.json")

    def ensure_table(self, name: str, cls_: Type[Record]):
        """Checks if the table exists in BQ and creates it otherwise.

        Fails if the table exists but has the wrong schema.
        """
        print(f"Ensuring table {name} exists.")
        bq = self.config.bigquery
        schema = generate_schema(cls_)

        partition = TimePartitioning(
            type_=TimePartitioningType.DAY,
            field="submission_date",
            require_partition_filter=True,
        )
        table = Table(f"{bq.project}.{bq.dataset}.{name}", schema=schema)
        table.time_partitioning = partition
        self.client.create_table(table, exists_ok=True)

    def get_table(self, name: str, cls_: Type[Record]) -> Table:
        if name not in self._tables:
            self.ensure_table(name, cls_)
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
                table = obj.pop("table")
                records.append(Record.from_dict(table, obj))
        except NotFound:
            pass

        tables = {}
        for record in records:
            if record.table not in tables:
                tables[record.table] = []
            tables[record.table].append(record)

        failed_records = []
        for name, rows in tables.items():
            print(f"Attempting to insert {len(rows)} records into table '{name}'")
            table = self.get_table(name, rows[0].__class__)

            # There's a 10MB limit on the `insert_rows` request, submit rows in
            # batches to avoid exceeding it.
            errors = []
            for batch in batched(rows, self.CHUNK_SIZE):
                errors.extend(self.client.insert_rows(table, [asdict(row) for row in batch], retry=False))

            if errors:
                print("The following records failed:")
                for error in errors:
                    pprint(error)
                    failed_records.append(rows[error["index"]])

            num_inserted = len(rows) - len(errors)
            print(f"Inserted {num_inserted} records in table '{table}'")

        self._record_backup.upload_from_string(json.dumps(failed_records))
