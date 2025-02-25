from abc import ABC, abstractmethod
from datetime import datetime, timezone
from dataclasses import fields, is_dataclass, dataclass
from typing import Any, Optional, Type
from typing import TypeAlias, Union, get_args, get_origin

import dacite
from aenum import Enum, NoAlias
from dacite.config import Config as DaciteConfig
from google.cloud.bigquery import SchemaField


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
            origin = get_origin(_type)
            args = get_args(_type)
        else:
            mode = "REQUIRED"

        if origin is list:
            mode = "REPEATED"
            _type = args[0]

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

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Record":
        current_date = datetime.now(timezone.utc).date()
        data["submission_date"] = current_date.strftime("%Y-%m-%d")
        return dacite.from_dict(
            data_class=cls, data=data, config=DaciteConfig(check_types=False)
        )

    @abstractmethod
    def __str__(self) -> str: ...


@dataclass
class Runs(Record):
    reason_created: BigQueryTypes.STRING
    reason_resolved: BigQueryTypes.STRING
    resolved: BigQueryTypes.TIMESTAMP
    run_id: BigQueryTypes.INTEGER
    scheduled: BigQueryTypes.TIMESTAMP
    started: Optional[BigQueryTypes.TIMESTAMP]
    state: BigQueryTypes.STRING
    task_id: BigQueryTypes.STRING
    worker_group: Optional[BigQueryTypes.STRING]
    worker_id: Optional[BigQueryTypes.STRING]

    def __str__(self):
        return f"{self.task_id} run {self.run_id}"


@dataclass
class Tags:
    created_for_user: Optional[BigQueryTypes.STRING]
    kind: Optional[BigQueryTypes.STRING]
    label: Optional[BigQueryTypes.STRING]
    os: Optional[BigQueryTypes.STRING]
    owned_by: Optional[BigQueryTypes.STRING]
    project: Optional[BigQueryTypes.STRING]
    trust_domain: Optional[BigQueryTypes.STRING]
    worker_implementation: Optional[BigQueryTypes.STRING]


@dataclass
class Tasks(Record):
    scheduler_id: BigQueryTypes.STRING
    task_group_id: BigQueryTypes.STRING
    task_id: BigQueryTypes.STRING
    task_queue_id: BigQueryTypes.STRING
    tags: Tags

    def __str__(self):
        return self.task_id


@dataclass
class Metrics(Record):
    instance_id: BigQueryTypes.STRING
    project: BigQueryTypes.STRING
    zone: BigQueryTypes.STRING
    uptime: BigQueryTypes.FLOAT
    interval_start_time: BigQueryTypes.TIMESTAMP
    interval_end_time: BigQueryTypes.TIMESTAMP

    def __str__(self):
        return f"worker {self.instance_id}"


def get_record_cls(table_type: str) -> Type[Record]:
    """Return the record class corresponding to the given table type.

    Args:
        table_type (str): Table for which to return a record class.

    Returns:
        Type[Record]: The record class for the corresponding table.
    """
    assert table_type in ("tasks", "runs", "metrics")
    for name, obj in globals().items():
        if name.lower() == table_type and issubclass(obj, Record):
            return obj

    raise Exception(f"Record not found for {table_type}!")
