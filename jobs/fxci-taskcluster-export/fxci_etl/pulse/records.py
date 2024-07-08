from dataclasses import dataclass
from typing import Optional

from fxci_etl.loaders.bigquery import Record


class Run(Record):
    reason_created: str
    reason_resolved: str
    resolved: str
    run_id: int
    scheduled: str
    started: Optional[str]
    state: str
    task_id: str
    worker_group: str
    worker_id: str

    @classmethod
    def table_name(cls):
        return "task_runs"

    def __str__(self):
        return f"{self.task_id} run {self.run_id}"


@dataclass
class Tag:
    key: str
    value: str


@dataclass
class Task(Record):
    scheduler_id: str
    task_group_id: str
    task_id: str
    task_queue_id: str
    tags: list[Tag]

    @classmethod
    def table_name(cls):
        return "tasks"

    def __str__(self):
        return self.task_id
