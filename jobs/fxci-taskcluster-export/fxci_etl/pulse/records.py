from dataclasses import dataclass
from typing import Optional

from fxci_etl.loaders.bigquery import Record, BigQueryTypes as t


@dataclass
class Run(Record):
    reason_created: t.STRING
    reason_resolved: t.STRING
    resolved: t.TIMESTAMP
    run_id: t.INTEGER
    scheduled: t.TIMESTAMP
    started: Optional[t.TIMESTAMP]
    state: t.STRING
    task_id: t.STRING
    worker_group: Optional[t.STRING]
    worker_id: Optional[t.STRING]

    def __str__(self):
        return f"{self.task_id} run {self.run_id}"


@dataclass
class Tag:
    key: t.STRING
    value: t.STRING


@dataclass
class Task(Record):
    scheduler_id: t.STRING
    task_group_id: t.STRING
    task_id: t.STRING
    task_queue_id: t.STRING
    tags: list[Tag]

    def __str__(self):
        return self.task_id
