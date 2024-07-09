import base64
import json
import traceback
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from pprint import pprint
from typing import Any, Optional

import dacite
from google.cloud import storage
from google.cloud.exceptions import NotFound
from kombu import Message

from fxci_etl.config import Config
from fxci_etl.pulse.records import Run, Task
from fxci_etl.loaders.bigquery import BigQueryLoader, Record


@dataclass
class Event:
    data: dict[str, Any]
    message: Optional[Message]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Event":
        return dacite.from_dict(data_class=cls, data=data)

    def to_dict(self):
        return {"data": self.data}


class PulseHandler(ABC):
    name = ""

    def __init__(self, config: Config):
        self.config = config

        if config.storage.credentials:
            storage_client = storage.Client.from_service_account_info(
                json.loads(base64.b64decode(config.storage.credentials).decode("utf8"))
            )
        else:
            storage_client = storage.Client()

        bucket = storage_client.bucket(config.storage.bucket)
        self._event_backup = bucket.blob(f"failed-pulse-events-{self.name}.json")
        self._buffer: list[Event] = []

    def __call__(self, data: dict[str, Any], message: Message) -> None:
        message.ack()
        event = Event(data, message)
        self._buffer.append(event)

    def process_buffer(self):
        try:
            # Load previously failed events from storage, maybe the issue is fixed.
            for obj in json.loads(self._event_backup.download_as_string()):
                self._buffer.append(Event.from_dict(obj))
        except NotFound:
            pass

        failed = []
        for event in self._buffer:
            try:
                self.process_event(event)
            except Exception:
                print(f"Error processing event in {self.name} handler:")
                pprint(event, indent=2)
                traceback.print_exc()
                failed.append(event.to_dict())

        # Save any failed events back to storage.
        self._event_backup.upload_from_string(json.dumps(failed))
        self._buffer = []
        self.on_processing_complete()

    @abstractmethod
    def process_event(self, event: Event) -> None: ...

    def on_processing_complete(self) -> None:
        pass


class BigQueryHandler(PulseHandler):
    name = "bigquery"

    def __init__(self, config: Config, **kwargs: Any):
        super().__init__(config, **kwargs)
        self.loader = BigQueryLoader(self.config)
        self.records: list[Record] = []

    def process_event(self, event):
        data = event.data
        status = data["status"]
        run = data["status"]["runs"][data["runId"]]
        run_record = {
            "task_id": status["taskId"],
            "reason_created": run["reasonCreated"],
            "reason_resolved": run["reasonResolved"],
            "resolved": run["resolved"],
            "run_id": data["runId"],
            "scheduled": run["scheduled"],
            "state": run["state"],
        }
        if "started" in run:
            run_record["started"] = run["started"]

        if "workerGroup" in run:
            run_record["worker_group"] = run["workerGroup"]

        if "workerId" in run:
            run_record["worker_id"] = run["workerId"]

        self.records.append(
            Run.from_dict(self.config.bigquery.tables.runs, run_record)
        )

        if data["runId"] == 0:
            # Only insert the task record for run 0 to avoid duplicate records.
            try:
                task_record = {
                    "scheduler_id": status["schedulerId"],
                    "tags": [],
                    "task_group_id": status["taskGroupId"],
                    "task_id": status["taskId"],
                    "task_queue_id": status["taskQueueId"],
                }
                # Tags can be missing if the run is in the exception state.
                if "task" in data and "tags" in data["task"]:
                    task_record["tags"] = [
                        {"key": k, "value": v} for k, v in data["task"]["tags"].items()
                    ]
                self.records.append(
                    Task.from_dict(self.config.bigquery.tables.tasks, task_record)
                )
            except Exception:
                # Don't insert the run without its corresponding task.
                self.records.pop()
                raise

    def on_processing_complete(self):
        if self.records:
            self.loader.insert(self.records)
            self.records = []
