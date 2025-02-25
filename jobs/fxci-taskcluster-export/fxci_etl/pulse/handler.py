import base64
import json
import re
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pprint import pprint
from typing import Any, Optional

import dacite
from google.cloud import storage
from google.cloud.exceptions import NotFound
from kombu import Message
from loguru import logger

from fxci_etl.config import Config
from fxci_etl.loaders.bigquery import BigQueryLoader
from fxci_etl.schemas import Record, Runs, Tasks, Tags


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
        self._count = 0

    def __call__(self, data: dict[str, Any], message: Message) -> None:
        self._count += 1
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
                logger.error(f"Error processing event in {self.name} handler:")
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
        self.task_records: list[Record] = []
        self.run_records: list[Record] = []

        self._convert_camel_case_re = re.compile(r"(?<!^)(?=[A-Z])")
        self._known_tags = set(Tags.__annotations__.keys())

    def _normalize_tag(self, tag: str) -> str | None:
        """Tags are not well standardized and can be in camel case, snake case,
        separated by dashes or even spaces. Ensure they all get normalized to
        snake case.

        If the normalization results in a known tag, return it. Otherwise return
        None.
        """
        tag = tag.replace("-", "_").replace(" ", "_")
        tag = self._convert_camel_case_re.sub("_", tag).lower()
        if tag in self._known_tags:
            return tag

    def process_event(self, event):
        data = event.data

        if data.get("runId") is None:
            # This can happen if `deadline` was exceeded before a run could
            # start. Ignore this case.
            return

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

        self.run_records.append(
            Runs.from_dict(run_record)
        )

        if data["runId"] == 0:
            # Only insert the task record for run 0 to avoid duplicate records.
            try:
                task_record = {
                    "scheduler_id": status["schedulerId"],
                    "tags": {},
                    "task_group_id": status["taskGroupId"],
                    "task_id": status["taskId"],
                    "task_queue_id": status["taskQueueId"],
                }
                # Tags can be missing if the run is in the exception state.
                if tags := data.get("task", {}).get("tags"):
                    for key, value in tags.items():
                        if key := self._normalize_tag(key):
                            task_record["tags"][key] = value

                self.task_records.append(
                    Tasks.from_dict(task_record)
                )
            except Exception:
                # Don't insert the run without its corresponding task.
                self.run_records.pop()
                raise

    def on_processing_complete(self):
        logger.info(f"Processed {self._count} pulse events")
        if self.task_records:
            task_loader = BigQueryLoader(self.config, "tasks")
            task_loader.insert(self.task_records)
            self.task_records = []

        if self.run_records:
            run_loader = BigQueryLoader(self.config, "runs")
            run_loader.insert(self.run_records)
            self.run_records = []
