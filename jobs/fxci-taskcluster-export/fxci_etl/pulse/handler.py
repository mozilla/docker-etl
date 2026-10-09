import base64
import json
import re
import threading
import traceback
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pprint import pprint
from typing import Any, Optional

import dacite
from google.cloud import storage
from google.cloud.exceptions import NotFound
from kombu import Message
from loguru import logger
import requests
import taskcluster

from fxci_etl.config import Config
from fxci_etl.loaders.bigquery import BigQueryLoader
from fxci_etl.schemas import Perfherder, Record, Runs, Tasks, Tags, TaskDefinitions


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

        bucket = self._bucket = storage_client.bucket(config.storage.bucket)
        self._event_backup = bucket.blob(f"failed-pulse-events-{self.name}.json")
        self._buffer: list[Event] = []
        self._count = 0
        self._queue = taskcluster.Queue({"rootUrl": config.taskcluster.rootUrl})

    def __call__(self, data: dict[str, Any], message: Message) -> None:
        self._count += 1
        # Several handlers can receive the same message, only ack it once.
        if not message.acknowledged:
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
        self._taskids_backup = self._bucket.blob(f"failed-pulse-task-ids-{self.name}.json")
        self.task_ids: set[str] = set()
        try:
            self.task_ids = set(json.loads(self._taskids_backup.download_as_string()))
        except NotFound:
            pass
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

        status = data.get("status")
        if status is None:
            return

        if status.get("state") == "unscheduled":
            # Newly created task, record the id to fetch its definition later
            self.task_ids.add(status["taskId"])
            return

        if data.get("runId") is None:
            # This can happen if `deadline` was exceeded before a run could
            # start. Ignore this case.
            return

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

        if self.task_ids:
            taskdef_loader = BigQueryLoader(self.config, "taskdefinitions", chunk_size=100)
            taskdefs = []
            def paginationHandler(response):
                taskdefs.extend(response["tasks"])
            try:
                self._queue.tasks(payload={"taskIds": list(self.task_ids)}, paginationHandler=paginationHandler)
                for task in taskdefs:
                    taskdef = task["task"]
                    taskdef["taskId"] = task["taskId"]
                taskdef_records = [TaskDefinitions.from_dict(task["task"]) for task in taskdefs]
                # FIXME insert isn't atomic, so if this fails we can end up with duplicate records
                taskdef_loader.insert(taskdef_records)
                self.task_ids.clear()
            finally:
                self._taskids_backup.upload_from_string(json.dumps(list(self.task_ids)))


class PerfherderHandler(PulseHandler):
    """Ingest the perfherder-data artifacts of completed and failed task runs."""

    name = "perfherder"
    max_workers = 32
    timeout = 60

    def __init__(self, config: Config, **kwargs: Any):
        super().__init__(config, **kwargs)
        self._runs_backup = self._bucket.blob(f"failed-pulse-runs-{self.name}.json")
        self.runs: set[tuple[str, int]] = set()
        try:
            self.runs = {
                (task_id, run_id)
                for task_id, run_id in json.loads(self._runs_backup.download_as_string())
            }
        except NotFound:
            pass
        self._local = threading.local()

    @staticmethod
    def is_perfherder_artifact(name: str) -> bool:
        # Same filter as treeherder, restricted to artifacts we can fetch
        # without credentials.
        return (
            name.startswith("public/")
            and name.endswith(".json")
            and "perfherder-data" in name
        )

    def process_event(self, event):
        data = event.data

        status = data.get("status")
        run_id = data.get("runId")
        if status is None or run_id is None:
            return

        if status["runs"][run_id]["state"] not in ("completed", "failed"):
            return

        self.runs.add((status["taskId"], run_id))

    @property
    def _session(self) -> requests.Session:
        # requests sessions aren't guaranteed to be thread safe.
        if not hasattr(self._local, "session"):
            self._local.session = requests.Session()
        return self._local.session

    def _get(self, url: str, **kwargs: Any) -> requests.Response:
        response = self._session.get(url, timeout=self.timeout, **kwargs)
        response.raise_for_status()
        return response

    def _list_artifacts(self, task_id: str, run_id: int) -> list[str]:
        url = self._queue.buildUrl("listArtifacts", task_id, run_id)
        names = []
        query = {}
        while True:
            response = self._get(url, params=query).json()
            names.extend(a["name"] for a in response["artifacts"])
            if not (token := response.get("continuationToken")):
                return names
            query["continuationToken"] = token

    def _fetch_run(self, task_id: str, run_id: int) -> list[Record]:
        records = []
        for name in self._list_artifacts(task_id, run_id):
            if not self.is_perfherder_artifact(name):
                continue

            url = self._queue.buildUrl("getArtifact", task_id, run_id, name)
            try:
                data = self._get(url).json()
            except ValueError:
                logger.warning(f"Skipping {name} of {task_id} run {run_id}: invalid JSON")
                continue

            if not isinstance(data, dict):
                logger.warning(f"Skipping {name} of {task_id} run {run_id}: not an object")
                continue

            framework = data.get("framework")
            records.append(
                Perfherder.from_dict(
                    {
                        "task_id": task_id,
                        "run_id": run_id,
                        "artifact": name,
                        "framework": framework.get("name")
                        if isinstance(framework, dict)
                        else None,
                        "data": data,
                    }
                )
            )
        return records

    def on_processing_complete(self):
        if not self.runs:
            return

        logger.info(f"Fetching perfherder artifacts for {len(self.runs)} task runs")
        records: list[Record] = []
        failed: set[tuple[str, int]] = set()
        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(self._fetch_run, *run): run for run in self.runs
                }
                for future in as_completed(futures):
                    run = futures[future]
                    try:
                        records.extend(future.result())
                    except requests.HTTPError as e:
                        status_code = e.response.status_code
                        if 400 <= status_code < 500 and status_code != 429:
                            # Retrying won't help (e.g. expired artifacts).
                            logger.warning(f"Skipping {run[0]} run {run[1]}: {e}")
                        else:
                            logger.error(f"Error fetching {run[0]} run {run[1]}: {e}")
                            failed.add(run)
                    except Exception as e:
                        logger.error(f"Error fetching {run[0]} run {run[1]}: {e}")
                        failed.add(run)

            if records:
                loader = BigQueryLoader(self.config, "perfherder", chunk_size=500)
                loader.insert(records)
            self.runs = failed
        finally:
            self._runs_backup.upload_from_string(json.dumps(sorted(self.runs)))
