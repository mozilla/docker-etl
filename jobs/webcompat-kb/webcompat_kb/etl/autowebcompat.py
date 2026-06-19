import itertools
import argparse
from collections import defaultdict
import logging
import os
from typing import (
    Iterable,
    Optional,
    Self,
)
from abc import ABC, abstractmethod
from collections.abc import Sequence, Mapping, MutableMapping
from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID

from bugdantic import bugzilla
from google.cloud import bigquery
from pydantic import BaseModel

from ..base import Context, EtlJob
from ..bqhelpers import BigQuery
from ..projectdata import Project
from ..hackbot import (
    ArtifactRef,
    BaseSummary,
    CreateRequest,
    Hackbot,
    HackbotAgentResult,
    HackbotConfig,
    Json,
    RunDoc,
    RunSummary,
)


class RunInfo(BaseModel):
    agent: str
    task_name: str
    source_key: str
    source_time: Optional[datetime]
    updated: bool


@dataclass(frozen=True)
class RunKey:
    agent: str
    task_name: str
    source_key: str
    run_key: str


class AttachmentData(BaseModel):
    file_name: str
    content_type: Optional[str]
    url: str


class NewBugInfo(BaseModel):
    number: int
    title: str
    url: str
    keywords: list[str]
    whiteboard: str
    user_story: str
    creation_time: datetime
    comments: list[str] = []
    attachments: list[AttachmentData] = []


class ScheduledRun(BaseModel):
    run_id: UUID
    agent: str
    task_name: str
    source_key: str
    run_key: str
    source_time: datetime
    requested_at: datetime
    request_data: Json
    extra_data: Mapping[str, Json]

    def full_key(self) -> RunKey:
        return RunKey(
            agent=self.agent,
            task_name=self.task_name,
            source_key=self.source_key,
            run_key=self.run_key,
        )

    def to_json(self) -> Mapping[str, Json]:
        rv = self.model_dump(mode="json")
        rv["source_time"] = self.source_time.replace(tzinfo=None).isoformat()
        return rv


class CompleteRun(BaseModel):
    run_id: UUID
    status: str
    created_at: datetime
    completed_at: datetime
    execution_name: Optional[str]
    results_prefix: str
    summary: Optional[RunSummary]
    artifacts: list[ArtifactRef]
    error: Optional[str]

    @classmethod
    def from_rundoc(cls, src: RunDoc) -> Self:
        return cls(
            run_id=src.run_id,
            status=src.status,
            created_at=src.created_at,
            completed_at=src.updated_at,
            execution_name=src.execution_name,
            results_prefix=src.results_prefix,
            summary=src.summary,
            artifacts=src.artifacts[:],
            error=src.error,
        )

    def to_json(self) -> Mapping[str, Json]:
        rv = self.model_dump(mode="json")
        rv["created_at"] = self.created_at.replace(tzinfo=None).isoformat()
        rv["completed_at"] = self.completed_at.replace(tzinfo=None).isoformat()
        return rv


class ReproResult(BaseModel):
    reproduced: bool
    summary: str
    steps: str


class AutowebcompatReproRequest(CreateRequest):
    bug_data: str
    bug_id: Optional[int] = None
    model: Optional[str] = None
    max_turns: Optional[int] = None
    effort: Optional[str] = None
    agent: str = "autowebcompat-repro"


class WebcompatReproResult(HackbotAgentResult):
    result: Optional[ReproResult] = None


class ReproSummary(BaseSummary):
    findings: WebcompatReproResult


class BigQueryService:
    def __init__(
        self, project: Project, bq_client: BigQuery, bz_client: bugzilla.Bugzilla
    ):
        self.project = project
        self.bq_client = bq_client
        self.bz_client = bz_client
        self.run_info_table = project["autowebcompat"]["import_runs"].table()
        self.scheduled_table = project["autowebcompat"]["hackbot_scheduled"].table()
        self.completed_table = project["autowebcompat"]["hackbot_completed"].table()

    def get_source_times(
        self,
    ) -> Mapping[tuple[str, str], Mapping[str, Optional[datetime]]]:
        rv: dict[tuple[str, str], dict[str, datetime]] = defaultdict(dict)
        rows = list(
            self.bq_client.query(f"""
    SELECT agent, task_name, source_key, MAX(source_time) AS source_time
    FROM `{self.run_info_table}`
    JOIN UNNEST(run_info) AS run_info
    GROUP BY agent, task_name, source_key
    """)
        )
        for item in rows:
            rv[(item.agent, item.task_name)][item.source_key] = item.source_time
        return rv

    def get_pending(self) -> list[UUID]:
        rows = self.bq_client.query(f"""
    SELECT run_id FROM `{self.scheduled_table}`
    EXCEPT DISTINCT
    SELECT run_id FROM `{self.completed_table}`
    """)
        return [UUID(item.run_id) for item in rows]

    def get_scheduled_by_uuid(
        self, run_uuids: Iterable[UUID]
    ) -> Mapping[UUID, ScheduledRun]:
        rows = self.bq_client.query(
            f"""
    SELECT * FROM `{self.scheduled_table}`
    WHERE run_id IN UNNEST(@run_ids)
    """,
            parameters=[
                bigquery.ArrayQueryParameter(
                    "run_ids", "STRING", [str(item) for item in run_uuids]
                )
            ],
        )
        return {
            UUID(row.run_id): ScheduledRun.model_validate(dict(row.items()))
            for row in rows
        }

    def get_scheduled_by_key(
        self,
        keys: Iterable[RunKey],
    ) -> set[str]:
        query = f"""
    SELECT agent, task_name, source_key, run_key
    FROM `{self.scheduled_table}`
    JOIN UNNEST(@keys) AS key USING(agent, task_name, source_key, run_key)
    LEFT JOIN `{self.completed_table}` AS completed_runs USING(run_id)
    WHERE completed_runs.run_id IS NULL
    """
        rows = self.bq_client.query(
            query,
            parameters=[
                bigquery.ArrayQueryParameter(
                    "keys",
                    "RECORD",
                    [
                        bigquery.StructQueryParameter(
                            None,
                            bigquery.ScalarQueryParameter("agent", "STRING", key.agent),
                            bigquery.ScalarQueryParameter(
                                "task_name", "STRING", key.task_name
                            ),
                            bigquery.ScalarQueryParameter(
                                "source_key", "STRING", key.source_key
                            ),
                            bigquery.ScalarQueryParameter(
                                "run_key", "STRING", key.run_key
                            ),
                        )
                        for key in keys
                    ],
                ),
            ],
        )
        return set(item.key for item in rows)

    def get_new_bugs(
        self,
        created_since: Optional[datetime],
    ) -> Mapping[int, NewBugInfo]:
        bugs_query = f"""
    SELECT number, title, url, keywords, whiteboard, user_story_raw as user_story, creation_time
    FROM `{self.project["webcompat_knowledge_base"]["site_reports"]}` as bugs
    WHERE
      resolution = "" AND
      whiteboard NOT LIKE "%[autowebcompat:processed]%" AND
      (
        (@created_since IS NOT NULL AND CAST(creation_time AS DATETIME) > @created_since) OR
        (
          @created_since IS NULL AND
          CAST(creation_time AS DATETIME) >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 1 WEEK)
        )
      )
    """
        bug_rows = self.bq_client.query(
            bugs_query,
            parameters=[
                bigquery.ScalarQueryParameter(
                    "created_since", "DATETIME", created_since
                )
            ],
        )
        new_bugs = {
            row.number: NewBugInfo.model_validate(dict(row.items())) for row in bug_rows
        }
        bug_ids = list(new_bugs.keys())
        fields = ["id", "comments", "attachments"]
        for result in self.bz_client.bugs(bug_ids, include_fields=fields):
            bug = new_bugs[result.id]
            if result.comments:
                bug.comments = [
                    item.text for item in result.comments if item.text is not None
                ]
            if result.attachments:
                bug.attachments = [
                    AttachmentData(
                        file_name=attachment.file_name,
                        content_type=attachment.content_type,
                        url=f"https://bugzilla.mozilla.org/attachment.cgi?id={attachment.id}",
                    )
                    for attachment in result.attachments
                    if attachment.file_name is not None
                ]

        return new_bugs

    def insert_new_runs(self, new_runs: Iterable[ScheduledRun]) -> None:
        rows = [item.to_json() for item in new_runs]
        self.bq_client.insert_rows(self.scheduled_table, rows)

    def insert_complete_runs(self, complete_runs: Iterable[RunDoc]) -> None:
        rows = [CompleteRun.from_rundoc(item).to_json() for item in complete_runs]
        self.bq_client.insert_rows(self.completed_table, rows)

    def record_update(self, run_infos: Iterable[RunInfo]) -> None:
        self.bq_client.insert_query(
            self.run_info_table,
            columns=[item.name for item in self.run_info_table.fields],
            query="SELECT CURRENT_DATETIME() as run_at, @run_infos AS run_info",
            parameters=[
                bigquery.ArrayQueryParameter(
                    "run_infos",
                    "RECORD",
                    [
                        bigquery.StructQueryParameter(
                            None,
                            bigquery.ScalarQueryParameter(
                                "agent", "STRING", run_info.agent
                            ),
                            bigquery.ScalarQueryParameter(
                                "task_name", "STRING", run_info.task_name
                            ),
                            bigquery.ScalarQueryParameter(
                                "source_key", "STRING", run_info.source_key
                            ),
                            bigquery.ScalarQueryParameter(
                                "source_time", "DATETIME", run_info.source_time
                            ),
                        )
                        for run_info in run_infos
                    ],
                ),
            ],
        )


def poll_pending(
    hackbot_client: Hackbot, run_uuids: list[UUID]
) -> Mapping[UUID, RunDoc]:
    complete_runs = {}
    for run_uuid in run_uuids:
        run_data, complete = hackbot_client.poll_run(run_uuid)
        if complete:
            complete_runs[run_data.run_id] = run_data
    return complete_runs


@dataclass
class BugUpdate:
    bug: bugzilla.BugUpdate
    add_attachments: list[bugzilla.AttachmentCreate] = field(default_factory=lambda: [])

    def has_updates(self) -> bool:
        if self.add_attachments:
            return True

        update_fields = self.bug.model_dump(exclude_none=True).keys() - {
            "ids",
            "id_or_alias",
        }
        return bool(update_fields)


class Updater(ABC):
    """Base class for updating a specific data source based on hackbot tasks.

    For example specific subclasses might implementing updates for different
    bug trackers, or just for writing results to the console."""

    @abstractmethod
    def fetch_data(self) -> None:
        """Fetch any data required to perform updates
        e.g. the latest state of bugs that might be updated"""
        ...

    @abstractmethod
    def update(self) -> None:
        """Perform the update"""
        ...


class BugzillaUpdater(Updater):
    def __init__(self, client: bugzilla.Bugzilla):
        self.client = client
        self.include_fields = set(["id"])
        self.bug_ids: set[int] = set()
        self.bug_updates: dict[int, tuple[bugzilla.Bug, BugUpdate]] = {}

    def add_include_fields(self, fields: Iterable[str]) -> None:
        self.include_fields |= set(fields)

    def add_bug_ids(self, bug_ids: Iterable[int]) -> None:
        self.bug_ids |= set(bug_ids)

    def fetch_data(self) -> None:
        for bug in self.client.bugs(
            bug_ids=list(self.bug_ids), include_fields=list(self.include_fields)
        ):
            assert bug.id is not None
            self.bug_updates[bug.id] = (
                bug,
                BugUpdate(bug=bugzilla.BugUpdate(ids=[bug.id])),
            )

    def update(self) -> None:
        for _, bug_update in self.bug_updates.values():
            if bug_update.has_updates():
                self.client.update_bugs(bug_update.bug)
            for attachment in bug_update.add_attachments:
                self.client.create_attachment(attachment)


@dataclass
class ScheduleRequest:
    source_key: str
    run_key: str
    request_data: BaseModel
    source_time: datetime
    extra_data: Optional[BaseModel] = None

    def full_key(self, agent: str, task_name: str) -> RunKey:
        return RunKey(
            agent=agent,
            task_name=task_name,
            source_key=self.source_key,
            run_key=self.run_key,
        )


class HackbotTask(ABC):
    """A base class for tasks that we want to run on hackbot e.g. reproducing an issue,
    diagnosing a bug.

    Each task run has a source_key identifying the data source and why the task was scheduled.
    It also has a run_key identifying the specific run of the task. The combination
    (agent, task_name, source_key, run_key) must be unique for any runnng task.
    """

    agent: str
    task_name: str

    def __init__(
        self,
        hackbot_client: Hackbot,
        bq_service: BigQueryService,
        source_times: Mapping[str, Optional[datetime]],
    ):
        self.hackbot_client = hackbot_client
        self.bq_service = bq_service
        self.source_times = source_times
        self.run_info: Optional[RunInfo] = None
        self.completed_runs: dict[UUID, tuple[ScheduledRun, RunDoc]] = {}
        self.scheduled: dict[str, list[ScheduledRun]] = defaultdict(list)

    def has_updates(self) -> bool:
        return bool(self.scheduled or self.completed_runs)

    def run_matches(self, run: ScheduledRun) -> bool:
        return (run.agent, run.task_name) == (self.agent, self.task_name)

    def take_completed_runs(
        self, complete_runs: MutableMapping[UUID, tuple[ScheduledRun, RunDoc]]
    ) -> None:
        for run_uuid, (scheduled_run, run_doc) in complete_runs.items():
            if self.run_matches(scheduled_run):
                self.completed_runs[run_uuid] = (scheduled_run, run_doc)
        for run_uuid in self.completed_runs:
            del complete_runs[run_uuid]

    def schedule(
        self, requests: Iterable[ScheduleRequest]
    ) -> Mapping[str, Sequence[ScheduledRun]]:
        scheduled_run_keys = self.bq_service.get_scheduled_by_key(
            [request.full_key(self.agent, self.task_name) for request in requests],
        )

        for request in requests:
            if (
                RunKey(self.agent, self.task_name, request.source_key, request.run_key)
                in scheduled_run_keys
            ):
                logging.info(
                    f"Already got a scheduled run for agent {self.agent}, task {self.task_name}, source {request.source_key}, run id {request.run_key}"
                )
                continue

            requested_at = datetime.now()
            logging.info(
                f"Scheduling run with agent {self.agent}, task {self.task_name}, source {request.source_key}, run id {request.run_key}"
            )
            run_ref = self.hackbot_client.create_run(self.get_request_data(request))
            self.scheduled[request.source_key].append(
                ScheduledRun(
                    run_id=run_ref.run_id,
                    agent=self.agent,
                    task_name=self.task_name,
                    source_key=request.source_key,
                    run_key=request.run_key,
                    source_time=request.source_time,
                    requested_at=requested_at,
                    request_data=request.request_data.model_dump(mode="json"),
                    extra_data=request.extra_data.model_dump(mode="json")
                    if request.extra_data
                    else {},
                )
            )

        return self.scheduled

    def new_run_info(self) -> Mapping[str, RunInfo]:
        rv = {}
        for source_key, runs in self.scheduled.items():
            rv[source_key] = RunInfo(
                agent=self.agent,
                task_name=self.task_name,
                source_key=source_key,
                source_time=max(run.source_time for run in runs),
                updated=True,
            )
        for source_key, source_time in self.source_times.items():
            rv[source_key] = RunInfo(
                agent=self.agent,
                task_name=self.task_name,
                source_key=source_key,
                source_time=source_time,
                updated=False,
            )
        return rv

    @staticmethod
    def key(*args: str) -> str:
        return ":".join(args)

    @abstractmethod
    def get_request_data(self, request: ScheduleRequest) -> CreateRequest:
        """Convert a ScheduleRequest object into the appropriate CreateRequest subclass
        for the agent"""
        ...

    @abstractmethod
    def create_new(self) -> Mapping[str, Sequence[ScheduledRun]]:
        """Scheudle new tasks based on BigQuery data (e.g. newly created bugs)"""
        ...

    @abstractmethod
    def configure_updater(self, updater: Updater) -> None:
        """Configure the updater so that it will fetch any data needed when populating the updates
        for this task"""
        ...

    @abstractmethod
    def populate_updates(self, updater: Updater) -> None:
        """Populate each updater with the updates that result from running this task"""
        ...


class BugExtraData(BaseModel):
    bug_id: int


class ReproTask(HackbotTask):
    agent: str = "autowebcompat-repro"
    task_name: str = "repro"

    def get_request_data(self, request: ScheduleRequest) -> AutowebcompatReproRequest:
        return AutowebcompatReproRequest(
            agent=self.agent, bug_data=request.request_data.model_dump_json()
        )

    def create_new(
        self,
    ) -> Mapping[str, Sequence[ScheduledRun]]:
        source_key = self.key("bugzilla", "creation")
        created_since = self.source_times.get(source_key)
        new_bugs = self.bq_service.get_new_bugs(created_since)
        logging.info(f"Found {len(new_bugs)} bugs that require reproduction")

        if not new_bugs:
            return {}

        requests = []
        for bug_number, bug_info in new_bugs.items():
            requests.append(
                ScheduleRequest(
                    source_key=source_key,
                    run_key=self.key(str(bug_number)),
                    source_time=bug_info.creation_time,
                    request_data=bug_info,
                    extra_data=BugExtraData(bug_id=bug_number),
                )
            )

        return self.schedule(requests)

    def configure_updater(self, updater: Updater) -> None:
        if not isinstance(updater, BugzillaUpdater):
            return

        bugzilla_scheduled = itertools.chain.from_iterable(
            value
            for key, value in self.scheduled.items()
            if key.startswith("bugzilla:")
        )
        updater.add_include_fields(["whiteboard"])
        updater.add_bug_ids(
            BugExtraData.model_validate(item.extra_data).bug_id
            for item in bugzilla_scheduled
        )
        updater.add_bug_ids(
            int(item.run_key) for item, _ in self.completed_runs.values()
        )

    def populate_updates(self, updater: Updater) -> None:
        if isinstance(updater, BugzillaUpdater):
            bugzilla_scheduled = itertools.chain.from_iterable(
                value
                for key, value in self.scheduled.items()
                if key.startswith("bugzilla:")
            )
            for scheduled in bugzilla_scheduled:
                bug_id = BugExtraData.model_validate(scheduled.extra_data).bug_id
                assert isinstance(bug_id, int)
                processed_token = "[autowebcompat:processed]"
                bug, bug_update = updater.bug_updates[bug_id]
                current_whiteboard = (
                    bug_update.bug.whiteboard
                    if bug_update.bug.whiteboard is not None
                    else bug.whiteboard
                )
                assert current_whiteboard is not None
                if processed_token not in current_whiteboard:
                    bug_update.bug.whiteboard = current_whiteboard + processed_token

            run_inputs = {
                run_id: NewBugInfo.model_validate(scheduled_run.request_data)
                for run_id, (scheduled_run, _) in self.completed_runs.items()
            }
            run_outputs = {
                run_id: ReproSummary.model_validate(run_doc.summary.model_dump())
                for run_id, (_, run_doc) in self.completed_runs.items()
                if run_doc.summary is not None
            }

            for uuid, output in run_outputs.items():
                bug_info = run_inputs[uuid]
                bug, bug_update = updater.bug_updates[bug_info.number]

                result = output.findings.result
                require_whiteboard = []
                if result is None:
                    require_whiteboard.append("[autowebcompat:repro-failed]")
                else:
                    require_whiteboard.append(
                        "[autowebcompat:repro-failed]"
                        if not result.reproduced
                        else "[autowebcompat:repro-success]"
                    )
                    if result.reproduced and result.steps:
                        bug_update.add_attachments.append(
                            bugzilla.AttachmentCreate.from_raw_data(
                                ids=[bug_info.number],
                                data=result.steps,
                                file_name="reproduction_steps.txt",
                                summary="Reproduction steps generated by autowebcompat bot",
                                comment=result.summary,
                                content_type="text/markdown",
                            )
                        )

                for token in require_whiteboard:
                    current_whiteboard = (
                        bug_update.bug.whiteboard
                        if bug_update.bug.whiteboard is not None
                        else bug.whiteboard
                    )
                    assert current_whiteboard is not None
                    if token not in current_whiteboard:
                        bug_update.bug.whiteboard = current_whiteboard + token
        else:
            raise ValueError(
                f"Don't know how to update for task {self} and updater type {updater}"
            )


def run(
    project: Project,
    bq_client: BigQuery,
    bz_client: bugzilla.Bugzilla,
    hackbot_client: Hackbot,
    check_pending: bool,
    updaters: Sequence[Updater],
    tasks: Sequence[type[HackbotTask]],
) -> None:
    bq_service = BigQueryService(project, bq_client, bz_client)
    source_times = bq_service.get_source_times()

    if check_pending:
        pending_runs = bq_service.get_pending()
        complete_runs = poll_pending(hackbot_client, pending_runs)
    else:
        complete_runs = {}

    complete_runs_scheduled = bq_service.get_scheduled_by_uuid(
        list(complete_runs.keys())
    )

    complete_runs_remaining = {
        run_id: (complete_runs_scheduled[run_id], run_doc)
        for run_id, run_doc in complete_runs.items()
    }

    new_runs: dict[str, Sequence[ScheduledRun]] = {}

    task_runners = []

    for task_cls in tasks:
        task_runner = task_cls(
            hackbot_client,
            bq_service,
            source_times.get((task_cls.agent, task_cls.task_name), {}),
        )
        task_runners.append(task_runner)
        task_runner.take_completed_runs(complete_runs_remaining)

        scheduled = task_runner.create_new()
        new_runs.update(scheduled)

        for updater in updaters:
            task_runner.configure_updater(updater)

    for updater in updaters:
        updater.fetch_data()
        for task_runner in task_runners:
            if task_runner.has_updates():
                task_runner.populate_updates(updater)
        updater.update()

    bq_service.insert_new_runs(itertools.chain.from_iterable(new_runs.values()))
    bq_service.insert_complete_runs(complete_runs.values())
    if new_runs or complete_runs:
        bq_service.record_update(
            itertools.chain.from_iterable(
                item.new_run_info().values() for item in task_runners
            )
        )


class AutowebcompatJob(EtlJob):
    name = "autowebcompat"

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(
            title="Autowebcompat", description="Autowebcompat arguments"
        )
        group.add_argument(
            "--hackbot-api-key",
            help="Hackbot API key",
            default=os.environ.get("HACKBOT_API_KEY"),
        )
        group.add_argument(
            "--hackbot-url",
            help="Hackbot url",
            default=os.environ.get("HACKBOT_URL"),
        )
        group.add_argument(
            "--autowebcompat-no-check-pending",
            help="Don't check pending runs in hackbot",
            dest="autowebcompat_check_pending",
            action="store_false",
            default=True,
        )

    def default_dataset(self, context: Context) -> str:
        return "autowebcompat"

    def main(self, context: Context) -> None:
        bz_config = bugzilla.BugzillaConfig(
            "https://bugzilla.mozilla.org",
            context.args.bugzilla_api_key,
            allow_writes=context.config.write,
        )
        bz_client = bugzilla.Bugzilla(bz_config)
        hackbot_client = Hackbot(
            HackbotConfig(
                context.args.hackbot_url,
                context.args.hackbot_api_key,
                allow_writes=context.config.write,
            )
        )

        run(
            context.project,
            context.bq_client,
            bz_client,
            hackbot_client,
            context.args.autowebcompat_check_pending,
            updaters=[BugzillaUpdater(bz_client)],
            tasks=[ReproTask],
        )
