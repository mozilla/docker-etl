import base64
import json
from collections import defaultdict
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Optional, cast
from unittest.mock import Mock, patch
from uuid import UUID

import httpx
import pytest
from bugdantic import bugzilla
from pydantic import BaseModel

from webcompat_kb import hackbot
from webcompat_kb.etl import autowebcompat
from webcompat_kb.etl.autowebcompat import Json

DATA_PATH = Path(__file__).parent / "data"


class BugAPIData(BaseModel):
    bug: Optional[bugzilla.Bug] = None
    bug_update: Optional[bugzilla.BugUpdate] = None
    site_report: Optional[autowebcompat.NewBugInfo] = None
    hackbot_scheduled: Optional[autowebcompat.ScheduledRun] = None
    hackbot_completed: Optional[autowebcompat.CompleteRun] = None


def load_data(filename: str) -> BugAPIData:
    data_file = DATA_PATH / filename

    with open(data_file) as f:
        bug_data = json.load(f)

    return BugAPIData.model_validate(bug_data)


def rundoc_from_bug_api(src: BugAPIData) -> autowebcompat.RunDoc:
    scheduled_run = src.hackbot_scheduled
    completed_run = src.hackbot_completed
    assert scheduled_run is not None
    assert completed_run is not None
    return autowebcompat.RunDoc(
        run_id=scheduled_run.run_id,
        agent=scheduled_run.agent,
        status=hackbot.RunStatus[completed_run.status],
        inputs=scheduled_run.request_data,
        created_at=completed_run.created_at,
        updated_at=completed_run.completed_at,
        execution_name=completed_run.execution_name,
        results_prefix=completed_run.results_prefix,
        summary=completed_run.summary,
        artifacts=completed_run.artifacts,
        error=completed_run.error,
    )


class MockHackbot(hackbot.Hackbot):
    def __init__(self):
        super().__init__(
            hackbot.HackbotConfig(base_url="https://hackbot.test/", allow_writes=True)
        )
        self.client = Mock()
        self.runs: dict[UUID, tuple[hackbot.RunDoc, bool]] = {}
        self.artifact_urls: dict[UUID, dict[str, str]] = defaultdict(dict)
        self.created: list[hackbot.CreateRequest] = []
        self.actions: dict[UUID, list[hackbot.RunAction]] = {}

    def create_run(self, request: hackbot.CreateRequest) -> hackbot.RunRef:
        self.created.append(request)
        return hackbot.RunRef(
            run_id=UUID(int=len(self.created)),
            agent=request.agent,
            status=hackbot.RunStatus.pending,
        )

    def poll_run(self, run_uuid: UUID) -> tuple[hackbot.RunDoc, bool]:
        if run_uuid in self.runs:
            return self.runs[run_uuid]
        # TODO this should really be a HTTPStatusError but we don't have a
        # request or response object
        raise httpx.HTTPError()

    def get_run_actions(self, run_uuid: UUID) -> list[hackbot.RunAction]:
        return self.actions.get(run_uuid, [])

    def get_artifact_url(self, run_uuid: UUID, artifact_path: str) -> str:
        if (
            run_uuid in self.artifact_urls
            and artifact_path in self.artifact_urls[run_uuid]
        ):
            return self.artifact_urls[run_uuid][artifact_path]
        # TODO this should really be a HTTPStatusError but we don't have a
        # request or response object
        raise httpx.HTTPError()


class MockBigQueryService(autowebcompat.BigQueryService):
    def __init__(self):
        self.bq_client = Mock()
        self.source_times = {}
        self.pending = []
        self.scheduled = []
        self.new_bugs = []
        self.diagnosis_bugs: list[autowebcompat.NewBugInfo] = []
        self.reproduce_bugs: list[autowebcompat.NewBugInfo] = []
        self.intervention_bugs: list[autowebcompat.NewBugInfo] = []

    def get_source_times(
        self,
    ) -> Mapping[tuple[str, str], Mapping[str, Optional[datetime]]]:
        return self.source_times

    def get_pending(self) -> list[UUID]:
        return [UUID(item["run_id"]) for item in self.pending]

    def get_scheduled_by_uuid(
        self, run_uuids: Iterable[UUID]
    ) -> Mapping[UUID, autowebcompat.ScheduledRun]:
        include_uuids = set(run_uuids)
        scheduled = {UUID(item["run_id"]): item for item in self.scheduled}
        return {
            key: autowebcompat.ScheduledRun.model_validate(value)
            for key, value in scheduled.items()
            if key in include_uuids
        }

    def get_scheduled_by_key(
        self,
        keys: Iterable[autowebcompat.RunKey],
    ) -> set[autowebcompat.RunKey]:
        rv = set()
        include_keys = set(keys)
        for item in self.scheduled:
            key = autowebcompat.RunKey(
                agent=item["agent"],
                task_name=item["task_name"],
                source_key=item["source_key"],
                run_key=item["run_key"],
            )
            if key in include_keys:
                rv.add(key)
        return rv

    def get_new_bugs(
        self, created_since: Optional[datetime]
    ) -> Mapping[int, autowebcompat.NewBugInfo]:
        rv = {}
        for item in self.new_bugs:
            new_bug = autowebcompat.NewBugInfo.model_validate(item)
            if not created_since or new_bug.creation_time > created_since:
                rv[new_bug.number] = new_bug
        return rv

    def get_bugs_with_whiteboard_token(
        self, whiteboard_token: str, include_extra_data: bool = True
    ) -> Mapping[int, autowebcompat.NewBugInfo]:
        if whiteboard_token == autowebcompat.DiagnosisTask.whiteboard_request_token:
            items = self.diagnosis_bugs
        elif whiteboard_token == autowebcompat.ReproTask.whiteboard_request_token:
            items = self.reproduce_bugs
        elif (
            whiteboard_token == autowebcompat.InterventionTask.whiteboard_request_token
        ):
            items = self.intervention_bugs
        else:
            raise ValueError(f"Unexpected whiteboard token {whiteboard_token}")
        return {bug.number: bug for bug in items if whiteboard_token in bug.whiteboard}


class MockBugzillaUpdater(autowebcompat.BugzillaUpdater):
    def __init__(self):
        super().__init__(Mock())
        self.bug_data: list[Json] = []

    def fetch_data(self) -> None:
        for bug_json in self.bug_data:
            bug = bugzilla.Bug.model_validate(bug_json)
            if bug.id in self.bug_ids:
                self.bug_updates[bug.id] = autowebcompat.BugUpdate(bug)


png_base64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABAQAAAAA3bvkkAAAACklEQVR4AWNgAAAAAgABc3UBGAAAAABJRU5ErkJggg=="


def run_repro_update(
    data_file: str,
    script_url: Optional[str] = None,
    script_data: Optional[bytes] = None,
) -> tuple[
    tuple[
        Optional[bugzilla.BugUpdate],
        Optional[bugzilla.CommentCreate],
        list[bugzilla.AttachmentCreate],
    ],
    Optional[dict[str, Json]],
]:
    """Run a repro task's Bugzilla update for a bug API fixture.

    :param data_file: name of the fixture file in the data directory to use.
    :param script_url: artifact name to add to the fixture's result as the
                       reproduction script.
    :param script_data: data returned when fetching `script_url`, or None to
                        simulate a failed fetch.
    :returns: the resulting (bug update, comment, attachments), along with the
              agent result from the fixture, if it has one.
    """
    screenshot_url = "https://hackbot.test/screenshot.png"
    resolved_script_url = "https://hackbot.test/script.mjs"

    hackbot = MockHackbot()
    bq_service = MockBigQueryService()
    task = autowebcompat.ReproTask(hackbot, bq_service, {})
    updater = MockBugzillaUpdater()

    bug_data = load_data(data_file)
    assert bug_data.bug is not None
    assert bug_data.hackbot_scheduled is not None
    assert bug_data.hackbot_completed is not None
    assert bug_data.hackbot_completed.summary is not None
    result = cast(
        Optional[dict[str, Json]],
        bug_data.hackbot_completed.summary.findings.get("result"),
    )

    if result is not None:
        artifact_urls = hackbot.artifact_urls[bug_data.hackbot_completed.run_id]
        artifact_urls[cast(str, result["screenshot_url"])] = screenshot_url
        if script_url is not None:
            result["script_url"] = script_url
            artifact_urls[script_url] = resolved_script_url

    run_doc = rundoc_from_bug_api(bug_data)
    updater.bug_data.append(bug_data.bug.model_dump())

    complete_runs = {
        run_doc.run_id: (
            bug_data.hackbot_scheduled,
            run_doc,
        )
    }
    task.take_completed_runs(complete_runs)

    assert complete_runs == {}
    task.configure_updater(updater)

    assert updater.include_fields == {"id", "whiteboard", "cf_user_story"}
    assert updater.bug_ids == {1903487}

    updater.fetch_data()

    def get_file(url: str, allowed_types: Optional[set[str]] = None) -> Optional[bytes]:
        if url == screenshot_url:
            return base64.b64decode(png_base64)
        if url == resolved_script_url:
            return script_data
        raise AssertionError(f"Unexpected fetch of {url}")

    with patch("webcompat_kb.etl.autowebcompat.try_get_file") as mock_get_file:
        mock_get_file.side_effect = get_file
        task.populate_updates(updater)

    updates = updater.bug_updates[1903487].get_updates()
    update = updates[0]
    assert update is not None
    assert bug_data.bug_update is not None
    assert update.whiteboard == bug_data.bug_update.whiteboard
    assert update.cf_user_story == bug_data.bug_update.cf_user_story

    return updates, result


def test_repro_bugzilla_update() -> None:
    (_, _, attachments), result = run_repro_update("bug-1903487.json")
    assert result is not None

    assert attachments[0].file_name == "autowebcompat-repro-steps.txt"
    assert base64.b64decode(attachments[0].data).decode("utf8") == result["steps"]
    assert attachments[1].file_name == "autowebcompat-repro-screenshot.png"
    assert attachments[1].data == png_base64


def test_repro_bugzilla_update_script() -> None:
    """A fetched script is attached in place of the reproduction steps."""
    script_source = "import puppeteer from 'puppeteer';\n"
    (_, _, attachments), _ = run_repro_update(
        "bug-1903487.json",
        script_url="reproduction-nightly.mjs",
        script_data=script_source.encode("utf8"),
    )

    file_names = [item.file_name for item in attachments]
    assert file_names == [
        "autowebcompat-repro-script.mjs",
        "autowebcompat-repro-screenshot.png",
    ]

    script_attachment = attachments[0]
    assert script_attachment.content_type == "text/javascript"
    assert base64.b64decode(script_attachment.data).decode("utf8") == script_source
    assert script_attachment.comment is not None


def test_repro_bugzilla_update_script_fetch_failed() -> None:
    """If the script can't be fetched we fall back to the steps."""
    (_, _, attachments), result = run_repro_update(
        "bug-1903487.json", script_url="reproduction-nightly.mjs", script_data=None
    )
    assert result is not None

    file_names = [item.file_name for item in attachments]
    assert file_names == [
        "autowebcompat-repro-steps.txt",
        "autowebcompat-repro-screenshot.png",
    ]
    assert base64.b64decode(attachments[0].data).decode("utf8") == result["steps"]


def test_repro_bugzilla_update_error() -> None:
    """A run that errored out is recorded as a failed reproduction.

    The expected whiteboard and user story are in the fixture, and checked by
    run_repro_update.
    """
    (_, comment, attachments), result = run_repro_update(
        data_file="bug-1903487-repro-error.json"
    )

    assert result is None
    assert comment is None
    assert attachments == []


@pytest.mark.parametrize(
    "data_file",
    ["bug-1903487-repro-rerun-error.json", "bug-1903487-repro-rerun-success.json"],
)
def test_repro_bugzilla_update_rerun(data_file: str) -> None:
    """A new result replaces the whiteboard and user story from an earlier run.

    The fixture bugs have data from a previous run with the opposite outcome;
    the expected whiteboard and user story contain only the new result, and are
    checked by run_repro_update.
    """
    run_repro_update(data_file)


def run_diagnosis_update(
    data_file: str,
    testcase_data: Optional[bytes] = None,
    result_overrides: Optional[dict[str, Json]] = None,
) -> tuple[
    tuple[
        Optional[bugzilla.BugUpdate],
        Optional[bugzilla.CommentCreate],
        list[bugzilla.AttachmentCreate],
    ],
    Optional[dict[str, Json]],
]:
    """Run a diagnosis task's Bugzilla update for a bug API fixture.

    :param data_file: name of the fixture file in the data directory to use.
    :param testcase_data: data returned when fetching the testcase artifact, or
                          None to simulate a failed fetch.
    :param result_overrides: values merged into the fixture's agent result
                             before the update runs.
    :returns: the resulting (bug update, comment, attachments), along with the
              agent result from the fixture, if it has one.
    """
    resolved_testcase_url = "https://hackbot.test/testcase.html"

    hackbot_client = MockHackbot()
    bq_service = MockBigQueryService()
    task = autowebcompat.DiagnosisTask(hackbot_client, bq_service, {})
    updater = MockBugzillaUpdater()

    bug_data = load_data(data_file)
    assert bug_data.bug is not None
    assert bug_data.hackbot_scheduled is not None
    assert bug_data.hackbot_completed is not None
    assert bug_data.hackbot_completed.summary is not None
    result = cast(
        Optional[dict[str, Json]],
        bug_data.hackbot_completed.summary.findings.get("result"),
    )

    if result is not None:
        if result_overrides:
            result.update(result_overrides)

        if result.get("testcase_url"):
            artifact_urls = hackbot_client.artifact_urls[
                bug_data.hackbot_completed.run_id
            ]
            artifact_urls[cast(str, result["testcase_url"])] = resolved_testcase_url

    run_doc = rundoc_from_bug_api(bug_data)
    updater.bug_data.append(bug_data.bug.model_dump())

    complete_runs = {run_doc.run_id: (bug_data.hackbot_scheduled, run_doc)}
    task.take_completed_runs(complete_runs)
    assert complete_runs == {}

    task.configure_updater(updater)
    assert updater.include_fields == {"id", "whiteboard", "cf_user_story"}
    assert updater.bug_ids == {1903487}

    updater.fetch_data()

    def get_file(url: str, allowed_types: Optional[set[str]] = None) -> Optional[bytes]:
        if url == resolved_testcase_url:
            return testcase_data
        raise AssertionError(f"Unexpected fetch of {url}")

    with patch("webcompat_kb.etl.autowebcompat.try_get_file") as mock_get_file:
        mock_get_file.side_effect = get_file
        task.populate_updates(updater)

    return updater.bug_updates[1903487].get_updates(), result


def test_diagnosis_bugzilla_update() -> None:
    """A successful diagnosis sets status fields, comments, and attaches the testcase."""
    testcase_source = "<!doctype html><title>reduced</title>\n"
    (update, comment, attachments), result = run_diagnosis_update(
        "bug-1903487-diagnosis.json", testcase_data=testcase_source.encode("utf8")
    )
    assert update is not None
    assert result is not None

    bug_data = load_data("bug-1903487-diagnosis.json")
    assert bug_data.bug_update is not None
    assert update.cf_user_story is not None
    assert update.cf_user_story == bug_data.bug_update.cf_user_story
    assert update.whiteboard is None

    assert comment is not None
    assert cast(str, result["root_cause"]) in comment.comment
    assert cast(str, result["evidence"]) in comment.comment

    assert len(attachments) == 1
    attachment = attachments[0]
    assert attachment.file_name == "autowebcompat-diagnosis-testcase.html"
    assert attachment.content_type == "text/html"
    assert base64.b64decode(attachment.data).decode("utf8") == testcase_source
    assert "autowebcompat-diagnosis-reason" not in update.cf_user_story


def test_diagnosis_bugzilla_update_testcase_fetch_failed() -> None:
    """A testcase that can't be fetched doesn't block the rest of the update."""
    (update, comment, attachments), _ = run_diagnosis_update(
        "bug-1903487-diagnosis.json", testcase_data=None
    )
    assert update is not None

    assert attachments == []
    assert comment is not None
    assert update.cf_user_story is not None
    assert "autowebcompat-diagnosis-status:success" in update.cf_user_story


def test_diagnosis_bugzilla_update_no_testcase() -> None:
    """A diagnosis with no reduced testcase still comments and sets status."""
    (update, comment, attachments), _ = run_diagnosis_update(
        "bug-1903487-diagnosis.json", result_overrides={"testcase_url": None}
    )
    assert update is not None

    assert attachments == []
    assert comment is not None
    assert update.cf_user_story is not None
    assert "autowebcompat-diagnosis-status:success" in update.cf_user_story


def test_diagnosis_bugzilla_update_not_reproduced() -> None:
    """A run that couldn't reproduce records the failure reason and no comment."""
    (update, comment, attachments), _ = run_diagnosis_update(
        "bug-1903487-diagnosis.json",
        result_overrides={
            "reproduced": False,
            "failure_reason": "blocked_captcha",
            "root_cause": None,
            "evidence": None,
            "testcase_url": None,
        },
    )
    assert update is not None

    assert update.whiteboard is None
    assert update.cf_user_story is not None
    assert "autowebcompat-diagnosis-status:failed" in update.cf_user_story
    assert "autowebcompat-diagnosis-reason:blocked_captcha" in update.cf_user_story
    assert comment is None
    assert attachments == []


def test_diagnosis_bugzilla_update_error() -> None:
    """A run that errored out is recorded as a failed diagnosis."""
    data_file = "bug-1903487-diagnosis-error.json"
    (update, comment, attachments), result = run_diagnosis_update(data_file=data_file)
    assert update is not None
    assert result is None

    bug_data = load_data(data_file)
    assert bug_data.bug_update is not None
    assert update.cf_user_story == bug_data.bug_update.cf_user_story
    assert update.whiteboard is None
    assert comment is None
    assert attachments == []


def test_diagnosis_schedule_updates_flag() -> None:
    """Scheduling a run consumes the request token from the whiteboard."""
    bug_data = load_data("bug-1903487-diagnosis-request.json")
    assert bug_data.bug is not None
    assert bug_data.bug_update is not None
    assert bug_data.site_report is not None

    hackbot_client = MockHackbot()
    bq_service = MockBigQueryService()
    bq_service.diagnosis_bugs.append(bug_data.site_report)
    task = autowebcompat.DiagnosisTask(hackbot_client, bq_service, {})
    updater = MockBugzillaUpdater()
    updater.bug_data.append(bug_data.bug.model_dump())

    scheduled = task.create_new()
    assert list(scheduled.keys()) == ["bugzilla:diagnose-flag"]
    assert len(hackbot_client.created) == 1
    request = hackbot_client.created[0]
    assert isinstance(request, autowebcompat.AutowebcompatDiagnosisRequest)
    assert request.agent == "autowebcompat-diagnosis"
    assert request.bug_id == 1903487

    task.configure_updater(updater)
    assert updater.bug_ids == {1903487}
    updater.fetch_data()
    task.populate_updates(updater)

    update, _, _ = updater.bug_updates[1903487].get_updates()
    assert update is not None
    assert update.whiteboard == bug_data.bug_update.whiteboard


def test_diagnosis_schedule_skips_in_flight() -> None:
    """A bug with an incomplete run isn't dispatched again."""
    bug_data = load_data("bug-1903487-diagnosis-request.json")
    assert bug_data.site_report is not None

    hackbot_client = MockHackbot()
    bq_service = MockBigQueryService()
    bq_service.diagnosis_bugs.append(bug_data.site_report)
    bq_service.scheduled.append(
        {
            "agent": "autowebcompat-diagnosis",
            "task_name": "diagnosis",
            "source_key": "bugzilla:diagnose-flag",
            "run_key": "1903487",
        }
    )
    task = autowebcompat.DiagnosisTask(hackbot_client, bq_service, {})

    assert task.create_new() == {}
    assert hackbot_client.created == []


def test_repro_schedule_whiteboard_request() -> None:
    """The reproduce token schedules a run on a bug that's already processed."""
    bug_data = load_data("bug-1903487-repro-request.json")
    assert bug_data.bug is not None
    assert bug_data.bug_update is not None
    assert bug_data.site_report is not None

    hackbot_client = MockHackbot()
    bq_service = MockBigQueryService()
    bq_service.reproduce_bugs.append(bug_data.site_report)
    task = autowebcompat.ReproTask(hackbot_client, bq_service, {})
    updater = MockBugzillaUpdater()
    updater.bug_data.append(bug_data.bug.model_dump())

    scheduled = task.create_new()
    assert list(scheduled.keys()) == ["bugzilla:reproduce-flag"]
    assert len(hackbot_client.created) == 1
    request = hackbot_client.created[0]
    assert isinstance(request, autowebcompat.AutowebcompatReproRequest)
    assert request.agent == "autowebcompat-repro"
    assert json.loads(request.bug_data)["number"] == 1903487

    task.configure_updater(updater)
    assert updater.bug_ids == {1903487}
    updater.fetch_data()
    task.populate_updates(updater)

    update, _, _ = updater.bug_updates[1903487].get_updates()
    assert update is not None
    assert update.whiteboard == bug_data.bug_update.whiteboard


def test_repro_schedule_skips_in_flight() -> None:
    """A bug with an incomplete repro run isn't dispatched again."""
    bug_data = load_data("bug-1903487-repro-request.json")
    assert bug_data.site_report is not None

    hackbot_client = MockHackbot()
    bq_service = MockBigQueryService()
    bq_service.reproduce_bugs.append(bug_data.site_report)
    bq_service.scheduled.append(
        {
            "agent": "autowebcompat-repro",
            "task_name": "repro",
            "source_key": "bugzilla:reproduce-flag",
            "run_key": "1903487",
        }
    )
    task = autowebcompat.ReproTask(hackbot_client, bq_service, {})

    assert task.create_new() == {}
    assert hackbot_client.created == []


def test_intervention_schedule_updates_flag() -> None:
    """Scheduling a run consumes the request token from the whiteboard."""
    bug_data = load_data("bug-2000604-intervention-request.json")
    assert bug_data.bug is not None
    assert bug_data.bug_update is not None
    assert bug_data.site_report is not None

    hackbot_client = MockHackbot()
    bq_service = MockBigQueryService()
    bq_service.intervention_bugs.append(bug_data.site_report)
    task = autowebcompat.InterventionTask(hackbot_client, bq_service, {})
    updater = MockBugzillaUpdater()
    updater.bug_data.append(bug_data.bug.model_dump())

    scheduled = task.create_new()
    assert list(scheduled.keys()) == ["bugzilla:intervention-flag"]
    assert len(hackbot_client.created) == 1
    request = hackbot_client.created[0]
    assert isinstance(request, autowebcompat.AutowebcompatInterventionRequest)
    assert request.agent == "autowebcompat-intervention"
    assert request.bug_id == 2000604

    task.configure_updater(updater)
    assert updater.bug_ids == {2000604}
    updater.fetch_data()
    task.populate_updates(updater)

    update, _, _ = updater.bug_updates[2000604].get_updates()
    assert update is not None
    assert update.whiteboard == bug_data.bug_update.whiteboard


def test_intervention_schedule_skips_in_flight() -> None:
    """A bug with an incomplete intervention run isn't dispatched again."""
    bug_data = load_data("bug-2000604-intervention-request.json")
    assert bug_data.site_report is not None

    hackbot_client = MockHackbot()
    bq_service = MockBigQueryService()
    bq_service.intervention_bugs.append(bug_data.site_report)
    bq_service.scheduled.append(
        {
            "agent": "autowebcompat-intervention",
            "task_name": "intervention",
            "source_key": "bugzilla:intervention-flag",
            "run_key": "2000604",
        }
    )
    task = autowebcompat.InterventionTask(hackbot_client, bq_service, {})

    assert task.create_new() == {}
    assert hackbot_client.created == []


def run_intervention_update(
    data_file: str,
    result_overrides: Optional[dict[str, Json]] = None,
) -> tuple[
    tuple[
        Optional[bugzilla.BugUpdate],
        Optional[bugzilla.CommentCreate],
        list[bugzilla.AttachmentCreate],
    ],
    Optional[dict[str, Json]],
]:

    bug_data = load_data(data_file)
    assert bug_data.bug is not None
    assert bug_data.hackbot_scheduled is not None
    assert bug_data.hackbot_completed is not None
    summary = bug_data.hackbot_completed.summary
    result = (
        cast(Optional[dict[str, Json]], summary.findings.get("result"))
        if summary is not None
        else None
    )
    if result is not None and result_overrides:
        result.update(result_overrides)

    task = autowebcompat.InterventionTask(MockHackbot(), MockBigQueryService(), {})
    updater = MockBugzillaUpdater()
    updater.bug_data.append(bug_data.bug.model_dump())

    run_doc = rundoc_from_bug_api(bug_data)
    complete_runs = {run_doc.run_id: (bug_data.hackbot_scheduled, run_doc)}
    task.take_completed_runs(complete_runs)
    assert complete_runs == {}

    task.configure_updater(updater)
    assert updater.include_fields == {"id", "whiteboard", "cf_user_story"}
    assert updater.bug_ids == {2000604}

    updater.fetch_data()
    task.populate_updates(updater)

    updates = updater.bug_updates[2000604].get_updates()
    update = updates[0]
    assert update is not None
    assert bug_data.bug_update is not None
    assert update.whiteboard == bug_data.bug_update.whiteboard
    if not result_overrides:
        assert update.cf_user_story == bug_data.bug_update.cf_user_story

    return updates, result


def test_intervention_bugzilla_update() -> None:
    """A verified intervention records success and clears the in-progress token.

    The expected whiteboard and user story are in the fixture, and checked by
    run_intervention_update.
    """
    (_, comment, attachments), result = run_intervention_update(
        "bug-2000604-intervention.json"
    )
    assert result is not None
    assert comment is None
    assert attachments == []


def test_intervention_bugzilla_update_failed() -> None:
    """A run that didn't deliver an intervention records the failure reason."""
    (update, comment, attachments), _ = run_intervention_update(
        "bug-2000604-intervention.json",
        result_overrides={
            "wrote_intervention": False,
            "fixed_with_intervention": False,
            "build_succeeded": False,
            "failure_reason": "no_puppeteer_script",
        },
    )
    assert update is not None
    assert update.cf_user_story is not None
    assert "autowebcompat-intervention-status:failed" in update.cf_user_story
    assert (
        "autowebcompat-intervention-reason:no_puppeteer_script" in update.cf_user_story
    )
    assert comment is None
    assert attachments == []


def test_intervention_bugzilla_update_error() -> None:
    """A run that errored out is recorded as a failed intervention.

    The expected whiteboard and user story are in the fixture, and checked by
    run_intervention_update.
    """
    (_, comment, attachments), result = run_intervention_update(
        "bug-2000604-intervention-error.json"
    )
    assert result is None
    assert comment is None
    assert attachments == []


def test_intervention_bugzilla_update_no_summary() -> None:
    """A run that failed without writing a summary is recorded as a failed
    intervention, rather than leaving the bug in progress.

    The expected whiteboard and user story are in the fixture, and checked by
    run_intervention_update.
    """
    (_, comment, attachments), result = run_intervention_update(
        "bug-2000604-intervention-no-summary.json"
    )
    assert result is None
    assert comment is None
    assert attachments == []


def submit_patch_action(status: str) -> hackbot.RunAction:
    return hackbot.RunAction(
        idx=0,
        type="phabricator.submit_patch",
        params={"bug_id": 2000604},
        status=status,
        result={"revision_id": 271234, "url": "https://phabricator.test/D271234"}
        if status == "applied"
        else None,
    )


@pytest.mark.parametrize(
    "data_file,action_count",
    [
        ("bug-2000604-intervention.json", 1),
        ("bug-2000604-intervention-error.json", 0),
        ("bug-2000604-intervention-no-summary.json", 0),
    ],
)
def test_complete_run_action_count(data_file: str, action_count: int) -> None:
    """Completed runs record how many actions the agent recorded, which aren't
    stored in the summary."""
    row = autowebcompat.CompleteRun.from_rundoc(
        rundoc_from_bug_api(load_data(data_file))
    ).to_json()

    assert row["action_count"] == action_count
    if row["summary"] is not None:
        assert "actions" not in cast(dict[str, Json], row["summary"])


def test_run_actions_to_json() -> None:
    """Actions are stored as hackbot reports them, with timezone-free datetimes."""
    applied_action = submit_patch_action("applied").model_copy(
        update={"applied_at": datetime(2026, 10, 14, 15, 4, 31, tzinfo=UTC)}
    )
    row = autowebcompat.RunActions(
        run_id=UUID(int=1),
        actions=[applied_action],
    ).to_json()

    assert row == {
        "run_id": str(UUID(int=1)),
        "actions": [
            {
                "idx": 0,
                "type": "phabricator.submit_patch",
                "params": {"bug_id": 2000604},
                "ref": None,
                "status": "applied",
                "result": {
                    "revision_id": 271234,
                    "url": "https://phabricator.test/D271234",
                },
                "error": None,
                "applied_at": "2026-10-14T15:04:31",
            }
        ],
    }


@pytest.mark.parametrize(
    "statuses,action_count,stored",
    [
        (["applied", "failed"], 2, True),
        (["applied", "pending"], 2, False),
        (["applied"], 2, False),
        ([], 1, False),
    ],
)
def test_fetch_actions(statuses: list[str], action_count: int, stored: bool) -> None:
    """Actions are only stored once all of them are recorded and applied (or failed)."""
    run = autowebcompat.RunNeedingActions(
        run_id=UUID(int=1),
        action_count=action_count,
    )
    hackbot_client = MockHackbot()
    hackbot_client.actions[run.run_id] = [
        submit_patch_action(status).model_copy(update={"idx": idx})
        for idx, status in enumerate(statuses)
    ]

    rows = autowebcompat.fetch_actions(hackbot_client, [run])

    assert bool(rows) == stored
    if stored:
        assert [action.status for action in rows[0].actions] == statuses


def test_fetch_actions_failed() -> None:
    """A failed fetch stores nothing, so it's retried on the next run."""
    run = autowebcompat.RunNeedingActions(
        run_id=UUID(int=1),
        action_count=1,
    )
    hackbot_client = MockHackbot()
    with patch.object(
        hackbot_client, "get_run_actions", side_effect=httpx.HTTPError("gone")
    ):
        assert autowebcompat.fetch_actions(hackbot_client, [run]) == []
