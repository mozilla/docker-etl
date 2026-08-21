import base64
import json
from collections import defaultdict
from collections.abc import Iterable, Mapping
from datetime import datetime
from pathlib import Path
from typing import Optional, cast
from unittest.mock import Mock, patch
from uuid import UUID

import httpx
from bugdantic import bugzilla
from pydantic import BaseModel

from webcompat_kb import hackbot
from webcompat_kb.etl import autowebcompat
from webcompat_kb.etl.autowebcompat import Json

DATA_PATH = Path(__file__).parent / "data"


class BugAPIData(BaseModel):
    bug: Optional[bugzilla.Bug] = None
    bug_update: Optional[bugzilla.BugUpdate] = None
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
        self.diagnosis_bugs: list[Json] = []

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

    def get_diagnosis_requested_bugs(
        self,
    ) -> Mapping[int, autowebcompat.DiagnosisBugInfo]:
        rv = {}
        for item in self.diagnosis_bugs:
            bug = autowebcompat.DiagnosisBugInfo.model_validate(item)
            rv[bug.number] = bug
        return rv


class MockBugzillaUpdater(autowebcompat.BugzillaUpdater):
    def __init__(self):
        super().__init__(Mock())
        self.bug_data: list[Json] = []

    def fetch_data(self) -> None:
        for bug_json in self.bug_data:
            bug = bugzilla.Bug.model_validate(bug_json)
            if bug.id in self.bug_ids:
                self.bug_updates[bug.id] = (
                    bug,
                    autowebcompat.BugUpdate(bug=bugzilla.BugUpdate(ids=[bug.id])),
                )


png_base64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABAQAAAAA3bvkkAAAACklEQVR4AWNgAAAAAgABc3UBGAAAAABJRU5ErkJggg=="


def run_repro_update(
    data_file: str,
    script_url: Optional[str] = None,
    script_data: Optional[bytes] = None,
) -> tuple[autowebcompat.BugUpdate, Optional[dict[str, Json]]]:
    """Run a repro task's Bugzilla update for a bug API fixture.

    :param data_file: name of the fixture file in the data directory to use.
    :param script_url: artifact name to add to the fixture's result as the
                       reproduction script.
    :param script_data: data returned when fetching `script_url`, or None to
                        simulate a failed fetch.
    :returns: the resulting bug update, along with the agent result from the
              fixture, if it has one.
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

    updates = updater.bug_updates[1903487][1]
    assert bug_data.bug_update is not None
    assert updates.bug.whiteboard == bug_data.bug_update.whiteboard
    assert updates.bug.cf_user_story == bug_data.bug_update.cf_user_story

    return updates, result


def test_repro_bugzilla_update() -> None:
    updates, result = run_repro_update("bug-1903487.json")
    assert result is not None

    assert updates.add_attachments[0].file_name == "autowebcompat-repro-steps.txt"
    assert (
        base64.b64decode(updates.add_attachments[0].data).decode("utf8")
        == result["steps"]
    )
    assert updates.add_attachments[1].file_name == "autowebcompat-repro-screenshot.png"
    assert updates.add_attachments[1].data == png_base64


def test_repro_bugzilla_update_script() -> None:
    """A fetched script is attached in place of the reproduction steps."""
    script_source = "import puppeteer from 'puppeteer';\n"
    updates, _ = run_repro_update(
        "bug-1903487.json",
        script_url="reproduction-nightly.mjs",
        script_data=script_source.encode("utf8"),
    )

    file_names = [item.file_name for item in updates.add_attachments]
    assert file_names == [
        "autowebcompat-repro-script.mjs",
        "autowebcompat-repro-screenshot.png",
    ]

    script_attachment = updates.add_attachments[0]
    assert script_attachment.content_type == "text/javascript"
    assert base64.b64decode(script_attachment.data).decode("utf8") == script_source
    assert script_attachment.comment is not None


def test_repro_bugzilla_update_script_fetch_failed() -> None:
    """If the script can't be fetched we fall back to the steps."""
    updates, result = run_repro_update(
        "bug-1903487.json", script_url="reproduction-nightly.mjs", script_data=None
    )
    assert result is not None

    file_names = [item.file_name for item in updates.add_attachments]
    assert file_names == [
        "autowebcompat-repro-steps.txt",
        "autowebcompat-repro-screenshot.png",
    ]
    assert (
        base64.b64decode(updates.add_attachments[0].data).decode("utf8")
        == result["steps"]
    )


def test_repro_bugzilla_update_error() -> None:
    """A run that errored out is recorded as a failed reproduction.

    The expected whiteboard and user story are in the fixture, and checked by
    run_repro_update.
    """
    updates, result = run_repro_update(data_file="bug-1903487-repro-error.json")

    assert result is None
    assert updates.bug.comment is None
    assert updates.add_attachments == []


def run_diagnosis_update(
    data_file: str,
    testcase_data: Optional[bytes] = None,
    result_overrides: Optional[dict[str, Json]] = None,
) -> tuple[autowebcompat.BugUpdate, Optional[dict[str, Json]]]:
    """Run a diagnosis task's Bugzilla update for a bug API fixture.

    :param data_file: name of the fixture file in the data directory to use.
    :param testcase_data: data returned when fetching the testcase artifact, or
                          None to simulate a failed fetch.
    :param result_overrides: values merged into the fixture's agent result
                             before the update runs.
    :returns: the resulting bug update, along with the agent result from the
              fixture, if it has one.
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

    return updater.bug_updates[1903487][1], result


def test_diagnosis_bugzilla_update() -> None:
    """A successful diagnosis sets status fields, comments, and attaches the testcase."""
    testcase_source = "<!doctype html><title>reduced</title>\n"
    updates, result = run_diagnosis_update(
        "bug-1903487-diagnosis.json", testcase_data=testcase_source.encode("utf8")
    )
    assert result is not None

    bug_data = load_data("bug-1903487-diagnosis.json")
    assert bug_data.bug_update is not None
    assert updates.bug.cf_user_story == bug_data.bug_update.cf_user_story
    assert updates.bug.whiteboard is None

    assert updates.bug.comment is not None
    assert cast(str, result["root_cause"]) in updates.bug.comment.body
    assert cast(str, result["evidence"]) in updates.bug.comment.body

    assert len(updates.add_attachments) == 1
    attachment = updates.add_attachments[0]
    assert attachment.file_name == "autowebcompat-diagnosis-testcase.html"
    assert attachment.content_type == "text/html"
    assert base64.b64decode(attachment.data).decode("utf8") == testcase_source


def test_diagnosis_bugzilla_update_testcase_fetch_failed() -> None:
    """A testcase that can't be fetched doesn't block the rest of the update."""
    updates, _ = run_diagnosis_update("bug-1903487-diagnosis.json", testcase_data=None)

    assert updates.add_attachments == []
    assert updates.bug.comment is not None
    assert updates.bug.cf_user_story is not None
    assert "autowebcompat-diagnosis-status:success" in updates.bug.cf_user_story


def test_diagnosis_bugzilla_update_no_testcase() -> None:
    """A diagnosis with no reduced testcase still comments and sets status."""
    updates, _ = run_diagnosis_update(
        "bug-1903487-diagnosis.json", result_overrides={"testcase_url": None}
    )

    assert updates.add_attachments == []
    assert updates.bug.comment is not None
    assert updates.bug.cf_user_story is not None
    assert "autowebcompat-diagnosis-status:success" in updates.bug.cf_user_story


def test_diagnosis_bugzilla_update_not_reproduced() -> None:
    """A run that couldn't reproduce records the failure reason and no comment."""
    updates, _ = run_diagnosis_update(
        "bug-1903487-diagnosis.json",
        result_overrides={
            "reproduced": False,
            "failure_reason": "blocked_captcha",
            "root_cause": None,
            "evidence": None,
            "testcase_url": None,
        },
    )

    assert updates.bug.whiteboard is None
    assert updates.bug.cf_user_story is not None
    assert "autowebcompat-diagnosis-status:failed" in updates.bug.cf_user_story
    assert "autowebcompat-diagnosis-reason:blocked_captcha" in updates.bug.cf_user_story
    assert updates.bug.comment is None
    assert updates.add_attachments == []


def test_diagnosis_bugzilla_update_error() -> None:
    """A run that errored out is recorded as a failed diagnosis."""
    data_file = "bug-1903487-diagnosis-error.json"
    updates, result = run_diagnosis_update(data_file=data_file)
    assert result is None

    bug_data = load_data(data_file)
    assert bug_data.bug_update is not None
    assert updates.bug.cf_user_story == bug_data.bug_update.cf_user_story
    assert updates.bug.whiteboard is None
    assert updates.bug.comment is None
    assert updates.add_attachments == []


def test_diagnosis_schedule_clears_flag() -> None:
    """Scheduling a run consumes the request token from the whiteboard."""
    hackbot_client = MockHackbot()
    bq_service = MockBigQueryService()
    bq_service.diagnosis_bugs.append(
        {"number": 1903487, "source_time": "2026-08-10T12:00:00"}
    )
    task = autowebcompat.DiagnosisTask(hackbot_client, bq_service, {})
    updater = MockBugzillaUpdater()
    updater.bug_data.append(
        {
            "id": 1903487,
            "whiteboard": "[autowebcompat:processed][autowebcompat:diagnose]",
            "cf_user_story": "",
        }
    )

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

    updates = updater.bug_updates[1903487][1]
    assert updates.bug.whiteboard == "[autowebcompat:processed]"
    assert updates.has_updates()


def test_diagnosis_schedule_skips_in_flight() -> None:
    """A bug with an incomplete run isn't dispatched again."""
    hackbot_client = MockHackbot()
    bq_service = MockBigQueryService()
    bq_service.diagnosis_bugs.append(
        {"number": 1903487, "source_time": "2026-08-10T12:00:00"}
    )
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
