import base64
from collections import defaultdict
from pathlib import Path
import json
from datetime import datetime
from typing import Mapping, Optional, Iterable, cast
from uuid import UUID
from unittest.mock import Mock, patch

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

    def poll_run(self, run_uuid: UUID) -> tuple[hackbot.RunDoc, bool]:
        if run_uuid in self.runs:
            return self.runs[run_uuid]
        # TODO this should really be a HTTPStatusError but we don't have a
        # request or response object
        raise httpx.HTTPError()

    def get_artifact_url(self, run_uuid: UUID, artifact_path: str) -> str:
        if run_uuid in self.artifact_urls:
            if artifact_path in self.artifact_urls[run_uuid]:
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
    script_url: Optional[str] = None,
    script_data: Optional[bytes] = None,
) -> tuple[autowebcompat.BugUpdate, dict[str, Json]]:
    """Run a repro task's Bugzilla update for the bug-1903487 fixture.

    If `script_url` is set it's added to the fixture's result as an artifact
    name, and `script_data` is what fetching it returns (None to simulate a
    failed fetch).
    """
    screenshot_url = "https://hackbot.test/screenshot.png"
    resolved_script_url = "https://hackbot.test/script.mjs"

    hackbot = MockHackbot()
    bq_service = MockBigQueryService()
    task = autowebcompat.ReproTask(hackbot, bq_service, {})
    updater = MockBugzillaUpdater()

    bug_data = load_data("bug-1903487.json")
    assert bug_data.bug is not None
    assert bug_data.hackbot_scheduled is not None
    assert bug_data.hackbot_completed is not None
    assert bug_data.hackbot_completed.summary is not None
    result = cast(
        dict[str, Json], bug_data.hackbot_completed.summary.findings["result"]
    )
    assert isinstance(result, dict)

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
    updates, result = run_repro_update()

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
    updates, result = run_repro_update(
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
        script_url="reproduction-nightly.mjs", script_data=None
    )

    file_names = [item.file_name for item in updates.add_attachments]
    assert file_names == [
        "autowebcompat-repro-steps.txt",
        "autowebcompat-repro-screenshot.png",
    ]
    assert (
        base64.b64decode(updates.add_attachments[0].data).decode("utf8")
        == result["steps"]
    )
