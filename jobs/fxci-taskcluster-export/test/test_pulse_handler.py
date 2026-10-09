from dataclasses import asdict
from typing import Any
from google.cloud.exceptions import NotFound
import pytest

import requests

from fxci_etl.pulse import handler as handler_module
from fxci_etl.pulse.handler import BigQueryHandler, Event, PerfherderHandler, storage


@pytest.fixture(autouse=True)
def mock_event_backup(mocker):
    storage_mock = mocker.MagicMock()
    storage_mock.bucket.return_value = mocker.MagicMock()
    blob_mock = mocker.MagicMock()
    blob_mock.download_as_string.side_effect = NotFound("")
    storage_mock.bucket.return_value.blob.return_value = blob_mock

    mocker.patch.object(storage, "Client", return_value=storage_mock)


@pytest.fixture
def run_bigquery(make_config):
    config = make_config()

    def inner(data: dict[str, Any]):
        event = Event.from_dict({"data": data})
        bq = BigQueryHandler(config)
        bq.process_event(event)
        return bq

    return inner


@pytest.fixture
def event():
    task_id = "abc"
    return {
        "runId": 0,
        "status": {
            "runs": [
                {
                    "reasonCreated": "just because",
                    "reasonResolved": "it finished",
                    "resolved": 1,
                    "scheduled": 2,
                    "state": "completed",
                },
            ],
            "schedulerId": "scheduler",
            "taskId": task_id,
            "taskGroupId": "group",
            "taskQueueId": "queue",
        },
        "task": {
            "tags": {
                "createdForUser": "user",
                "owned by": "user",
                "trust_domain": "domain",
                "worker-implementation": "worker",
            }
        },
    }


@pytest.fixture
def task_defined_event():
    return {
        "status": {
            "taskId": "abc",
            "provisionerId": "prov",
            "workerType": "worker-type",
            "taskQueueId": "prov/worker-type",
            "schedulerId": "sched",
            "projectId": "project",
            "taskGroupId": "abc",
            "deadline": "1970-01-01T00:00:00.000Z",
            "expires": "1970-01-01T00:00:00.000Z",
            "retriesLeft": 0,
            "state": "unscheduled",
            "runs": [],
        },
        "task": {
            "tags": {
                "createdForUser": "user",
                "owned by": "user",
                "trust_domain": "domain",
                "worker-implementation": "worker",
            },
        },
    }


def test_big_query_handler_no_run_id(run_bigquery):
    bq = run_bigquery({})
    assert bq.task_records == []
    assert bq.run_records == []
    assert bq.task_ids == set()


def test_big_query_handler_run_0(run_bigquery, event):
    bq = run_bigquery(event)
    assert len(bq.task_records) == 1
    assert len(bq.run_records) == 1
    assert bq.task_ids == set()

    tags = [t for t, v in asdict(bq.task_records[0])["tags"].items() if v is not None]
    assert len(event["task"]["tags"]) == len(tags)


def test_big_query_handler_run_1(run_bigquery, event):
    event["runId"] = 1
    event["status"]["runs"].append(event["status"]["runs"][0].copy())
    bq = run_bigquery(event)
    assert len(bq.task_records) == 0
    assert len(bq.run_records) == 1
    assert bq.task_ids == set()


def test_big_query_handler_task_defined(run_bigquery, task_defined_event):
    bq = run_bigquery(task_defined_event)
    assert len(bq.task_records) == 0
    assert len(bq.run_records) == 0
    assert bq.task_ids == {"abc"}


ROOT_URL = "https://firefox-ci-tc.services.mozilla.com/api/queue/v1"


@pytest.fixture
def perfherder_handler(make_config):
    return PerfherderHandler(make_config())


@pytest.fixture
def mock_taskcluster(mocker):
    """Serve canned responses from a dict mapping URL to JSON payload.

    A payload may be an int, in which case it is used as an error status code.
    """
    responses = {}

    def get(self, url, params=None, timeout=None):
        if params and "continuationToken" in params:
            url = f"{url}?continuationToken={params['continuationToken']}"
        payload = responses.get(url, 404)
        response = mocker.MagicMock()
        if isinstance(payload, int):
            response.status_code = payload
            response.raise_for_status.side_effect = requests.HTTPError(
                response=response
            )
        else:
            response.json.return_value = payload
        return response

    mocker.patch.object(requests.Session, "get", get)
    return responses


@pytest.fixture
def mock_loader(mocker):
    return mocker.patch.object(handler_module, "BigQueryLoader")


def test_perfherder_handler_filters_runs(perfherder_handler, event, task_defined_event):
    perfherder_handler.process_event(Event.from_dict({"data": task_defined_event}))
    perfherder_handler.process_event(Event.from_dict({"data": {}}))
    assert perfherder_handler.runs == set()

    event["status"]["runs"][0]["state"] = "exception"
    perfherder_handler.process_event(Event.from_dict({"data": event}))
    assert perfherder_handler.runs == set()

    for state in ("completed", "failed"):
        event["status"]["runs"][0]["state"] = state
        perfherder_handler.process_event(Event.from_dict({"data": event}))
    assert perfherder_handler.runs == {("abc", 0)}


@pytest.mark.parametrize(
    "name,expected",
    [
        ("public/test_info/perfherder-data.json", True),
        ("public/build/perfherder-data-building.json", True),
        ("public/test_info/perfherder-data.txt", False),
        ("public/logs/live_backing.log", False),
        ("private/perfherder-data.json", False),
    ],
)
def test_perfherder_is_perfherder_artifact(name, expected):
    assert PerfherderHandler.is_perfherder_artifact(name) == expected


def test_perfherder_handler_fetches_artifacts(
    perfherder_handler, mock_taskcluster, mock_loader
):
    build = {"framework": {"name": "build_metrics"}, "suites": []}
    talos = {"framework": {"name": "talos"}, "suites": [{"name": "ts_paint"}]}
    mock_taskcluster.update(
        {
            f"{ROOT_URL}/task/abc/runs/0/artifacts": {
                "artifacts": [
                    {"name": "public/logs/live_backing.log"},
                    {"name": "public/build/perfherder-data-building.json"},
                ],
                "continuationToken": "next",
            },
            f"{ROOT_URL}/task/abc/runs/0/artifacts?continuationToken=next": {
                "artifacts": [
                    {"name": "public/test_info/perfherder-data.json"},
                    {"name": "public/test_info/perfherder-data-bad.json"},
                ],
            },
            f"{ROOT_URL}/task/abc/runs/0/artifacts/public%2Fbuild%2Fperfherder-data-building.json": build,
            f"{ROOT_URL}/task/abc/runs/0/artifacts/public%2Ftest_info%2Fperfherder-data.json": talos,
            f"{ROOT_URL}/task/abc/runs/0/artifacts/public%2Ftest_info%2Fperfherder-data-bad.json": [],
            f"{ROOT_URL}/task/def/runs/1/artifacts": {"artifacts": []},
        }
    )
    perfherder_handler.runs = {("abc", 0), ("def", 1)}
    perfherder_handler.on_processing_complete()

    mock_loader.assert_called_once()
    records = mock_loader.return_value.insert.call_args[0][0]
    assert sorted(
        (r.task_id, r.run_id, r.artifact, r.framework, r.data) for r in records
    ) == [
        ("abc", 0, "public/build/perfherder-data-building.json", "build_metrics", build),
        ("abc", 0, "public/test_info/perfherder-data.json", "talos", talos),
    ]
    assert perfherder_handler.runs == set()
    perfherder_handler._runs_backup.upload_from_string.assert_called_with("[]")


def test_perfherder_handler_retries_server_errors(
    perfherder_handler, mock_taskcluster, mock_loader
):
    mock_taskcluster.update(
        {
            f"{ROOT_URL}/task/abc/runs/0/artifacts": 503,
            f"{ROOT_URL}/task/def/runs/0/artifacts": 404,
        }
    )
    perfherder_handler.runs = {("abc", 0), ("def", 0)}
    perfherder_handler.on_processing_complete()

    mock_loader.assert_not_called()
    assert perfherder_handler.runs == {("abc", 0)}
    perfherder_handler._runs_backup.upload_from_string.assert_called_with(
        '[["abc", 0]]'
    )
