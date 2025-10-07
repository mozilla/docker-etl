from dataclasses import asdict
from typing import Any
from google.cloud.exceptions import NotFound
import pytest

from fxci_etl.pulse.handler import BigQueryHandler, Event, storage


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
