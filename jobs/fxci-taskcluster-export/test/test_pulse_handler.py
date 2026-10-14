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
def run_worker_event(make_config):
    config = make_config()

    def inner(exchange: str, data: dict[str, Any]):
        event = Event(data=data, message=None, exchange=exchange)
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


@pytest.fixture
def worker_requested_event():
    return {
        "workerPoolId": "gecko-t/t-linux-docker-amd",
        "providerId": "google",
        "workerId": "worker-1",
        "workerGroup": "us-central1",
        "capacity": 1,
        "launchConfigId": "lc-1",
        "timestamp": "2026-09-24T16:22:00.000Z",
    }


@pytest.fixture
def worker_removed_event():
    return {
        "workerPoolId": "gecko-t/t-linux-docker-amd",
        "providerId": "google",
        "workerId": "worker-1",
        "workerGroup": "us-central1",
        "capacity": 1,
        "launchConfigId": "lc-1",
        "reason": "terminateAfter time exceeded",
        "timestamp": "2026-09-24T18:00:00.000Z",
    }


@pytest.fixture
def worker_pool_error_event():
    return {
        "workerPoolId": "gecko-t/t-linux-docker-amd",
        "providerId": "google",
        "workerGroup": "us-central1",
        "launchConfigId": "lc-1",
        "errorId": "err-1",
        "kind": "creation-error",
        "title": "Instance failed to start",
        "timestamp": "2026-09-24T16:23:00.000Z",
    }


def test_worker_requested_event(run_worker_event, worker_requested_event):
    bq = run_worker_event(
        "exchange/taskcluster-worker-manager/v1/worker-requested",
        worker_requested_event,
    )
    assert len(bq.worker_records) == 1
    record = bq.worker_records[0]
    assert record.event == "requested"
    assert record.worker_pool_id == worker_requested_event["workerPoolId"]
    assert record.provider_id == worker_requested_event["providerId"]
    assert record.worker_id == worker_requested_event["workerId"]
    assert record.worker_group == worker_requested_event["workerGroup"]
    assert record.capacity == worker_requested_event["capacity"]
    assert record.launch_config_id == worker_requested_event["launchConfigId"]
    assert record.timestamp == worker_requested_event["timestamp"]
    assert record.reason is None
    assert record.error_id is None
    assert record.error_kind is None
    assert record.error_title is None
    assert record.error_code is None


def test_worker_running_event(run_worker_event, worker_requested_event):
    bq = run_worker_event(
        "exchange/taskcluster-worker-manager/v1/worker-running",
        worker_requested_event,
    )
    assert bq.worker_records[0].event == "running"


def test_worker_stopped_event(run_worker_event, worker_requested_event):
    bq = run_worker_event(
        "exchange/taskcluster-worker-manager/v1/worker-stopped",
        worker_requested_event,
    )
    assert bq.worker_records[0].event == "stopped"
    assert bq.worker_records[0].reason is None


def test_worker_removed_event_keeps_reason(run_worker_event, worker_removed_event):
    bq = run_worker_event(
        "exchange/taskcluster-worker-manager/v1/worker-removed",
        worker_removed_event,
    )
    record = bq.worker_records[0]
    assert record.event == "removed"
    assert record.reason == worker_removed_event["reason"]


def test_worker_pool_error_event_missing_worker_id(
    run_worker_event, worker_pool_error_event
):
    bq = run_worker_event(
        "exchange/taskcluster-worker-manager/v1/worker-pool-error",
        worker_pool_error_event,
    )
    record = bq.worker_records[0]
    assert record.event == "pool-error"
    assert record.worker_id is None
    assert record.error_id == worker_pool_error_event["errorId"]
    assert record.error_kind == worker_pool_error_event["kind"]
    assert record.error_title == worker_pool_error_event["title"]
    assert record.error_code is None


def make_worker_record(run_worker_event, data, exchange="worker-requested"):
    bq = run_worker_event(
        f"exchange/taskcluster-worker-manager/v1/{exchange}",
        data,
    )
    return bq.worker_records[0]


def test_worker_event_natural_key_includes_worker_identity(
    run_worker_event, worker_requested_event
):
    record = make_worker_record(run_worker_event, worker_requested_event)
    assert record.natural_key() == (
        "requested",
        worker_requested_event["workerPoolId"],
        worker_requested_event["providerId"],
        worker_requested_event["workerGroup"],
        worker_requested_event["workerId"],
        worker_requested_event["timestamp"],
    )


def test_worker_event_natural_key_differs_by_worker_group(
    run_worker_event, worker_requested_event
):
    other_group = {**worker_requested_event, "workerGroup": "europe-west1"}
    first = make_worker_record(run_worker_event, worker_requested_event)
    second = make_worker_record(run_worker_event, other_group)
    assert first.natural_key() != second.natural_key()


def test_worker_event_natural_key_differs_by_provider_id(
    run_worker_event, worker_requested_event
):
    other_provider = {**worker_requested_event, "providerId": "aws"}
    first = make_worker_record(run_worker_event, worker_requested_event)
    second = make_worker_record(run_worker_event, other_provider)
    assert first.natural_key() != second.natural_key()


def test_worker_event_str_includes_worker_id(run_worker_event, worker_requested_event):
    record = make_worker_record(run_worker_event, worker_requested_event)
    assert str(record) == "requested for gecko-t/t-linux-docker-amd/worker-1"


def test_worker_pool_error_event_str_has_no_worker_id(
    run_worker_event, worker_pool_error_event
):
    record = make_worker_record(
        run_worker_event, worker_pool_error_event, "worker-pool-error"
    )
    assert str(record) == "pool-error for gecko-t/t-linux-docker-amd"


def test_task_event_with_no_exchange_still_yields_runs_and_tasks(run_bigquery, event):
    bq = run_bigquery(event)
    assert len(bq.task_records) == 1
    assert len(bq.run_records) == 1
    assert bq.worker_records == []


def test_event_round_trip_keeps_exchange():
    event = Event(
        data={"foo": "bar"},
        message=None,
        exchange="exchange/taskcluster-worker-manager/v1/worker-requested",
    )
    round_tripped = Event.from_dict(event.to_dict())
    assert round_tripped.exchange == event.exchange
    assert round_tripped.data == event.data
