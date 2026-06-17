from types import SimpleNamespace

import pytest

from fxci_etl.loaders.bigquery import BigQueryLoader
from fxci_etl.schemas import Runs


def _make_loader(chunk_size=5000, max_bytes=9_000_000, max_row_bytes=None):
    return SimpleNamespace(
        chunk_size=chunk_size,
        max_batch_bytes=max_bytes,
        max_row_bytes=max_row_bytes if max_row_bytes is not None else max_bytes,
        table_name="project.dataset.table",
    )


def _make_run(task_id, run_id):
    return Runs(
        submission_date="2024-01-01",
        task_id=task_id,
        run_id=run_id,
        reason_created="scheduled",
        reason_resolved="completed",
        resolved="2024-01-01T00:00:00Z",
        scheduled="2024-01-01T00:00:00Z",
        started=None,
        state="completed",
        worker_group=None,
        worker_id=None,
    )


@pytest.mark.parametrize(
    "inputs, expected_keys",
    [
        pytest.param(
            [("task1", 0), ("task1", 0), ("task2", 0)],
            [("task1", 0), ("task2", 0)],
            id="removes_exact_duplicates",
        ),
        pytest.param(
            [("task1", 0), ("task1", 1)],
            [("task1", 0), ("task1", 1)],
            id="keeps_different_run_ids",
        ),
        pytest.param(
            [("task2", 0), ("task1", 0), ("task2", 0)],
            [("task2", 0), ("task1", 0)],
            id="preserves_order",
        ),
    ],
)
def test_deduplicate(inputs, expected_keys):
    records = [_make_run(task_id, run_id) for task_id, run_id in inputs]
    loader = _make_loader()

    result = BigQueryLoader._deduplicate(loader, records)

    assert [(r.task_id, r.run_id) for r in result] == expected_keys


def test_chunk_batches_respect_record_count():
    records = [{"value": "x"} for _ in range(5)]
    loader = _make_loader(chunk_size=2, max_bytes=1000)

    batches = list(BigQueryLoader._chunk_batches(loader, records))

    assert batches == [
        [{"value": "x"}, {"value": "x"}],
        [{"value": "x"}, {"value": "x"}],
        [{"value": "x"}],
    ]


def test_chunk_batches_respect_serialized_size():
    records = [
        {"value": "x" * 30},
        {"value": "y" * 30},
        {"value": "z" * 5},
    ]
    loader = _make_loader(chunk_size=100, max_bytes=65)

    batches = list(BigQueryLoader._chunk_batches(loader, records))

    assert batches == [
        [{"value": "x" * 30}],
        [{"value": "y" * 30}, {"value": "z" * 5}],
    ]


def test_chunk_batches_skip_oversized_row():
    records = [
        {"value": "x" * 100},
        {"value": "y"},
    ]
    loader = _make_loader(chunk_size=100, max_bytes=1000, max_row_bytes=50)

    batches = list(BigQueryLoader._chunk_batches(loader, records))

    assert batches == [
        [{"value": "y"}],
    ]
