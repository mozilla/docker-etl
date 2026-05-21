from types import SimpleNamespace

from fxci_etl.loaders.bigquery import BigQueryLoader


def _make_loader(chunk_size, max_bytes, max_row_bytes=None):
    return SimpleNamespace(
        chunk_size=chunk_size,
        max_batch_bytes=max_bytes,
        max_row_bytes=max_row_bytes if max_row_bytes is not None else max_bytes,
    )


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
