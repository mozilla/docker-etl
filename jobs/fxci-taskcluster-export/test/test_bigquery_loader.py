from fxci_etl.loaders.bigquery import _insert_batches


def test_insert_batches_respect_record_count():
    records = [{"value": "x"} for _ in range(5)]

    batches = _insert_batches(records, max_count=2, max_bytes=1000)

    assert batches == [
        [{"value": "x"}, {"value": "x"}],
        [{"value": "x"}, {"value": "x"}],
        [{"value": "x"}],
    ]


def test_insert_batches_respect_serialized_size():
    records = [
        {"value": "x" * 30},
        {"value": "y" * 30},
        {"value": "z" * 5},
    ]

    batches = _insert_batches(records, max_count=100, max_bytes=65)

    assert batches == [
        [{"value": "x" * 30}],
        [{"value": "y" * 30}, {"value": "z" * 5}],
    ]


def test_insert_batches_allow_oversized_single_row():
    records = [
        {"value": "x" * 100},
        {"value": "y"},
    ]

    batches = _insert_batches(records, max_count=100, max_bytes=50)

    assert batches == [
        [{"value": "x" * 100}],
        [{"value": "y"}],
    ]
