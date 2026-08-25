"""Where a run's rows and blobs go, and that the columns written are the columns declared."""

import datetime
import json
import pathlib

from highwind import output_writing, units
from highwind.discovery import Experiment

AS_OF = datetime.date(2026, 8, 1)

EXPERIMENT = Experiment(
    slug="an-experiment",
    start_date=datetime.date(2026, 7, 1),
    end_date=None,
    reference_branch="control",
    treatment_branches=("treatment",),
    unit=units.resolve("firefox_desktop", "normandy_id"),
)

CONFIDENT_CELL = dict(
    metric="active_hours",
    window="cumu:1",
    window_kind="cumulative",
    window_start=0,
    window_end=6,
    branch="treatment",
    reference_branch="control",
    state="confident",
    n_reference=400,
    n_treatment=400,
    point=10.0,
    lower=4.0,
    upper=16.0,
    theta=0.5,
)

# The state that makes an inferred schema unusable: no point, lower, upper or theta at all.
ERROR_CELL = dict(
    metric="active_hours",
    window="cumu:2",
    window_kind="cumulative",
    window_start=0,
    window_end=13,
    branch="treatment",
    reference_branch="control",
    state="error",
    error="RuntimeError: boom",
)

AGGREGATES = dict(n=400, sum=400.0, sumsq=800.0, pre_sum=400.0, pre_sumsq=800.0, xp=600.0)


class FakeLoadJob:
    def result(self):
        return None


class RecordingClient:
    """Records what would have been created and loaded, in place of a BigQuery client."""

    def __init__(self):
        self.created = []
        self.loads = []

    def create_table(self, table, exists_ok=False):
        self.created.append((table, exists_ok))
        return table

    def load_table_from_json(self, rows, target, job_config=None):
        self.loads.append((target, rows, job_config))
        return FakeLoadJob()


class FakeBlob:
    def __init__(self, bucket_name, key):
        self.bucket_name = bucket_name
        self.key = key


class FakeBucket:
    def __init__(self, name):
        self.name = name

    def blob(self, key):
        return FakeBlob(self.name, key)


class FakeStorage:
    def bucket(self, name):
        return FakeBucket(name)


def write_one_run(client, outputs=None):
    return output_writing.write_tables(
        client,
        AS_OF,
        {"an-experiment": [CONFIDENT_CELL, ERROR_CELL]},
        {"an-experiment": {("active_hours", "cumu:1", "control"): AGGREGATES}},
        outputs or output_writing.Outputs(),
    )


def test_state_counts_tallies_the_states_a_run_produced():
    results = [{"state": "confident"}, {"state": "forming"}, {"state": "confident"}]

    assert output_writing.state_counts(results) == {"confident": 2, "forming": 1}


def test_state_counts_of_nothing_is_empty():
    assert output_writing.state_counts([]) == {}


def test_a_prefix_with_a_path_component_becomes_a_bucket_and_a_key():
    blob = output_writing.blob_for(FakeStorage(), "an-experiment", "gs://mozanalysis/highwind")

    assert blob.bucket_name == "mozanalysis"
    assert blob.key == "highwind/an-experiment.json"


def test_a_bare_bucket_prefix_puts_the_blob_at_the_root():
    blob = output_writing.blob_for(FakeStorage(), "an-experiment", "gs://mozanalysis")

    assert blob.bucket_name == "mozanalysis"
    assert blob.key == "an-experiment.json"


def test_a_nested_prefix_keeps_every_path_component():
    blob = output_writing.blob_for(FakeStorage(), "an-experiment", "gs://a-bucket/one/two")

    assert blob.bucket_name == "a-bucket"
    assert blob.key == "one/two/an-experiment.json"


def test_each_table_is_created_partitioned_on_the_run_date_before_anything_is_loaded():
    client = RecordingClient()

    write_one_run(client)

    assert [table.time_partitioning.field for table, _ in client.created] == [
        output_writing.PARTITION_FIELD,
        output_writing.PARTITION_FIELD,
    ]
    assert all(exists_ok for _, exists_ok in client.created)


def test_a_run_writes_one_partition_of_each_table():
    client = RecordingClient()

    written = write_one_run(client)

    assert written == (2, 1)
    assert [target for target, _, _ in client.loads] == [
        f"{output_writing.RESULTS_TABLE}$20260801",
        f"{output_writing.SUFFICIENT_STATS_TABLE}$20260801",
    ]
    assert all(
        config.write_disposition == "WRITE_TRUNCATE" for _, _, config in client.loads
    )


def test_the_schema_is_declared_rather_than_inferred_from_the_rows():
    client = RecordingClient()

    write_one_run(client)

    for _, rows, config in client.loads:
        assert config.autodetect is False
        declared = {field.name for field in config.schema}
        for row in rows:
            assert set(row) <= declared

    # An error cell carries no interval at all, so which columns the rows contain varies with the
    # day's mix of states while the declared schema does not.
    results_rows, results_config = client.loads[0][1], client.loads[0][2]
    assert {"point", "lower", "upper", "theta"} <= {
        field.name for field in results_config.schema
    }
    assert not any("point" in row for row in results_rows if row["state"] == "error")


def test_every_row_carries_the_run_date_the_slug_and_the_pipeline_version():
    client = RecordingClient()

    write_one_run(client)
    results_rows = client.loads[0][1]
    stats_rows = client.loads[1][1]

    assert all(row["as_of_date"] == "2026-08-01" for row in results_rows + stats_rows)
    assert all(row["slug"] == "an-experiment" for row in results_rows + stats_rows)
    assert all(
        row["pipeline_version"] == output_writing.PIPELINE_VERSION for row in results_rows
    )


def test_the_aggregates_are_keyed_out_to_columns_the_statistics_can_be_joined_on():
    client = RecordingClient()

    write_one_run(client)
    stats_row = client.loads[1][1][0]

    assert stats_row["metric"] == "active_hours"
    assert stats_row["window"] == "cumu:1"
    assert stats_row["branch"] == "control"
    assert {key: stats_row[key] for key in AGGREGATES} == AGGREGATES


def test_a_run_with_no_rows_leaves_the_partition_alone():
    client = RecordingClient()

    written = output_writing.write_tables(
        client, AS_OF, {}, {}, output_writing.Outputs()
    )

    assert written == (0, 0)
    assert client.loads == []


def test_a_local_run_writes_the_experiments_json_to_a_directory(tmp_path):
    outputs = output_writing.Outputs(local_blob_dir=str(tmp_path / "out"))

    path = output_writing.write_blob(
        None, EXPERIMENT, AS_OF, [CONFIDENT_CELL, ERROR_CELL], outputs
    )
    payload = json.loads(pathlib.Path(path).read_text())

    assert payload["metrics_meta"]["slug"] == "an-experiment"
    assert payload["metrics_meta"]["as_of_date"] == "2026-08-01"
    assert payload["metrics_meta"]["pipeline_version"] == output_writing.PIPELINE_VERSION
    assert payload["metrics_meta"]["branches"] == ["control", "treatment"]
    assert payload["statistics"] == [CONFIDENT_CELL, ERROR_CELL]
    assert payload["errors"] == [ERROR_CELL]
