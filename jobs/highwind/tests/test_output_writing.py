"""Where a run's rows and blobs go, and that the columns written are the columns declared."""

import datetime
import json
import logging
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

AGGREGATES = dict(
    n=400,
    sum=400.0,
    sum_squares=800.0,
    pre_sum=400.0,
    pre_sum_squares=800.0,
    sum_x_pre=600.0,
)


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


def log_rows_of(emit, as_of=AS_OF):
    """The rows a run log makes of whatever `emit` logs through a logger of its own."""
    run_log = output_writing.RunLog(as_of)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(run_log)
    try:
        emit(logger)
    finally:
        logger.removeHandler(run_log)
    return run_log.rows


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
    assert blob.key == "highwind/an_experiment.json"


def test_a_bare_bucket_prefix_puts_the_blob_at_the_root():
    blob = output_writing.blob_for(FakeStorage(), "an-experiment", "gs://mozanalysis")

    assert blob.bucket_name == "mozanalysis"
    assert blob.key == "an_experiment.json"


def test_a_nested_prefix_keeps_every_path_component():
    blob = output_writing.blob_for(FakeStorage(), "an-experiment", "gs://a-bucket/one/two")

    assert blob.bucket_name == "a-bucket"
    assert blob.key == "one/two/an_experiment.json"


def test_an_object_is_named_with_underscores_wherever_its_slug_has_hyphens():
    # The slug is the key the ingest looks an experiment up by, so only the separator changes.
    assert output_writing.blob_name("new-tab-tab-groups-promo-151") == (
        "new_tab_tab_groups_promo_151.json"
    )
    assert output_writing.blob_name("already_underscored") == "already_underscored.json"


def test_each_table_is_created_partitioned_on_the_run_date_before_anything_is_loaded():
    client = RecordingClient()

    write_one_run(client)

    assert [table.time_partitioning.field for table, _ in client.created] == [
        output_writing.PARTITION_FIELD,
        output_writing.PARTITION_FIELD,
    ]
    assert all(exists_ok for _, exists_ok in client.created)


def test_each_table_is_clustered_so_one_experiment_can_be_read_without_the_partition():
    client = RecordingClient()

    write_one_run(client)

    assert [table.clustering_fields for table, _ in client.created] == [
        output_writing.CLUSTERING_FIELDS,
        output_writing.CLUSTERING_FIELDS,
    ]


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
    assert all(
        row["experiment_slug"] == "an-experiment"
        for row in results_rows + stats_rows
    )
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


def test_a_logged_record_becomes_a_row_of_exactly_the_columns_declared():
    rows = log_rows_of(lambda logger: logger.info("the run started"))

    assert len(rows) == 1
    assert set(rows[0]) == {field.name for field in output_writing.LOG_SCHEMA}
    assert rows[0]["as_of_date"] == "2026-08-01"
    assert rows[0]["log_level"] == "INFO"
    assert rows[0]["message"] == "the run started"
    assert rows[0]["source"] == output_writing.LOG_SOURCE
    assert rows[0]["exception"] is None
    assert rows[0]["exception_type"] is None


def test_a_failure_is_recorded_with_its_type_and_the_traceback_of_the_line_that_raised():
    def emit(logger):
        try:
            raise RuntimeError("the query failed")
        except RuntimeError:
            logger.exception("an experiment failed")

    rows = log_rows_of(emit)

    assert rows[0]["log_level"] == "ERROR"
    assert rows[0]["exception_type"] == "RuntimeError"
    assert "the query failed" in rows[0]["exception"]
    assert 'raise RuntimeError("the query failed")' in rows[0]["exception"]


def test_a_record_asked_for_an_exception_with_none_in_flight_carries_none():
    rows = log_rows_of(lambda logger: logger.error("nothing raised", exc_info=True))

    assert rows[0]["exception"] is None
    assert rows[0]["exception_type"] is None


def test_what_a_record_is_about_becomes_the_columns_it_can_be_looked_up_by():
    rows = log_rows_of(
        lambda logger: logger.warning(
            "worth a look",
            extra={"experiment_slug": "an-experiment", "metric": "active_hours"},
        )
    )

    assert rows[0]["experiment_slug"] == "an-experiment"
    assert rows[0]["metric"] == "active_hours"
    assert rows[0]["log_level"] == "WARNING"


def test_the_columns_this_job_does_not_fill_yet_are_declared_and_left_empty():
    # Declared so that analysing on another basis, or in segments, is a value to write rather than
    # a column to add and every reader of the table to change.
    rows = log_rows_of(lambda logger: logger.info("the run started"))

    assert rows[0]["analysis_basis"] is None
    assert rows[0]["segment"] is None
    assert rows[0]["analysis_period"] is None


def test_a_runs_whole_log_is_one_load_into_its_own_dates_partition():
    def emit(logger):
        logger.info("the run started")
        logger.warning("a recipe was skipped")
        logger.error("an experiment failed")

    client = RecordingClient()
    rows = log_rows_of(emit)

    written = output_writing.write_log_table(
        client, AS_OF, rows, output_writing.Outputs()
    )

    assert written == 3
    assert [target for target, _, _ in client.loads] == [
        f"{output_writing.LOG_TABLE}$20260801"
    ]
    _, loaded, config = client.loads[0]
    assert len(loaded) == 3
    assert config.write_disposition == "WRITE_TRUNCATE"
    assert config.autodetect is False


def test_the_log_table_is_partitioned_and_clustered_like_the_tables_it_sits_beside():
    client = RecordingClient()
    rows = log_rows_of(lambda logger: logger.info("the run started"))

    output_writing.write_log_table(client, AS_OF, rows, output_writing.Outputs())
    table, exists_ok = client.created[0]

    assert table.time_partitioning.field == output_writing.PARTITION_FIELD
    assert table.clustering_fields == output_writing.CLUSTERING_FIELDS
    assert exists_ok


def test_a_local_run_writes_the_experiments_json_to_a_directory(tmp_path):
    outputs = output_writing.Outputs(local_blob_dir=str(tmp_path / "out"))

    path = output_writing.write_blob(
        None, EXPERIMENT, AS_OF, [CONFIDENT_CELL, ERROR_CELL], outputs
    )
    payload = json.loads(pathlib.Path(path).read_text())

    assert pathlib.Path(path).name == "an_experiment.json"

    assert payload["metrics_metadata"]["experiment_slug"] == "an-experiment"
    assert payload["metrics_metadata"]["as_of_date"] == "2026-08-01"
    assert payload["metrics_metadata"]["pipeline_version"] == output_writing.PIPELINE_VERSION
    assert payload["metrics_metadata"]["branches"] == ["control", "treatment"]
    assert payload["statistics"] == [CONFIDENT_CELL, ERROR_CELL]
    assert payload["errors"] == [ERROR_CELL]
