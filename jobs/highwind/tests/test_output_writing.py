"""Where a run's rows and blobs go, and that the columns written are the columns declared."""

import datetime
import json
import logging
import pathlib

import pytest
from mozilla_nimbus_schemas.highwind import HighwindAnalysis

from gbstats.frequentist.tests import sequential_interval_halfwidth

from highwind import discovery, gbstats_compute, output_writing, units
from highwind.discovery import Experiment
from highwind.metrics import CUMULATIVE_WEEKLY, DISJOINT_WEEKLY, Metric, per_unit_sum

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


def run_log_of(emit, as_of=AS_OF):
    """The run log, and its rows, for whatever `emit` logs through a logger of its own."""
    run_log = output_writing.RunLog(as_of)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(run_log)
    try:
        emit(logger)
    finally:
        logger.removeHandler(run_log)
    return run_log, run_log.rows


def log_rows_of(emit, as_of=AS_OF):
    """The rows a run log makes of whatever `emit` logs through a logger of its own."""
    return run_log_of(emit, as_of)[1]


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


def test_a_runs_whole_log_is_one_appending_load_carrying_its_own_dates():
    # Appended, and to the table rather than to a partition decorator, so the rows land in the
    # partition their own as_of_date names.
    def emit(logger):
        logger.info("the run started")
        logger.warning("a recipe was skipped")
        logger.error("an experiment failed")

    client = RecordingClient()
    rows = log_rows_of(emit)

    written = output_writing.write_log_table(client, rows, output_writing.Outputs())

    assert written == 3
    assert [target for target, _, _ in client.loads] == [output_writing.LOG_TABLE]
    _, loaded, config = client.loads[0]
    assert len(loaded) == 3
    assert {row["as_of_date"] for row in loaded} == {AS_OF.isoformat()}
    assert config.write_disposition == "WRITE_APPEND"
    assert config.autodetect is False


def test_rerunning_a_date_keeps_the_earlier_attempts_log_rather_than_replacing_it():
    # The failed attempt is usually why the rerun exists, so replacing the date's partition would
    # delete the explanation on the way to producing the fix.
    client = RecordingClient()
    first = log_rows_of(lambda logger: logger.error("the first attempt died"))
    second = log_rows_of(lambda logger: logger.info("the rerun succeeded"))

    output_writing.write_log_table(client, first, output_writing.Outputs())
    output_writing.write_log_table(client, second, output_writing.Outputs())

    dispositions = {config.write_disposition for _, _, config in client.loads}
    assert dispositions == {"WRITE_APPEND"}
    assert all(target == output_writing.LOG_TABLE for target, _, _ in client.loads)
    assert [row["message"] for _, loaded, _ in client.loads for row in loaded] == [
        "the first attempt died",
        "the rerun succeeded",
    ]


def test_every_row_a_run_logs_carries_that_runs_own_start_time():
    # The only thing separating one attempt at a date from another, now that both are kept. Read
    # off each handler rather than compared between two wall clock readings, so the test does not
    # depend on two runs being constructed far enough apart to differ.
    def emit(logger):
        logger.info("the run started")
        logger.info("the run finished")

    first, first_rows = run_log_of(emit)
    second, second_rows = run_log_of(emit)

    assert {row["run_started_at"] for row in first_rows} == {
        first.run_started_at.isoformat()
    }
    assert {row["run_started_at"] for row in second_rows} == {
        second.run_started_at.isoformat()
    }
    assert first.run_started_at.tzinfo == datetime.timezone.utc


def test_the_log_table_is_partitioned_and_clustered_like_the_tables_it_sits_beside():
    client = RecordingClient()
    rows = log_rows_of(lambda logger: logger.info("the run started"))

    output_writing.write_log_table(client, rows, output_writing.Outputs())
    table, exists_ok = client.created[0]

    assert table.time_partitioning.field == output_writing.PARTITION_FIELD
    assert table.clustering_fields == output_writing.CLUSTERING_FIELDS
    assert exists_ok


CUMULATIVE_METRIC = Metric(
    name="active_hours",
    source="clients_daily",
    reducer=per_unit_sum("active_hours_sum"),
    window_rules=(CUMULATIVE_WEEKLY,),
    friendly_name="Active hours",
    description="Time with user input.",
)

DISJOINT_METRIC = Metric(
    name="retained",
    source="clients_daily",
    reducer=per_unit_sum("pings_aggregated_by_this_row"),
    window_rules=(DISJOINT_WEEKLY,),
    friendly_name="Retained",
)

THREE_ARMS = Experiment(
    slug="three-arms",
    start_date=datetime.date(2026, 7, 1),
    end_date=None,
    reference_branch="control",
    treatment_branches=("treatment-a", "treatment-b"),
    unit=units.resolve("firefox_desktop", "group_id"),
)


class UploadingBlob:
    def __init__(self, key, uploads):
        self.key = key
        self.uploads = uploads
        self.metadata = None

    def upload_from_string(self, data, content_type=None):
        self.uploads.append((self.key, self.metadata, data, content_type))


class UploadingBucket:
    def __init__(self, uploads):
        self.uploads = uploads

    def blob(self, key):
        return UploadingBlob(key, self.uploads)


class UploadingStorage:
    def __init__(self):
        self.uploads = []

    def bucket(self, name):
        return UploadingBucket(self.uploads)


def result(metric, index, kind, state, branch="treatment", **extra):
    window = discovery.window_at({"kind": kind, "length": 7}, index)
    return dict(
        metric=metric,
        window=window.label,
        window_kind=window.kind,
        window_start=window.start,
        window_end=window.end,
        branch=branch,
        reference_branch="control",
        state=state,
        **extra,
    )


def aggregates(n):
    return dict(AGGREGATES, n=n)


def build(
    experiment=EXPERIMENT,
    as_of=AS_OF,
    metrics=None,
    results=(),
    cells=None,
    units=None,
    problems=(),
):
    return output_writing.build_analysis(
        experiment,
        as_of,
        metrics or [CUMULATIVE_METRIC, DISJOINT_METRIC],
        list(results),
        cells or {},
        units or {},
        list(problems),
    )


def unit_cell(n, mean):
    return dict(
        n=n,
        sum=n * mean,
        sum_squares=n * (mean * mean + 1.0),
        pre_sum=float(n),
        pre_sum_squares=float(n),
        sum_x_pre=n * mean,
    )


def covariate_cell():
    return dict(
        n=4, sum=8.0, sum_squares=20.0, pre_sum=4.0, pre_sum_squares=6.0, sum_x_pre=10.0
    )


def values_of(window):
    return [(branch.branch, branch.value.point) for branch in window.branches]


def is_empty(interval):
    return (interval.point, interval.lower, interval.upper) == (None, None, None)


def windows_of(analysis, metric_slug):
    metric = next(metric for metric in analysis.metrics if metric.slug == metric_slug)
    assert [segment.segment for segment in metric.segments] == ["all_enrolled"]
    return metric.segments[0].windows


def spans(windows):
    return [
        (window.window.kind.value, window.window.start_day, window.window.end_day)
        for window in windows
    ]


def test_the_blob_round_trips_through_the_published_schema():
    analysis = build(
        results=[result("active_hours", 1, "cumulative", "confident", point=5.0, lower=1.0,
                        upper=9.0, n_reference=40, n_treatment=41, theta=0.1)],
        cells={("active_hours", "cumu:1", "control"): aggregates(40)},
    )

    assert HighwindAnalysis.model_validate_json(analysis.model_dump_json()) == analysis


def test_the_metadata_describes_the_experiment_reference_branch_first():
    analysis = build(experiment=THREE_ARMS)

    assert analysis.metadata.schema_version == output_writing.SCHEMA_VERSION
    assert analysis.metadata.experiment_slug == "three-arms"
    assert analysis.metadata.as_of_date == AS_OF
    assert analysis.metadata.pipeline_version == output_writing.PIPELINE_VERSION
    assert analysis.metadata.start_date == datetime.date(2026, 7, 1)
    assert analysis.metadata.end_date is None
    assert analysis.metadata.analysis_unit.value == "profile_group_id"
    assert analysis.metadata.reference_branch == "control"
    assert analysis.metadata.branches == ["control", "treatment-a", "treatment-b"]
    assert analysis.metadata.generated_at.tzinfo is not None


def test_there_is_one_all_enrolled_segment_counting_units_per_branch_reference_first():
    analysis = build(
        experiment=THREE_ARMS,
        cells={("active_hours", "cumu:1", "control"): aggregates(7)},
        units={"treatment-a": 48, "control": 50, "another-slugs-branch": 9},
    )

    [segment] = analysis.segments
    assert segment.slug == "all_enrolled"
    assert segment.friendly_name == "All enrolled"
    assert [(branch.branch, branch.units) for branch in segment.branches] == [
        ("control", 50),
        ("treatment-a", 48),
        ("treatment-b", 0),
    ]


def test_metrics_keep_their_display_order_and_names():
    analysis = build()

    described = [
        (metric.slug, metric.friendly_name, metric.description) for metric in analysis.metrics
    ]
    assert described == [
        ("active_hours", "Active hours", "Time with user input."),
        ("retained", "Retained", None),
    ]


def test_a_day_one_experiment_gets_one_not_started_window_per_rule_per_metric():
    day_one = Experiment(
        slug="day-one",
        start_date=AS_OF,
        end_date=None,
        reference_branch="control",
        treatment_branches=("treatment-a", "treatment-b"),
        unit=units.resolve("firefox_desktop", "normandy_id"),
    )

    analysis = build(experiment=day_one)

    for metric_slug, kind in (("active_hours", "cumulative"), ("retained", "disjoint")):
        [window] = windows_of(analysis, metric_slug)
        assert (window.window.kind.value, window.window.start_day, window.window.end_day) == (
            kind,
            0,
            6,
        )
        assert window.window.matures_on == datetime.date(2026, 8, 8)
        assert window.is_summary is True
        assert [(branch.branch, branch.n, is_empty(branch.value))
                for branch in window.branches] == [
            ("control", 0, True),
            ("treatment-a", 0, True),
            ("treatment-b", 0, True),
        ]
        assert [comparison.branch for comparison in window.comparisons] == [
            "treatment-a",
            "treatment-b",
        ]
        for comparison in window.comparisons:
            assert comparison.reference_branch == "control"
            assert comparison.state.value == "not_started"
            assert comparison.direction.value == "neutral"
            assert (comparison.n_reference, comparison.n_treatment) == (0, 0)
            assert is_empty(comparison.relative)
            assert is_empty(comparison.absolute)
            assert comparison.error is None


def test_matured_windows_come_first_then_the_next_one_as_not_started():
    analysis = build()

    cumulative = windows_of(analysis, "active_hours")
    assert spans(cumulative) == [
        ("cumulative", 0, 6),
        ("cumulative", 0, 13),
        ("cumulative", 0, 20),
        ("cumulative", 0, 27),
        ("cumulative", 0, 34),
    ]
    assert [window.comparisons[0].state.value for window in cumulative] == [
        "error",
        "error",
        "error",
        "error",
        "not_started",
    ]
    assert cumulative[-1].window.matures_on == datetime.date(2026, 8, 5)
    assert spans(windows_of(analysis, "retained"))[-1] == ("disjoint", 28, 34)


def test_an_ended_experiment_reports_no_window_that_has_not_matured():
    ended = Experiment(
        slug="ended",
        start_date=datetime.date(2026, 7, 1),
        end_date=AS_OF,
        reference_branch="control",
        treatment_branches=("treatment",),
        unit=units.resolve("firefox_desktop", "normandy_id"),
    )

    analysis = build(experiment=ended)

    assert analysis.metadata.end_date == AS_OF
    assert spans(windows_of(analysis, "active_hours"))[-1] == ("cumulative", 0, 27)
    assert all(
        window.comparisons[0].state.value != "not_started"
        for metric in analysis.metrics
        for window in metric.segments[0].windows
    )


def test_the_summary_is_the_largest_settled_cumulative_window():
    analysis = build(
        results=[
            result("active_hours", 1, "cumulative", "forming", point=1.0, lower=-1.0, upper=3.0),
            result("active_hours", 2, "cumulative", "confident", point=2.0, lower=1.0, upper=3.0),
            result("active_hours", 3, "cumulative", "confident", point=-2.0, lower=-3.0,
                   upper=-1.0),
            result("active_hours", 4, "cumulative", "insufficient_data", n_reference=1,
                   n_treatment=1),
        ]
    )

    windows = windows_of(analysis, "active_hours")
    assert [window.is_summary for window in windows] == [False, False, True, False, False]
    assert [window.comparisons[0].direction.value for window in windows] == [
        "neutral",
        "positive",
        "negative",
        "neutral",
        "neutral",
    ]
    relative = windows[1].comparisons[0].relative
    assert (relative.point, relative.lower, relative.upper) == (2.0, 1.0, 3.0)
    assert is_empty(windows[1].comparisons[0].absolute)


def test_a_disjoint_only_metric_summarises_on_its_latest_settled_window():
    analysis = build(
        results=[
            result("retained", 1, "disjoint", "forming", point=1.0, lower=-1.0, upper=3.0),
            result("retained", 2, "disjoint", "forming", point=1.0, lower=-1.0, upper=3.0),
            result("retained", 3, "disjoint", "insufficient_data"),
        ]
    )

    windows = windows_of(analysis, "retained")
    assert [window.is_summary for window in windows] == [False, True, False, False, False]


def test_with_nothing_settled_the_summary_is_the_first_window_to_mature():
    analysis = build(
        results=[
            result("active_hours", index, "cumulative", "insufficient_data")
            for index in range(1, 5)
        ]
    )

    windows = windows_of(analysis, "active_hours")
    assert [window.is_summary for window in windows] == [True, False, False, False, False]


def test_every_metric_has_exactly_one_summary_window_per_segment():
    analysis = build(experiment=THREE_ARMS, metrics=[CUMULATIVE_METRIC, DISJOINT_METRIC])

    for metric in analysis.metrics:
        for segment in metric.segments:
            assert sum(window.is_summary for window in segment.windows) == 1


def test_a_matured_window_with_no_data_is_insufficient_rather_than_not_started():
    analysis = build(
        results=[result("active_hours", 1, "cumulative", "not_started")],
        cells={("active_hours", "cumu:1", "control"): aggregates(12)},
    )

    [comparison] = windows_of(analysis, "active_hours")[0].comparisons
    assert comparison.state.value == "insufficient_data"
    assert (comparison.n_reference, comparison.n_treatment) == (12, 0)


def test_each_branch_carries_its_units_for_the_window():
    analysis = build(
        experiment=THREE_ARMS,
        cells={
            ("active_hours", "cumu:1", "control"): aggregates(50),
            ("active_hours", "cumu:1", "treatment-b"): aggregates(47),
        },
    )

    branches = windows_of(analysis, "active_hours")[0].branches
    assert [(branch.branch, branch.n) for branch in branches] == [
        ("control", 50),
        ("treatment-a", 0),
        ("treatment-b", 47),
    ]
    assert all(is_empty(branch.value) for branch in branches)


def test_a_branch_value_is_its_mean_when_the_covariate_carries_no_adjustment():
    analysis = build(
        experiment=THREE_ARMS,
        results=[
            result("active_hours", 1, "cumulative", "forming", branch=branch, point=1.0,
                   lower=-1.0, upper=3.0)
            for branch in ("treatment-a", "treatment-b")
        ],
        cells={
            ("active_hours", "cumu:1", "control"): unit_cell(100, 2.0),
            ("active_hours", "cumu:1", "treatment-a"): unit_cell(100, 3.0),
            ("active_hours", "cumu:1", "treatment-b"): unit_cell(1, 5.0),
        },
    )

    assert values_of(windows_of(analysis, "active_hours")[0]) == [
        ("control", 2.0),
        ("treatment-a", 3.0),
        ("treatment-b", None),
    ]


def test_a_branch_value_is_adjusted_by_the_windows_pooled_covariate_slope():
    analysis = build(
        results=[result("active_hours", 1, "cumulative", "forming", point=0.0, lower=-1.0,
                        upper=1.0)],
        cells={
            ("active_hours", "cumu:1", "control"): covariate_cell(),
            ("active_hours", "cumu:1", "treatment"): covariate_cell(),
        },
    )

    [(_, reference), (_, treatment)] = values_of(windows_of(analysis, "active_hours")[0])
    assert reference == pytest.approx(2.0)
    assert treatment == pytest.approx(2.0)


def test_a_branch_value_stays_on_the_metrics_scale_when_the_pre_periods_differ():
    shifted = dict(
        n=4, sum=12.0, sum_squares=38.0, pre_sum=8.0, pre_sum_squares=18.0, sum_x_pre=26.0
    )
    baseline = dict(
        n=4, sum=8.0, sum_squares=18.0, pre_sum=4.0, pre_sum_squares=6.0, sum_x_pre=10.0
    )
    analysis = build(
        results=[result("active_hours", 1, "cumulative", "forming", point=0.0, lower=-1.0,
                        upper=1.0)],
        cells={
            ("active_hours", "cumu:1", "control"): baseline,
            ("active_hours", "cumu:1", "treatment"): shifted,
        },
    )

    [(_, reference), (_, treatment)] = values_of(windows_of(analysis, "active_hours")[0])
    assert reference == pytest.approx(2.5)
    assert treatment == pytest.approx(2.5)


def test_an_errored_comparison_leaves_its_branch_without_a_value():
    cells = {
        ("active_hours", "cumu:1", branch): unit_cell(100, mean)
        for branch, mean in (("control", 2.0), ("treatment-a", 3.0), ("treatment-b", 4.0))
    }
    three = build(
        experiment=THREE_ARMS,
        results=[
            result("active_hours", 1, "cumulative", "error", branch="treatment-a", error="boom"),
            result("active_hours", 1, "cumulative", "forming", branch="treatment-b", point=1.0,
                   lower=-1.0, upper=3.0),
        ],
        cells=cells,
    )
    two = build(
        results=[result("active_hours", 1, "cumulative", "error", error="boom")],
        cells={
            ("active_hours", "cumu:1", "control"): unit_cell(100, 2.0),
            ("active_hours", "cumu:1", "treatment"): unit_cell(100, 3.0),
        },
    )

    assert values_of(windows_of(three, "active_hours")[0]) == [
        ("control", 2.0),
        ("treatment-a", None),
        ("treatment-b", 4.0),
    ]
    assert values_of(windows_of(two, "active_hours")[0]) == [
        ("control", None),
        ("treatment", None),
    ]


def test_a_window_that_has_not_matured_has_no_branch_values():
    analysis = build()

    assert values_of(windows_of(analysis, "active_hours")[-1]) == [
        ("control", None),
        ("treatment", None),
    ]


def test_an_error_cell_carries_its_reason_and_its_log_record_names_the_window():
    def emit(logger):
        try:
            raise ZeroDivisionError("division by zero")
        except ZeroDivisionError:
            logger.exception(
                "an-experiment active_hours cumu:2 treatment failed",
                extra={
                    "experiment_slug": "an-experiment",
                    "metric": "active_hours",
                    "segment": "all_enrolled",
                    "window": discovery.window_at(CUMULATIVE_WEEKLY, 2),
                },
            )
        logger.info("progress is not an error")

    run_log, _ = run_log_of(emit)
    analysis = build(
        results=[ERROR_CELL], problems=run_log.problems_for("an-experiment")
    )

    comparison = windows_of(analysis, "active_hours")[1].comparisons[0]
    assert comparison.state.value == "error"
    assert comparison.error == "RuntimeError: boom"
    assert comparison.direction.value == "neutral"
    [error] = analysis.errors
    assert error.log_level.value == "ERROR"
    assert error.exception_type == "ZeroDivisionError"
    assert "division by zero" in error.exception
    assert error.metric == "active_hours"
    assert error.segment == "all_enrolled"
    assert (error.window.kind.value, error.window.start_day, error.window.end_day) == (
        "cumulative",
        0,
        13,
    )
    assert error.window.matures_on == datetime.date(2026, 7, 15)
    assert error.filename == "test_output_writing.py"
    assert error.func_name == "emit"


def test_the_run_log_keeps_warnings_about_this_experiment_and_the_run_but_no_other():
    def emit(logger):
        logger.warning("about this one", extra={"experiment_slug": "an-experiment"})
        logger.warning("about another", extra={"experiment_slug": "another"})
        logger.error("about the run")
        logger.info("progress", extra={"experiment_slug": "an-experiment"})

    run_log, _ = run_log_of(emit)

    messages = [
        (problem["message"], problem["log_level"].value)
        for problem in run_log.problems_for("an-experiment")
    ]
    assert messages == [("about this one", "WARNING"), ("about the run", "ERROR")]


def test_a_refused_experiment_gets_its_metadata_and_errors_and_nothing_else():
    run_log, _ = run_log_of(
        lambda logger: logger.warning(
            "skipped an-experiment: 400d old", extra={"experiment_slug": "an-experiment"}
        )
    )

    analysis = output_writing.build_refusal(
        EXPERIMENT, AS_OF, run_log.problems_for("an-experiment")
    )

    assert analysis.metadata.experiment_slug == "an-experiment"
    assert analysis.segments == []
    assert analysis.metrics == []
    assert [(error.message, error.window, error.metric) for error in analysis.errors] == [
        ("skipped an-experiment: 400d old", None, None)
    ]
    assert HighwindAnalysis.model_validate_json(analysis.model_dump_json()) == analysis


def test_a_local_run_writes_the_experiments_json_to_a_directory(tmp_path):
    outputs = output_writing.Outputs(local_blob_dir=str(tmp_path / "out"))
    analysis = build(results=[CONFIDENT_CELL, ERROR_CELL])

    path = output_writing.write_blob(None, analysis, outputs)
    text = pathlib.Path(path).read_text()

    assert pathlib.Path(path).name == "an_experiment.json"
    assert HighwindAnalysis.model_validate_json(text) == analysis
    assert set(json.loads(text)) == {"metadata", "segments", "metrics", "errors"}


def test_a_published_blob_carries_its_provenance_as_object_metadata():
    storage = UploadingStorage()
    analysis = build()

    path = output_writing.write_blob(storage, analysis, output_writing.Outputs())

    [(key, metadata, data, content_type)] = storage.uploads
    assert path == "gs://mozanalysis/highwind/an_experiment.json"
    assert key == "highwind/an_experiment.json"
    assert content_type == "application/json"
    assert metadata == {
        "as_of_date": "2026-08-01",
        "generated_at": analysis.metadata.generated_at.isoformat(),
        "pipeline_version": output_writing.PIPELINE_VERSION,
    }
    assert HighwindAnalysis.model_validate_json(data) == analysis


def test_a_comparison_carries_the_absolute_interval_beside_the_relative_one():
    analysis = build(
        results=[
            result(
                "active_hours", 1, "cumulative", "confident", point=10.0, lower=4.0,
                upper=16.0, n_reference=400, n_treatment=400,
                absolute=dict(point=0.2, lower=0.08, upper=0.32),
            )
        ]
    )

    comparison = windows_of(analysis, "active_hours")[0].comparisons[0]
    assert (comparison.relative.point, comparison.relative.lower, comparison.relative.upper) == (
        10.0,
        4.0,
        16.0,
    )
    assert (comparison.absolute.point, comparison.absolute.lower, comparison.absolute.upper) == (
        0.2,
        0.08,
        0.32,
    )
    assert comparison.direction.value == "positive"
    assert HighwindAnalysis.model_validate_json(analysis.model_dump_json()) == analysis


def test_the_absolute_interval_stays_out_of_the_statistics_table():
    client = RecordingClient()
    cell = dict(CONFIDENT_CELL, absolute=dict(point=0.2, lower=0.08, upper=0.32))

    output_writing.write_tables(
        client, AS_OF, {"an-experiment": [cell]}, {}, output_writing.Outputs()
    )

    [row] = client.loads[0][1]
    assert "absolute" not in row
    assert row["point"] == 10.0


def branch_interval(n):
    analysis = build(
        results=[result("active_hours", 1, "cumulative", "forming", point=1.0, lower=-1.0,
                        upper=3.0)],
        cells={
            ("active_hours", "cumu:1", "control"): unit_cell(n, 2.0),
            ("active_hours", "cumu:1", "treatment"): unit_cell(n, 3.0),
        },
    )
    return windows_of(analysis, "active_hours")[0].branches[0].value


def test_a_branch_interval_is_the_always_valid_halfwidth_around_its_value():
    interval = branch_interval(100)

    expected = sequential_interval_halfwidth(
        100 / 99, 100, gbstats_compute.tuning_parameter(100), 0.05
    )
    assert interval.lower < interval.point < interval.upper
    assert interval.point == pytest.approx(2.0)
    assert interval.upper - interval.point == pytest.approx(expected)
    assert interval.point - interval.lower == pytest.approx(expected)


def test_a_branch_interval_narrows_as_its_units_grow():
    small, large = branch_interval(100), branch_interval(10000)

    assert large.upper - large.lower < small.upper - small.lower
    assert large.lower < large.point < large.upper


def test_a_comparison_without_a_relative_interval_has_no_absolute_interval():
    analysis = build(
        results=[result("active_hours", 1, "cumulative", "insufficient_data", n_reference=1,
                        n_treatment=1)]
    )

    comparison = windows_of(analysis, "active_hours")[0].comparisons[0]
    assert comparison.state.value == "insufficient_data"
    assert is_empty(comparison.relative)
    assert is_empty(comparison.absolute)
