"""Whether a run failed as a whole, and what it records about the ways it can fail."""

import datetime
import logging
import types

import pytest
from metric_config_parser.aggregation import BucketedAggregation, Combine
from metric_config_parser.aggregation_plan import AggregationSource
from mozilla_nimbus_schemas.highwind import HighwindAnalysis

from highwind import analysis, output_writing, segments, sql_generation, units
from highwind.discovery import Experiment, MetricDisplay, SegmentDisplay
from highwind.gbstats_compute import (
    CONFIDENT,
    ERROR,
    FORMING,
    INSUFFICIENT_DATA,
    NOT_STARTED,
)
from highwind.metrics import CUMULATIVE_WEEKLY, Metric

AS_OF = datetime.date(2026, 8, 20)

EXPERIMENT = Experiment(
    slug="a-slug",
    start_date=datetime.date(2026, 7, 1),
    end_date=None,
    reference_branch="control",
    treatment_branches=("treatment-a",),
    unit=units.resolve("firefox_desktop", "normandy_id"),
    metrics={
        "active_hours": MetricDisplay(
            friendly_name=None, description=None, bigger_is_better=None
        )
    },
)

# One resolved metric, standing in for what `split_resolved_metrics()` would build off a real
# aggregation plan: these tests exercise the run's orchestration, not metric resolution itself (see
# test_metrics.py for that).
TEST_METRICS = [
    Metric(
        name="active_hours",
        source="clients_daily",
        reducer=BucketedAggregation(
            bucket_aggregate="SUM(active_hours_sum)",
            combine=Combine.SUM,
            no_rows="0",
            finalize="{combined}",
        ),
        window_rules=(CUMULATIVE_WEEKLY,),
    )
]
TEST_SOURCES = {
    "clients_daily": AggregationSource(
        data_source="clients_daily",
        from_expression="mozdata.telemetry.clients_daily",
        client_id_column="client_id",
        group_id_column="profile_group_id",
        analysis_units=("client_id", "profile_group_id"),
        submission_date_column="submission_date",
        columns=("active_hours_sum",),
    )
}


class FakeLoadJob:
    def result(self):
        return None


class RecordingClient:
    """Stands in for a BigQuery client, recording what a run loaded."""

    def __init__(self):
        self.created = []
        self.loads = []

    def create_table(self, table, exists_ok=False):
        self.created.append(table)
        return table

    def load_table_from_json(self, rows, target, job_config=None):
        self.loads.append((target, rows))
        return FakeLoadJob()

    @property
    def log_rows(self):
        return [row for _, rows in self.loads for row in rows]


class RecordingBlob:
    def __init__(self, uploaded, payloads, key):
        self.uploaded = uploaded
        self.payloads = payloads
        self.key = key
        self.metadata = None

    def upload_from_string(self, data, content_type=None):
        self.uploaded.append(self.key)
        self.payloads[self.key] = HighwindAnalysis.model_validate_json(data)


class RecordingBucket:
    def __init__(self, uploaded, payloads):
        self.uploaded = uploaded
        self.payloads = payloads

    def blob(self, key):
        return RecordingBlob(self.uploaded, self.payloads, key)


class RecordingStorage:
    """Stands in for a GCS client, recording the object names a run published."""

    def __init__(self):
        self.uploaded = []
        self.payloads = {}

    def bucket(self, name):
        return RecordingBucket(self.uploaded, self.payloads)


def summary(slug, states, error=None):
    return dict(
        slug=slug,
        cells=sum(states.values()),
        states=states,
        error=error,
    )


def test_a_run_with_nothing_to_analyse_is_not_a_failure():
    assert analysis.systemic_failure([]) is False


def test_a_run_whose_every_cell_errored_is_a_failure():
    summaries = [summary("a", {ERROR: 120}), summary("b", {ERROR: 84})]

    assert analysis.systemic_failure(summaries) is True


def test_one_failed_experiment_beside_a_working_one_is_not_a_failure():
    summaries = [
        summary("a", {ERROR: 120}, error="RuntimeError: boom"),
        summary("b", {CONFIDENT: 3, FORMING: 9}),
    ]

    assert analysis.systemic_failure(summaries) is False


def test_an_experiment_that_failed_before_producing_any_cell_still_counts_as_failed():
    assert analysis.systemic_failure([summary("a", {}, error="RuntimeError: boom")]) is True


def test_a_quiet_day_where_no_window_has_matured_is_not_a_failure():
    assert analysis.systemic_failure([summary("a", {}), summary("b", {})]) is False


def test_a_run_that_produced_only_immature_cells_is_not_a_failure():
    summaries = [summary("a", {NOT_STARTED: 40}), summary("b", {INSUFFICIENT_DATA: 12})]

    assert analysis.systemic_failure(summaries) is False


def test_it_turns_on_cell_state_rather_than_on_how_many_cells_there_are():
    # A failing experiment emits a full grid of error cells, so a large grid is not evidence of
    # health and a small one is not evidence of failure.
    large_grid_all_errors = summary("a", {ERROR: 4000})
    one_good_cell = summary("b", {CONFIDENT: 1})

    assert analysis.systemic_failure([large_grid_all_errors]) is True
    assert analysis.systemic_failure([large_grid_all_errors, one_good_cell]) is False


def test_a_failure_while_recording_a_failure_is_still_returned_not_raised():
    # The one path a run with no failures never reaches, so it needs covering deliberately: a
    # malformed metric set fails the analysis, and then fails the recording of that failure too. It
    # has to come back as a summary, because raising here escapes through future.result() and costs
    # every other experiment its results.
    recorded, results = analysis.analyze_experiment(
        EXPERIMENT, [], None, {}, "the shared scan failed"
    )

    assert results == []
    assert recorded["slug"] == "a-slug"
    assert recorded["cells"] == 0
    assert "recording it also failed" in recorded["error"]


def test_the_run_report_counts_cells_and_errors_across_experiments(caplog):
    caplog.set_level(logging.INFO)

    analysis.report_run(
        [summary("a", {CONFIDENT: 1, ERROR: 1}), summary("b", {FORMING: 2})],
        [
            dict(
                source="clients_daily",
                wall_seconds=1.0,
                gigabytes_scanned=1.0,
                slot_hours=1.0,
                rows=2,
            )
        ],
    )

    assert "2 experiments (0 failed outright)" in caplog.text
    assert "error rate 25.00%" in caplog.text


def test_the_run_report_says_so_rather_than_dividing_by_zero_cells(caplog):
    caplog.set_level(logging.INFO)

    analysis.report_run([summary("a", {})], [])

    assert "no cells produced" in caplog.text


def test_an_experiment_whose_cells_are_all_not_started_is_reported_as_worth_a_look(caplog):
    caplog.set_level(logging.INFO)

    analysis.report_anomalies(
        [
            summary("a", {NOT_STARTED: 12}),
            summary("b", {CONFIDENT: 4, FORMING: 8}),
            summary("c", {}),
        ]
    )

    assert "cohort or join empty" in caplog.text
    assert "no window has matured" not in caplog.text
    # Each is attributed to the experiment it is about, so a reader looking one up finds the reason
    # against its slug rather than in a line they have to parse. The healthy one is not flagged.
    flagged = [
        record.experiment_slug
        for record in caplog.records
        if record.levelno == logging.WARNING
    ]
    assert flagged == ["a"]


def test_a_refused_recipe_is_recorded_against_its_slug_with_the_reason(caplog):
    client = RecordingClient()

    with analysis.collecting_run_log(AS_OF, client, output_writing.Outputs(), True):
        analysis.report_selection(
            [EXPERIMENT], [("a-refused-slug", "reference=None others=()", None)], AS_OF
        )

    refusals = [row for row in client.log_rows if row["log_level"] == "WARNING"]
    assert len(refusals) == 1
    assert refusals[0]["experiment_slug"] == "a-refused-slug"
    assert "reference=None" in refusals[0]["message"]


def test_an_experiment_that_fails_outright_is_recorded_against_its_slug():
    # It produces no results row of its own to carry an error on, unlike a cell that failed, so the
    # log is the only place its failure can be found.
    client = RecordingClient()

    with analysis.collecting_run_log(AS_OF, client, output_writing.Outputs(), True):
        analysis.analyze_experiment(
            EXPERIMENT, [], None, {}, "the shared scan failed"
        )

    failures = [row for row in client.log_rows if row["log_level"] == "ERROR"]
    assert failures
    assert all(row["experiment_slug"] == "a-slug" for row in failures)
    assert all(row["exception_type"] for row in failures)
    assert all(row["exception"] for row in failures)


def test_the_log_is_written_even_when_the_run_it_covers_raises():
    # The run this table is worth most for is the one that died, so the write cannot be a step the
    # run reaches only by finishing. The run's own failure becomes a third row, on top of whatever
    # it had already logged, so the table shows the raise and not just what preceded it.
    client = RecordingClient()

    with pytest.raises(RuntimeError):
        with analysis.collecting_run_log(AS_OF, client, output_writing.Outputs(), True):
            analysis.report_selection([], [("a-slug", "a reason", None)], AS_OF)
            raise RuntimeError("whatever ended the run")

    assert [row["experiment_slug"] for row in client.log_rows] == [None, "a-slug", None]


def test_a_run_that_fails_before_logging_anything_still_gets_an_error_row():
    # The crash this exists for: a run that dies in discovery, before its first log statement, used
    # to leave the table with no row at all for the date.
    client = RecordingClient()

    with pytest.raises(AttributeError):
        with analysis.collecting_run_log(AS_OF, client, output_writing.Outputs(), True):
            raise AttributeError("'dict' object has no attribute 'name'")

    failures = [row for row in client.log_rows if row["log_level"] == "ERROR"]
    assert len(failures) == 1
    assert failures[0]["experiment_slug"] is None
    assert failures[0]["exception_type"] == "AttributeError"
    assert "'dict' object has no attribute 'name'" in failures[0]["exception"]


def test_a_run_that_writes_no_tables_does_not_write_its_failure_row_either():
    # Filtered, sampled, dry-run and validate-only runs write none of the three tables, the log
    # among them, so their failures stay out of it the same way their results do.
    client = RecordingClient()

    with pytest.raises(RuntimeError):
        with analysis.collecting_run_log(AS_OF, client, output_writing.Outputs(), False):
            raise RuntimeError("whatever ended the run")

    assert client.loads == []


def test_a_partial_run_writes_no_log_rows_any_more_than_it_writes_results():
    client = RecordingClient()
    write_tables = analysis.writes_tables(
        False, ["a-slug"], None, None, output_writing.Outputs()
    )

    with analysis.collecting_run_log(
        AS_OF, client, output_writing.Outputs(), write_tables
    ):
        analysis.report_selection([EXPERIMENT], [("a-slug", "a reason", None)], AS_OF)

    assert write_tables is False
    assert client.loads == []


def test_the_log_table_is_written_by_exactly_the_runs_that_write_the_result_tables():
    # One rule for all three tables: a log describing a run whose rows were withheld would replace
    # the log of the run whose rows are in the partition.
    published = output_writing.Outputs()
    local = output_writing.Outputs(local_blob_dir="test_output")

    assert analysis.writes_tables(False, None, None, None, published) is True
    assert analysis.writes_tables(True, None, None, None, published) is False
    assert analysis.writes_tables(False, ["a"], None, None, published) is False
    assert analysis.writes_tables(False, None, 5, None, published) is False
    assert analysis.writes_tables(False, None, None, 1, published) is False
    assert analysis.writes_tables(False, None, None, None, local) is False


def test_a_log_that_cannot_be_written_does_not_replace_whatever_ended_the_run():
    # The run logged its own failure row above, and writing that row is itself what fails here: the
    # original exception still has to be the one that surfaces, not a complaint about logging it.
    class FailingClient:
        def create_table(self, table, exists_ok=False):
            raise RuntimeError("the log table could not be created")

    with pytest.raises(RuntimeError, match="whatever ended the run"):
        with analysis.collecting_run_log(
            AS_OF, FailingClient(), output_writing.Outputs(), True
        ):
            raise RuntimeError("whatever ended the run")


def test_a_sampled_run_is_treated_as_partial_so_it_cannot_overwrite_the_partition():
    # A sampled run computes every experiment from a fraction of its cohort, so its rows look
    # complete while being wrong. The partition write replaces the whole day, so it has to be gated
    # the same way a --slug or --limit run is.
    assert analysis.is_partial_run(only_slugs=None, limit=None, sample_percent=1) is True
    assert analysis.is_partial_run(only_slugs=["a"], limit=None, sample_percent=None) is True
    assert analysis.is_partial_run(only_slugs=None, limit=5, sample_percent=None) is True
    assert analysis.is_partial_run(only_slugs=None, limit=None, sample_percent=None) is False


def test_a_sampled_run_publishes_no_blobs_where_a_filtered_run_publishes_its_own():
    # The two kinds of partial run differ here, unlike at the table write. A filtered run computes
    # each experiment it covered from that experiment's whole cohort, so its blobs are the right
    # answer for those slugs. A sampled run's are wrong for every slug, and a blob is named by its
    # experiment alone, so publishing one would replace a correct answer with a sampled one.
    published = output_writing.Outputs()

    # `--slug` and `--limit` reach this only through the sample, which they do not set.
    assert analysis.writes_blobs(False, None, published) is True
    assert analysis.writes_blobs(False, 1, published) is False


def test_a_sample_directed_at_a_local_directory_is_still_written():
    # Nothing reads a local directory as published results, so a sample can be inspected there.
    local = output_writing.Outputs(local_blob_dir="test_output")

    assert analysis.writes_blobs(False, 1, local) is True


def test_a_run_that_only_validates_its_sql_writes_no_blobs_at_all():
    assert analysis.writes_blobs(True, None, output_writing.Outputs()) is False


def publish(storage, experiments, refused, results_by_slug, run_log):
    outputs = output_writing.Outputs()
    analysis.publish_refusals(
        storage,
        AS_OF,
        [(experiment.slug, "a reason", experiment) for experiment in refused],
        run_log,
        outputs,
        2,
    )
    analysis.publish_analyses(
        storage,
        AS_OF,
        experiments,
        {experiment.slug: TEST_METRICS for experiment in experiments},
        results_by_slug,
        {},
        {},
        {},
        {},
        run_log,
        outputs,
        2,
    )


def test_analysed_and_refused_experiments_each_get_a_blob_carrying_their_own_log():
    refused = Experiment(
        slug="an-old-slug",
        start_date=datetime.date(2025, 7, 1),
        end_date=None,
        reference_branch="control",
        treatment_branches=("treatment-a",),
        unit=units.resolve("firefox_desktop", "normandy_id"),
    )
    storage, client = RecordingStorage(), RecordingClient()
    windows = analysis.run_windows(EXPERIMENT, AS_OF, TEST_METRICS)

    with analysis.collecting_run_log(
        AS_OF, client, output_writing.Outputs(), False
    ) as run_log:
        analysis.report_selection(
            [EXPERIMENT], [("an-old-slug", "416d old", refused)], AS_OF
        )
        _, results = analysis.analyze_experiment(EXPERIMENT, windows, TEST_METRICS, {}, None)
        publish(storage, [EXPERIMENT], [refused], {"a-slug": results}, run_log)

    assert sorted(storage.uploaded) == ["highwind/a_slug.json", "highwind/an_old_slug.json"]
    analysed = storage.payloads["highwind/a_slug.json"]
    refusal = storage.payloads["highwind/an_old_slug.json"]
    assert [metric.slug for metric in analysed.metrics] == [
        metric.name for metric in TEST_METRICS
    ]
    assert analysed.errors == []
    assert refusal.metrics == []
    assert refusal.segments == []
    assert [error.message for error in refusal.errors] == ["skipped an-old-slug: 416d old"]


def test_a_run_level_failure_reaches_every_experiments_blob():
    storage, client = RecordingStorage(), RecordingClient()

    with analysis.collecting_run_log(
        AS_OF, client, output_writing.Outputs(), False
    ) as run_log:
        logging.getLogger("highwind.analysis").error("the shared scan failed")
        publish(storage, [EXPERIMENT], [], {}, run_log)

    errors = storage.payloads["highwind/a_slug.json"].errors
    assert [(error.message, error.log_level.value) for error in errors] == [
        ("the shared scan failed", "ERROR")
    ]


def test_a_blob_that_cannot_be_built_is_logged_rather_than_raised():
    broken = Experiment(
        slug="a-broken-slug",
        start_date=datetime.date(2026, 7, 1),
        end_date=None,
        reference_branch="control",
        treatment_branches=("treatment-a",),
        unit=units.AnalysisUnit(
            kind="nimbus_id",
            enrollment_column="nimbus_id",
            clustered_sample_id=False,
        ),
    )
    storage, client = RecordingStorage(), RecordingClient()

    with analysis.collecting_run_log(
        AS_OF, client, output_writing.Outputs(), False
    ) as run_log:
        publish(storage, [broken, EXPERIMENT], [], {}, run_log)

    assert storage.uploaded == ["highwind/a_slug.json"]
    failures = [row for row in run_log.rows if row["log_level"] == "ERROR"]
    assert [row["experiment_slug"] for row in failures] == ["a-broken-slug"]
    assert failures[0]["exception_type"] == "ValueError"


class FakeQueryJob:
    def __init__(self, rows):
        self.rows = rows
        self.slot_millis = 0
        self.total_bytes_processed = 0

    def result(self):
        return self.rows


class UnitCountFailingClient:
    """Stands in for a BigQuery client whose metric queries succeed and whose unit count fails."""

    ROW = dict(
        slug="a-slug",
        metric="a-metric",
        window_label="week:1",
        segment="all_enrolled",
        branch="control",
        n=10,
        sum=5.0,
        sum_squares=5.0,
        pre_sum=4.0,
        pre_sum_squares=4.0,
        sum_x_pre=3.0,
        winsorize_mode="off",
        post_winsorize_threshold=None,
        pre_winsorize_threshold=None,
        clipped_share=None,
    )

    def __init__(self):
        self.cohort_table = None

    def query(self, sql, job_config=None):
        if job_config is not None and job_config.destination is not None:
            self.cohort_table = job_config.destination
            return FakeQueryJob([])
        if self.cohort_table and sql == sql_generation.branch_units_query(
            self.cohort_table
        ):
            raise RuntimeError("the unit count failed")
        return FakeQueryJob([self.ROW])

    def get_table(self, table):
        return types.SimpleNamespace(expires=None)

    def update_table(self, table, fields):
        return table


TEST_SEGMENT_SOURCES = {
    "clients_last_seen": segments.SegmentSource(
        name="clients_last_seen",
        from_expression="mozdata.telemetry.clients_last_seen",
        window_start=0,
        window_end=0,
        client_id_column="client_id",
        group_id_column="profile_group_id",
        submission_date_column="submission_date",
    ),
    "newtab": segments.SegmentSource(
        name="newtab",
        from_expression="mozdata.telemetry.newtab_clients_daily",
        window_start=-7,
        window_end=-1,
        client_id_column="client_id",
        group_id_column="profile_group_id",
        submission_date_column="submission_date",
    ),
}
TEST_RESOLVED_SEGMENTS = {
    "regular_user": segments.Segment(
        name="regular_user",
        source="clients_last_seen",
        select_expression="COALESCE(LOGICAL_OR(is_regular_user_v3), FALSE)",
    ),
    "suggest_user": segments.Segment(
        name="suggest_user",
        source="newtab",
        select_expression="COALESCE(LOGICAL_OR(uses_suggest), FALSE)",
    ),
}


class UnitSegmentsFailingClient:
    """Stands in for a BigQuery client whose cohort and metric queries succeed and whose unit
    segments materialization fails."""

    ROW = UnitCountFailingClient.ROW

    def __init__(self):
        self.cohort_table = None

    def query(self, sql, job_config=None):
        if job_config is not None and job_config.destination is not None:
            if "highwind_unit_segments" in str(job_config.destination):
                raise RuntimeError("the unit segments write failed")
            self.cohort_table = job_config.destination
            return FakeQueryJob([])
        if "MIN(enrollment_date)" in sql:
            return FakeQueryJob([])
        return FakeQueryJob([self.ROW])

    def get_table(self, table):
        return types.SimpleNamespace(expires=None)

    def update_table(self, table, fields):
        return table


def test_a_failed_segment_source_marks_only_its_own_segments_cells_as_error(caplog):
    caplog.set_level(logging.ERROR)
    resolved_metrics = {"active_hours": TEST_METRICS[0]}
    segment_names_by_slug = {"a-slug": ("regular_user",)}
    run = sql_generation.Run(
        [EXPERIMENT],
        {"a-slug": analysis.run_windows(EXPERIMENT, AS_OF, TEST_METRICS)},
        AS_OF,
        analysis.COVARIATE_DAYS,
        segments_by_slug=segment_names_by_slug,
    )

    (
        cells_by_slug, units_by_slug, extra_segment_units_by_slug, returned_segment_names,
        timings, failure, source_failures,
    ) = analysis.gather_sufficient_statistics(
        UnitSegmentsFailingClient(), run, resolved_metrics, TEST_SOURCES,
        TEST_RESOLVED_SEGMENTS, TEST_SEGMENT_SOURCES, segment_names_by_slug, AS_OF, False,
    )

    assert failure is None
    # The segment's name is kept, not dropped: its failure shows on its own cells instead.
    assert returned_segment_names == segment_names_by_slug
    assert extra_segment_units_by_slug == {}
    assert list(cells_by_slug["a-slug"]) == [("a-metric", "week:1", "all_enrolled", "control")]
    assert set(source_failures) == {("clients_last_seen", "client_id")}
    clients_last_seen_error = source_failures[("clients_last_seen", "client_id")]
    assert "the unit segments write failed" in str(clients_last_seen_error)
    assert timings
    assert "segment source 'clients_last_seen'" in caplog.text

    segment_failures = analysis.segment_failures_for_experiment(
        EXPERIMENT, returned_segment_names, TEST_RESOLVED_SEGMENTS, source_failures
    )
    windows = analysis.run_windows(EXPERIMENT, AS_OF, TEST_METRICS)
    _, results = analysis.analyze_experiment(
        EXPERIMENT, windows, TEST_METRICS, cells_by_slug["a-slug"], failure,
        analysis.experiment_segment_names(EXPERIMENT, returned_segment_names),
        segment_failures,
    )

    states_by_segment = {result["segment"]: result["state"] for result in results}
    assert states_by_segment["regular_user"] == ERROR
    assert states_by_segment["all_enrolled"] != ERROR


class TwoSegmentSourcesOneFailingClient:
    """Stands in for a BigQuery client with two segment sources, one of which fails; the cohort
    and metric queries succeed."""

    ALL_ENROLLED_ROW = UnitCountFailingClient.ROW
    REGULAR_USER_ROW = dict(UnitCountFailingClient.ROW, segment="regular_user")

    def __init__(self):
        self.cohort_table = None
        self.unit_segment_queries = []

    def query(self, sql, job_config=None):
        if job_config is not None and job_config.destination is not None:
            destination = str(job_config.destination)
            if "highwind_unit_segments" in destination:
                self.unit_segment_queries.append(sql)
                if "newtab" in sql:
                    raise RuntimeError("the newtab segment source failed")
                return FakeQueryJob([])
            self.cohort_table = job_config.destination
            return FakeQueryJob([])
        if "MIN(enrollment_date)" in sql or "COUNT(*) AS units" in sql:
            return FakeQueryJob([])
        return FakeQueryJob([self.ALL_ENROLLED_ROW, self.REGULAR_USER_ROW])

    def get_table(self, table):
        return types.SimpleNamespace(expires=None)

    def update_table(self, table, fields):
        return table


def test_one_failing_segment_source_leaves_the_runs_other_segment_intact(caplog):
    caplog.set_level(logging.ERROR)
    resolved_metrics = {"active_hours": TEST_METRICS[0]}
    segment_names_by_slug = {"a-slug": ("regular_user", "suggest_user")}
    run = sql_generation.Run(
        [EXPERIMENT],
        {"a-slug": analysis.run_windows(EXPERIMENT, AS_OF, TEST_METRICS)},
        AS_OF,
        analysis.COVARIATE_DAYS,
        segments_by_slug=segment_names_by_slug,
    )

    (
        cells_by_slug, units_by_slug, extra_segment_units_by_slug, returned_segment_names,
        timings, failure, source_failures,
    ) = analysis.gather_sufficient_statistics(
        TwoSegmentSourcesOneFailingClient(), run, resolved_metrics, TEST_SOURCES,
        TEST_RESOLVED_SEGMENTS, TEST_SEGMENT_SOURCES, segment_names_by_slug, AS_OF, False,
    )

    assert failure is None
    assert set(source_failures) == {("newtab", "client_id")}
    assert set(cells_by_slug["a-slug"]) == {
        ("a-metric", "week:1", "all_enrolled", "control"),
        ("a-metric", "week:1", "regular_user", "control"),
    }
    assert "segment source 'newtab'" in caplog.text

    segment_failures = analysis.segment_failures_for_experiment(
        EXPERIMENT, returned_segment_names, TEST_RESOLVED_SEGMENTS, source_failures
    )
    windows = analysis.run_windows(EXPERIMENT, AS_OF, TEST_METRICS)
    _, results = analysis.analyze_experiment(
        EXPERIMENT, windows, TEST_METRICS, cells_by_slug["a-slug"], failure,
        analysis.experiment_segment_names(EXPERIMENT, returned_segment_names),
        segment_failures,
    )

    states_by_segment = {result["segment"]: result["state"] for result in results}
    assert states_by_segment["suggest_user"] == ERROR
    assert states_by_segment["regular_user"] != ERROR
    assert states_by_segment["all_enrolled"] != ERROR


def test_segment_failures_for_experiment_scopes_by_source_and_analysis_unit():
    error = RuntimeError("boom")
    source_failures = {("clients_last_seen", "client_id"): error}

    failures = analysis.segment_failures_for_experiment(
        EXPERIMENT,
        {"a-slug": ("regular_user", "suggest_user")},
        TEST_RESOLVED_SEGMENTS,
        source_failures,
    )

    assert failures == {"regular_user": error}


def test_cohort_enrollment_bounds_by_slug_is_none_without_a_materialized_cohort():
    run = sql_generation.Run(
        [EXPERIMENT],
        {"a-slug": analysis.run_windows(EXPERIMENT, AS_OF, TEST_METRICS)},
        AS_OF,
        analysis.COVARIATE_DAYS,
        segments_by_slug={"a-slug": ("regular_user",)},
    )

    assert analysis.cohort_enrollment_bounds_by_slug(object(), run, None) is None


def test_cohort_enrollment_bounds_by_slug_is_none_when_nothing_selects_a_segment():
    run = sql_generation.Run(
        [EXPERIMENT],
        {"a-slug": analysis.run_windows(EXPERIMENT, AS_OF, TEST_METRICS)},
        AS_OF,
        analysis.COVARIATE_DAYS,
    )

    assert analysis.cohort_enrollment_bounds_by_slug(object(), run, "a.cohort_table") is None


class CohortEnrollmentBoundsClient:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    def query(self, sql, job_config=None):
        self.queries.append(sql)
        return FakeQueryJob(self.rows)


def test_cohort_enrollment_bounds_by_slug_reads_the_materialized_cohort():
    run = sql_generation.Run(
        [EXPERIMENT],
        {"a-slug": analysis.run_windows(EXPERIMENT, AS_OF, TEST_METRICS)},
        AS_OF,
        analysis.COVARIATE_DAYS,
        segments_by_slug={"a-slug": ("regular_user",)},
    )
    client = CohortEnrollmentBoundsClient(
        [
            {
                "slug": "a-slug",
                "earliest": datetime.date(2026, 7, 5),
                "latest": datetime.date(2026, 7, 9),
            }
        ]
    )

    bounds = analysis.cohort_enrollment_bounds_by_slug(client, run, "a.cohort_table")

    assert bounds == {"a-slug": (datetime.date(2026, 7, 5), datetime.date(2026, 7, 9))}
    [sql] = client.queries
    assert "'a-slug'" in sql


def test_cohort_enrollment_bounds_by_slug_falls_back_to_none_when_the_query_fails(caplog):
    caplog.set_level(logging.ERROR)
    run = sql_generation.Run(
        [EXPERIMENT],
        {"a-slug": analysis.run_windows(EXPERIMENT, AS_OF, TEST_METRICS)},
        AS_OF,
        analysis.COVARIATE_DAYS,
        segments_by_slug={"a-slug": ("regular_user",)},
    )

    class FailingClient:
        def query(self, sql, job_config=None):
            raise RuntimeError("the bounds query failed")

    bounds = analysis.cohort_enrollment_bounds_by_slug(FailingClient(), run, "a.cohort_table")

    assert bounds is None
    assert "the cohort enrollment bounds query failed" in caplog.text


def test_a_failed_unit_count_keeps_the_cells_the_shared_scan_computed(caplog):
    # The unit counts are display-only, so losing them must not turn every experiment's computed
    # statistics into an error grid.
    resolved_metrics = {"active_hours": TEST_METRICS[0]}
    run = sql_generation.Run(
        [EXPERIMENT],
        {"a-slug": analysis.run_windows(EXPERIMENT, AS_OF, TEST_METRICS)},
        AS_OF,
        analysis.COVARIATE_DAYS,
    )

    (
        cells_by_slug, units_by_slug, extra_segment_units_by_slug, segment_names_by_slug,
        timings, failure, source_failures,
    ) = analysis.gather_sufficient_statistics(
        UnitCountFailingClient(), run, resolved_metrics, TEST_SOURCES, {}, {}, {}, AS_OF, False
    )

    assert failure is None
    assert units_by_slug == {}
    assert list(cells_by_slug) == ["a-slug"]
    assert cells_by_slug["a-slug"][("a-metric", "week:1", "all_enrolled", "control")]["n"] == 10
    assert segment_names_by_slug == {}
    assert source_failures == {}
    assert timings
    assert "the branch unit count failed" in caplog.text


def experiment_selecting(segment_names):
    return Experiment(
        slug="a-slug",
        start_date=datetime.date(2026, 7, 1),
        end_date=None,
        reference_branch="control",
        treatment_branches=("treatment",),
        unit=units.resolve("firefox_desktop", "normandy_id"),
        segments={
            name: SegmentDisplay(friendly_name=None, description=None)
            for name in segment_names
        },
    )


def test_an_experiment_that_selects_no_segment_resolves_an_empty_tuple():
    experiment = experiment_selecting([])

    segment_names_by_slug, _, _ = analysis.resolve_run_segments([experiment], configs=object())

    assert segment_names_by_slug == {"a-slug": ()}


def test_a_resolving_parser_fills_segment_names_and_warns_only_about_what_did_not_resolve(
    monkeypatch, caplog
):
    caplog.set_level(logging.WARNING)

    class FakeSegmentMetric:
        def __init__(self, name, data_source, select_expression):
            self.name = name
            self.data_source = data_source
            self.select_expression = select_expression

    class FakePlan:
        def __init__(self, sources, segments, missing):
            self.sources = sources
            self.segments = segments
            self.missing = missing

    def fake_segment_plan(configs, app_name, names):
        return FakePlan(
            sources={},
            segments={
                "regular_user": FakeSegmentMetric(
                    "regular_user", "clients_last_seen", "COALESCE(LOGICAL_OR(p), FALSE)"
                )
            },
            missing=[name for name in names if name != "regular_user"],
        )

    monkeypatch.setattr(segments, "_segment_plan", fake_segment_plan)
    experiment = experiment_selecting(["regular_user", "an_inline_segment"])

    segment_names_by_slug, resolved_segments, _ = analysis.resolve_run_segments(
        [experiment], configs=object()
    )

    assert segment_names_by_slug == {"a-slug": ("regular_user",)}
    assert set(resolved_segments) == {"regular_user"}
    assert (
        "a-slug: segment 'an_inline_segment' not computed: "
        f"{segments.NOT_RESOLVED_REASON}" in caplog.text
    )
    assert "regular_user' not computed" not in caplog.text


def test_experiment_segment_names_puts_all_enrolled_first():
    experiment = experiment_selecting(["regular_user"])

    names = analysis.experiment_segment_names(
        experiment, {"a-slug": ("regular_user", "new_profile")}
    )

    assert names == ("all_enrolled", "regular_user", "new_profile")


def test_experiment_segment_names_is_just_all_enrolled_for_an_unlisted_slug():
    experiment = experiment_selecting(["regular_user"])

    assert analysis.experiment_segment_names(experiment, {}) == ("all_enrolled",)
