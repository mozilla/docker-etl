"""Whether a run failed as a whole, and what it records about the ways it can fail."""

import datetime
import logging

import pytest

from highwind import analysis, output_writing, units
from highwind.discovery import Experiment
from highwind.gbstats_compute import (
    CONFIDENT,
    ERROR,
    FORMING,
    INSUFFICIENT_DATA,
    NOT_STARTED,
)
from highwind.metrics import metric_definitions

AS_OF = datetime.date(2026, 8, 20)

EXPERIMENT = Experiment(
    slug="a-slug",
    start_date=datetime.date(2026, 7, 1),
    end_date=None,
    reference_branch="control",
    treatment_branches=("treatment-a",),
    unit=units.resolve("firefox_desktop", "normandy_id"),
)


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
    def __init__(self, uploaded, key):
        self.uploaded = uploaded
        self.key = key
        self.metadata = None

    def upload_from_string(self, data, content_type=None):
        self.uploaded.append(self.key)


class RecordingBucket:
    def __init__(self, uploaded):
        self.uploaded = uploaded

    def blob(self, key):
        return RecordingBlob(self.uploaded, key)


class RecordingStorage:
    """Stands in for a GCS client, recording the object names a run published."""

    def __init__(self):
        self.uploaded = []

    def bucket(self, name):
        return RecordingBucket(self.uploaded)


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
        None,
        EXPERIMENT,
        datetime.date(2026, 8, 20),
        [],
        {"clients_daily": None},
        {},
        "the shared scan failed",
        None,
        write_blobs=False,
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


def test_an_experiment_whose_cells_are_all_immature_is_reported_as_worth_a_look(caplog):
    caplog.set_level(logging.INFO)

    analysis.report_anomalies(
        [
            summary("a", {NOT_STARTED: 12}),
            summary("b", {CONFIDENT: 4, FORMING: 8}),
            summary("c", {}),
        ]
    )

    assert "cohort or join empty" in caplog.text
    assert "no cells: no window has matured yet" in caplog.text
    # Each is attributed to the experiment it is about, so a reader looking one up finds the reason
    # against its slug rather than in a line they have to parse. The healthy one is not flagged.
    flagged = [
        record.experiment_slug
        for record in caplog.records
        if record.levelno == logging.WARNING
    ]
    assert flagged == ["a", "c"]


def test_a_refused_recipe_is_recorded_against_its_slug_with_the_reason(caplog):
    client = RecordingClient()

    with analysis.collecting_run_log(AS_OF, client, output_writing.Outputs(), True):
        analysis.report_selection(
            [EXPERIMENT], [("a-refused-slug", "reference=None others=()")], AS_OF
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
            None,
            EXPERIMENT,
            AS_OF,
            [],
            {"clients_daily": None},
            {},
            "the shared scan failed",
            None,
            write_blobs=False,
        )

    failures = [row for row in client.log_rows if row["log_level"] == "ERROR"]
    assert failures
    assert all(row["experiment_slug"] == "a-slug" for row in failures)
    assert all(row["exception_type"] for row in failures)
    assert all(row["exception"] for row in failures)


def test_the_log_is_written_even_when_the_run_it_covers_raises():
    # The run this table is worth most for is the one that died, so the write cannot be a step the
    # run reaches only by finishing.
    client = RecordingClient()

    with pytest.raises(RuntimeError):
        with analysis.collecting_run_log(AS_OF, client, output_writing.Outputs(), True):
            analysis.report_selection([], [("a-slug", "a reason")], AS_OF)
            raise RuntimeError("whatever ended the run")

    assert [row["experiment_slug"] for row in client.log_rows] == [None, "a-slug"]


def test_a_partial_run_writes_no_log_rows_any_more_than_it_writes_results():
    client = RecordingClient()
    write_tables = analysis.writes_tables(
        False, ["a-slug"], None, None, output_writing.Outputs()
    )

    with analysis.collecting_run_log(
        AS_OF, client, output_writing.Outputs(), write_tables
    ):
        analysis.report_selection([EXPERIMENT], [("a-slug", "a reason")], AS_OF)

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


def test_an_experiments_blob_is_written_only_when_the_run_may_publish_one():
    # The gate has to bite where the blob is actually written, which is inside the per-experiment
    # work rather than beside the table write at the end of the run.
    metrics_by_source = metric_definitions()
    windows = analysis.run_windows(EXPERIMENT, AS_OF, metrics_by_source)
    outputs = output_writing.Outputs()
    published, withheld = RecordingStorage(), RecordingStorage()

    for storage, write_blobs in ((published, True), (withheld, False)):
        analysis.analyze_experiment(
            storage,
            EXPERIMENT,
            AS_OF,
            windows,
            metrics_by_source,
            {},
            "the shared scan failed",
            outputs,
            write_blobs=write_blobs,
        )

    assert published.uploaded == ["highwind/a_slug.json"]
    assert withheld.uploaded == []
