"""Whether a run failed as a whole, which is what the process exit code turns on."""

import datetime

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


def test_the_run_report_counts_cells_and_errors_across_experiments(capsys):
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
    printed = capsys.readouterr().out

    assert "2 experiments (0 failed outright)" in printed
    assert "error rate 25.00%" in printed


def test_the_run_report_says_so_rather_than_dividing_by_zero_cells(capsys):
    analysis.report_run([summary("a", {})], [])
    printed = capsys.readouterr().out

    assert "no cells produced" in printed


def test_an_experiment_whose_cells_are_all_immature_is_reported_as_worth_a_look(capsys):
    analysis.report_anomalies(
        [
            summary("a", {NOT_STARTED: 12}),
            summary("b", {CONFIDENT: 4, FORMING: 8}),
            summary("c", {}),
        ]
    )
    printed = capsys.readouterr().out

    assert "cohort or join empty" in printed
    assert "no cells: no window has matured yet" in printed
    assert "\n   b " not in printed


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
