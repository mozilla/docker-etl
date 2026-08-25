"""Whether a run failed as a whole, which is what the process exit code turns on."""

import datetime

from highwind import analysis, units
from highwind.discovery import Experiment
from highwind.gbstats_compute import (
    CONFIDENT,
    ERROR,
    FORMING,
    INSUFFICIENT_DATA,
    NOT_STARTED,
)

EXPERIMENT = Experiment(
    slug="a-slug",
    start_date=datetime.date(2026, 7, 1),
    end_date=None,
    reference_branch="control",
    treatment_branches=("treatment-a",),
    unit=units.resolve("firefox_desktop", "normandy_id"),
)


def outcome(slug, states, error=None):
    return dict(
        slug=slug,
        cells=sum(states.values()),
        states=states,
        error=error,
    )


def test_a_run_with_nothing_to_analyse_is_not_a_failure():
    assert analysis.systemic_failure([]) is False


def test_a_run_whose_every_cell_errored_is_a_failure():
    outcomes = [outcome("a", {ERROR: 120}), outcome("b", {ERROR: 84})]

    assert analysis.systemic_failure(outcomes) is True


def test_one_failed_experiment_beside_a_working_one_is_not_a_failure():
    outcomes = [
        outcome("a", {ERROR: 120}, error="RuntimeError: boom"),
        outcome("b", {CONFIDENT: 3, FORMING: 9}),
    ]

    assert analysis.systemic_failure(outcomes) is False


def test_an_experiment_that_failed_before_producing_any_cell_still_counts_as_failed():
    assert analysis.systemic_failure([outcome("a", {}, error="RuntimeError: boom")]) is True


def test_a_quiet_day_where_no_window_has_matured_is_not_a_failure():
    assert analysis.systemic_failure([outcome("a", {}), outcome("b", {})]) is False


def test_a_run_that_produced_only_immature_cells_is_not_a_failure():
    outcomes = [outcome("a", {NOT_STARTED: 40}), outcome("b", {INSUFFICIENT_DATA: 12})]

    assert analysis.systemic_failure(outcomes) is False


def test_it_turns_on_cell_state_rather_than_on_how_many_cells_there_are():
    # A failing experiment emits a full grid of error cells, so a large grid is not evidence of
    # health and a small one is not evidence of failure.
    large_grid_all_errors = outcome("a", {ERROR: 4000})
    one_good_cell = outcome("b", {CONFIDENT: 1})

    assert analysis.systemic_failure([large_grid_all_errors]) is True
    assert analysis.systemic_failure([large_grid_all_errors, one_good_cell]) is False


def test_a_failure_while_recording_a_failure_is_still_returned_not_raised():
    # The one path a run with no failures never reaches, so it needs covering deliberately: a
    # malformed metric set fails the analysis, and then fails the recording of that failure too. It
    # has to come back as an outcome, because raising here escapes through future.result() and costs
    # every other experiment its results.
    outcome, results = analysis.analyze_experiment(
        None,
        EXPERIMENT,
        datetime.date(2026, 8, 20),
        [],
        {"clients_daily": None},
        {},
        "the shared scan failed",
        None,
        dry_run=True,
    )

    assert results == []
    assert outcome["slug"] == "a-slug"
    assert outcome["cells"] == 0
    assert "recording it also failed" in outcome["error"]


def test_the_run_report_counts_cells_and_errors_across_experiments(capsys):
    analysis.report_run(
        [outcome("a", {CONFIDENT: 1, ERROR: 1}), outcome("b", {FORMING: 2})],
        [dict(source="clients_daily", wall_s=1.0, gb_scanned=1.0, slot_hours=1.0, rows=2)],
    )
    printed = capsys.readouterr().out

    assert "2 experiments (0 failed outright)" in printed
    assert "error rate 25.00%" in printed


def test_the_run_report_says_so_rather_than_dividing_by_zero_cells(capsys):
    analysis.report_run([outcome("a", {})], [])
    printed = capsys.readouterr().out

    assert "no cells produced" in printed


def test_an_experiment_whose_cells_are_all_immature_is_reported_as_worth_a_look(capsys):
    analysis.report_anomalies(
        [
            outcome("a", {NOT_STARTED: 12}),
            outcome("b", {CONFIDENT: 4, FORMING: 8}),
            outcome("c", {}),
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
