"""The cell grid, which has to be complete in every state for the run report to mean anything."""

import datetime

import pytest

from highwind import gbstats_compute, metrics, units
from highwind.discovery import Experiment, Window

WINDOWS = [
    Window(label="cumu:1", start=0, end=6, kind="cumulative"),
    Window(label="week:1", start=0, end=6, kind="disjoint"),
    Window(label="week:2", start=7, end=13, kind="disjoint"),
]

CONTINUOUS = metrics.Metric(
    name="active_hours",
    source="clients_daily",
    reducer=metrics.per_unit_sum("active_hours_sum"),
    window_rules=(metrics.CUMULATIVE_WEEKLY,),
)
BINARY = metrics.Metric(
    name="retained_dau",
    source="active_users",
    reducer=metrics.per_unit_any("is_dau"),
    window_rules=(metrics.DISJOINT_WEEKLY,),
)

EXPERIMENT = Experiment(
    slug="a-slug",
    start_date=datetime.date(2026, 7, 1),
    end_date=None,
    reference_branch="control",
    treatment_branches=("treatment-a", "treatment-b"),
    unit=units.resolve("firefox_desktop", "normandy_id"),
)


def branch(units, mean, second_moment, cross_moment):
    """One branch's aggregates, for units whose covariate mean equals their metric mean."""
    return dict(
        n=units,
        sum=units * mean,
        sumsq=units * second_moment,
        pre_sum=units * 1.0,
        pre_sumsq=units * 2.0,
        xp=units * cross_moment,
    )


def test_a_metric_only_gets_the_windows_of_the_families_it_declares():
    continuous = gbstats_compute.windows_for_metric(CONTINUOUS, WINDOWS)
    binary = gbstats_compute.windows_for_metric(BINARY, WINDOWS)

    assert [window.label for window in continuous] == ["cumu:1"]
    assert [window.label for window in binary] == ["week:1", "week:2"]


def test_the_grid_is_one_cell_per_metric_window_and_treatment_branch():
    results = gbstats_compute.compute_statistics(
        EXPERIMENT, [CONTINUOUS, BINARY], WINDOWS, cells={}
    )

    cells = {(result["metric"], result["window"], result["branch"]) for result in results}
    assert len(results) == 6
    assert cells == {
        ("active_hours", "cumu:1", "treatment-a"),
        ("active_hours", "cumu:1", "treatment-b"),
        ("retained_dau", "week:1", "treatment-a"),
        ("retained_dau", "week:1", "treatment-b"),
        ("retained_dau", "week:2", "treatment-a"),
        ("retained_dau", "week:2", "treatment-b"),
    }


def test_a_cell_with_no_aggregates_is_not_started_rather_than_absent():
    results = gbstats_compute.compute_statistics(
        EXPERIMENT, [CONTINUOUS], WINDOWS, cells={}
    )

    assert {result["state"] for result in results} == {gbstats_compute.NOT_STARTED}
    assert all("point" not in result for result in results)


def test_a_failure_marks_every_declared_cell_an_error_so_the_run_can_see_it():
    results = gbstats_compute.compute_statistics(
        EXPERIMENT,
        [CONTINUOUS, BINARY],
        WINDOWS,
        cells={},
        failure=RuntimeError("the source query failed"),
    )

    assert len(results) == 6
    assert {result["state"] for result in results} == {gbstats_compute.ERROR}
    assert all("the source query failed" in result["error"] for result in results)


def test_a_branch_below_the_minimum_units_is_insufficient_data_not_an_error():
    cells = {
        ("active_hours", "cumu:1", "control"): branch(400, 1.0, 2.0, 1.5),
        ("active_hours", "cumu:1", "treatment-a"): branch(1, 1.0, 2.0, 1.5),
    }
    results = {
        result["branch"]: result
        for result in gbstats_compute.compute_statistics(
            EXPERIMENT, [CONTINUOUS], WINDOWS, cells
        )
    }

    assert results["treatment-a"]["state"] == gbstats_compute.INSUFFICIENT_DATA
    assert results["treatment-a"]["n_reference"] == 400
    assert results["treatment-a"]["n_treatment"] == 1
    # Nothing was computed for this branch at all, which is a different state again.
    assert results["treatment-b"]["state"] == gbstats_compute.NOT_STARTED


def test_a_populated_pair_produces_a_relative_interval_around_the_difference():
    cells = {
        ("active_hours", "cumu:1", "control"): branch(1000, 1.0, 2.0, 1.5),
        ("active_hours", "cumu:1", "treatment-a"): branch(1000, 1.1, 2.42, 1.65),
    }
    results = {
        result["branch"]: result
        for result in gbstats_compute.compute_statistics(
            EXPERIMENT, [CONTINUOUS], WINDOWS, cells
        )
    }
    treatment = results["treatment-a"]

    assert treatment["state"] in (gbstats_compute.FORMING, gbstats_compute.CONFIDENT)
    assert treatment["point"] == pytest.approx(10.0, abs=1.0)
    assert treatment["lower"] < treatment["point"] < treatment["upper"]
    assert treatment["theta"] > 0


def test_theta_is_zero_when_the_covariate_never_moves():
    flat = dict(n=40, sum=40.0, sumsq=80.0, pre_sum=0.0, pre_sumsq=0.0, xp=0.0)

    assert gbstats_compute.pooled_theta([flat, flat]) == 0.0


def test_theta_is_the_slope_of_the_metric_on_the_covariate():
    # Two units per branch, each unit's value exactly twice its covariate, so the slope is 2.
    branches = [
        dict(n=2, sum=6.0, sumsq=20.0, pre_sum=3.0, pre_sumsq=5.0, xp=10.0),
        dict(n=2, sum=14.0, sumsq=100.0, pre_sum=7.0, pre_sumsq=25.0, xp=50.0),
    ]

    assert gbstats_compute.pooled_theta(branches) == pytest.approx(2.0)


def test_an_interval_clear_of_zero_is_what_separates_confident_from_forming():
    assert gbstats_compute.excludes_zero({"lower": 0.5, "upper": 2.0}) is True
    assert gbstats_compute.excludes_zero({"lower": -2.0, "upper": -0.5}) is True
    assert gbstats_compute.excludes_zero({"lower": -0.5, "upper": 2.0}) is False


# ----------------------------------------------------- the sequential tuning parameter ----


def test_the_tuning_parameter_reaches_the_test_on_the_scale_gbstats_reads_it_at():
    # gbstats multiplies the mixture variance it derives from this number by its own unit count,
    # which is the two branches summed. A parameter set on any other scale is silently mistuned, and
    # nothing downstream would say so, since it moves width rather than validity.
    test = gbstats_compute.build_t_test(
        gbstats_compute.adjusted(branch(6000, 1.0, 2.0, 1.5), 0.5),
        gbstats_compute.adjusted(branch(4000, 1.0, 2.0, 1.5), 0.5),
    )

    assert test.n == 10000
    assert test.sequential_tuning_parameter == 10000
    assert test.sequential_tuning_parameter != 5000  # gbstats' own default


def test_the_tuning_parameter_holds_still_while_a_cohort_grows():
    # The always-valid guarantee is a property of ONE confidence sequence fixed ahead of the data.
    # Retuning from each run's own unit count would read the narrowest member of a family of
    # sequences at every look, which none of them covers, so the parameter is quantised to hold
    # still from run to run as units mature.
    growing = [700_000, 800_000, 900_000, 1_000_000, 1_200_000]

    assert {gbstats_compute.tuning_parameter(count) for count in growing} == {1_000_000}


def test_the_tuning_parameter_tracks_the_unit_count_across_scales():
    # It has to hold still without going stale: a parameter far above the unit count widens the
    # interval by orders of magnitude, so a fixed constant is not an alternative to the grid.
    for count, expected in (
        (4, 10),
        (900, 1000),
        (20_000, 10_000),
        (40_000, 100_000),
        (60_000_000, 100_000_000),
    ):
        assert gbstats_compute.tuning_parameter(count) == expected

    assert gbstats_compute.tuning_parameter(0) == 1


# ---------------------------------------------------------- theta across every branch ----


def branch_moments(units, post_mean, post_variance, pre_mean, pre_variance, covariance):
    """One branch's six aggregates, written as the moments they encode.

    The branches differ in their covariate mean as well as their slope, which is the case CUPED
    exists for and the case where the choice of theta moves the point estimate rather than only the
    width.
    """
    return dict(
        n=units,
        sum=units * post_mean,
        sumsq=units * post_mean**2 + (units - 1) * post_variance,
        pre_sum=units * pre_mean,
        pre_sumsq=units * pre_mean**2 + (units - 1) * pre_variance,
        xp=units * post_mean * pre_mean + (units - 1) * covariance,
    )


def three_branch_cells(window="cumu:1"):
    """One (metric, window) whose three branches have visibly different covariate slopes."""
    return {
        ("active_hours", window, "control"): branch_moments(
            1000, 1.0, 1.0, 1.0, 1.0, 0.5
        ),
        ("active_hours", window, "treatment-a"): branch_moments(
            1000, 1.5, 1.2, 1.3, 1.4, 0.9
        ),
        ("active_hours", window, "treatment-b"): branch_moments(
            1000, 0.9, 0.8, 0.7, 0.6, 0.2
        ),
    }


def test_every_comparison_of_one_metric_and_window_shares_one_theta():
    results = gbstats_compute.compute_statistics(
        EXPERIMENT, [CONTINUOUS], WINDOWS, three_branch_cells()
    )
    thetas = {result["branch"]: result["theta"] for result in results}

    assert set(thetas) == {"treatment-a", "treatment-b"}
    assert thetas["treatment-a"] == thetas["treatment-b"]


def test_three_branches_pool_a_different_theta_than_either_pair_would():
    # The assertion that pins the change: pooling over all branches is not the same number as
    # pooling over the pair being compared, so a run's intervals move rather than being relabelled.
    cells = three_branch_cells()
    reference = cells[("active_hours", "cumu:1", "control")]
    results = {
        result["branch"]: result
        for result in gbstats_compute.compute_statistics(
            EXPERIMENT, [CONTINUOUS], WINDOWS, cells
        )
    }

    for treatment in ("treatment-a", "treatment-b"):
        candidate = cells[("active_hours", "cumu:1", treatment)]
        pairwise = gbstats_compute.pooled_theta([reference, candidate])
        per_pair = gbstats_compute.sequential_interval(reference, candidate, pairwise)

        assert results[treatment]["theta"] != pytest.approx(pairwise, rel=1e-6)
        assert results[treatment]["point"] != pytest.approx(
            per_pair["point"], rel=1e-6
        )


def test_theta_pools_only_the_arms_that_reported_the_window():
    # A branch with no cell for this window contributes nothing rather than crashing the group.
    cells = three_branch_cells()
    del cells[("active_hours", "cumu:1", "treatment-b")]
    window = next(w for w in WINDOWS if w.label == "cumu:1")

    assert gbstats_compute.window_theta(
        EXPERIMENT, CONTINUOUS, window, cells
    ) == pytest.approx(
        gbstats_compute.pooled_theta(
            [
                cells[("active_hours", "cumu:1", "control")],
                cells[("active_hours", "cumu:1", "treatment-a")],
            ]
        )
    )
    assert gbstats_compute.window_theta(EXPERIMENT, CONTINUOUS, window, {}) == 0.0
