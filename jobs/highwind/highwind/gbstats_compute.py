"""SECTION 4: GBSTATS COMPUTE.

Turns per-branch sufficient statistics into one result per (metric, window, comparison), each in
exactly one state.

Every cell in the expected grid gets a result, including cells nothing was computed for. That is
load-bearing rather than tidy: without it, an experiment whose queries failed writes no rows and
therefore reports a zero error rate, so the worst failure would be the most invisible.
"""

import math

from gbstats.frequentist.tests import SequentialConfig, SequentialTwoSidedTTest
from gbstats.models.statistics import RegressionAdjustedStatistic, SampleMeanStatistic

# A window's estimator needs at least this many matured units per branch to be attempted. Purely
# mechanical: a variance wants more than one observation, and below that the estimator divides by
# zero rather than returning a wide interval.
MIN_UNITS = 2

# The grid the sequential tuning parameter is quantised onto, as a ratio between adjacent points.
# See `tuning_parameter` for why it is quantised at all.
TUNING_GRID = 10

ERROR = "error"
NOT_STARTED = "not_started"
INSUFFICIENT_DATA = "insufficient_data"
FORMING = "forming"
CONFIDENT = "confident"


def compute_statistics(experiment, metrics, windows, cells, failure=None):
    """One result per (metric, window, treatment branch), covering the whole expected grid.

    `failure` short-circuits every cell to `error`, which is how a query-level failure is recorded
    against the experiment's declared cells rather than vanishing.
    """
    results = []
    for metric in metrics:
        for window in windows_for_metric(metric, windows):
            theta = window_theta(experiment, metric, window, cells)
            for treatment in experiment.treatment_branches:
                results.append(
                    compute_cell(
                        experiment, metric, window, treatment, cells, theta, failure
                    )
                )
    return results


def window_theta(experiment, metric, window, cells):
    """Fit one CUPED slope for a (metric, window), pooled over every branch that reported.

    One theta per group rather than one per contrast. Every branch's adjusted mean is then shifted
    by the same coefficient, so the contrasts of an experiment with three or more branches stay
    differences of one quantity and can be read against each other; a slope fitted per pair makes
    each comparison a different adjusted metric under one metric's name. Pooling also fits the slope
    on the whole cohort rather than on a fraction of it, and keeps it independent of which pair is
    being read, which is what stops the adjustment being a function of the contrast it is meant to
    sharpen.
    """
    reported = [
        cells[(metric.name, window.label, branch)]
        for branch in experiment.branches
        if (metric.name, window.label, branch) in cells
    ]
    return pooled_theta(reported)


def compute_cell(experiment, metric, window, treatment, cells, theta, failure):
    """One cell: its state, and its interval when it has one."""
    identity = dict(
        metric=metric.name,
        window=window.label,
        window_kind=window.kind,
        window_start=window.start,
        window_end=window.end,
        branch=treatment,
        reference_branch=experiment.reference_branch,
    )
    if failure:
        return dict(identity, state=ERROR, error=str(failure)[:500])

    reference = cells.get((metric.name, window.label, experiment.reference_branch))
    candidate = cells.get((metric.name, window.label, treatment))
    if reference is None or candidate is None:
        return dict(identity, state=NOT_STARTED)
    if min(reference["n"], candidate["n"]) < MIN_UNITS:
        return dict(
            identity,
            state=INSUFFICIENT_DATA,
            n_reference=reference["n"],
            n_treatment=candidate["n"],
        )

    try:
        interval = sequential_interval(reference, candidate, theta)
    except Exception as error:  # the state IS the error classification
        return dict(
            identity, state=ERROR, error=f"{type(error).__name__}: {error}"[:500]
        )
    if interval["point"] is None:
        return dict(
            identity,
            state=INSUFFICIENT_DATA,
            n_reference=reference["n"],
            n_treatment=candidate["n"],
        )
    state = CONFIDENT if excludes_zero(interval) else FORMING
    return dict(
        identity,
        state=state,
        n_reference=reference["n"],
        n_treatment=candidate["n"],
        **interval,
    )


def sequential_interval(reference, treatment, theta):
    """Compute the always-valid relative interval for one comparison, as percentages.

    The group's one theta is applied to both branches, so the covariate adjustment is identical on
    each side of the contrast and cannot move the difference it is meant to sharpen.
    """
    test = build_t_test(adjusted(reference, theta), adjusted(treatment, theta))
    result = test.compute_result()
    if result.expected is None or result.ci is None:
        return dict(point=None, lower=None, upper=None, theta=theta)
    return dict(
        point=result.expected * 100,
        lower=result.ci[0] * 100,
        upper=result.ci[1] * 100,
        theta=theta,
    )


def build_t_test(reference_statistic, treatment_statistic):
    """Construct the sequential test for one pair of branches.

    gbstats takes a list of pairs and would compare several in one test. Each is built on its own
    here so the tuning parameter can be set from that pair's own combined unit count: a test
    covering several pairs would have to share one parameter across pairs of different sizes, and
    the width penalty for a mistuned parameter is steep. See `tuning_parameter`.
    """
    return SequentialTwoSidedTTest(
        [(reference_statistic, treatment_statistic)],
        SequentialConfig(
            difference_type="relative",
            sequential_tuning_parameter=tuning_parameter(
                reference_statistic.n + treatment_statistic.n
            ),
        ),
    )


def tuning_parameter(units_in_test):
    """Where on the unit scale the confidence sequence is made narrowest, from the units it has.

    An mSPRT is narrowest near one chosen sample size and wider away from it. gbstats reduces this
    number to a mixture variance and only ever uses it multiplied by its own unit count, which is
    the two branches summed rather than either branch alone, so the parameter belongs on that same
    scale: setting it to the summed count puts the sequence within a fraction of a percent of its
    narrowest, while a per-branch count leaves a small avoidable premium and the library default
    leaves a large one at the cohort sizes analysed here.

    Quantised onto a coarse grid rather than taken at the count itself, and the reason is the
    guarantee rather than the width. An always-valid interval is honest at every look because one
    confidence sequence is fixed ahead of the data; re-deriving the parameter from each run's own
    count would instead read, at every look, the narrowest member of a whole family of sequences,
    and that lower envelope is covered by none of them. Since the width is minimised exactly at the
    count, reading it that way is the least conservative choice available rather than an incidental
    one. On a grid the parameter is fixed for a (metric, window) from run to run apart from an
    occasional crossing while the cohort is still growing, and stops moving altogether once
    enrolment closes, in exchange for at most a few percent of width against exact tuning.

    A single fixed constant would be stricter still, and is rejected because the penalty is sharply
    asymmetric: a parameter far above the unit count widens the interval by orders of magnitude,
    where one far below it widens by tens of percent. A grid that tracks the count is never more
    than half a grid step out in either direction, which no constant can promise across the range of
    cohort sizes here.

    Productionising should replace this with the recipe's design-time expected unit count, which is
    fixed before any data is read and removes the residual entirely.
    """
    return TUNING_GRID ** round(math.log(max(units_in_test, 1), TUNING_GRID))


def pooled_theta(branches):
    """Fit the CUPED slope, cov(pre, post) / var(pre), pooled over the branches passed in.

    Falls back to no adjustment when the covariate has no variance, rather than dividing by zero.
    """
    n = sum(branch["n"] for branch in branches)
    if n < 2:
        return 0.0
    sum_post = sum(branch["sum"] for branch in branches)
    sum_pre = sum(branch["pre_sum"] for branch in branches)
    pre_variance = (
        sum(branch["pre_sumsq"] for branch in branches) - sum_pre * sum_pre / n
    ) / (n - 1)
    if pre_variance <= 0:
        return 0.0
    covariance = (
        sum(branch["xp"] for branch in branches) - sum_pre * sum_post / n
    ) / (n - 1)
    return covariance / pre_variance


def adjusted(cell, theta):
    """One branch's sufficient statistics as a CUPED-adjusted sample mean.

    gbstats forms the adjusted mean and variance from these six numbers alone; nothing per-unit is
    needed, which is the property the whole pipeline is built around.
    """
    return RegressionAdjustedStatistic(
        n=cell["n"],
        post_statistic=SampleMeanStatistic(
            n=cell["n"], sum=cell["sum"], sum_squares=cell["sumsq"]
        ),
        pre_statistic=SampleMeanStatistic(
            n=cell["n"], sum=cell["pre_sum"], sum_squares=cell["pre_sumsq"]
        ),
        post_pre_sum_of_products=cell["xp"],
        theta=theta,
    )


def excludes_zero(interval):
    """Report whether the interval lies wholly above or wholly below zero."""
    return interval["lower"] > 0 or interval["upper"] < 0


def windows_for_metric(metric, windows):
    """Select the windows this metric declares, out of the run's full window set."""
    kinds = {rule["kind"] for rule in metric.window_rules}
    return [window for window in windows if window.kind in kinds]
