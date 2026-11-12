"""Resolving a run's metric names against metric-hub's aggregation plan."""

import types

import attr
import pytest
from metric_config_parser.aggregation import (
    Aggregation,
    BucketedAggregation,
    Combine,
    Operator,
    Threshold,
)
from metric_config_parser.aggregation_plan import AggregationMetric, AggregationPlan

from highwind import metrics
from highwind.discovery import Experiment, MetricDisplay
from highwind import units

REDUCER = BucketedAggregation(
    bucket_aggregate="COALESCE(SUM(active_hours_sum), 0)",
    combine=Combine.SUM,
    no_rows="0",
    finalize="{combined}",
)


# `AggregationMetric` as EXP-8004 will shape it once metric-hub's `nimbus/` definitions and
# metric-config-parser both carry the two winsorize fields. Used only to prove
# `split_resolved_metrics` reads them once they exist; today's real `AggregationMetric` (used by
# every other test here, via `aggregation_metric()`) has neither field.
@attr.s(auto_attribs=True, frozen=True)
class FutureAggregationMetric:
    name: str
    data_source: str
    aggregation: object
    cumulative_window: int | None
    incremental_window: int | None
    repeat_windows: bool
    winsorize_fraction: float | None
    winsorize_threshold: float | None


def aggregation_metric(
    data_source="clients_daily",
    cumulative_window=7,
    incremental_window=None,
    repeat_windows=True,
    reducer=REDUCER,
):
    return AggregationMetric(
        name="active_hours",
        data_source=data_source,
        aggregation=reducer,
        cumulative_window=cumulative_window,
        incremental_window=incremental_window,
        repeat_windows=repeat_windows,
    )


def plan_of(metrics_by_name, missing=()):
    return AggregationPlan(sources={}, metrics=metrics_by_name, missing=list(missing))


def metric_definition(aggregation=Aggregation.SUM, threshold=None):
    """Stand in for a metric-hub `MetricDefinition`: `_is_yes_no_aggregation` only ever reads
    `.aggregation` and `.threshold`, so a plain namespace carrying those two is enough."""
    return types.SimpleNamespace(aggregation=aggregation, threshold=threshold)


class FakeConfigs:
    """Stands in for a `ConfigCollection`, resolving each name to a fixed `MetricDefinition`.

    Defaults an unlisted name to a plain SUM with no threshold, so a test that does not care
    about yes/no resolution does not have to set one up.
    """

    def __init__(self, definitions=None):
        self.definitions = definitions or {}

    def get_nimbus_metric_definition(self, name, app_name):
        return self.definitions.get(name, metric_definition())


def test_a_cumulative_window_becomes_a_cumulative_rule():
    rules, reason = metrics.window_rules(aggregation_metric(cumulative_window=7))

    assert rules == ({"kind": "cumulative", "length": 7},)
    assert reason is None


def test_an_incremental_window_becomes_a_disjoint_rule():
    rules, reason = metrics.window_rules(
        aggregation_metric(cumulative_window=None, incremental_window=7)
    )

    assert rules == ({"kind": "disjoint", "length": 7},)
    assert reason is None


def test_both_windows_together_become_both_rules():
    rules, reason = metrics.window_rules(
        aggregation_metric(cumulative_window=14, incremental_window=7)
    )

    assert rules == (
        {"kind": "cumulative", "length": 14},
        {"kind": "disjoint", "length": 7},
    )
    assert reason is None


def test_no_window_at_all_is_unsupported():
    rules, reason = metrics.window_rules(
        aggregation_metric(cumulative_window=None, incremental_window=None)
    )

    assert rules is None
    assert "no window" in reason


def test_a_non_repeating_window_is_unsupported():
    # Highwind's window rules always repeat for the life of the experiment; a metric that declares
    # a single window that stops has no equivalent rule.
    rules, reason = metrics.window_rules(aggregation_metric(repeat_windows=False))

    assert rules is None
    assert "repeat_windows" in reason


def test_an_incremental_window_that_is_not_one_bucket_is_reported_not_raised():
    rules, reason = metrics.window_rules(
        aggregation_metric(cumulative_window=None, incremental_window=14)
    )

    assert rules is None
    assert "incremental_window" in reason


def test_a_cumulative_window_that_is_not_a_multiple_of_the_bucket_is_reported():
    rules, reason = metrics.window_rules(aggregation_metric(cumulative_window=10))

    assert rules is None
    assert "cumulative_window" in reason


def test_a_cumulative_window_that_is_a_multiple_of_the_bucket_is_accepted():
    rules, reason = metrics.window_rules(aggregation_metric(cumulative_window=14))

    assert rules == ({"kind": "cumulative", "length": 14},)
    assert reason is None


def test_resolve_reports_a_misfit_window_the_same_way_as_repeat_windows_false():
    plan = plan_of({"active_hours": aggregation_metric(cumulative_window=10)})

    resolved, unresolved = metrics.split_resolved_metrics(plan, FakeConfigs())

    assert resolved == {}
    assert "cumulative_window" in unresolved["active_hours"]


def test_resolve_splits_missing_names_into_unresolved_with_a_reason():
    plan = plan_of({}, missing=["no_such_metric"])

    resolved, unresolved = metrics.split_resolved_metrics(plan, FakeConfigs())

    assert resolved == {}
    assert "no nimbus/ definition" in unresolved["no_such_metric"]


def test_resolve_reports_an_unsupported_window_the_same_way_as_a_missing_name():
    plan = plan_of({"active_hours": aggregation_metric(repeat_windows=False)})

    resolved, unresolved = metrics.split_resolved_metrics(plan, FakeConfigs())

    assert resolved == {}
    assert "repeat_windows" in unresolved["active_hours"]


def test_resolve_builds_a_metric_from_a_resolvable_plan_entry():
    plan = plan_of({"active_hours": aggregation_metric()})

    resolved, unresolved = metrics.split_resolved_metrics(plan, FakeConfigs())

    assert unresolved == {}
    metric = resolved["active_hours"]
    assert metric.name == "active_hours"
    assert metric.source == "clients_daily"
    assert metric.reducer is REDUCER
    assert metric.window_rules == ({"kind": "cumulative", "length": 7},)
    assert metric.friendly_name is None
    assert metric.description is None


def test_group_by_source_groups_resolved_metrics_by_their_data_source():
    resolved = {
        "active_hours": metrics.Metric(
            name="active_hours",
            source="clients_daily",
            reducer=REDUCER,
            window_rules=(metrics.CUMULATIVE_WEEKLY,),
        ),
        "retained_dau": metrics.Metric(
            name="retained_dau",
            source="active_users",
            reducer=REDUCER,
            window_rules=(metrics.DISJOINT_WEEKLY,),
        ),
        "uri_count": metrics.Metric(
            name="uri_count",
            source="clients_daily",
            reducer=REDUCER,
            window_rules=(metrics.CUMULATIVE_WEEKLY,),
        ),
    }

    by_source = metrics.group_by_source(resolved)

    assert {metric.name for metric in by_source["clients_daily"]} == {
        "active_hours",
        "uri_count",
    }
    assert {metric.name for metric in by_source["active_users"]} == {"retained_dau"}


def experiment_with(metric_names, display=None):
    return Experiment(
        slug="a-slug",
        start_date=None,
        end_date=None,
        reference_branch="control",
        treatment_branches=("treatment",),
        unit=units.resolve("firefox_desktop", "normandy_id"),
        metrics={
            name: (display or {}).get(
                name, MetricDisplay(friendly_name=None, description=None, bigger_is_better=None)
            )
            for name in metric_names
        },
    )


def test_for_experiment_keeps_only_the_names_it_asked_for():
    resolved = {
        "active_hours": metrics.Metric(
            name="active_hours",
            source="clients_daily",
            reducer=REDUCER,
            window_rules=(metrics.CUMULATIVE_WEEKLY,),
        ),
        "retained_dau": metrics.Metric(
            name="retained_dau",
            source="active_users",
            reducer=REDUCER,
            window_rules=(metrics.DISJOINT_WEEKLY,),
        ),
    }
    experiment = experiment_with(["active_hours"])

    chosen, problems = metrics.metrics_for_experiment(experiment, resolved, {})

    assert [metric.name for metric in chosen] == ["active_hours"]
    assert problems == []


def test_for_experiment_attaches_its_own_display_text():
    resolved = {
        "active_hours": metrics.Metric(
            name="active_hours",
            source="clients_daily",
            reducer=REDUCER,
            window_rules=(metrics.CUMULATIVE_WEEKLY,),
        )
    }
    display = MetricDisplay(
        friendly_name="Active hours", description="Time with input.", bigger_is_better=True
    )
    experiment = experiment_with(["active_hours"], display={"active_hours": display})

    chosen, _ = metrics.metrics_for_experiment(experiment, resolved, {})

    assert chosen[0].friendly_name == "Active hours"
    assert chosen[0].description == "Time with input."


def test_for_experiment_reports_a_name_it_asked_for_that_did_not_resolve():
    experiment = experiment_with(["an_outcome_metric"])

    chosen, problems = metrics.metrics_for_experiment(
        experiment, {}, {"an_outcome_metric": "no nimbus/ definition for this app"}
    )

    assert chosen == []
    assert len(problems) == 1
    assert problems[0].name == "an_outcome_metric"
    assert "no nimbus/ definition" in problems[0].reason


# ------------------------------------------------------------------------- winsorization ----


def test_todays_aggregation_metric_resolves_with_no_winsorize_fields_set():
    # EXP-8004 has not shipped, so today's real AggregationMetric carries neither field, and
    # split_resolved_metrics reads that absence as None rather than raising.
    plan = plan_of({"active_hours": aggregation_metric()})

    resolved, _ = metrics.split_resolved_metrics(plan, FakeConfigs())

    assert resolved["active_hours"].winsorize_fraction is None
    assert resolved["active_hours"].winsorize_threshold is None


def test_split_resolved_metrics_reads_the_winsorize_fields_once_they_exist():
    plan = plan_of(
        {
            "active_hours": FutureAggregationMetric(
                name="active_hours",
                data_source="clients_daily",
                aggregation=REDUCER,
                cumulative_window=7,
                incremental_window=None,
                repeat_windows=True,
                winsorize_fraction=0.01,
                winsorize_threshold=None,
            )
        }
    )

    resolved, _ = metrics.split_resolved_metrics(plan, FakeConfigs())

    assert resolved["active_hours"].winsorize_fraction == 0.01
    assert resolved["active_hours"].winsorize_threshold is None


def test_for_experiment_carries_the_winsorize_fields_forward():
    resolved = {
        "active_hours": metrics.Metric(
            name="active_hours",
            source="clients_daily",
            reducer=REDUCER,
            window_rules=(metrics.CUMULATIVE_WEEKLY,),
            winsorize_fraction=0.02,
        )
    }
    experiment = experiment_with(["active_hours"])

    chosen, _ = metrics.metrics_for_experiment(experiment, resolved, {})

    assert chosen[0].winsorize_fraction == 0.02
    assert chosen[0].winsorize_threshold is None


def metric_with(is_yes_no=False, winsorize_fraction=None, winsorize_threshold=None):
    return metrics.Metric(
        name="a-metric",
        source="clients_daily",
        reducer=REDUCER,
        window_rules=(metrics.CUMULATIVE_WEEKLY,),
        winsorize_fraction=winsorize_fraction,
        winsorize_threshold=winsorize_threshold,
        is_yes_no=is_yes_no,
    )


# --------------------------------------------------------------- yes/no aggregation detection ----


def test_a_plain_sum_is_not_a_yes_no_aggregation():
    assert metrics._is_yes_no_aggregation(metric_definition(Aggregation.SUM)) is False


def test_a_plain_count_is_not_a_yes_no_aggregation():
    assert metrics._is_yes_no_aggregation(metric_definition(Aggregation.COUNT)) is False


def test_count_where_is_not_a_yes_no_aggregation_on_its_own():
    assert metrics._is_yes_no_aggregation(metric_definition(Aggregation.COUNT_WHERE)) is False


def test_any_is_a_yes_no_aggregation():
    assert metrics._is_yes_no_aggregation(metric_definition(Aggregation.ANY)) is True


def test_recency_within_is_a_yes_no_aggregation():
    assert metrics._is_yes_no_aggregation(metric_definition(Aggregation.RECENCY_WITHIN)) is True


def test_a_threshold_on_any_aggregation_makes_it_yes_no():
    threshold = Threshold(op=Operator.GT, value=0)

    assert (
        metrics._is_yes_no_aggregation(metric_definition(Aggregation.COUNT, threshold)) is True
    )


def test_a_summed_metric_whose_finalize_contains_a_cast_is_still_continuous():
    # EXP-8013 proposes a `cast_as_int` option that would wrap a plain sum's finalize in
    # CAST(... AS INT64), the same SQL shape a real yes/no metric's comparison-cast produces. The
    # structured fields this job reads instead, aggregation=SUM and no threshold, are unaffected
    # by what the rendered SQL happens to contain.
    cast_as_int_reducer = BucketedAggregation(
        bucket_aggregate="SUM(active_hours_sum)",
        combine=Combine.SUM,
        no_rows="0",
        finalize="CAST({combined} AS INT64)",
    )
    plan = plan_of({"active_hours": aggregation_metric(reducer=cast_as_int_reducer)})
    configs = FakeConfigs({"active_hours": metric_definition(Aggregation.SUM, threshold=None)})

    resolved, _ = metrics.split_resolved_metrics(plan, configs)

    assert resolved["active_hours"].is_yes_no is False
    assert metrics.winsorize_mode(resolved["active_hours"]).mode == metrics.WinsorizeMode.FRACTION


def test_split_resolved_metrics_reads_is_yes_no_off_the_metric_definition():
    plan = plan_of({"is_default_browser": aggregation_metric()})
    configs = FakeConfigs({"is_default_browser": metric_definition(Aggregation.ANY)})

    resolved, _ = metrics.split_resolved_metrics(plan, configs)

    assert resolved["is_default_browser"].is_yes_no is True


def test_for_experiment_carries_is_yes_no_forward():
    resolved = {
        "is_default_browser": metrics.Metric(
            name="is_default_browser",
            source="clients_daily",
            reducer=REDUCER,
            window_rules=(metrics.CUMULATIVE_WEEKLY,),
            is_yes_no=True,
        )
    }
    experiment = experiment_with(["is_default_browser"])

    chosen, _ = metrics.metrics_for_experiment(experiment, resolved, {})

    assert chosen[0].is_yes_no is True


# --------------------------------------------------------------------------- winsorize_mode ----


def test_an_unset_metric_defaults_to_the_jobs_own_fraction():
    setting = metrics.winsorize_mode(metric_with())

    assert setting == metrics.Winsorize(
        metrics.WinsorizeMode.FRACTION, metrics.DEFAULT_WINSORIZE_FRACTION
    )


def test_an_explicit_zero_fraction_is_off():
    setting = metrics.winsorize_mode(metric_with(winsorize_fraction=0))

    assert setting == metrics.Winsorize(metrics.WinsorizeMode.OFF)


def test_an_explicit_fraction_is_used_as_is():
    setting = metrics.winsorize_mode(metric_with(winsorize_fraction=0.02))

    assert setting == metrics.Winsorize(metrics.WinsorizeMode.FRACTION, 0.02)


def test_a_fixed_threshold_is_used_as_is():
    setting = metrics.winsorize_mode(metric_with(winsorize_threshold=500))

    assert setting == metrics.Winsorize(metrics.WinsorizeMode.FIXED, 500)


def test_setting_both_fields_is_refused():
    with pytest.raises(ValueError):
        metrics.winsorize_mode(
            metric_with(winsorize_fraction=0.01, winsorize_threshold=500)
        )


def test_a_yes_no_metric_is_never_clipped_even_with_a_fraction_set():
    setting = metrics.winsorize_mode(metric_with(is_yes_no=True, winsorize_fraction=0.02))

    assert setting == metrics.Winsorize(metrics.WinsorizeMode.OFF)


def test_a_yes_no_metric_is_never_clipped_even_with_a_threshold_set():
    setting = metrics.winsorize_mode(metric_with(is_yes_no=True, winsorize_threshold=500))

    assert setting == metrics.Winsorize(metrics.WinsorizeMode.OFF)
