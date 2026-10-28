"""Resolving a run's metric names against metric-hub's aggregation plan."""

from metric_config_parser.aggregation import BucketedAggregation, Combine
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


def aggregation_metric(
    data_source="clients_daily",
    cumulative_window=7,
    incremental_window=None,
    repeat_windows=True,
):
    return AggregationMetric(
        name="active_hours",
        data_source=data_source,
        aggregation=REDUCER,
        cumulative_window=cumulative_window,
        incremental_window=incremental_window,
        repeat_windows=repeat_windows,
    )


def plan_of(metrics_by_name, missing=()):
    return AggregationPlan(sources={}, metrics=metrics_by_name, missing=list(missing))


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

    resolved, unresolved = metrics.split_resolved_metrics(plan)

    assert resolved == {}
    assert "cumulative_window" in unresolved["active_hours"]


def test_resolve_splits_missing_names_into_unresolved_with_a_reason():
    plan = plan_of({}, missing=["no_such_metric"])

    resolved, unresolved = metrics.split_resolved_metrics(plan)

    assert resolved == {}
    assert "no nimbus/ definition" in unresolved["no_such_metric"]


def test_resolve_reports_an_unsupported_window_the_same_way_as_a_missing_name():
    plan = plan_of({"active_hours": aggregation_metric(repeat_windows=False)})

    resolved, unresolved = metrics.split_resolved_metrics(plan)

    assert resolved == {}
    assert "repeat_windows" in unresolved["active_hours"]


def test_resolve_builds_a_metric_from_a_resolvable_plan_entry():
    plan = plan_of({"active_hours": aggregation_metric()})

    resolved, unresolved = metrics.split_resolved_metrics(plan)

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
