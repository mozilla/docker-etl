"""Window generation and the age refusal, which are pure functions of a run date."""

import datetime
import sqlite3
import types

import pytest

from highwind import discovery, units
from highwind.metrics import CUMULATIVE_WEEKLY, DISJOINT_WEEKLY, Metric, per_unit_sum

AS_OF = datetime.date(2026, 8, 1)


def experiment(start_date, end_date=None):
    return discovery.Experiment(
        slug="a-slug",
        start_date=start_date,
        end_date=end_date,
        reference_branch="control",
        treatment_branches=("treatment-a", "treatment-b"),
        unit=units.resolve("firefox_desktop", "normandy_id"),
    )


def metric(*window_rules):
    return Metric(
        name="a-metric",
        source="clients_daily",
        reducer=per_unit_sum("a_column"),
        window_rules=window_rules,
    )


def test_cumulative_windows_are_labelled_cumu_and_all_start_at_enrollment():
    windows = discovery.generate_windows(CUMULATIVE_WEEKLY, tenure_days=21)

    assert [window.label for window in windows] == ["cumu:1", "cumu:2", "cumu:3"]
    assert [(window.start, window.end) for window in windows] == [(0, 6), (0, 13), (0, 20)]
    assert [window.length for window in windows] == [7, 14, 21]


def test_disjoint_windows_are_labelled_week_and_partition_tenure():
    windows = discovery.generate_windows(DISJOINT_WEEKLY, tenure_days=21)

    assert [window.label for window in windows] == ["week:1", "week:2", "week:3"]
    assert [(window.start, window.end) for window in windows] == [(0, 6), (7, 13), (14, 20)]
    assert [window.length for window in windows] == [7, 7, 7]


def test_a_window_is_generated_only_once_a_unit_could_have_completed_it():
    assert discovery.generate_windows(CUMULATIVE_WEEKLY, tenure_days=6) == []
    assert [window.label for window in discovery.generate_windows(CUMULATIVE_WEEKLY, 7)] == [
        "cumu:1"
    ]
    assert [window.label for window in discovery.generate_windows(CUMULATIVE_WEEKLY, 13)] == [
        "cumu:1"
    ]
    assert [window.label for window in discovery.generate_windows(CUMULATIVE_WEEKLY, 14)] == [
        "cumu:1",
        "cumu:2",
    ]


def test_an_unrecognised_window_kind_is_refused_rather_than_guessed_at():
    with pytest.raises(ValueError):
        discovery.generate_windows({"kind": "rolling", "length": 7}, tenure_days=30)


def test_the_window_series_stops_at_the_horizon_however_long_the_experiment_ran():
    windows = discovery.windows_for(metric(CUMULATIVE_WEEKLY), tenure_days=10 * 365)

    assert len(windows) == discovery.MAX_WINDOW_DAYS // 7
    assert windows[-1].end < discovery.MAX_WINDOW_DAYS


def test_a_metric_declaring_both_families_gets_the_windows_of_each():
    windows = discovery.windows_for(
        metric(CUMULATIVE_WEEKLY, DISJOINT_WEEKLY), tenure_days=14
    )

    assert [window.label for window in windows] == ["cumu:1", "cumu:2", "week:1", "week:2"]


def test_tenure_is_counted_from_the_first_enrollment_to_the_run_date():
    assert experiment(AS_OF - datetime.timedelta(days=30)).tenure_days(AS_OF) == 30
    assert experiment(AS_OF).tenure_days(AS_OF) == 0


def test_a_recipe_is_too_old_from_the_age_limit_onwards():
    limit = discovery.MAX_EXPERIMENT_AGE_DAYS
    day_before = experiment(AS_OF - datetime.timedelta(days=limit - 1))
    on_the_limit = experiment(AS_OF - datetime.timedelta(days=limit))
    day_after = experiment(AS_OF - datetime.timedelta(days=limit + 1))

    assert day_before.too_old(AS_OF) is False
    assert on_the_limit.too_old(AS_OF) is True
    assert day_after.too_old(AS_OF) is True


def test_branches_lists_the_reference_first():
    assert experiment(AS_OF).branches == ("control", "treatment-a", "treatment-b")


def mirror_row(**overrides):
    """One row of the mirror, in the shape `discover` reads."""
    return types.SimpleNamespace(
        **{
            "slug": "a-slug",
            "app_name": "firefox_desktop",
            "randomization_unit": "normandy_id",
            "start_date": AS_OF - datetime.timedelta(days=30),
            "end_date": None,
            "reference_branch": "control",
            "branch_slugs": ["control", "treatment"],
            **overrides,
        }
    )


class FakeClient:
    """Stands in for a BigQuery client returning a fixed mirror result."""

    def __init__(self, rows):
        self.rows = rows

    def query(self, sql, job_config=None):
        return types.SimpleNamespace(result=lambda: self.rows)


def test_an_experiment_is_analysed_at_the_unit_its_own_recipe_randomized_on():
    experiments, skipped = discovery.discover(
        FakeClient(
            [
                mirror_row(slug="grouped", randomization_unit="group_id"),
                mirror_row(slug="by-client", randomization_unit="normandy_id"),
            ]
        ),
        AS_OF,
    )

    assert skipped == []
    assert {experiment.slug: experiment.unit.kind for experiment in experiments} == {
        "grouped": units.PROFILE_GROUP_ID,
        "by-client": units.CLIENT_ID,
    }


def test_a_randomization_unit_with_no_analysis_unit_is_skipped_not_defaulted():
    # The refusal is the point: analysing at a grain the recipe did not randomize on produces
    # intervals that look no different from correct ones.
    experiments, skipped = discovery.discover(
        FakeClient([mirror_row(randomization_unit="nimbus_id")]), AS_OF
    )

    assert experiments == []
    assert len(skipped) == 1
    slug, why, refused = skipped[0]
    assert slug == "a-slug"
    assert refused is None
    assert "firefox_desktop" in why
    assert "nimbus_id" in why


def end_date_clause():
    """The one clause of the discovery query that decides a recipe by its end date."""
    clauses = [
        line.strip().removeprefix("AND ").strip()
        for line in discovery.DISCOVERY_SQL.splitlines()
        if line.strip().startswith("AND ") and "end_date" in line
    ]
    assert len(clauses) == 1, f"expected one end_date clause, found {clauses}"
    return clauses[0]


def selected(end_date, as_of=AS_OF):
    """Whether the query selects a recipe with this end date on `as_of`.

    The decision belongs to the query rather than to `discover`, because filtering in Python would
    fetch every recipe the mirror holds only to drop it. So the clause is read back out of the query
    and evaluated, rather than restated here: a restatement can agree with itself while the query
    says something else. ISO dates order lexicographically, so the comparison means the same thing
    in sqlite as it does in BigQuery.
    """
    clause = end_date_clause().replace("@as_of", f"'{as_of.isoformat()}'")
    with sqlite3.connect(":memory:") as connection:
        rows = connection.execute(
            f"SELECT 1 FROM (SELECT ? AS end_date) WHERE {clause}",
            (end_date.isoformat() if end_date else None,),
        ).fetchall()
    return rows != []


def test_a_live_recipe_is_selected():
    assert selected(None) is True


def test_a_recipe_whose_end_date_is_still_ahead_is_selected():
    assert selected(AS_OF + datetime.timedelta(days=1)) is True
    assert selected(AS_OF + datetime.timedelta(days=30)) is True


def test_a_recipe_is_selected_on_the_run_date_it_ends_on():
    # The run this exists for. Windows mature against the run date, so the tier a recipe's units
    # completed on their last day under treatment is reportable only on a run whose as_of reached
    # the end date, and without this run the frozen result stops a tier short of it.
    assert selected(AS_OF) is True


def test_a_recipe_is_dropped_once_the_run_date_is_past_its_end_date():
    # A window here would reach past the treatment period for some of its units, so it is not a
    # contrast. One day past is enough to drop it, and it stays dropped.
    assert selected(AS_OF - datetime.timedelta(days=1)) is False
    assert selected(AS_OF - datetime.timedelta(days=30)) is False


def test_a_slug_filter_reports_only_refusals_rather_than_every_recipe_it_passed_over():
    # The slugs were named by the caller, so the rest of the mirror is out of scope rather than
    # declined, and listing it would bury the refusals the list exists to surface.
    experiments, skipped = discovery.discover(
        FakeClient([mirror_row(slug="wanted"), mirror_row(slug="not-wanted")]),
        AS_OF,
        only_slugs=["wanted"],
    )

    assert [experiment.slug for experiment in experiments] == ["wanted"]
    assert skipped == []


def test_a_unit_this_app_does_not_declare_is_skipped_even_where_another_app_declares_it():
    experiments, skipped = discovery.discover(
        FakeClient([mirror_row(app_name="fenix", randomization_unit="group_id")]), AS_OF
    )

    assert experiments == []
    assert [why for _, why, _ in skipped if "fenix" in why and "group_id" in why]


def test_a_recipe_refused_for_its_age_carries_the_experiment_its_blob_is_built_from():
    start = AS_OF - datetime.timedelta(days=discovery.MAX_EXPERIMENT_AGE_DAYS)
    experiments, skipped = discovery.discover(
        FakeClient([mirror_row(start_date=start)]), AS_OF
    )

    assert experiments == []
    slug, why, refused = skipped[0]
    assert slug == "a-slug"
    assert "limit" in why
    assert refused.slug == "a-slug"
    assert refused.start_date == start


def test_a_day_one_experiment_reports_the_first_window_of_each_rule_as_upcoming():
    windows = discovery.reported_windows(
        metric(DISJOINT_WEEKLY, CUMULATIVE_WEEKLY), experiment(AS_OF), AS_OF
    )

    assert [(window.kind, window.start, window.end) for window in windows] == [
        ("cumulative", 0, 6),
        ("disjoint", 0, 6),
    ]
    assert not any(
        discovery.has_matured(experiment(AS_OF), window, AS_OF) for window in windows
    )


def test_a_running_experiment_reports_its_matured_windows_and_the_next_one():
    running = experiment(AS_OF - datetime.timedelta(days=15))

    windows = discovery.reported_windows(metric(CUMULATIVE_WEEKLY), running, AS_OF)

    assert [window.end for window in windows] == [6, 13, 20]
    assert [discovery.has_matured(running, window, AS_OF) for window in windows] == [
        True,
        True,
        False,
    ]


def test_an_ended_experiment_reports_no_upcoming_window():
    ended = experiment(AS_OF - datetime.timedelta(days=15), end_date=AS_OF)

    windows = discovery.reported_windows(metric(CUMULATIVE_WEEKLY), ended, AS_OF)

    assert ended.ended(AS_OF) is True
    assert [window.end for window in windows] == [6, 13]


def test_an_end_date_still_ahead_is_not_an_ended_experiment():
    running = experiment(AS_OF - datetime.timedelta(days=15), end_date=AS_OF.replace(day=2))

    assert running.ended(AS_OF) is False


def test_no_upcoming_window_is_reported_past_the_horizon():
    old = experiment(AS_OF - datetime.timedelta(days=300))

    windows = discovery.reported_windows(metric(CUMULATIVE_WEEKLY), old, AS_OF)

    assert len(windows) == discovery.MAX_WINDOW_DAYS // 7
    assert all(discovery.has_matured(old, window, AS_OF) for window in windows)


def test_a_window_matures_the_day_after_its_last_day_since_the_start():
    started = experiment(datetime.date(2026, 7, 1))
    first, second = discovery.generate_windows(CUMULATIVE_WEEKLY, tenure_days=14)

    assert discovery.matures_on(started, first) == datetime.date(2026, 7, 8)
    assert discovery.matures_on(started, second) == datetime.date(2026, 7, 15)
