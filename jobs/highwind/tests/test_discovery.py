"""Window generation and the age refusal, which are pure functions of a run date."""

import datetime
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
    assert [w.label for w in discovery.generate_windows(CUMULATIVE_WEEKLY, 7)] == ["cumu:1"]
    assert [w.label for w in discovery.generate_windows(CUMULATIVE_WEEKLY, 13)] == ["cumu:1"]
    assert [w.label for w in discovery.generate_windows(CUMULATIVE_WEEKLY, 14)] == [
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
    assert {e.slug: e.unit.kind for e in experiments} == {
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
    slug, why = skipped[0]
    assert slug == "a-slug"
    assert "firefox_desktop" in why
    assert "nimbus_id" in why


def test_an_ended_recipe_is_excluded_by_the_query_rather_than_fetched_and_discarded():
    # Asserted on the SQL because that is where the exclusion now lives. Filtering in Python instead
    # would fetch every recently-ended recipe only to drop it, and reporting each one as skipped
    # would bury the refusals the skipped list exists to surface.
    assert "end_date IS NULL" in discovery.DISCOVERY_SQL


def test_a_slug_filter_reports_only_refusals_rather_than_every_recipe_it_passed_over():
    # The slugs were named by the caller, so the rest of the mirror is out of scope rather than
    # declined, and listing it would bury the refusals the list exists to surface.
    experiments, skipped = discovery.discover(
        FakeClient([mirror_row(slug="wanted"), mirror_row(slug="not-wanted")]),
        AS_OF,
        only_slugs=["wanted"],
    )

    assert [e.slug for e in experiments] == ["wanted"]
    assert skipped == []


def test_a_unit_this_app_does_not_declare_is_skipped_even_where_another_app_declares_it():
    experiments, skipped = discovery.discover(
        FakeClient([mirror_row(app_name="fenix", randomization_unit="group_id")]), AS_OF
    )

    assert experiments == []
    assert [why for _, why in skipped if "fenix" in why and "group_id" in why]
