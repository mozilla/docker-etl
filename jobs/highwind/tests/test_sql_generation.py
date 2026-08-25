"""The bucket grid, the maturity cap, and where a metric's threshold is applied."""

import datetime

import pytest

from highwind import discovery, metrics, sql_generation, units
from highwind.discovery import Experiment, Window

AS_OF = datetime.date(2026, 8, 1)
COVARIATE_DAYS = 28

CLIENT_UNIT = units.resolve("firefox_desktop", "normandy_id")
GROUP_UNIT = units.resolve("firefox_desktop", "group_id")


def experiment(slug, start_date, unit=CLIENT_UNIT):
    return Experiment(
        slug=slug,
        start_date=start_date,
        end_date=None,
        reference_branch="control",
        treatment_branches=("treatment",),
        unit=unit,
    )


def cumulative_windows(count):
    return [
        Window(label=f"cumu:{index}", start=0, end=index * 7 - 1, kind="cumulative")
        for index in range(1, count + 1)
    ]


def disjoint_windows(count):
    return [
        Window(
            label=f"week:{index}",
            start=(index - 1) * 7,
            end=index * 7 - 1,
            kind="disjoint",
        )
        for index in range(1, count + 1)
    ]


def run_of(windows_by_slug, sample_percent=None, units_by_slug=None):
    units_by_slug = units_by_slug or {}
    experiments = [
        experiment(
            slug,
            AS_OF - datetime.timedelta(days=60),
            units_by_slug.get(slug, CLIENT_UNIT),
        )
        for slug in windows_by_slug
    ]
    return sql_generation.Run(
        experiments, windows_by_slug, AS_OF, COVARIATE_DAYS, sample_percent
    )


def named(name):
    return next(metric for metric in metrics.GUARDRAILS if metric.name == name)


def test_a_weekly_window_set_yields_a_weekly_bucket_grid():
    windows = [*cumulative_windows(4), *disjoint_windows(4)]

    assert sql_generation.bucket_length(windows) == 7


def test_disjoint_windows_of_different_lengths_have_no_common_grid():
    windows = [
        Window(label="week:1", start=0, end=6, kind="disjoint"),
        Window(label="fortnight:1", start=7, end=20, kind="disjoint"),
    ]

    with pytest.raises(ValueError):
        sql_generation.bucket_length(windows)


def test_a_cumulative_window_that_does_not_start_at_enrollment_is_refused():
    windows = [Window(label="cumu:2", start=7, end=13, kind="cumulative")]

    with pytest.raises(ValueError):
        sql_generation.bucket_length(windows)


def test_a_cumulative_window_off_the_grid_is_refused():
    windows = [
        *disjoint_windows(1),
        Window(label="cumu:1", start=0, end=9, kind="cumulative"),
    ]

    with pytest.raises(ValueError):
        sql_generation.bucket_length(windows)


def test_each_experiment_carries_the_number_of_buckets_its_windows_reach():
    run = run_of({"three-weeks": cumulative_windows(3), "one-week": disjoint_windows(1)})

    assert run.length == 7
    assert run.buckets_by_slug == {"three-weeks": 3, "one-week": 1}


def test_an_experiment_with_no_matured_window_takes_no_part_in_the_shared_queries():
    run = run_of({"mature": cumulative_windows(2), "too-young": []})

    assert run.slugs == ["mature"]
    assert "too-young" not in run.meta_cte()


def test_the_per_slug_facts_are_inlined_one_row_per_experiment():
    run = run_of({"b-slug": cumulative_windows(2), "a-slug": cumulative_windows(5)})
    meta = run.meta_cte()

    assert "STRUCT('a-slug' AS slug, 5 AS matured_buckets)" in meta
    assert "STRUCT('b-slug' AS slug, 2 AS matured_buckets)" in meta
    assert meta.index("'a-slug'") < meta.index("'b-slug'")


def test_matured_buckets_caps_elapsed_buckets_at_the_experiments_own_horizon():
    run = run_of({"a-slug": cumulative_windows(3)})
    expression = run.matured_buckets()

    assert expression.startswith("LEAST(")
    assert expression.endswith("m.matured_buckets)")
    assert (
        f"DIV(DATE_DIFF(DATE '{AS_OF}', c.enrollment_date, DAY), {run.length})"
        in expression
    )


def test_the_scan_reaches_back_one_covariate_window_before_the_oldest_enrollment():
    run = run_of({"a-slug": cumulative_windows(3)})

    assert run.earliest_start() == AS_OF - datetime.timedelta(days=60)
    assert run.first_source_date() == run.earliest_start() - datetime.timedelta(
        days=COVARIATE_DAYS
    )


def test_a_binary_metrics_bucket_aggregate_carries_no_threshold():
    run = run_of({"a-slug": cumulative_windows(2)})
    sql = sql_generation.bucket_totals_cte([named("retained")], run)

    assert "SUM(pings_aggregated_by_this_row) AS retained__raw" in sql
    assert "> 0" not in sql
    assert "AS INT64" not in sql


def test_a_binary_metric_is_thresholded_once_outside_its_running_total():
    value = sql_generation.window_value(named("retained"), "cumulative")

    assert value.count("AS INT64") == 1
    assert "SUM(IF(g.bucket >= 0, b.retained__raw, NULL)) OVER unit_to_date" in value
    assert value.index("OVER unit_to_date") < value.index("> 0 AS INT64")


def test_a_disjoint_window_reads_one_bucket_and_thresholds_that():
    value = sql_generation.window_value(named("retained_dau"), "disjoint")

    assert value.count("AS INT64") == 1
    assert "OVER" not in value
    assert "b.retained_dau__raw" in value


def test_a_continuous_metric_is_summed_with_no_threshold_at_all():
    value = sql_generation.window_value(named("active_hours"), "cumulative")

    assert "AS INT64" not in value
    assert value == (
        "COALESCE(SUM(IF(g.bucket >= 0, b.active_hours__raw, NULL)) OVER unit_to_date, 0)"
    )


def test_a_metric_that_combines_with_min_takes_a_running_minimum():
    metric = metrics.Metric(
        name="a-metric",
        source="active_users",
        reducer=metrics.per_unit_min_below("days_since_seen", 3, 30),
        window_rules=(metrics.CUMULATIVE_WEEKLY,),
    )
    value = sql_generation.window_value(metric, "cumulative")

    assert "MIN(IF(g.bucket >= 0, b.a-metric__raw, NULL)) OVER unit_to_date" in value
    assert value.count("AS INT64") == 1
    assert value.endswith("< 3 AS INT64)")


def test_the_covariate_is_read_off_the_pre_bucket_and_thresholded_once():
    value = sql_generation.covariate_value(named("retained"))

    assert f"g.bucket = {sql_generation.PRE_BUCKET}" in value
    assert "OVER unit_all" in value
    assert value.count("AS INT64") == 1


def test_a_units_value_defaults_to_the_metrics_no_rows_value():
    below_three = sql_generation.window_value(
        named("active_in_last_3_days_legacy"), "disjoint"
    )

    assert "COALESCE" in below_three
    assert ", 30)" in below_three


def test_the_rollup_keys_every_metric_by_name_so_the_schema_is_metric_independent():
    sql = sql_generation.rollup_select([named("active_hours"), named("retained_dau")])

    assert "STRUCT('active_hours' AS metric" in sql
    assert "STRUCT('retained_dau' AS metric" in sql
    for aggregate in ("n", "sum", "sumsq", "pre_sum", "pre_sumsq", "xp"):
        assert f"AS {aggregate}" in sql
    assert "WHERE bucket >= 0" in sql


def test_the_bucket_grid_only_reaches_as_far_as_a_unit_has_matured():
    run = run_of({"a-slug": cumulative_windows(3)})
    sql = sql_generation.unit_buckets_cte(run)

    assert f"GENERATE_ARRAY({sql_generation.PRE_BUCKET}, {run.matured_buckets()} - 1)" in sql


def test_a_client_randomized_run_samples_the_sources_on_the_clustered_column():
    # The two forms select the same clients but are not interchangeable. A source table is clustered
    # on sample_id, so only the bare column lets BigQuery skip blocks; the cohort has no such column
    # and has to derive it from the id.
    run = run_of({"a-slug": cumulative_windows(2)}, sample_percent=1)

    cohort = sql_generation.cohort_query(run)
    source = sql_generation.source_cte(
        sql_generation.SOURCES["clients_daily"], run, CLIENT_UNIT
    )

    assert "udf.safe_sample_id(unit_id) < 1" in cohort
    assert "AND sample_id < 1" in source


def test_a_group_randomized_run_samples_the_sources_on_the_group_rather_than_the_client():
    # sample_id is hashed from the client id, so using it here would keep some clients of a profile
    # group and drop others, and the run would aggregate partial groups.
    run = run_of(
        {"a-slug": cumulative_windows(2)},
        sample_percent=1,
        units_by_slug={"a-slug": GROUP_UNIT},
    )

    source = sql_generation.source_cte(
        sql_generation.SOURCES["clients_daily"], run, GROUP_UNIT
    )

    assert "udf.safe_sample_id(profile_group_id) < 1" in source
    assert "AND sample_id < 1" not in source


def test_an_unsampled_run_restricts_neither_the_cohort_nor_the_sources():
    run = run_of({"a-slug": cumulative_windows(2)})

    assert "safe_sample_id" not in sql_generation.cohort_query(run)
    for unit in (CLIENT_UNIT, GROUP_UNIT):
        source = sql_generation.source_cte(
            sql_generation.SOURCES["clients_daily"], run, unit
        )

        assert "sample_id <" not in source
        assert "safe_sample_id" not in source


def test_the_sql_and_discovery_build_a_window_label_the_same_way():
    # Two sites construct a window label: discovery declares it, and the rollup SQL rebuilds it from
    # bucket arithmetic. The statistics look a cell up by that label, so if the two disagree every
    # lookup misses and every cell reports `not_started`, which is a total failure that still
    # reports a clean error rate. Driven through the real generator rather than this file's window
    # fixtures, which would only prove the fixtures agree with the SQL.
    for kind, rule in (
        ("cumulative", metrics.CUMULATIVE_WEEKLY),
        ("disjoint", metrics.DISJOINT_WEEKLY),
    ):
        metric = metrics.Metric(
            name="probe",
            source="clients_daily",
            reducer=metrics.per_unit_sum("active_hours_sum"),
            window_rules=(rule,),
        )
        windows = discovery.windows_for(metric, tenure_days=60)

        assert windows, f"the generator emitted no {kind} window to check"
        assert set(sql_generation._PREFIX) == {"cumulative", "disjoint"}
        for window in windows:
            assert window.kind == kind
            assert window.label.startswith(f"{sql_generation._PREFIX[kind]}:")


def test_a_source_query_reads_the_column_the_experiments_unit_names():
    run = run_of({"a-slug": cumulative_windows(2)})
    spec = sql_generation.SOURCES["clients_daily"]

    assert "profile_group_id AS unit_id" in sql_generation.source_cte(
        spec, run, GROUP_UNIT
    )
    assert "client_id AS unit_id" in sql_generation.source_cte(spec, run, CLIENT_UNIT)


def test_the_cohort_picks_each_slugs_own_id_out_of_the_enrollment_event():
    run = run_of(
        {"grouped": cumulative_windows(2), "by-client": cumulative_windows(2)},
        units_by_slug={"grouped": GROUP_UNIT},
    )
    cohort = sql_generation.cohort_query(run)

    assert (
        "STRUCT('grouped' AS slug, 'control' AS branch, 'profile_group_id' AS unit_kind)"
        in cohort
    )
    assert (
        "STRUCT('by-client' AS slug, 'control' AS branch, 'client_id' AS unit_kind)"
        in cohort
    )
    assert "WHEN 'profile_group_id' THEN e.profile_group_id" in cohort
    assert "WHEN 'client_id' THEN e.legacy_telemetry_client_id" in cohort


def test_a_single_unit_run_resolves_the_id_without_a_branch_to_take():
    run = run_of({"a-slug": cumulative_windows(2)})
    cohort = sql_generation.cohort_query(run)

    assert "e.legacy_telemetry_client_id AS unit_id" in cohort
    assert "CASE" not in cohort
    assert "profile_group_id" not in cohort


def test_the_dedup_and_the_branch_cap_both_key_on_the_resolved_unit():
    # A unit seen on two branches is dropped, and the cap keeps a deterministic share of an arm.
    # Both have to be the randomized unit: applied to a client inside a group-randomized experiment
    # they would split groups across the cap and count one contradictory assignment as many.
    run = run_of(
        {"grouped": cumulative_windows(2)}, units_by_slug={"grouped": GROUP_UNIT}
    )
    cohort = sql_generation.cohort_query(run)

    assert "GROUP BY slug, unit_kind, unit_id" in cohort
    assert "HAVING COUNT(DISTINCT branch_slug) = 1" in cohort
    assert "MOD(ABS(FARM_FINGERPRINT(a.unit_id))" in cohort


def test_a_group_randomized_run_drops_groups_that_fan_out_to_a_fleet():
    # At group grain a cloned fleet is one analysis unit carrying an impossible value, and it lands
    # whole in whichever arm its id hashes to, so it has to go before the branch cap sees it.
    run = run_of(
        {"grouped": cumulative_windows(2)}, units_by_slug={"grouped": GROUP_UNIT}
    )
    cohort = sql_generation.cohort_query(run)

    assert f"e.{sql_generation.FLEET_CLIENT_COLUMN}" in cohort
    assert (
        f"OR COUNT(DISTINCT {sql_generation.FLEET_CLIENT_COLUMN}) "
        f"<= {sql_generation.MAX_CLIENTS_PER_GROUP})" in cohort
    )
    assert f"unit_kind != '{units.PROFILE_GROUP_ID}'" in cohort


def test_a_client_randomized_run_counts_no_clients_per_group():
    # The unit already IS the client, so the per-group count is a test of nothing, and the column it
    # would need is not worth carrying through the scan.
    run = run_of({"by-client": cumulative_windows(2)})
    cohort = sql_generation.cohort_query(run)

    assert "COUNT(DISTINCT branch_slug) = 1" in cohort
    assert f"COUNT(DISTINCT {sql_generation.FLEET_CLIENT_COLUMN})" not in cohort
    assert units.PROFILE_GROUP_ID not in cohort


def test_a_mixed_run_applies_the_fleet_filter_to_the_group_slugs_only():
    # One cohort query serves both units, so the exclusion is conditioned on the unit kind rather
    # than on which query it is in.
    run = run_of(
        {"grouped": cumulative_windows(2), "by-client": cumulative_windows(2)},
        units_by_slug={"grouped": GROUP_UNIT},
    )
    cohort = sql_generation.cohort_query(run)

    assert f"COUNT(DISTINCT {sql_generation.FLEET_CLIENT_COLUMN})" in cohort
    assert f"AND (unit_kind != '{units.PROFILE_GROUP_ID}'" in cohort


def test_the_cohort_carries_the_unit_and_each_source_query_reads_only_its_own():
    run = run_of({"a-slug": cumulative_windows(2)})

    assert "a.unit_kind" in sql_generation.cohort_query(run)
    assert "WHERE unit_kind = 'profile_group_id'" in sql_generation.cohort_source(
        "a.cohort_table", GROUP_UNIT
    )
    assert "WHERE unit_kind = 'client_id'" in sql_generation.cohort_source(
        "a.cohort_table", CLIENT_UNIT
    )


def test_a_run_is_split_into_one_query_per_source_and_unit():
    run = run_of(
        {"grouped": cumulative_windows(2), "by-client": cumulative_windows(2)},
        units_by_slug={"grouped": GROUP_UNIT},
    )
    metrics_by_source = {
        "clients_daily": [named("active_hours")],
        "active_users": [named("retained_dau")],
    }
    queries = sql_generation.build_queries(run, metrics_by_source, "a.cohort_table")

    assert set(queries) == {
        "clients_daily/client_id",
        "clients_daily/profile_group_id",
        "active_users/client_id",
        "active_users/profile_group_id",
    }
    assert "'grouped'" in queries["clients_daily/profile_group_id"]
    assert "'grouped'" not in queries["clients_daily/client_id"]


def test_a_run_whose_experiments_all_randomize_alike_stays_one_query_per_source():
    run = run_of({"a-slug": cumulative_windows(2), "b-slug": cumulative_windows(2)})
    queries = sql_generation.build_queries(
        run, {"clients_daily": [named("active_hours")]}, "a.cohort_table"
    )

    assert set(queries) == {"clients_daily/client_id"}


def test_each_units_source_scan_reaches_back_only_as_far_as_its_own_experiments():
    # The sub-runs are what make this true: one unit's oldest experiment must not widen the other's
    # scan, which it would if every source query were built from the whole run.
    windows = cumulative_windows(2)
    experiments = [
        experiment("old-and-grouped", AS_OF - datetime.timedelta(days=200), GROUP_UNIT),
        experiment("new-by-client", AS_OF - datetime.timedelta(days=10), CLIENT_UNIT),
    ]
    run = sql_generation.Run(
        experiments,
        {"old-and-grouped": windows, "new-by-client": windows},
        AS_OF,
        COVARIATE_DAYS,
    )
    by_unit = run.by_unit()

    assert by_unit[GROUP_UNIT].earliest_start() == AS_OF - datetime.timedelta(days=200)
    assert by_unit[CLIENT_UNIT].earliest_start() == AS_OF - datetime.timedelta(days=10)
    assert run.earliest_start() == AS_OF - datetime.timedelta(days=200)
