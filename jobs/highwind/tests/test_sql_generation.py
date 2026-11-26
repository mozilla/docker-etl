"""The bucket grid, the maturity cap, and where a metric's threshold is applied."""

import dataclasses
import datetime

import pytest
from metric_config_parser.aggregation import BucketedAggregation, Combine
from metric_config_parser.aggregation_plan import AggregationSource

from highwind import discovery, metrics, segments, sql_generation, units
from highwind.discovery import Experiment, Window

AS_OF = datetime.date(2026, 8, 1)
COVARIATE_DAYS = 28

CLIENT_UNIT = units.resolve("firefox_desktop", "normandy_id")
GROUP_UNIT = units.resolve("firefox_desktop", "group_id")

CLIENTS_DAILY_SOURCE = AggregationSource(
    data_source="clients_daily",
    from_expression="mozdata.telemetry.clients_daily",
    client_id_column="client_id",
    group_id_column="profile_group_id",
    analysis_units=("client_id", "profile_group_id"),
    submission_date_column="submission_date",
    columns=("active_hours_sum", "pings_aggregated_by_this_row"),
)
ACTIVE_USERS_SOURCE = AggregationSource(
    data_source="active_users",
    from_expression=(
        "(\n    SELECT * FROM mozdata.telemetry.desktop_active_users\n    WHERE is_desktop\n)"
    ),
    client_id_column="client_id",
    group_id_column="profile_group_id",
    analysis_units=("client_id", "profile_group_id"),
    submission_date_column="submission_date",
    columns=("is_dau", "days_active_bits"),
)


def reducer(bucket_aggregate, combine=Combine.SUM, no_rows="0", finalize="{combined}"):
    return BucketedAggregation(
        bucket_aggregate=bucket_aggregate, combine=combine, no_rows=no_rows, finalize=finalize
    )


# The handful of desktop guardrail shapes these tests exercise, built directly rather than read off
# metric-hub: these tests are about sql_generation.py's own composition of a reducer into the bucket
# grid, not about what metric-hub happens to return today.
FIXTURE_METRICS = {
    "active_hours": metrics.Metric(
        name="active_hours",
        source="clients_daily",
        reducer=reducer("SUM(active_hours_sum)"),
        window_rules=(metrics.CUMULATIVE_WEEKLY,),
    ),
    "retained": metrics.Metric(
        name="retained",
        source="clients_daily",
        reducer=reducer(
            "SUM(pings_aggregated_by_this_row)", finalize="CAST({combined} > 0 AS INT64)"
        ),
        window_rules=(metrics.DISJOINT_WEEKLY,),
        is_yes_no=True,
    ),
    "retained_dau": metrics.Metric(
        name="retained_dau",
        source="active_users",
        reducer=reducer("COUNTIF(is_dau)", finalize="CAST({combined} > 0 AS INT64)"),
        window_rules=(metrics.DISJOINT_WEEKLY,),
        is_yes_no=True,
    ),
    "active_in_last_3_days_legacy": metrics.Metric(
        name="active_in_last_3_days_legacy",
        source="active_users",
        reducer=reducer(
            "MIN(mozfun.bits28.days_since_seen(days_active_bits))",
            combine=Combine.MIN,
            no_rows="30",
            finalize="CAST({combined} < 3 AS INT64)",
        ),
        window_rules=(metrics.DISJOINT_WEEKLY,),
        is_yes_no=True,
    ),
}


def experiment(slug, start_date, unit=CLIENT_UNIT, metric_names=()):
    return Experiment(
        slug=slug,
        start_date=start_date,
        end_date=None,
        reference_branch="control",
        treatment_branches=("treatment",),
        unit=unit,
        metrics={
            name: discovery.MetricDisplay(
                friendly_name=None, description=None, bigger_is_better=None
            )
            for name in metric_names
        },
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


def run_of(
    windows_by_slug,
    sample_percent=None,
    units_by_slug=None,
    metric_names_by_slug=None,
    segments_by_slug=None,
):
    units_by_slug = units_by_slug or {}
    metric_names_by_slug = metric_names_by_slug or {}
    experiments = [
        experiment(
            slug,
            AS_OF - datetime.timedelta(days=60),
            units_by_slug.get(slug, CLIENT_UNIT),
            metric_names_by_slug.get(slug, ()),
        )
        for slug in windows_by_slug
    ]
    return sql_generation.Run(
        experiments,
        windows_by_slug,
        AS_OF,
        COVARIATE_DAYS,
        sample_percent,
        segments_by_slug=segments_by_slug,
    )


def named(name):
    return FIXTURE_METRICS[name]


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
    assert "too-young" not in run.experiment_meta_cte()


def test_the_per_slug_facts_are_inlined_one_row_per_experiment():
    run = run_of({"b-slug": cumulative_windows(2), "a-slug": cumulative_windows(5)})
    meta = run.experiment_meta_cte()

    assert "STRUCT('a-slug' AS slug, 5 AS matured_buckets)" in meta
    assert "STRUCT('b-slug' AS slug, 2 AS matured_buckets)" in meta
    assert meta.index("'a-slug'") < meta.index("'b-slug'")


def test_matured_buckets_caps_elapsed_buckets_at_the_experiments_own_horizon():
    run = run_of({"a-slug": cumulative_windows(3)})
    expression = run.matured_buckets()

    assert expression.startswith("LEAST(")
    assert expression.endswith("experiment_meta.matured_buckets)")
    assert (
        f"DIV(DATE_DIFF(DATE '{AS_OF}', cohort.enrollment_date, DAY), {run.length})"
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
    assert (
        "SUM(IF(grid.bucket >= 0, bucket_total.retained__raw, NULL)) OVER unit_to_date"
        in value
    )
    assert value.index("OVER unit_to_date") < value.index("> 0 AS INT64")


def test_a_disjoint_window_reads_one_bucket_and_thresholds_that():
    value = sql_generation.window_value(named("retained_dau"), "disjoint")

    assert value.count("AS INT64") == 1
    assert "OVER" not in value
    assert "bucket_total.retained_dau__raw" in value


def test_a_continuous_metric_is_summed_with_no_threshold_at_all():
    value = sql_generation.window_value(named("active_hours"), "cumulative")

    assert "AS INT64" not in value
    assert value == (
        "COALESCE(SUM(IF(grid.bucket >= 0, bucket_total.active_hours__raw, NULL))"
        " OVER unit_to_date, 0)"
    )


def test_a_metric_that_combines_with_min_takes_a_running_minimum():
    metric = metrics.Metric(
        name="a-metric",
        source="active_users",
        reducer=reducer(
            "MIN(days_since_seen)",
            combine=Combine.MIN,
            no_rows="30",
            finalize="CAST({combined} < 3 AS INT64)",
        ),
        window_rules=(metrics.CUMULATIVE_WEEKLY,),
    )
    value = sql_generation.window_value(metric, "cumulative")

    assert (
        "MIN(IF(grid.bucket >= 0, bucket_total.a-metric__raw, NULL)) OVER unit_to_date"
        in value
    )
    assert value.count("AS INT64") == 1
    assert value.endswith("< 3 AS INT64)")


def test_the_covariate_is_read_off_the_pre_bucket_and_thresholded_once():
    value = sql_generation.covariate_value(named("retained"))

    assert f"grid.bucket = {sql_generation.PRE_BUCKET}" in value
    assert "OVER unit_all" in value
    assert value.count("AS INT64") == 1


def test_a_units_value_defaults_to_the_metrics_no_rows_value():
    below_three = sql_generation.window_value(
        named("active_in_last_3_days_legacy"), "disjoint"
    )

    assert "COALESCE" in below_three
    assert ", 30)" in below_three


def test_the_rollup_keys_every_metric_by_name_so_the_schema_is_metric_independent():
    # Both metrics explicitly off, so this stays a test of the schema rather than of winsorization.
    run = run_of({"a-slug": cumulative_windows(2)})
    off_active_hours = dataclasses.replace(named("active_hours"), winsorize_quantile=1)
    sql = sql_generation.rollup_select([off_active_hours, named("retained_dau")], run)

    assert "STRUCT('active_hours' AS metric" in sql
    assert "STRUCT('retained_dau' AS metric" in sql
    for aggregate in (
        "n",
        "sum",
        "sum_squares",
        "pre_sum",
        "pre_sum_squares",
        "sum_x_pre",
        "winsorize_mode",
        "post_winsorize_threshold",
        "pre_winsorize_threshold",
        "clipped_share",
    ):
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
        CLIENTS_DAILY_SOURCE, run, CLIENT_UNIT
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
        CLIENTS_DAILY_SOURCE, run, GROUP_UNIT
    )

    assert "udf.safe_sample_id(profile_group_id) < 1" in source
    assert "AND sample_id < 1" not in source


def test_an_unsampled_run_restricts_neither_the_cohort_nor_the_sources():
    run = run_of({"a-slug": cumulative_windows(2)})

    assert "safe_sample_id" not in sql_generation.cohort_query(run)
    for unit in (CLIENT_UNIT, GROUP_UNIT):
        source = sql_generation.source_cte(
            CLIENTS_DAILY_SOURCE, run, unit
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
            reducer=None,
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
    source_definition = CLIENTS_DAILY_SOURCE

    assert "profile_group_id AS unit_id" in sql_generation.source_cte(
        source_definition, run, GROUP_UNIT
    )
    assert "client_id AS unit_id" in sql_generation.source_cte(
        source_definition, run, CLIENT_UNIT
    )


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
    assert "WHEN 'profile_group_id' THEN event.profile_group_id" in cohort
    assert "WHEN 'client_id' THEN event.legacy_telemetry_client_id" in cohort


def test_a_single_unit_run_resolves_the_id_without_a_branch_to_take():
    run = run_of({"a-slug": cumulative_windows(2)})
    cohort = sql_generation.cohort_query(run)

    assert "event.legacy_telemetry_client_id AS unit_id" in cohort
    assert "CASE" not in cohort
    assert "profile_group_id" not in cohort


def test_the_dedup_and_the_branch_cap_both_key_on_the_resolved_unit():
    # A unit seen on two branches is dropped, and the cap keeps a deterministic share of a branch.
    # Both have to be the randomized unit: applied to a client inside a group-randomized experiment
    # they would split groups across the cap and count one contradictory assignment as many.
    run = run_of(
        {"grouped": cumulative_windows(2)}, units_by_slug={"grouped": GROUP_UNIT}
    )
    cohort = sql_generation.cohort_query(run)

    assert "GROUP BY slug, unit_kind, unit_id" in cohort
    assert "HAVING COUNT(DISTINCT branch_slug) = 1" in cohort
    assert "MOD(ABS(FARM_FINGERPRINT(assignment.unit_id))" in cohort


def test_the_cohort_carries_the_unit_and_each_source_query_reads_only_its_own():
    run = run_of({"a-slug": cumulative_windows(2)})

    assert "assignment.unit_kind" in sql_generation.cohort_query(run)
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
        metric_names_by_slug={
            "grouped": ("active_hours", "retained_dau"),
            "by-client": ("active_hours", "retained_dau"),
        },
    )
    resolved_metrics = {
        "active_hours": named("active_hours"),
        "retained_dau": named("retained_dau"),
    }
    sources = {"clients_daily": CLIENTS_DAILY_SOURCE, "active_users": ACTIVE_USERS_SOURCE}
    queries = sql_generation.build_queries(run, resolved_metrics, sources, "a.cohort_table")

    assert set(queries) == {
        "clients_daily/client_id",
        "clients_daily/profile_group_id",
        "active_users/client_id",
        "active_users/profile_group_id",
    }
    assert "'grouped'" in queries["clients_daily/profile_group_id"]
    assert "'grouped'" not in queries["clients_daily/client_id"]


def test_a_run_whose_experiments_all_randomize_alike_stays_one_query_per_source():
    run = run_of(
        {"a-slug": cumulative_windows(2), "b-slug": cumulative_windows(2)},
        metric_names_by_slug={"a-slug": ("active_hours",), "b-slug": ("active_hours",)},
    )
    queries = sql_generation.build_queries(
        run,
        {"active_hours": named("active_hours")},
        {"clients_daily": CLIENTS_DAILY_SOURCE},
        "a.cohort_table",
    )

    assert set(queries) == {"clients_daily/client_id"}


def test_a_source_used_only_at_one_unit_builds_no_query_for_the_other():
    run = run_of(
        {"grouped": cumulative_windows(2), "by-client": cumulative_windows(2)},
        units_by_slug={"grouped": GROUP_UNIT},
        metric_names_by_slug={
            "grouped": ("retained_dau",),
            "by-client": ("active_hours",),
        },
    )
    resolved_metrics = {
        "active_hours": named("active_hours"),
        "retained_dau": named("retained_dau"),
    }
    sources = {"clients_daily": CLIENTS_DAILY_SOURCE, "active_users": ACTIVE_USERS_SOURCE}

    queries = sql_generation.build_queries(run, resolved_metrics, sources, "a.cohort_table")

    assert set(queries) == {"active_users/profile_group_id", "clients_daily/client_id"}


def test_each_units_query_carries_only_its_own_experiments_metrics():
    run = run_of(
        {"grouped": cumulative_windows(2), "by-client": cumulative_windows(2)},
        units_by_slug={"grouped": GROUP_UNIT},
        metric_names_by_slug={
            "grouped": ("active_hours",),
            "by-client": ("active_hours", "retained"),
        },
    )
    resolved_metrics = {"active_hours": named("active_hours"), "retained": named("retained")}
    sources = {"clients_daily": CLIENTS_DAILY_SOURCE}

    queries = sql_generation.build_queries(run, resolved_metrics, sources, "a.cohort_table")

    assert "retained__raw" not in queries["clients_daily/profile_group_id"]
    assert "retained__raw" in queries["clients_daily/client_id"]


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


def test_selecting_scopes_a_run_to_the_experiments_that_select_a_given_metric():
    windows = cumulative_windows(2)
    selecting = experiment(
        "selecting", AS_OF - datetime.timedelta(days=200), CLIENT_UNIT, ("active_hours",)
    )
    not_selecting = experiment(
        "not-selecting", AS_OF - datetime.timedelta(days=10), CLIENT_UNIT, ("retained_dau",)
    )
    run = sql_generation.Run(
        [selecting, not_selecting],
        {"selecting": windows, "not-selecting": windows},
        AS_OF,
        COVARIATE_DAYS,
    )

    scoped = run.selecting({"active_hours"})

    assert scoped.slugs == ["selecting"]
    assert scoped.earliest_start() == AS_OF - datetime.timedelta(days=200)


def test_a_source_selected_by_the_runs_earliest_experiment_keeps_the_runs_own_bound():
    # Both experiments select the same source, so the earliest selecting experiment is the
    # run's own earliest, and the source's bound is exactly what it was before scoping.
    windows = cumulative_windows(2)
    earliest = experiment(
        "earliest", AS_OF - datetime.timedelta(days=200), CLIENT_UNIT, ("active_hours",)
    )
    latest = experiment(
        "latest", AS_OF - datetime.timedelta(days=10), CLIENT_UNIT, ("active_hours",)
    )
    run = sql_generation.Run(
        [earliest, latest],
        {"earliest": windows, "latest": windows},
        AS_OF,
        COVARIATE_DAYS,
    )
    queries = sql_generation.build_queries(
        run,
        {"active_hours": named("active_hours")},
        {"clients_daily": CLIENTS_DAILY_SOURCE},
        "a.cohort_table",
    )

    bound = AS_OF - datetime.timedelta(days=200 + COVARIATE_DAYS)
    assert f"DATE '{bound}'" in queries["clients_daily/client_id"]


@pytest.mark.parametrize("unit", [CLIENT_UNIT, GROUP_UNIT])
def test_a_source_selected_only_by_a_recent_experiment_is_bounded_by_its_own_start(unit):
    windows = cumulative_windows(2)
    old_experiment = experiment(
        "old-slug", AS_OF - datetime.timedelta(days=200), unit, ("active_hours",)
    )
    new_experiment = experiment(
        "new-slug", AS_OF - datetime.timedelta(days=10), unit, ("retained_dau",)
    )
    run = sql_generation.Run(
        [old_experiment, new_experiment],
        {"old-slug": windows, "new-slug": windows},
        AS_OF,
        COVARIATE_DAYS,
    )
    resolved_metrics = {
        "active_hours": named("active_hours"),
        "retained_dau": named("retained_dau"),
    }
    sources = {"clients_daily": CLIENTS_DAILY_SOURCE, "active_users": ACTIVE_USERS_SOURCE}

    queries = sql_generation.build_queries(run, resolved_metrics, sources, "a.cohort_table")

    old_bound = AS_OF - datetime.timedelta(days=200 + COVARIATE_DAYS)
    new_bound = AS_OF - datetime.timedelta(days=10 + COVARIATE_DAYS)
    assert f"DATE '{old_bound}'" in queries[f"clients_daily/{unit.kind}"]
    assert f"DATE '{new_bound}'" in queries[f"active_users/{unit.kind}"]
    assert f"DATE '{old_bound}'" not in queries[f"active_users/{unit.kind}"]


def test_an_experiment_not_selecting_a_source_is_excluded_from_its_query():
    windows = cumulative_windows(2)
    old_experiment = experiment(
        "old-slug", AS_OF - datetime.timedelta(days=200), CLIENT_UNIT, ("active_hours",)
    )
    new_experiment = experiment(
        "new-slug", AS_OF - datetime.timedelta(days=10), CLIENT_UNIT, ("retained_dau",)
    )
    run = sql_generation.Run(
        [old_experiment, new_experiment],
        {"old-slug": windows, "new-slug": windows},
        AS_OF,
        COVARIATE_DAYS,
    )
    resolved_metrics = {
        "active_hours": named("active_hours"),
        "retained_dau": named("retained_dau"),
    }
    sources = {"clients_daily": CLIENTS_DAILY_SOURCE, "active_users": ACTIVE_USERS_SOURCE}

    queries = sql_generation.build_queries(run, resolved_metrics, sources, "a.cohort_table")

    assert "'old-slug'" not in queries["active_users/client_id"]
    assert "'new-slug'" not in queries["clients_daily/client_id"]


def test_branch_units_are_counted_off_the_runs_cohort_table():
    sql = sql_generation.branch_units_query("a-project.a_dataset.a_cohort")

    assert "FROM `a-project.a_dataset.a_cohort`" in sql
    assert "COUNT(*) AS units" in sql
    assert "GROUP BY slug, branch" in sql


# ------------------------------------------------------------------------- winsorization ----


def quantile_metric(quantile):
    return dataclasses.replace(named("active_hours"), winsorize_quantile=quantile)


def fixed_metric(threshold):
    return dataclasses.replace(named("active_hours"), winsorize_threshold=threshold)


def off_metric():
    return dataclasses.replace(named("active_hours"), winsorize_quantile=1)


def test_an_off_metric_leaves_the_rollup_exactly_as_it_was_before_winsorization():
    # The same assertions the metric-independent-schema test made before winsorize_mode existed,
    # so an explicitly off metric still produces exactly today's SQL for post and pre.
    run = run_of({"a-slug": cumulative_windows(2)})

    sql = sql_generation.rollup_select([off_metric()], run)

    assert "active_hours__cumulative AS post" in sql
    assert "active_hours__pre AS pre" in sql
    assert "LEAST" not in sql
    assert "reference_quantiles" not in sql
    assert "'off' AS winsorize_mode" in sql
    assert "WHERE bucket >= 0" in sql
    assert "GROUP BY slug, branch, segment, metric_row.window_label, metric_row.metric" in sql


def test_a_yes_no_metric_is_never_clipped_in_the_rollup():
    run = run_of({"a-slug": disjoint_windows(1)})

    sql = sql_generation.rollup_select([named("retained_dau")], run)

    assert "LEAST" not in sql
    assert "reference_quantiles" not in sql
    assert "'off' AS winsorize_mode" in sql


def test_a_quantile_metric_needs_a_quantile_cte():
    run = run_of({"a-slug": cumulative_windows(2)})

    quantiles = sql_generation.reference_quantiles_cte([quantile_metric(0.995)], run)

    assert quantiles is not None
    offset = round(0.995 * sql_generation.QUANTILE_RESOLUTION)
    assert f"[OFFSET({offset})] AS active_hours__cumulative__post_winsorize_quantile" in quantiles


def test_reference_quantiles_cte_is_none_when_no_metric_needs_one():
    run = run_of({"a-slug": cumulative_windows(2)})

    assert sql_generation.reference_quantiles_cte([off_metric()], run) is None
    assert sql_generation.reference_quantiles_cte([named("retained_dau")], run) is None


def test_reference_quantiles_cte_reads_each_metrics_own_quantile_as_an_offset():
    run = run_of({"a-slug": cumulative_windows(2)})

    quantiles = sql_generation.reference_quantiles_cte([quantile_metric(0.98)], run)

    offset = round(0.98 * sql_generation.QUANTILE_RESOLUTION)
    assert (
        f"APPROX_QUANTILES(unit_windows.active_hours__cumulative, "
        f"{sql_generation.QUANTILE_RESOLUTION})[OFFSET({offset})] "
        f"AS active_hours__cumulative__post_winsorize_quantile" in quantiles
    )
    assert (
        f"APPROX_QUANTILES(unit_windows.active_hours__pre, "
        f"{sql_generation.QUANTILE_RESOLUTION})[OFFSET({offset})] "
        f"AS active_hours__covariate_quantile" in quantiles
    )
    assert "COUNT(*) AS reference_n" in quantiles
    assert "GROUP BY unit_windows.slug, unit_windows.bucket" in quantiles


def test_reference_quantiles_cte_restricts_to_each_slugs_own_reference_branch():
    run = run_of({"a-slug": cumulative_windows(2)})

    quantiles = sql_generation.reference_quantiles_cte([quantile_metric(0.98)], run)

    assert "STRUCT('a-slug' AS slug, 'control' AS branch)" in quantiles
    assert "ON reference_branch.slug = unit_windows.slug" in quantiles
    assert "AND reference_branch.branch = unit_windows.branch" in quantiles


def test_reference_quantiles_cte_selects_quantile_metrics_through_the_shared_helper(monkeypatch):
    # reference_quantiles_cte and rollup_select each decide which metrics need a quantile; if
    # reference_quantiles_cte ever grew its own copy of that filter instead of calling
    # _quantile_mode_metrics, the two could silently drift apart on a future change to the
    # filter (e.g. a new winsorize mode). Spying on the shared helper, rather than just comparing
    # outputs, is what catches that even when the two filters still happen to agree today.
    run = run_of({"a-slug": cumulative_windows(2)})
    calls = []
    original = sql_generation._quantile_mode_metrics

    def spy(metrics):
        calls.append(metrics)
        return original(metrics)

    monkeypatch.setattr(sql_generation, "_quantile_mode_metrics", spy)

    sql_generation.reference_quantiles_cte([quantile_metric(0.98), off_metric()], run)

    assert calls == [[quantile_metric(0.98), off_metric()]]


def test_quantile_mode_clips_the_post_value_at_the_reference_quantile():
    run = run_of({"a-slug": cumulative_windows(1)})

    sql = sql_generation.rollup_select([quantile_metric(0.98)], run)

    assert "LEFT JOIN reference_quantiles AS rq" in sql
    assert "ON rq.slug = unit_windows.slug AND rq.bucket = unit_windows.bucket" in sql
    assert (
        "LEAST(active_hours__cumulative, "
        "IF((rq.reference_n IS NULL OR rq.reference_n * (1 - 0.98) < 1), NULL, "
        "rq.active_hours__cumulative__post_winsorize_quantile))" in sql
    )
    assert "WHERE unit_windows.bucket >= 0" in sql
    assert "GROUP BY unit_windows.slug, unit_windows.branch," in sql


def test_the_covariate_is_clipped_at_its_own_reference_quantile():
    run = run_of({"a-slug": cumulative_windows(1)})

    sql = sql_generation.rollup_select([quantile_metric(0.98)], run)

    assert (
        "LEAST(active_hours__pre, "
        "IF((rq.reference_n IS NULL OR rq.reference_n * (1 - 0.98) < 1), NULL, "
        "rq.active_hours__covariate_quantile))" in sql
    )


def test_too_few_reference_units_is_recorded_distinctly_from_a_clipped_quantile():
    run = run_of({"a-slug": cumulative_windows(1)})

    sql = sql_generation.rollup_select([quantile_metric(0.98)], run)

    assert "'skipped_too_few'" in sql
    assert "'quantile'" in sql
    assert "rq.reference_n * (1 - 0.98) < 1" in sql


def test_the_quantile_threshold_does_not_depend_on_which_branch_it_clips():
    # The clip expression never reads `branch`, so the one expression generated here runs
    # against every branch's own rows: the reference branch's threshold, not each branch's own.
    run = run_of({"a-slug": cumulative_windows(1)})

    sql = sql_generation.rollup_select([quantile_metric(0.98)], run)

    assert "active_hours__cumulative__post_winsorize_quantile" in sql
    assert "branch" not in sql_generation._clip_fields(
        quantile_metric(0.98), "cumulative", run
    )["post"]


def test_fixed_mode_scales_the_post_winsorize_threshold_by_the_window_length_in_buckets():
    run = run_of({"a-slug": cumulative_windows(3)})

    sql = sql_generation.rollup_select([fixed_metric(500)], run)

    assert "LEAST(active_hours__cumulative, 500 * (unit_windows.bucket + 1))" in sql
    assert "reference_quantiles" not in sql


def test_fixed_mode_scales_the_pre_winsorize_threshold_by_the_covariate_window_length():
    run = run_of({"a-slug": cumulative_windows(1)})

    sql = sql_generation.rollup_select([fixed_metric(500)], run)

    covariate_weeks = COVARIATE_DAYS / run.length
    assert f"LEAST(active_hours__pre, {500 * covariate_weeks})" in sql


def test_a_disjoint_fixed_window_is_not_scaled_past_its_own_single_bucket():
    metric = dataclasses.replace(
        named("retained"),
        reducer=named("active_hours").reducer,
        winsorize_threshold=10,
        is_yes_no=False,
    )
    run = run_of({"a-slug": disjoint_windows(1)})

    sql = sql_generation.rollup_select([metric], run)

    assert "LEAST(retained__disjoint, 10 * 1)" in sql


def test_thresholds_and_the_clipped_share_are_recorded_alongside_the_aggregates():
    run = run_of({"a-slug": cumulative_windows(1)})

    sql = sql_generation.rollup_select([quantile_metric(0.98)], run)

    assert "ANY_VALUE(metric_row.winsorize_mode) AS winsorize_mode" in sql
    assert "ANY_VALUE(metric_row.post_winsorize_threshold) AS post_winsorize_threshold" in sql
    assert "ANY_VALUE(metric_row.pre_winsorize_threshold) AS pre_winsorize_threshold" in sql
    assert "SAFE_DIVIDE(SUM(metric_row.was_clipped), COUNT(*)) AS clipped_share" in sql


def test_a_multi_arm_run_still_builds_one_shared_threshold_expression():
    # Multiple treatment branches share one Experiment/cohort shape here; the generated SQL has
    # no branch-specific clause, which is what makes the same threshold apply to every arm.
    run = run_of({"a-slug": cumulative_windows(1)})

    sql = sql_generation.rollup_select([quantile_metric(0.98)], run)

    assert sql.count("LEFT JOIN reference_quantiles AS rq") == 1


def test_build_source_query_assembles_every_stage_for_a_multi_metric_source():
    # The full query for a source carrying one quantile metric and one yes/no (off) metric: every
    # stage is present, in order, and the reference-quantiles CTE is added only because
    # active_hours needs one.
    run = run_of(
        {"a-slug": cumulative_windows(2)},
        metric_names_by_slug={"a-slug": ("active_hours", "retained")},
    )

    sql = sql_generation.build_source_query(
        run,
        "clients_daily",
        [quantile_metric(0.995), named("retained")],
        CLIENTS_DAILY_SOURCE,
        "a.cohort_table",
        CLIENT_UNIT,
    )

    assert sql.startswith("WITH cohort AS (")
    for stage in (
        "experiment_meta AS (",
        "source_rows AS (",
        "bucket_totals AS (",
        "unit_buckets AS (",
        "unit_windows AS (",
        "reference_quantiles AS (",
    ):
        assert stage in sql, stage
    assert sql.index("unit_windows AS (") < sql.index("reference_quantiles AS (")
    assert "active_hours__raw" in sql
    assert "retained__raw" in sql
    assert "STRUCT('active_hours' AS metric" in sql
    assert "STRUCT('retained' AS metric" in sql
    assert "'off' AS winsorize_mode" in sql
    assert sql.rstrip().endswith(
        "GROUP BY unit_windows.slug, unit_windows.branch, segment, metric_row.window_label, "
        "metric_row.metric"
    )


def test_build_source_query_adds_no_quantile_cte_when_every_metric_is_off():
    run = run_of({"a-slug": disjoint_windows(1)})

    sql = sql_generation.build_source_query(
        run, "active_users", [named("retained_dau")], ACTIVE_USERS_SOURCE, "a.cohort_table",
        CLIENT_UNIT,
    )

    assert "reference_quantiles" not in sql
    assert "LEAST(retained_dau" not in sql


def metric_with_name(name, winsorize_quantile=None, winsorize_threshold=None, is_yes_no=False):
    """A continuous metric distinguished only by name and winsorize setting, for tests that need
    several metrics with different settings to coexist in one source without colliding on the
    column names their own struct/quantile entries are keyed by."""
    return metrics.Metric(
        name=name,
        source="clients_daily",
        reducer=reducer(f"SUM({name}_sum)"),
        window_rules=(metrics.CUMULATIVE_WEEKLY,),
        winsorize_quantile=winsorize_quantile,
        winsorize_threshold=winsorize_threshold,
        is_yes_no=is_yes_no,
    )


def test_a_source_with_mixed_winsorize_settings_clips_each_metric_on_its_own_terms():
    # One data source, three metrics each in a different mode: quantile, fixed and off. Each
    # must get its own clip, independent of the other two, and the off metric must be untouched.
    run = run_of({"a-slug": cumulative_windows(1)})
    quantile_m = metric_with_name("metric_quantile", winsorize_quantile=0.98)
    fixed_m = metric_with_name("metric_fixed", winsorize_threshold=500)
    off_m = metric_with_name("metric_off", winsorize_quantile=1)

    sql = sql_generation.rollup_select([quantile_m, fixed_m, off_m], run)

    assert (
        "LEAST(metric_quantile__cumulative, "
        "IF((rq.reference_n IS NULL OR rq.reference_n * (1 - 0.98) < 1), NULL, "
        "rq.metric_quantile__cumulative__post_winsorize_quantile))" in sql
    )
    assert "LEAST(metric_fixed__cumulative, 500 * (unit_windows.bucket + 1))" in sql
    assert "metric_off__cumulative AS post" in sql
    assert "LEAST(metric_off" not in sql
    assert "metric_off__cumulative__post_winsorize_quantile" not in sql
    assert "metric_fixed__cumulative__post_winsorize_quantile" not in sql

    quantiles = sql_generation.reference_quantiles_cte([quantile_m, fixed_m, off_m], run)

    assert "metric_quantile__cumulative__post_winsorize_quantile" in quantiles
    assert "metric_fixed" not in quantiles
    assert "metric_off" not in quantiles


CLIENTS_LAST_SEEN_SOURCE = segments.SegmentSource(
    name="clients_last_seen",
    from_expression="mozdata.telemetry.clients_last_seen",
    window_start=0,
    window_end=0,
    client_id_column="client_id",
    group_id_column="profile_group_id",
    submission_date_column="submission_date",
)
NEWTAB_SOURCE = segments.SegmentSource(
    name="newtab",
    from_expression="mozdata.telemetry.newtab_clients_daily",
    window_start=-7,
    window_end=-1,
    client_id_column="client_id",
    group_id_column="profile_group_id",
    submission_date_column="submission_date",
)

REGULAR_USER = segments.Segment(
    name="regular_user",
    source="clients_last_seen",
    select_expression="COALESCE(LOGICAL_OR(is_regular_user_v3), FALSE)",
)
NEW_PROFILE = segments.Segment(
    name="new_profile",
    source="clients_last_seen",
    select_expression="COALESCE(LOGICAL_OR(is_new_profile), FALSE)",
)
SUGGEST_USER = segments.Segment(
    name="suggest_user",
    source="newtab",
    select_expression="COALESCE(LOGICAL_OR(uses_suggest), FALSE)",
)


def test_unit_windows_cte_defaults_every_unit_to_just_all_enrolled_when_dark():
    sql = sql_generation.unit_windows_cte([named("active_hours")], needs_segments=False)

    assert "['all_enrolled'] AS segments" in sql
    assert "unit_segments" not in sql
    assert "grid.unit_id" in sql


def test_unit_windows_cte_joins_unit_segments_when_a_run_resolved_one():
    sql = sql_generation.unit_windows_cte([named("active_hours")], needs_segments=True)

    assert "ARRAY_CONCAT(['all_enrolled'], COALESCE(unit_segments.extra_segments, []))" in sql
    assert "LEFT JOIN unit_segments" in sql
    assert "ON unit_segments.slug = grid.slug AND unit_segments.unit_id = grid.unit_id" in sql


def test_a_unit_matching_no_resolved_segment_still_carries_all_enrolled():
    # The LEFT JOIN leaves `unit_segments.extra_segments` NULL for a unit that matched none of the
    # run's resolved segments; COALESCE to an empty array before the CONCAT is what keeps
    # `all_enrolled` in its `segments` array regardless, rather than that unit vanishing from the
    # breakdown it would get if the array were NULL and UNNEST dropped the row.
    sql = sql_generation.unit_windows_cte([named("active_hours")], needs_segments=True)

    assert "COALESCE(unit_segments.extra_segments, [])" in sql
    assert sql.index("['all_enrolled']") < sql.index("COALESCE(unit_segments.extra_segments")


def test_rollup_select_always_unnests_a_segment_and_groups_by_it():
    run = run_of({"a-slug": cumulative_windows(1)})

    sql = sql_generation.rollup_select([off_metric()], run)

    assert "UNNEST(unit_windows.segments) AS segment" in sql
    assert "GROUP BY slug, branch, segment, metric_row.window_label, metric_row.metric" in sql
    assert "  segment,\n" in sql


def test_unit_segments_source_is_an_empty_literal_without_a_table():
    sql = sql_generation.unit_segments_source(None)

    assert "UNNEST(ARRAY<STRUCT<slug STRING, unit_id STRING, segment STRING>>[])" in sql


def test_unit_segments_source_reads_the_materialized_table():
    sql = sql_generation.unit_segments_source("a.dataset.a_unit_segments_table")

    assert "FROM `a.dataset.a_unit_segments_table`" in sql


def test_segment_source_query_guards_membership_by_the_selecting_experiments():
    # Two experiments share the clients_last_seen source; only one selected regular_user, so the
    # other's units must never report membership in it even though the join covers both.
    run = run_of(
        {"selects-it": cumulative_windows(1), "does-not": cumulative_windows(1)},
        segments_by_slug={"selects-it": ("regular_user",)},
    )

    sql = sql_generation.segment_source_query(
        run, [REGULAR_USER], CLIENTS_LAST_SEEN_SOURCE, "a.cohort_table", CLIENT_UNIT
    )

    assert "IF(e.slug IN ('selects-it'), " in sql
    assert "'does-not'" not in sql
    assert "STRUCT('regular_user' AS name" in sql
    assert "GROUP BY e.slug, e.unit_id" in sql
    assert "WHERE matched.member" in sql


def test_segment_source_query_follows_mozanalysiss_window_join_shape():
    # A resolved segment's select_expression can reference mozanalysis's `e`/`ds` aliases directly.
    run = run_of({"a-slug": cumulative_windows(1)}, segments_by_slug={"a-slug": ("suggest_user",)})

    sql = sql_generation.segment_source_query(
        run, [SUGGEST_USER], NEWTAB_SOURCE, "a.cohort_table", CLIENT_UNIT
    )

    assert "LEFT JOIN mozdata.telemetry.newtab_clients_daily AS ds" in sql
    assert "ON ds.client_id = e.unit_id" in sql
    assert "DATE_ADD(e.enrollment_date, INTERVAL -7 DAY)" in sql
    assert "DATE_ADD(e.enrollment_date, INTERVAL -1 DAY)" in sql


def test_segment_source_query_also_bounds_the_join_by_a_static_date_range():
    # A source like clients_last_seen requires a partition filter on submission_date, and a
    # condition that only ever references the other side of the join (e.enrollment_date) does not
    # qualify as one. The static bound, keyed off the run's own extent rather than a per-row value,
    # is what lets BigQuery eliminate partitions; mozanalysis's own query carries both.
    run = run_of({"a-slug": cumulative_windows(1)}, segments_by_slug={"a-slug": ("suggest_user",)})

    sql = sql_generation.segment_source_query(
        run, [SUGGEST_USER], NEWTAB_SOURCE, "a.cohort_table", CLIENT_UNIT
    )

    assert f"DATE_ADD(DATE '{run.earliest_start()}'," in sql
    assert f"DATE_ADD(DATE '{run.as_of}'," in sql
    assert sql.count("BETWEEN") == 2


def test_segment_source_query_bounds_only_by_the_selecting_experiments_enrollment_dates():
    # Without the cohort's own enrollment dates (the fallback path, as under --validate-sql), an
    # experiment that never selects a segment from this source must still not widen the scan to
    # its own (possibly much older) start date.
    old_experiment = experiment("old-and-not-selecting", AS_OF - datetime.timedelta(days=200))
    new_experiment = experiment("new-and-selecting", AS_OF - datetime.timedelta(days=10))
    run = sql_generation.Run(
        [old_experiment, new_experiment],
        {
            "old-and-not-selecting": cumulative_windows(1),
            "new-and-selecting": cumulative_windows(1),
        },
        AS_OF,
        COVARIATE_DAYS,
        segments_by_slug={"new-and-selecting": ("suggest_user",)},
    )

    sql = sql_generation.segment_source_query(
        run, [SUGGEST_USER], NEWTAB_SOURCE, "a.cohort_table", CLIENT_UNIT
    )

    assert f"DATE_ADD(DATE '{new_experiment.start_date}'," in sql
    assert f"DATE_ADD(DATE '{old_experiment.start_date}'," not in sql


def test_segment_source_query_caps_the_latest_bound_at_a_selecting_experiments_end_date():
    # Still the fallback path (no enrollment_bounds_by_slug): a still-live experiment reads
    # through as_of, and an ended one is capped at its own declared end date.
    ended = dataclasses.replace(
        experiment("ended", AS_OF - datetime.timedelta(days=100)),
        end_date=AS_OF - datetime.timedelta(days=30),
    )
    run = sql_generation.Run(
        [ended],
        {"ended": cumulative_windows(1)},
        AS_OF,
        COVARIATE_DAYS,
        segments_by_slug={"ended": ("suggest_user",)},
    )

    sql = sql_generation.segment_source_query(
        run, [SUGGEST_USER], NEWTAB_SOURCE, "a.cohort_table", CLIENT_UNIT
    )

    assert f"DATE_ADD(DATE '{ended.end_date}'," in sql
    assert f"DATE_ADD(DATE '{AS_OF}'," not in sql


def test_segment_source_query_bounds_by_the_cohorts_own_enrollment_dates_when_known():
    # The cohort's actual MIN/MAX(enrollment_date) is the real population a segment source's
    # window should cover; a still-live experiment's declared end date reaches well past its last
    # real enrollment, so the cohort's own dates win over the fallback whenever they are known.
    run = run_of({"a-slug": cumulative_windows(1)}, segments_by_slug={"a-slug": ("suggest_user",)})
    enrollment_bounds_by_slug = {"a-slug": (datetime.date(2026, 5, 1), datetime.date(2026, 5, 20))}

    sql = sql_generation.segment_source_query(
        run, [SUGGEST_USER], NEWTAB_SOURCE, "a.cohort_table", CLIENT_UNIT,
        enrollment_bounds_by_slug,
    )

    assert "DATE_ADD(DATE '2026-05-01'," in sql
    assert "DATE_ADD(DATE '2026-05-20'," in sql
    assert f"DATE_ADD(DATE '{run.earliest_start()}'," not in sql


def test_segment_source_query_caps_the_cohorts_own_latest_date_at_as_of():
    run = run_of({"a-slug": cumulative_windows(1)}, segments_by_slug={"a-slug": ("suggest_user",)})
    past_as_of = AS_OF + datetime.timedelta(days=5)
    enrollment_bounds_by_slug = {"a-slug": (datetime.date(2026, 5, 1), past_as_of)}

    sql = sql_generation.segment_source_query(
        run, [SUGGEST_USER], NEWTAB_SOURCE, "a.cohort_table", CLIENT_UNIT,
        enrollment_bounds_by_slug,
    )

    assert f"DATE_ADD(DATE '{AS_OF}'," in sql
    assert f"DATE_ADD(DATE '{past_as_of}'," not in sql


def test_segment_source_query_ignores_a_selecting_slug_missing_from_the_known_bounds():
    # A slug the cohort query did not cover (nothing enrolled) falls back to its own declared
    # dates rather than crashing on a missing key.
    run = run_of({"a-slug": cumulative_windows(1)}, segments_by_slug={"a-slug": ("suggest_user",)})

    sql = sql_generation.segment_source_query(
        run, [SUGGEST_USER], NEWTAB_SOURCE, "a.cohort_table", CLIENT_UNIT,
        {"some-other-slug": (datetime.date(2026, 5, 1), datetime.date(2026, 5, 20))},
    )

    assert f"DATE_ADD(DATE '{run.earliest_start()}'," in sql


def test_cohort_enrollment_bounds_query_filters_to_the_given_slugs_and_groups_by_slug():
    sql = sql_generation.cohort_enrollment_bounds_query("a.cohort_table", {"a-slug", "b-slug"})

    assert "FROM `a.cohort_table`" in sql
    assert "MIN(enrollment_date) AS earliest" in sql
    assert "MAX(enrollment_date) AS latest" in sql
    assert "'a-slug'" in sql
    assert "'b-slug'" in sql
    assert "GROUP BY slug" in sql


def test_segment_source_query_applies_the_runs_sample_to_the_source_side():
    run = run_of(
        {"a-slug": cumulative_windows(1)},
        segments_by_slug={"a-slug": ("suggest_user",)},
        sample_percent=10,
    )

    sql = sql_generation.segment_source_query(
        run, [SUGGEST_USER], NEWTAB_SOURCE, "a.cohort_table", CLIENT_UNIT
    )

    assert "AND sample_id < 10" in sql


def test_segment_source_query_is_unsampled_without_a_run_sample():
    run = run_of({"a-slug": cumulative_windows(1)}, segments_by_slug={"a-slug": ("suggest_user",)})

    sql = sql_generation.segment_source_query(
        run, [SUGGEST_USER], NEWTAB_SOURCE, "a.cohort_table", CLIENT_UNIT
    )

    assert "sample_id <" not in sql
    assert "safe_sample_id" not in sql


def test_segment_source_queries_is_empty_when_no_experiment_resolved_a_segment():
    run = run_of({"a-slug": cumulative_windows(1)})

    assert sql_generation.segment_source_queries(run, {}, {}, "a.cohort_table") == {}


def test_segment_source_queries_has_one_query_per_source_and_unit():
    run = run_of(
        {"grouped": cumulative_windows(1), "by-client": cumulative_windows(1)},
        units_by_slug={"grouped": GROUP_UNIT},
        segments_by_slug={
            "grouped": ("regular_user",),
            "by-client": ("regular_user", "suggest_user"),
        },
    )
    resolved_segments = {
        "regular_user": REGULAR_USER,
        "suggest_user": SUGGEST_USER,
    }
    segment_sources = {"clients_last_seen": CLIENTS_LAST_SEEN_SOURCE, "newtab": NEWTAB_SOURCE}

    queries = sql_generation.segment_source_queries(
        run, resolved_segments, segment_sources, "a.cohort"
    )

    assert set(queries) == {
        ("clients_last_seen", GROUP_UNIT.kind),
        ("clients_last_seen", CLIENT_UNIT.kind),
        ("newtab", CLIENT_UNIT.kind),
    }
    assert "'grouped'" in queries[("clients_last_seen", GROUP_UNIT.kind)]
    assert "'by-client'" in queries[("clients_last_seen", CLIENT_UNIT.kind)]
    assert "'by-client'" in queries[("newtab", CLIENT_UNIT.kind)]


def test_segment_source_queries_skips_a_source_no_selected_segment_needs():
    run = run_of(
        {"a-slug": cumulative_windows(1)}, segments_by_slug={"a-slug": ("regular_user",)}
    )
    resolved_segments = {"regular_user": REGULAR_USER, "suggest_user": SUGGEST_USER}
    segment_sources = {"clients_last_seen": CLIENTS_LAST_SEEN_SOURCE, "newtab": NEWTAB_SOURCE}

    queries = sql_generation.segment_source_queries(
        run, resolved_segments, segment_sources, "a.cohort"
    )

    assert set(queries) == {("clients_last_seen", CLIENT_UNIT.kind)}


def test_build_source_query_adds_no_unit_segments_cte_when_the_run_is_dark():
    run = run_of({"a-slug": cumulative_windows(2)})

    sql = sql_generation.build_source_query(
        run, "clients_daily", [named("active_hours")], CLIENTS_DAILY_SOURCE, "a.cohort_table",
        CLIENT_UNIT,
    )

    assert "unit_segments AS (" not in sql


def test_build_source_query_adds_the_unit_segments_cte_when_the_run_resolved_one():
    run = run_of(
        {"a-slug": cumulative_windows(2)}, segments_by_slug={"a-slug": ("regular_user",)}
    )

    sql = sql_generation.build_source_query(
        run, "clients_daily", [named("active_hours")], CLIENTS_DAILY_SOURCE, "a.cohort_table",
        CLIENT_UNIT, "a.unit_segments_table",
    )

    assert "unit_segments AS (" in sql
    assert sql.index("unit_segments AS (") < sql.index("unit_windows AS (")
    assert "a.unit_segments_table" in sql


def test_segment_units_query_joins_cohort_to_unit_segments():
    sql = sql_generation.segment_units_query("a.cohort_table", "a.unit_segments_table")

    assert "FROM `a.cohort_table` AS cohort" in sql
    assert "JOIN `a.unit_segments_table` AS seg" in sql
    assert "ON seg.slug = cohort.slug AND seg.unit_id = cohort.unit_id" in sql
    assert "GROUP BY cohort.slug, cohort.branch, seg.segment" in sql


def test_a_run_needs_segments_only_when_one_of_its_own_experiments_resolved_one():
    dark = run_of({"a-slug": cumulative_windows(1)})
    lit = run_of({"a-slug": cumulative_windows(1)}, segments_by_slug={"a-slug": ("regular_user",)})

    assert dark.needs_segments is False
    assert lit.needs_segments is True
