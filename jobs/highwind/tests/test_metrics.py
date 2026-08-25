"""The per-unit reducers, whose parts have to compose over buckets without moving a threshold."""

from highwind import metrics


def test_a_sum_reduces_with_a_sum_and_finalizes_to_itself():
    reducer = metrics.per_unit_sum("active_hours_sum")

    assert reducer.bucket_agg == "SUM(active_hours_sum)"
    assert reducer.combine == "SUM"
    assert reducer.no_rows == "0"
    assert reducer.finalize("combined") == "combined"


def test_distinct_days_sums_over_buckets_because_buckets_partition_tenure():
    reducer = metrics.per_unit_distinct_days()

    assert reducer.bucket_agg == "COUNT(DISTINCT submission_date)"
    assert reducer.combine == "SUM"
    assert reducer.finalize("combined") == "combined"


def test_any_counts_per_bucket_and_thresholds_the_combined_count():
    reducer = metrics.per_unit_any("is_default_browser")

    assert reducer.bucket_agg == "COUNTIF(is_default_browser)"
    assert reducer.combine == "SUM"
    assert reducer.finalize("combined") == "CAST(combined > 0 AS INT64)"


def test_countif_keeps_the_count_rather_than_reducing_it_to_a_flag():
    reducer = metrics.per_unit_countif("is_dau")

    assert reducer.bucket_agg == "COUNTIF(is_dau)"
    assert reducer.finalize("combined") == "combined"


def test_sum_positive_thresholds_the_summed_total_not_each_bucket():
    reducer = metrics.per_unit_sum_positive("pings_aggregated_by_this_row")

    assert reducer.bucket_agg == "SUM(pings_aggregated_by_this_row)"
    assert reducer.combine == "SUM"
    assert reducer.finalize("combined") == "CAST(combined > 0 AS INT64)"


def test_min_below_combines_with_min_and_carries_its_own_missing_value():
    reducer = metrics.per_unit_min_below("days_since_seen", 3, 30)

    assert reducer.bucket_agg == "MIN(days_since_seen)"
    assert reducer.combine == "MIN"
    assert reducer.no_rows == "30"
    assert reducer.finalize("combined") == "CAST(combined < 3 AS INT64)"


def test_scaling_multiplies_the_finalized_value_and_leaves_the_bucket_aggregate_alone():
    inner = metrics.per_unit_countif("is_dau")
    scaled = metrics.per_unit_scaled(inner, 1000)

    assert scaled.bucket_agg == inner.bucket_agg
    assert scaled.combine == inner.combine
    assert scaled.no_rows == inner.no_rows
    assert scaled.finalize("combined") == "(combined) * 1000"


def test_scaling_a_binary_metric_thresholds_before_it_multiplies():
    scaled = metrics.per_unit_scaled(metrics.per_unit_any("is_dau"), 1000)

    assert scaled.finalize("combined") == "(CAST(combined > 0 AS INT64)) * 1000"


def test_binary_retention_metrics_take_disjoint_weeks():
    by_name = {metric.name: metric for metric in metrics.GUARDRAILS}

    for name in metrics.RETENTION_METRICS:
        assert by_name[name].window_rules == (metrics.DISJOINT_WEEKLY,)
    assert by_name["active_hours"].window_rules == (metrics.CUMULATIVE_WEEKLY,)


def test_every_metric_reads_a_source_the_run_knows_how_to_scan():
    by_source = metrics.metric_definitions()

    assert set(by_source) == set(metrics.SOURCES)
    assert sum(len(group) for group in by_source.values()) == len(metrics.GUARDRAILS)
    for source, group in by_source.items():
        assert {metric.source for metric in group} == {source}


def test_each_source_declares_a_table_and_the_columns_its_metrics_name():
    for spec in metrics.SOURCES.values():
        assert spec["table"].count(".") == 2
        assert spec["columns"]
