"""SECTION 1: METRIC DEFINITIONS.

The Firefox Desktop production default guardrail set, hard-coded. In the real system these come
from metric-hub through metric-config-parser; here they are literal so the proof of concept has no
cross-repo dependency. The shapes below are what the declarative format has to be able to express,
so this file doubles as the target for that work.

Each metric carries three things: which source table it reads, how to reduce a unit's rows inside a
window to one number, and which windows it declares.
"""

from dataclasses import dataclass
from typing import Callable

# Window rules are a KIND and a LENGTH, never enumerated bounds. Enumerated bounds terminate the
# series: four weekly windows go quiet after week four even though the experiment runs for months.
# A rule extends for as long as the experiment does. Length 7 for both families keeps a young
# experiment producing something in its first week while still extending indefinitely.
CUMULATIVE_WEEKLY = {"kind": "cumulative", "length": 7}
DISJOINT_WEEKLY = {"kind": "disjoint", "length": 7}

# Retention is a 0/1 "was the unit active in this window", so a cumulative window is ~1.0 by
# construction and says nothing. Those metrics take disjoint weeks instead.
# `is_pinned` and `is_default_browser` are also 0/1 and also monotone over a cumulative window, so
# the same argument arguably applies to them. They are left cumulative to match the desktop
# guardrail set being ported; whether they should move is a question for the metric definitions.
RETENTION_METRICS = {"retained", "retained_dau", "active_in_last_3_days_legacy"}


# ------------------------------------------------------------------- per-unit reducers ----
# A reducer is how one unit's rows become one number, split into composable parts because the
# aggregation reduces a unit's rows once per disjoint bucket and then composes buckets into
# windows:
#
#   bucket_aggregate   reduces the unit's rows inside ONE bucket to a raw number
#   combine            how those raw numbers compose over the buckets a window spans, SUM or MIN
#   no_rows            the raw value a unit with no rows in the window takes
#   finalize           turns the combined raw number into the metric's value
#
# The split exists to keep the 0/1 metrics correct. A threshold must be applied ONCE, to the
# combined raw value, never per bucket: `retained` carries a SUM per bucket and tests `> 0` at the
# end, because per-bucket thresholds combined afterwards answer a different question as soon as the
# column can be negative or the combination is anything but an OR.


def _identity(combined):
    return combined


def _above_zero(combined):
    return f"CAST({combined} > 0 AS INT64)"


@dataclass(frozen=True)
class Reducer:
    """How one unit's rows in one window become one number, in bucket-composable parts."""

    bucket_aggregate: str
    combine: str = "SUM"
    no_rows: str = "0"
    finalize: Callable[[str], str] = _identity


def per_unit_distinct_days():
    """Distinct days the unit appears inside the window.

    Safe to SUM across buckets only because buckets partition tenure: a submission_date falls in
    exactly one bucket, so per-bucket distinct counts add up to the window's distinct count with no
    double counting. A combiner that summed overlapping ranges would be wrong here.
    """
    return Reducer(bucket_aggregate="COUNT(DISTINCT submission_date)")


def per_unit_sum(column):
    """Sum of `column` across the unit's rows inside the window."""
    return Reducer(bucket_aggregate=f"SUM({column})")


def per_unit_any(condition):
    """1 if `condition` held on any of the unit's rows inside the window, else 0.

    Carries the COUNTIF per bucket and tests it once at the end, so the bucket grid never sees the
    threshold. This 0/1 reduction is what lets binary metrics travel the same sufficient-statistics
    path as the continuous ones, as a linear probability model.
    """
    return Reducer(bucket_aggregate=f"COUNTIF({condition})", finalize=_above_zero)


def per_unit_countif(condition):
    """Count the unit's rows inside the window on which `condition` held.

    Rows, which is what metric-hub specifies and what production computes. The sources are one row
    per client per day, so a unit carrying several clients counts one calendar day once per client,
    and a row count can exceed the distinct-day count of a metric it nominally contains. The
    distinct-day form was measured instead and rejected: each exact COUNT(DISTINCT) is its own
    shuffle in the stage that carries essentially all of the cost, for no change in bytes scanned.
    """
    return Reducer(bucket_aggregate=f"COUNTIF({condition})")


def per_unit_sum_positive(column):
    """1 if the unit's summed `column` over the window is positive, else 0.

    The raw SUM is what travels, and `> 0` is applied to the summed total. Testing each bucket and
    combining the answers is a different metric: it asks whether any single bucket was positive,
    which diverges from the window total the moment `column` can be negative.

    `no_rows` keeps metric-hub's 0-fill. SUM returns NULL when a unit has no rows in the window,
    and that NULL would otherwise make the unit's value NULL rather than 0, which the rollup drops
    from the numerator while still counting the unit in `n`.
    """
    return Reducer(bucket_aggregate=f"SUM({column})", finalize=_above_zero)


def per_unit_min_below(expression, threshold, no_rows_value):
    """1 if the minimum of `expression` over the unit's rows in the window is below `threshold`.

    Combines with MIN rather than SUM, since the minimum over a window is the minimum of its
    buckets' minimums. `no_rows_value` is the sentinel a unit with no rows in the window gets,
    applied after the combination for the same reason the thresholds are.
    """
    return Reducer(
        bucket_aggregate=f"MIN({expression})",
        combine="MIN",
        no_rows=str(no_rows_value),
        finalize=lambda combined: f"CAST({combined} < {threshold} AS INT64)",
    )


def per_unit_scaled(reducer, factor):
    """Multiply `reducer` by a constant.

    The constant cancels in everything reported here: it divides out of a relative difference, and
    out of theta, where it scales numerator and denominator alike. So a scaled metric and its
    unscaled twin produce identical intervals, differing only in the raw sums. Both are kept because
    both are in the set being ported, and production tells them apart by reading each with a
    different statistic; under one uniform method that distinction disappears.
    """
    return Reducer(
        bucket_aggregate=reducer.bucket_aggregate,
        combine=reducer.combine,
        no_rows=reducer.no_rows,
        finalize=lambda combined: f"({reducer.finalize(combined)}) * {factor}",
    )


# ------------------------------------------------------------------------ definitions ----


@dataclass(frozen=True)
class Metric:
    """One metric: where it reads from, how it reduces a unit's rows, and its windows."""

    name: str
    source: str
    reducer: Reducer
    window_rules: tuple = (CUMULATIVE_WEEKLY,)
    friendly_name: str | None = None
    description: str | None = None


SEARCH_DOCS = "https://docs.telemetry.mozilla.org/datasets/search.html"
DAU_DOCS = (
    "https://mozilla-hub.atlassian.net/wiki/spaces/DATA/pages/314704478/"
    "Daily+Active+Users+DAU+Metric"
)

DISPLAY = {
    "days_of_use": (
        "Days of use",
        "The number of days in the interval that each client sent a main ping.",
    ),
    "qualified_cumulative_days_of_use": (
        "QCDOU",
        "The number of days in the interval that each client sent a main ping, given that the "
        "client had >0 active hours and >0 URIs loaded.",
    ),
    "active_hours": (
        "Active hours",
        "Measures the amount of time (in 5-second increments) during which Firefox received user "
        "input from a keyboard or mouse. The Firefox window does not need to be focused.",
    ),
    "uri_count": (
        "URIs visited",
        "Counts the total number of URIs visited. Includes within-page navigation events (e.g. to "
        "anchors).",
    ),
    "is_pinned": (
        "Is Pinned (Windows Taskbar)",
        "Was Firefox pinned to the Windows Taskbar at any point during the interval?",
    ),
    "is_default_browser": (
        "Is Default Browser",
        "Was Firefox the default browser at any point during the interval?",
    ),
    "retained": (
        "Retained",
        "Records whether a client submitted any pings (i.e. used Firefox). Note: As of June 2026, "
        'this metric is being deprecated in favor of "Retained (DAU)", which better matches the '
        'conventional definition of Retention based on "active" instead of "seen" now recommended '
        "by Data Science team and used for reporting in other contexts.",
    ),
    "search_count": (
        "SAP searches",
        "Counts the number of searches a user performed through Firefox's Search Access Points. "
        f"Learn more in the [search data documentation]({SEARCH_DOCS}).",
    ),
    "ad_clicks": (
        "Ad clicks",
        "Counts clicks on ads on search engine result pages with a Mozilla partner tag.",
    ),
    "searches_with_ads": (
        "Search result pages with ads",
        "Counts search result pages served with advertising. Users may not actually see these ads "
        "thanks to e.g. ad-blockers. Learn more in the [search analysis documentation]"
        "(https://mozilla-private.report/search-analysis-docs/book/in_content_searches.html).",
    ),
    "organic_search_count": (
        "Organic searches",
        "Counts organic searches, which are searches that are _not_ performed through a Firefox "
        "SAP and which are not monetizable. Learn more in the [search data documentation]"
        f"({SEARCH_DOCS}).",
    ),
    "tagged_search_count": (
        "Tagged SAP searches",
        "Counts the number of searches a user performed through Firefox's Search Access Points "
        "that were submitted with a partner code and were potentially revenue-generating. Learn "
        f"more in the [search data documentation]({SEARCH_DOCS}).",
    ),
    "tagged_follow_on_search_count": (
        "Tagged follow-on searches",
        "Counts the number of follow-on searches with a Mozilla partner tag. These are additional "
        "searches that users performed from a search engine results page after executing a tagged "
        f"search through a SAP. Learn more in the [search data documentation]({SEARCH_DOCS}).",
    ),
    "retained_dau": (
        "Retained (DAU)",
        "Whether the client had at least one DAU-qualifying day in the analysis window (is_dau = "
        "TRUE on any day). Conventionally expressed as a percentage rate: The percentage of "
        "clients from the originating cohort that were then active in the later period. Most "
        "typically, this is measured as Week 2 Retention in order to balance timeliness and "
        "accuracy. But when time permits, Data Science recommends using Week 4 Retention as more "
        "representative of long-term effects.",
    ),
    "active_in_last_3_days_legacy": (
        "3 Days Retention",
        "Records whether a client submitted any pings (i.e. used Firefox) on any of the last 3 "
        "days. Uses legacy telemetry.",
    ),
    "client_level_daily_active_users_v2": (
        "Firefox Desktop Client-Level DAU",
        f"Client-level DAU. The logic is [detailed on the Confluence DAU page]({DAU_DOCS}) and is "
        "automatically cross-checked, actively monitored, and change controlled. This metric "
        "needs to be aggregated by `submission_date`. If it is not aggregated by "
        '`submission_date`, it is similar to a "days of use" metric, and not DAU.',
    ),
    "daily_active_users_per_1000_clients_legacy": (
        "DAU per 1,000 clients",
        f"This metric uses our [canonical, supported definition of Daily Active Users (DAU)]"
        f"({DAU_DOCS}), expressed in a format suited for use in experiments. In an experimental "
        "comparison, it describes the *additional (incremental) DAU* seen on each day from a "
        "treatment. The units are expressed in terms of thousands of clients enrolled or exposed, "
        "so the effect can be scaled to either the observed or expected rollout population as "
        "needed to estimate absolute DAU impact. Effects are averaged to a per-day basis over "
        'each analysis period. Since feature changes often show a strong "novelty effect", this '
        "metric is best interpreted over Week 4 or later, in order to better estimate what the "
        "lasting steady-state effects are.",
    ),
}


def _metric(name, source, reducer):
    rules = (DISJOINT_WEEKLY,) if name in RETENTION_METRICS else (CUMULATIVE_WEEKLY,)
    friendly_name, description = DISPLAY[name]
    return Metric(
        name=name,
        source=source,
        reducer=reducer,
        window_rules=rules,
        friendly_name=friendly_name,
        description=description,
    )


CLIENTS_DAILY = "clients_daily"
SEARCH = "search"
ACTIVE_USERS = "active_users"

GUARDRAILS = [
    _metric("days_of_use", CLIENTS_DAILY, per_unit_distinct_days()),
    _metric(
        "qualified_cumulative_days_of_use",
        CLIENTS_DAILY,
        per_unit_countif(
            "active_hours_sum > 0 AND "
            "scalar_parent_browser_engagement_total_uri_count_normal_and_private_mode_sum > 0"
        ),
    ),
    _metric("active_hours", CLIENTS_DAILY, per_unit_sum("active_hours_sum")),
    _metric(
        "uri_count",
        CLIENTS_DAILY,
        per_unit_sum("scalar_parent_browser_engagement_total_uri_count_sum"),
    ),
    _metric(
        "is_pinned",
        CLIENTS_DAILY,
        per_unit_any("scalar_parent_os_environment_is_taskbar_pinned"),
    ),
    _metric("is_default_browser", CLIENTS_DAILY, per_unit_any("is_default_browser")),
    _metric(
        "retained", CLIENTS_DAILY, per_unit_sum_positive("pings_aggregated_by_this_row")
    ),
    _metric("search_count", SEARCH, per_unit_sum("sap")),
    _metric("ad_clicks", SEARCH, per_unit_sum("ad_click")),
    _metric("searches_with_ads", SEARCH, per_unit_sum("search_with_ads")),
    _metric("organic_search_count", SEARCH, per_unit_sum("organic")),
    _metric("tagged_search_count", SEARCH, per_unit_sum("tagged_sap")),
    _metric("tagged_follow_on_search_count", SEARCH, per_unit_sum("tagged_follow_on")),
    _metric("retained_dau", ACTIVE_USERS, per_unit_any("is_dau")),
    _metric(
        "active_in_last_3_days_legacy",
        ACTIVE_USERS,
        per_unit_min_below("mozfun.bits28.days_since_seen(days_active_bits)", 3, 30),
    ),
    _metric(
        "client_level_daily_active_users_v2", ACTIVE_USERS, per_unit_countif("is_dau")
    ),
    _metric(
        "daily_active_users_per_1000_clients_legacy",
        ACTIVE_USERS,
        per_unit_scaled(per_unit_countif("is_dau"), 1000),
    ),
]

# One entry per source table: where to read it, which columns the metrics above need, and any
# restriction the metric-hub data source carries. Keeping the restriction in the source CTE's WHERE
# is the same filter in one scan. The column each table records the analysis unit in comes from the
# unit rather than from here, since it varies by unit and not by table.
SOURCES = {
    CLIENTS_DAILY: dict(
        table="moz-fx-data-shared-prod.telemetry.clients_daily",
        columns=[
            "active_hours_sum",
            "scalar_parent_browser_engagement_total_uri_count_sum",
            "scalar_parent_browser_engagement_total_uri_count_normal_and_private_mode_sum",
            "scalar_parent_os_environment_is_taskbar_pinned",
            "is_default_browser",
            "pings_aggregated_by_this_row",
        ],
    ),
    SEARCH: dict(
        table="moz-fx-data-shared-prod.search.search_clients_engines_sources_daily",
        columns=[
            "sap",
            "ad_click",
            "search_with_ads",
            "organic",
            "tagged_sap",
            "tagged_follow_on",
        ],
    ),
    # metric-hub's firefox_desktop_active_users_view is
    # (SELECT * FROM ...telemetry.desktop_active_users WHERE is_desktop).
    ACTIVE_USERS: dict(
        table="moz-fx-data-shared-prod.telemetry.desktop_active_users",
        where="is_desktop",
        columns=["is_dau", "days_active_bits"],
    ),
}


def metric_definitions():
    """Group the run's metric set by the source table each one reads.

    Grouped because one query per source is what makes adding a metric to an
    already-scanned table nearly free, which is the property the cost model rests on.
    """
    by_source = {}
    for metric in GUARDRAILS:
        by_source.setdefault(metric.source, []).append(metric)
    return by_source
