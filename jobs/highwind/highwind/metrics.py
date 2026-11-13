"""SECTION 1: METRIC DEFINITIONS.

Metrics and the data sources they read come from metric-hub's `nimbus/` definitions, resolved
through metric-config-parser's `aggregation_plan()`. Each experiment names its own metric set (see
`discovery.py`, which reads it off the Experimenter mirror's already-resolved `metric_config`), and
this module turns the union of every experiment's names, for one run, into the reducers and window
rules `sql_generation.py` composes into one shared scan per source.

A metric-hub metric's aggregation shape (which source it reads, how a unit's rows reduce to one
number) does not depend on which experiment asked for it, so it is resolved once per run. A metric's
windows are declared the same way: metric-hub's `cumulative_window` and `incremental_window` are day
counts, carried over as Highwind's own cumulative and disjoint window rules. Only a *repeating*
family has a Highwind equivalent (see `sql_generation.py`'s window-rule docstring), so a metric
whose `repeat_windows` is false, or that declares neither window, has no window rule Highwind can
build and is reported unresolved rather than silently dropped or mis-rendered. The same holds for
a window that does not fit Highwind's own bucket grid (`BUCKET_LENGTH`): an `incremental_window`
must be exactly one bucket and a `cumulative_window` an exact multiple of one, or there is no grid
to read it off, and reporting it here is what keeps one metric's misfit window from failing the
whole run (see `sql_generation.bucket_length`).
"""

from dataclasses import dataclass
from enum import Enum

import attr
from metric_config_parser.aggregation import Aggregation
from metric_config_parser.aggregation_plan import aggregation_plan
from metric_config_parser.config import ConfigCollection

APP_NAME = "firefox_desktop"

# The metric-hub aggregations whose per-unit value is always 0 or 1. `threshold` turns any
# aggregation into one of these too (see `_is_yes_no_aggregation`), so this set alone is not the
# whole test.
YES_NO_AGGREGATIONS = frozenset({Aggregation.ANY, Aggregation.RECENCY_WITHIN})

CUMULATIVE = "cumulative"
DISJOINT = "disjoint"

# The upper-tail quantile (cut point) of the reference branch's distribution a metric is clipped
# at when it sets neither `winsorize_quantile` nor `winsorize_threshold`. 0.995 mirrors
# Jetstream's own linear-mean default of dropping the top 0.5% (EXP-8000's "Decided" list), so
# turning winsorization on here changes nothing about today's metric behaviour until a metric
# opts out. A later ticket (EXP-8008), once Data Science has validated the reference-branch
# method, switches this one constant to 1 (off).
DEFAULT_WINSORIZE_QUANTILE = 0.995


class WinsorizeMode(Enum):
    """How a metric's window is clipped: not at all, at a reference-branch quantile, or at a
    fixed per-week value. `TOO_FEW_UNITS` is a QUANTILE window the reference branch could not
    support (see `sql_generation.reference_quantiles_cte`), resolved at query time rather than
    here, so it never comes out of `winsorize_mode` below."""

    OFF = "off"
    QUANTILE = "quantile"
    FIXED = "fixed"
    TOO_FEW_UNITS = "skipped_too_few"


@dataclass(frozen=True)
class Winsorize:
    """One metric's resolved winsorize setting: a mode, and the one value that mode uses.

    `value` is the quantile QUANTILE clips at, or the per-week threshold FIXED scales; it is
    unused (and left None) for OFF.
    """

    mode: WinsorizeMode
    value: float | None = None


# Highwind's own bucket grid (see `sql_generation.py`'s `Run` and `bucket_length`): every window a
# run builds is read off buckets this many days long, no matter which metrics the run happens to
# carry. Fixed here, at resolution time, rather than derived from the windows a run's metrics
# declare, so a metric whose window does not fit it is reported unresolved instead of changing the
# grid every other metric in the run is read against.
BUCKET_LENGTH = 7

# The only lengths metric-hub's desktop guardrails declare today. Kept as named fixtures because
# every window rule built from the plan is one of these two shapes; nothing downstream depends on
# the names themselves.
CUMULATIVE_WEEKLY = {"kind": CUMULATIVE, "length": BUCKET_LENGTH}
DISJOINT_WEEKLY = {"kind": DISJOINT, "length": BUCKET_LENGTH}


@dataclass(frozen=True)
class Metric:
    """One metric: which metric-hub source it reads, how it reduces a unit's rows, its windows,
    the display text the experiment that asked for it carried in the mirror, and its winsorize
    setting.

    Args:
        name (str): The metric's name, as metric-hub and the Experimenter mirror know it.
        source (str): Name of the metric-hub data source this metric reads from; the key
            `sql_generation.py` groups metrics by to build one query per source.
        reducer (object): How one unit's rows become one number: a `BucketedAggregation`
            (`metric_config_parser/aggregation.py`) resolved from metric-hub's own aggregation.
        window_rules (tuple): The window rules (e.g. `CUMULATIVE_WEEKLY`, `DISJOINT_WEEKLY`, or a
            metric-hub-declared length) this metric's windows are generated from.
        friendly_name (str, optional): Display name the mirror carried for this metric, for the
            experiment that asked for it. Defaults to None (no experiment-specific display text).
        description (str, optional): Display description, from the same source as
            `friendly_name`. Defaults to None.
        winsorize_quantile (float, optional): The upper-tail quantile (cut point) of the
            reference branch's distribution to clip at, e.g. 0.995 keeps everything at or below
            the reference branch's own 99.5th percentile. EXP-8004's field: metric-hub's own
            `nimbus/` metric definitions will carry it once that ships, and
            metric-config-parser's `AggregationMetric`
            (`metric_config_parser/aggregation_plan.py`) will resolve it the same way it resolves
            everything else `Metric` carries. Defaults to None, meaning "not set", exactly as an
            unset field will read once EXP-8004 lands; see `_winsorize_fields` below for where
            that absence is read defensively, in this one place. A metric sets at most one of
            `winsorize_quantile` and `winsorize_threshold` (setting both is a `winsorize_mode`
            `ValueError`); there is no separate mode field, since `winsorize_mode` infers
            quantile, fixed or off entirely from which one (if either) is set.
        winsorize_threshold (float, optional): A fixed per-week value to clip at, the other half
            of the same EXP-8004 field pair described above (see `winsorize_quantile`). Defaults
            to None, meaning "not set".
        is_yes_no (bool, optional): Whether this metric's per-unit value is always 0 or 1.
            Resolved once, in `split_resolved_metrics`, from the metric-hub `MetricDefinition`'s
            own `aggregation`/`threshold` fields (see `_is_yes_no_aggregation`) rather than read
            again from the reducer wherever it is needed. Defaults to False.
    """

    name: str
    source: str
    reducer: object
    window_rules: tuple
    friendly_name: str | None = None
    description: str | None = None
    winsorize_quantile: float | None = None
    winsorize_threshold: float | None = None
    is_yes_no: bool = False


@dataclass(frozen=True)
class UnresolvedMetric:
    """A metric name an experiment asked for that this run could not compute, and why."""

    name: str
    reason: str


def load_metric_hub():
    """Load metric-hub's own `nimbus/` definitions, once per run.

    A fresh clone of `mozilla/metric-hub` every call (see metric-config-parser's
    `ConfigCollection.from_github_repo`), so the caller does this once for the whole run rather than
    once per experiment.
    """
    return ConfigCollection.from_github_repo()


def build_aggregation_plan(configs, metric_names):
    """Resolve one run's metric names against metric-hub's `nimbus/` definitions for this app."""
    return aggregation_plan(configs, APP_NAME, sorted(set(metric_names)))


def window_rules(aggregation_metric):
    """The window rules Highwind can build from a plan metric's window fields, or why not.

    Returns `(rules, None)` when the metric has at least one window family, repeats it, and that
    family fits Highwind's bucket grid, or `(None, reason)` otherwise. A metric with neither
    `cumulative_window` nor `incremental_window` set has nothing for Highwind to compute a window
    over at all; one that sets `repeat_windows` false declares a single window that stops, which
    none of Highwind's window rules can express, since every rule it has ever supported extends for
    as long as the experiment runs (see `sql_generation.py`'s `Run` docstring). An
    `incremental_window` that is not exactly one `BUCKET_LENGTH`, or a `cumulative_window` that is
    not an exact multiple of one, has no grid to read off: a disjoint window must BE one bucket and
    a cumulative one is summed from whole buckets (see `sql_generation.bucket_length`'s own
    backstop validation). Mapping any of these cases onto a repeating rule would compute windows
    the metric never declared, or on a grid that does not fit.
    """
    no_window = (
        aggregation_metric.cumulative_window is None
        and aggregation_metric.incremental_window is None
    )
    if no_window:
        return None, "no window declared"
    if not aggregation_metric.repeat_windows:
        return None, (
            "repeat_windows is false; Highwind's window rules always repeat and cannot "
            "express a one-off window"
        )
    incremental = aggregation_metric.incremental_window
    if incremental is not None and incremental != BUCKET_LENGTH:
        return None, (
            f"incremental_window is {incremental} days, not {BUCKET_LENGTH}; a disjoint window "
            f"must be exactly one bucket on Highwind's {BUCKET_LENGTH}-day grid"
        )
    cumulative = aggregation_metric.cumulative_window
    if cumulative is not None and cumulative % BUCKET_LENGTH:
        return None, (
            f"cumulative_window is {cumulative} days, not a multiple of Highwind's "
            f"{BUCKET_LENGTH}-day bucket grid"
        )
    rules = []
    if cumulative is not None:
        rules.append({"kind": CUMULATIVE, "length": cumulative})
    if incremental is not None:
        rules.append({"kind": DISJOINT, "length": incremental})
    return tuple(rules), None


def _winsorize_fields(aggregation_metric):
    """Read `winsorize_quantile`/`winsorize_threshold` off a plan metric, defensively.

    EXP-8004 adds both fields to metric-config-parser's `AggregationMetric`
    (`metric_config_parser/aggregation_plan.py`); this job's current pin of that package does not
    carry them, so a direct `aggregation_metric.winsorize_quantile` would raise `AttributeError`
    on every metric today. Checked through attrs' own field registry rather than a direct read,
    since that is the one place this absence is handled: once the pin moves past EXP-8004 every
    `AggregationMetric` declares both fields, `declared` is always true, and this function (and
    the `attr` import) can be deleted in favour of two plain attribute reads at the call site.
    """
    declared = attr.fields_dict(type(aggregation_metric))
    if "winsorize_quantile" not in declared or "winsorize_threshold" not in declared:
        return None, None
    return aggregation_metric.winsorize_quantile, aggregation_metric.winsorize_threshold


def _is_yes_no_aggregation(metric_definition):
    """Whether a metric-hub metric's per-unit value is always 0 or 1.

    True for the `any` and `recency_within` aggregations, and for any aggregation a `threshold`
    was applied to: metric_config_parser's `aggregation.py` (`aggregation_bucketed`) finalizes
    all three the same way, through a `CAST` of a comparison. Read off `MetricDefinition`'s own
    structured `aggregation` and `threshold` fields rather than off the rendered
    `BucketedAggregation` SQL, so a metric whose finalize happens to contain a `CAST` for an
    unrelated reason, such as EXP-8013's proposed `cast_as_int` (which would cast a plain sum,
    not a comparison, to an integer), is never mistaken for one. `AggregationMetric`
    (`metric_config_parser/aggregation_plan.py`) does not carry `aggregation`/`threshold`
    forward itself, which is why `split_resolved_metrics` looks the `MetricDefinition` up
    separately through `configs.get_nimbus_metric_definition` rather than reading them off the
    plan.
    """
    return (
        metric_definition.aggregation in YES_NO_AGGREGATIONS
        or metric_definition.threshold is not None
    )


def split_resolved_metrics(plan, configs):
    """Split one run's plan into the metrics Highwind can compute and the ones it cannot, with why.

    A name fails to resolve two ways: no `nimbus/` definition at all (`plan.missing`), or a
    definition whose windows Highwind has no rule for (`window_rules` above). Both are reported by
    name and reason rather than raised, so neither one metric nor an unrelated experiment is lost
    over it.

    `configs` is the same `ConfigCollection` the plan was built from, looked up again here for
    each resolved name's own `MetricDefinition`, since the plan itself carries only the already
    reduced `BucketedAggregation` and drops `aggregation`/`threshold` on the way. A name that
    resolved through `aggregation_plan()` always has a `MetricDefinition` to find this way, so
    the lookup failing would mean the two disagree about what resolved; treated as not yes/no
    rather than raised, since nothing here depends on it being impossible.
    """
    resolved, unresolved = {}, {}
    for name in plan.missing:
        unresolved[name] = "no nimbus/ definition for this app"
    for name, aggregation_metric in plan.metrics.items():
        rules, reason = window_rules(aggregation_metric)
        if rules is None:
            unresolved[name] = reason
            continue
        winsorize_quantile, winsorize_threshold = _winsorize_fields(aggregation_metric)
        metric_definition = configs.get_nimbus_metric_definition(name, APP_NAME)
        is_yes_no = metric_definition is not None and _is_yes_no_aggregation(metric_definition)
        resolved[name] = Metric(
            name=name,
            source=aggregation_metric.data_source,
            reducer=aggregation_metric.aggregation,
            window_rules=rules,
            winsorize_quantile=winsorize_quantile,
            winsorize_threshold=winsorize_threshold,
            is_yes_no=is_yes_no,
        )
    return resolved, unresolved


def winsorize_mode(metric):
    """Resolve one metric's `winsorize_quantile`/`winsorize_threshold` into a mode and value.

    `winsorize_quantile` is the cut point, not the tail share, so its range is `(0, 1]` and a
    quantile of 1 means off (nothing is above the 100th percentile). `winsorize_threshold` has no
    such off value: metric-hub (EXP-8004) requires a fixed threshold to be strictly positive, so
    quantile 1 is the only way to turn winsorization off. A yes/no metric is never clipped
    regardless of its fields, since clipping a 0/1 value changes nothing.
    """
    if metric.is_yes_no:
        return Winsorize(WinsorizeMode.OFF)
    quantile = metric.winsorize_quantile
    threshold = metric.winsorize_threshold
    if quantile is not None and threshold is not None:
        raise ValueError(
            f"metric '{metric.name}' sets both winsorize_quantile and winsorize_threshold"
        )
    if quantile is not None and not 0 < quantile <= 1:
        raise ValueError(
            f"metric '{metric.name}' has winsorize_quantile {quantile}, which must be in "
            f"(0, 1]: 1 means off, and a quantile at or below 0 is not a point on the "
            f"distribution"
        )
    if threshold is not None and threshold <= 0:
        raise ValueError(
            f"metric '{metric.name}' has winsorize_threshold {threshold}, which must be > 0: "
            f"winsorize_quantile = 1 is how a metric turns winsorization off"
        )
    if quantile == 1:
        return Winsorize(WinsorizeMode.OFF)
    if quantile is not None:
        return Winsorize(WinsorizeMode.QUANTILE, quantile)
    if threshold is not None:
        return Winsorize(WinsorizeMode.FIXED, threshold)
    return Winsorize(WinsorizeMode.QUANTILE, DEFAULT_WINSORIZE_QUANTILE)


def group_by_source(resolved_metrics):
    """Group resolved metrics by the metric-hub source they read.

    Grouped because one query per source is what makes adding a metric to an already-scanned table
    nearly free, which is the property the cost model rests on (see `sql_generation.py`).
    """
    by_source = {}
    for metric in resolved_metrics.values():
        by_source.setdefault(metric.source, []).append(metric)
    return by_source


def metrics_for_experiment(experiment, resolved_metrics, unresolved_metrics):
    """One experiment's own metric list, with its own display text, and the names it could not get.

    A metric's aggregation shape is the same for every experiment that reads it, so
    `resolved_metrics` is resolved once for the whole run; this keeps only the names the experiment
    itself asked for and attaches the friendly name and description the mirror carried for THAT
    experiment, which can differ from another experiment's text for a metric of the same name.
    """
    chosen, problems = [], []
    for name in experiment.metric_names:
        metric = resolved_metrics.get(name)
        if metric is None:
            reason = unresolved_metrics.get(name, "no nimbus/ definition for this app")
            problems.append(UnresolvedMetric(name=name, reason=reason))
            continue
        display = experiment.metrics.get(name)
        chosen.append(
            Metric(
                name=metric.name,
                source=metric.source,
                reducer=metric.reducer,
                window_rules=metric.window_rules,
                friendly_name=display.friendly_name if display else None,
                description=display.description if display else None,
                winsorize_quantile=metric.winsorize_quantile,
                winsorize_threshold=metric.winsorize_threshold,
                is_yes_no=metric.is_yes_no,
            )
        )
    return chosen, problems
