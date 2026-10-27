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

from metric_config_parser.aggregation_plan import aggregation_plan
from metric_config_parser.config import ConfigCollection

APP_NAME = "firefox_desktop"

CUMULATIVE = "cumulative"
DISJOINT = "disjoint"

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
    and the display text the experiment that asked for it carried in the mirror."""

    name: str
    source: str
    reducer: object
    window_rules: tuple
    friendly_name: str | None = None
    description: str | None = None


@dataclass(frozen=True)
class UnresolvedMetric:
    """A metric name an experiment asked for that this run could not compute, and why."""

    name: str
    reason: str


def load_configs():
    """Load metric-hub's own `nimbus/` definitions, once per run.

    A fresh clone of `mozilla/metric-hub` every call (see metric-config-parser's
    `ConfigCollection.from_github_repo`), so the caller does this once for the whole run rather than
    once per experiment.
    """
    return ConfigCollection.from_github_repo()


def load_plan(configs, metric_names):
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


def resolve(plan):
    """Split one run's plan into the metrics Highwind can compute and the ones it cannot, with why.

    A name fails to resolve two ways: no `nimbus/` definition at all (`plan.missing`), or a
    definition whose windows Highwind has no rule for (`window_rules` above). Both are reported by
    name and reason rather than raised, so neither one metric nor an unrelated experiment is lost
    over it.
    """
    resolved, unresolved = {}, {}
    for name in plan.missing:
        unresolved[name] = "no nimbus/ definition for this app"
    for name, aggregation_metric in plan.metrics.items():
        rules, reason = window_rules(aggregation_metric)
        if rules is None:
            unresolved[name] = reason
            continue
        resolved[name] = Metric(
            name=name,
            source=aggregation_metric.data_source,
            reducer=aggregation_metric.aggregation,
            window_rules=rules,
        )
    return resolved, unresolved


def group_by_source(resolved_metrics):
    """Group resolved metrics by the metric-hub source they read.

    Grouped because one query per source is what makes adding a metric to an already-scanned table
    nearly free, which is the property the cost model rests on (see `sql_generation.py`).
    """
    by_source = {}
    for metric in resolved_metrics.values():
        by_source.setdefault(metric.source, []).append(metric)
    return by_source


def for_experiment(experiment, resolved_metrics, unresolved_metrics):
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
            )
        )
    return chosen, problems
