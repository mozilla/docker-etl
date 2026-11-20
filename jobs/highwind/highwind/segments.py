"""SECTION 1B: SEGMENT DEFINITIONS.

Segments are a per-unit population split read from metric-hub, resolved the same way metrics are
(`metrics.py`): each experiment names its own selected segments (`discovery.py`, off the
Experimenter mirror's already-resolved `metric_config.segments`), and this module resolves the
union of every experiment's names, for one run, against metric-hub's `nimbus/` segment definitions
through metric-config-parser's `segment_plan()`. Every unit also belongs to
`discovery.ALL_ENROLLED`, the trivial segment that needs no resolution and applies to every unit
regardless of what else resolves, so it never appears here.

`segment_plan()` is detected explicitly rather than imported unconditionally, so the installed
parser's absence of the function reads as "nothing resolves" rather than an `ImportError` the run
has to catch. Until a parser that has it is installed, every requested segment name is reported
unresolved the same way an unported outcome metric is, and every unit's only segment stays
`all_enrolled`, which is what keeps a run's output unchanged apart from the segment column/key
`sql_generation.py` and `gbstats_compute.py` add.
"""

from dataclasses import dataclass

try:
    from metric_config_parser.segment_plan import segment_plan as _segment_plan
except ImportError:
    _segment_plan = None

from . import discovery
from .metrics import APP_NAME

NOT_AVAILABLE_REASON = "segment_plan() is not available in the installed metric-config-parser"
NOT_RESOLVED_REASON = "no nimbus/ segment definition for this app"
ALL_ENROLLED_RESERVED_REASON = (
    "all_enrolled is the built-in segment every experiment gets; a metric-hub segment reusing "
    "that name is not computed"
)


@dataclass(frozen=True)
class SegmentSource:
    """One metric-hub segment data source, resolved for this app.

    Carries the same per-unit id columns `units.AnalysisUnit.source_column` already reads off a
    metric's `AggregationSource`, so a segment source is read by the exact same code.
    """

    name: str
    from_expression: str
    window_start: int
    window_end: int
    client_id_column: str
    group_id_column: str
    submission_date_column: str


@dataclass(frozen=True)
class Segment:
    """One resolved segment: which source it reads and its per-unit boolean SQL."""

    name: str
    source: str
    select_expression: str


@dataclass(frozen=True)
class UnresolvedSegment:
    """A segment name an experiment selected that this run could not compute, and why."""

    name: str
    reason: str


def build_segment_plan(configs, segment_names):
    """Resolve one run's segment names against metric-hub, once, for every experiment's own names.

    Returns `(sources, segments, unresolved)`: `sources` keyed by metric-hub source name,
    `segments` keyed by segment name, and `unresolved` a name-to-reason map for anything
    `segment_plan()` could not resolve, or for everything when the installed parser has no
    `segment_plan()` at all (see the module docstring).
    """
    names = sorted(set(segment_names))
    if _segment_plan is None:
        return {}, {}, {name: NOT_AVAILABLE_REASON for name in names}
    plan = _segment_plan(configs, APP_NAME, names)
    sources = {
        name: SegmentSource(
            name=source.data_source,
            from_expression=source.from_expression,
            window_start=source.window_start,
            window_end=source.window_end,
            client_id_column=source.client_id_column,
            group_id_column=source.group_id_column,
            submission_date_column=source.submission_date_column,
        )
        for name, source in plan.sources.items()
    }
    segments = {
        name: Segment(
            name=segment.name,
            source=segment.data_source,
            select_expression=segment.select_expression,
        )
        for name, segment in plan.segments.items()
    }
    unresolved = {name: NOT_RESOLVED_REASON for name in plan.missing}
    return sources, segments, unresolved


def segments_for_experiment(experiment, resolved_segments, unresolved_segments):
    """One experiment's own resolved segment names, plus the ones it asked for and could not get.

    `all_enrolled` is not part of this: it is added uniformly by whatever reads this list, since
    every experiment gets it whether or not it resolved anything here. A selected name of
    `discovery.ALL_ENROLLED` itself is always reported unresolved, even if metric-hub happens to
    have a segment definition by that name, since it is reserved for the one every experiment
    already gets.
    """
    chosen, problems = [], []
    for name in experiment.segment_names:
        if name == discovery.ALL_ENROLLED:
            problems.append(UnresolvedSegment(name=name, reason=ALL_ENROLLED_RESERVED_REASON))
            continue
        segment = resolved_segments.get(name)
        if segment is None:
            reason = unresolved_segments.get(name, NOT_RESOLVED_REASON)
            problems.append(UnresolvedSegment(name=name, reason=reason))
            continue
        chosen.append(segment)
    return chosen, problems
