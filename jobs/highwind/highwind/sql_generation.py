"""SECTION 2: SQL GENERATION.

One query per (source table, analysis unit) for the WHOLE RUN, not per experiment. Each returns
per-branch sufficient statistics in long format, one row per (slug, branch, window, metric), so the
result's width is fixed no matter how many experiments, metrics or windows a run covers.

Each source is read once per unit, over the union of that unit's experiments' date ranges, and
grouped by slug. Those ranges overlap almost entirely, so reading per experiment would read the same
calendar day once per experiment covering it. One consequence worth knowing: the SCAN is set by the
longest experiment's range rather than by how many experiments there are, so an experiment inside
that range adds little to it. The join below still grows with the cohort.

Splitting by unit rather than resolving the join key inside one query is what keeps every join a
plain equality on a single column. The alternative, one scan whose join key is chosen per row,
turns the largest join in the job into one over an expression, which is the shape that costs a
planner its hash join. A run covering both units pays for two passes over each source instead; a
run whose experiments all randomize the same way is one query per source as before.

Window membership is arithmetic rather than a join. Every window is built out of one grid of
disjoint buckets, and a source row's bucket is `DIV(tenure_day, bucket_length)`, so the row is read
once no matter how many windows an experiment has. Each source query is six CTEs, of which three
do the aggregation:

    cohort           the run's assignment table, one row per (slug, unit)
    experiment_meta  per-slug facts, currently how many buckets each experiment has matured
    source_rows      the single scan, bounded to the columns this source's metrics name

    A  bucket_totals   one pass over source rows, grouped to (slug, unit, bucket), one raw
                       aggregate per metric. Metrics are columns here, bounded by the metric set,
                       and the output is smaller than its own input.
       unit_buckets    every (slug, unit, bucket) a unit has matured, whether or not it reported.
                       This is what makes `n` the cohort rather than the reporting subset of it.
    B  unit_windows    each cumulative window as a running total across buckets, which is a window
                       function over a units-by-buckets table rather than a second pass over
                       source rows. Thresholds are applied here, once, to the combined value.
    C  the rollup      unpivot metrics to rows and aggregate to branch, so the output schema stays
                       independent of the metric set.

Nothing per-unit leaves BigQuery. The per-unit stages are intermediate CTEs inside the same query
that aggregates them away.
"""

import datetime
import math
import textwrap

from .metrics import SOURCES
from .units import PROFILE_GROUP_ID

# The bucket a unit's pre-enrollment rows land in. The covariate is one fixed window rather than
# one per analysis window, so it is a single extra bucket that sorts before every other and is
# read back out of the same pass.
PRE_BUCKET = -1

# Granularity of the branch-balancing hash. A unit is kept when its hash modulo this falls under
# the branch's retention rate times this, so it sets how finely a rate can be expressed: one part
# per million is far below the sampling error on any branch large enough to need balancing.
BALANCE_RESOLUTION = 1_000_000

# A profile group fanning out to more than this many client ids is a cloned or bot fleet rather
# than a household. The threshold and the distribution behind it are the SQL spec's, §3.4 and §1.4:
# a real group is one client, and the tail runs orders of magnitude above that. It matters at group
# grain in a way it does not at client grain, because the whole fleet collapses into a single
# analysis unit carrying a physically impossible value and lands entirely in whichever branch its id
# hashes to.
#
# Detected over enrollment events rather than over `baseline_clients_daily` across the window as the
# spec has it. That is an approximation and it under-detects: a fleet only shows the clients that
# enrolled, so one whose members enrolled sparsely can stay under the threshold and survive. The
# spec's version needs its own scan of a client-daily source, where this rides along on the scan the
# cohort already does.
MAX_CLIENTS_PER_GROUP = 10

# The id a group's fan-out is counted over. Present on the enrollment event beside the group id, so
# counting it costs a column rather than a join.
FLEET_CLIENT_COLUMN = "legacy_telemetry_client_id"

# How many times the smallest branch the largest is allowed to be. Chosen at the knee of the
# precision-versus-units curve for a 90/10 split: 4:1 discards about half the units for ~6% wider
# intervals, where 1:1 discards 80% for 34%. Above ~6:1 there is little left to save, below ~3:1
# the interval cost climbs steeply.
MAX_BRANCH_RATIO = 4

# Sample of the analysed population, as a percentage, or None for all of it, carried on `Run`.
#
# Both sides of the join have to be sampled on the SAME id, or they select two populations that
# barely intersect, and because the join preserves unmatched units at zero the result is a quietly
# deflated metric rather than an error. So the sample is always a hash of the analysis unit itself,
# via `udf.safe_sample_id`, which is the function behind the tables' own `sample_id` column and was
# verified to reproduce it on every row of a test partition. It returns NULL for anything that is
# not a 36-byte uuid, which every unit here is, and a unit that was not would select no rows at all.
#
# Where the unit IS the legacy client id, the source side can be written as the bare `sample_id`
# column instead. That selects exactly the same clients and prunes storage rather than reading and
# discarding, so the scan falls roughly in proportion to the sample. (It is the second clustering
# key behind `normalized_channel` on two of the three, which weakens the pruning for a predicate on
# `sample_id` alone.) Where the unit is anything else, that column is a hash of the wrong id: it
# would keep some members of a unit and drop others, so a sampled run would aggregate partial units
# and report wrong numbers rather than a smaller correct sample. Those runs hash the unit on both
# sides and give up the pruning, which is the right trade because `--sample-percent` is a
# development lever, and `is_partial_run` and `writes_blobs` already keep its numbers out of both
# the tables and the published blobs.


def sample_clause(column, sample_percent, prefix="AND "):
    """Restrict to a sample of units, keyed on the analysis unit's own id.

    Qualified, because the UDF is defined once in the shared-prod project and this job's client
    defaults to a different one.
    """
    if sample_percent is None:
        return ""
    return f"{prefix}mozdata.udf.safe_sample_id({column}) < {sample_percent}"


# ------------------------------------------------------------------------------- the run ----


class Run:
    """Everything the shared queries need to know about the experiments in one run.

    Holds the bucket grid, which is shared: the bucket LENGTH is
    shared by every experiment because every window rule is weekly, and the only thing that differs
    per experiment is how many buckets it has matured, which rides along as a per-slug column.
    """

    def __init__(
        self, experiments, windows_by_slug, as_of, covariate_days, sample_percent=None
    ):
        """Derive the run's bucket grid from the windows its experiments declare."""
        self.as_of = as_of
        self.covariate_days = covariate_days
        self.sample_percent = sample_percent
        # Only experiments with at least one matured window take part in the shared queries. A
        # younger one contributes no rows and would only widen the scan.
        self.experiments = [
            experiment
            for experiment in experiments
            if windows_by_slug.get(experiment.slug)
        ]
        self.windows_by_slug = windows_by_slug
        every_window = [
            window
            for experiment in self.experiments
            for window in windows_by_slug[experiment.slug]
        ]
        self.length = bucket_length(every_window) if every_window else 7
        self.buckets_by_slug = {
            experiment.slug: max(
                (window.end + 1) // self.length
                for window in windows_by_slug[experiment.slug]
            )
            for experiment in self.experiments
        }

    @property
    def slugs(self):
        """Return the slugs taking part in this run's shared queries."""
        return [experiment.slug for experiment in self.experiments]

    @property
    def units(self):
        """Return the analysis units this run's experiments randomized on, in a stable order."""
        return sorted(
            {experiment.unit for experiment in self.experiments},
            key=lambda unit: unit.kind,
        )

    def by_unit(self):
        """Split into one sub-run per analysis unit, each covering only that unit's experiments.

        The source queries are built from these rather than from the whole run, so each reads only
        the range its own experiments need: a run whose group-randomized experiments are all recent
        does not scan back to the oldest client-randomized one to serve them.
        """
        return {
            unit: Run(
                [
                    experiment
                    for experiment in self.experiments
                    if experiment.unit == unit
                ],
                self.windows_by_slug,
                self.as_of,
                self.covariate_days,
                self.sample_percent,
            )
            for unit in self.units
        }

    def earliest_start(self):
        """Return the start date of the run's oldest experiment."""
        return min(experiment.start_date for experiment in self.experiments)

    def first_source_date(self):
        """Return the oldest submission_date any experiment in the run needs.

        The union bound: one experiment's covariate reaches furthest back, and reading from there
        once serves every experiment.
        """
        return self.earliest_start() - datetime.timedelta(days=self.covariate_days)

    def experiment_meta_cte(self):
        """Per-slug facts the shared queries join against: how many buckets each has matured.

        A literal rather than a table because it is one short row per experiment, derived from the
        mirror and the run date, and inlining it lets BigQuery treat it as a broadcast side of the
        join instead of another scan.
        """
        rows = ",\n            ".join(
            f"STRUCT('{slug}' AS slug, {count} AS matured_buckets)"
            for slug, count in sorted(self.buckets_by_slug.items())
        )
        return f"SELECT * FROM UNNEST([\n            {rows}])"

    def matured_buckets(self):
        """How many whole buckets a unit has completed, capped at its experiment's horizon.

        The cap is what makes the window horizon save anything. Without it a long-tenured unit
        still generates every bucket it ever matured, and the ones past the horizon carry no
        window label, so they are built, joined, sorted and only then discarded. It also bounds the
        source rows each unit contributes, since stage A reaches only this far.
        """
        elapsed = (
            f"DIV(DATE_DIFF(DATE '{self.as_of}', cohort.enrollment_date, DAY), "
            f"{self.length})"
        )
        return f"LEAST({elapsed}, experiment_meta.matured_buckets)"


def bucket_length(windows):
    """Determine the disjoint bucket length every window in this run is built from.

    Validated rather than assumed. A cumulative window is summed from whole buckets, so its length
    has to be an exact multiple of the bucket length or there is no grid to sum it over, and a
    disjoint window has to BE one bucket or it cannot be read off a single row. Both current rules
    are 7 days so this holds trivially, but it is a real constraint on the window vocabulary and a
    silently wrong answer if a future rule breaks it.
    """
    disjoint = {window.length for window in windows if window.kind == "disjoint"}
    if len(disjoint) > 1:
        raise ValueError(
            f"disjoint windows must all share one length to form a bucket grid, got "
            f"{sorted(disjoint)}"
        )
    length = (
        disjoint.pop() if disjoint else math.gcd(*[window.length for window in windows])
    )
    for window in windows:
        if window.kind == "disjoint" and window.length != length:
            raise ValueError(
                f"disjoint window {window.label} is {window.length} days, but the bucket grid is "
                f"{length}; a disjoint window must be exactly one bucket"
            )
        if window.kind == "cumulative" and window.start != 0:
            raise ValueError(
                f"cumulative window {window.label} starts at {window.start}, not 0"
            )
        if window.start % length or (window.end + 1) % length:
            raise ValueError(
                f"window {window.label} [{window.start},{window.end}] does not land on the "
                f"{length}-day bucket grid; cumulative window lengths must be exact multiples of "
                f"the disjoint length"
            )
    return length


# ------------------------------------------------------------------------------ the cohort ----


def cohort_query(run):
    """Every unit's slug, branch and first enrollment date, for every experiment at once.

    One pass over `events_stream` for the whole run rather than one per experiment. Enrollment
    events for all slugs sit in the same partitions, so extracting them together and partitioning by
    slug afterwards costs one scan rather than one per experiment. That holds across analysis units
    too: the enrollment event carries every id the run could key on, so which one a slug uses is
    resolved after the scan rather than by scanning again.

    Read from `events_stream` rather than `enrollment_status` in `nimbus_targeting_context`. The
    targeting-context ping carries an events array and fires on every Nimbus evaluation, so scanning
    it over an experiment's lifetime costs two orders of magnitude more, and this proof of concept
    only analyses experiments. Rollouts will need `enrollment_status`, since it is the only source
    that records a rollout's assignment, and that cost has to be paid then rather than avoided.

    Cloned fleets are dropped here too, on the runs whose grain they distort. See
    `MAX_CLIENTS_PER_GROUP`.

    A unit is taken at its FIRST enrollment in each experiment, so a client that re-enrols is
    anchored once. Units seen on more than one branch of the same experiment are dropped rather than
    resolved: a contradictory assignment is a data problem, and picking a branch would silently bias
    the comparison it feeds. A unit in several DIFFERENT experiments is kept in each, which is why
    the grouping is by (slug, unit) rather than by unit.

    Lopsided branches are then capped rather than equalised, per slug. A holdback splits 90/10 or
    worse, so the large branch carries most of the units the aggregation has to move while adding
    little to the precision of the contrast, whose standard error goes as sqrt(1/n_ref + 1/n_treat)
    and is dominated by the smaller branch. But "adds little" is not "adds nothing", and equalising
    is the expensive way to find that out: on a 90/10 split, cutting the large branch to 1:1
    discards 80% of the units and widens the interval by 34%, whereas capping at 4:1 discards about
    half for roughly 6%. The knee of that curve is the point of the cap.

    The sampling is a hash of the unit id, so it is deterministic across runs and nested as the rate
    moves, which stops the cohort churning from day to day and the results jittering for reasons
    unrelated to the data. It is a no-op on any experiment already inside the ratio.
    """
    return textwrap.dedent(f"""
        WITH valid_branch AS (
        {_indent(branch_lookup(run), 10)}
        ),
        assigned AS (
        {_indent(assignment_query(run), 10)}
        ),
        branch_sizes AS (
          SELECT slug, branch, COUNT(*) AS units FROM assigned GROUP BY slug, branch
        ),
        smallest_branch AS (
          SELECT slug, MIN(units) AS smallest FROM branch_sizes GROUP BY slug
        )
        SELECT
          assignment.slug,
          assignment.unit_kind,
          assignment.unit_id,
          assignment.branch,
          assignment.enrollment_date
        FROM assigned AS assignment
        JOIN branch_sizes AS branch_size USING (slug, branch)
        JOIN smallest_branch AS smallest USING (slug)
        WHERE MOD(ABS(FARM_FINGERPRINT(assignment.unit_id)), {BALANCE_RESOLUTION})
              < {BALANCE_RESOLUTION}
                * LEAST(1.0, {MAX_BRANCH_RATIO} * smallest.smallest / branch_size.units)
    """).strip()


def branch_lookup(run):
    """List the (slug, branch, unit kind) triples this run recognises.

    Inlined so the enrollment scan can discard an event naming a branch the mirror does not list,
    which happens when a recipe is edited after launch, without a second pass to find out. The unit
    rides along on the same rows, so the join that validates the branch is also the join that says
    which of the event's ids this slug is analysed on.
    """
    rows = ",\n    ".join(
        f"STRUCT('{experiment.slug}' AS slug, '{branch}' AS branch, "
        f"'{experiment.unit.kind}' AS unit_kind)"
        for experiment in run.experiments
        for branch in experiment.branches
    )
    return f"SELECT * FROM UNNEST([\n    {rows}])"


def assignment_query(run):
    """Each unit's branch and first enrollment date per slug, before any balancing.

    The enrollment event carries an id per unit the run could key on, and the resolved one is
    selected per slug. Which id matters: a source table records a client as `client_id`, so
    carrying the Glean id through instead would match nothing while still producing a full cohort,
    and every metric would silently sum to zero rather than fail.
    """
    slugs = ", ".join(f"'{slug}'" for slug in run.slugs)
    columns = {unit.enrollment_column for unit in run.units}
    if detects_fleets(run):
        columns.add(FLEET_CLIENT_COLUMN)
    id_columns = ",\n      ".join(sorted(columns))
    return textwrap.dedent(f"""
        SELECT
          slug,
          unit_kind,
          unit_id,
          ANY_VALUE(branch_slug) AS branch,
          MIN(enrollment_date) AS enrollment_date
        FROM (
          SELECT
            event.slug,
            valid.unit_kind,
            {enrollment_unit_id(run)} AS unit_id,
            event.branch_slug,
            event.enrollment_date{fleet_select(run)}
          FROM (
            SELECT
              JSON_VALUE(event_extra, '$.experiment') AS slug,
              {id_columns},
              JSON_VALUE(event_extra, '$.branch') AS branch_slug,
              DATE(submission_timestamp) AS enrollment_date
            FROM `mozdata.firefox_desktop.events_stream`
            WHERE DATE(submission_timestamp)
                  BETWEEN '{run.earliest_start()}' AND '{run.as_of}'
              AND event_category = 'nimbus_events'
              AND event_name = 'enrollment'
              AND JSON_VALUE(event_extra, '$.experiment') IN ({slugs})
          ) AS event
          JOIN valid_branch AS valid
            ON valid.slug = event.slug AND valid.branch = event.branch_slug
        )
        WHERE unit_id IS NOT NULL
          {sample_clause("unit_id", run.sample_percent, prefix="AND ")}
        GROUP BY slug, unit_kind, unit_id
        HAVING COUNT(DISTINCT branch_slug) = 1{fleet_having(run)}
    """).strip()


def detects_fleets(run):
    """Whether any experiment in this run is analysed at the grain a fleet collapses into."""
    return any(unit.kind == PROFILE_GROUP_ID for unit in run.units)


def fleet_select(run):
    """Carry the client id up to the aggregation, where a group's fan-out is counted."""
    return f",\n            event.{FLEET_CLIENT_COLUMN}" if detects_fleets(run) else ""


def fleet_having(run):
    """Drop cloned fleets, at the unit kind the exclusion is meaningful for.

    Conditional on the unit kind because the cohort holds every experiment in the run at once, and a
    client-randomized slug's unit already IS the client: counting clients per unit there is a test
    of nothing, and a client sharing a group with a fleet would be dropped for the fleet's fan-out
    rather than its own.
    """
    if not detects_fleets(run):
        return ""
    return (
        f"\n   AND (unit_kind != '{PROFILE_GROUP_ID}'\n"
        f"        OR COUNT(DISTINCT {FLEET_CLIENT_COLUMN}) <= {MAX_CLIENTS_PER_GROUP})"
    )


def enrollment_unit_id(run):
    """Pick each slug's own id out of the enrollment event, by the unit it randomized on.

    One expression rather than one scan per unit: the ids sit in the same row, so choosing between
    them is free, whereas reading the enrollment partitions again is not. The choice is safe here
    for the same reason it would not be on a source join, which is where the cohort's single
    resolved column is what keeps every join an equality.
    """
    run_units = run.units
    if len(run_units) == 1:
        return f"event.{run_units[0].enrollment_column}"
    cases = " ".join(
        f"WHEN '{unit.kind}' THEN event.{unit.enrollment_column}" for unit in run_units
    )
    return f"CASE valid.unit_kind {cases} END"


# ----------------------------------------------------------------------- the source queries ----


def build_queries(run, metrics_by_source, cohort_table):
    """One SQL string per (source table, analysis unit), covering every experiment in the run.

    Keyed by both, because a run whose experiments randomize on different units reads each source
    once per unit and the two are separate queries with separate costs.
    """
    return {
        f"{source}/{unit.kind}": build_source_query(
            unit_run, source, metrics, cohort_table, unit
        )
        for unit, unit_run in run.by_unit().items()
        for source, metrics in metrics_by_source.items()
    }


def build_source_query(run, source, metrics, cohort_table, unit):
    """Build the full query for one source: cohort, per-slug facts, rows, stages, rollup."""
    source_definition = SOURCES[source]
    return textwrap.dedent(f"""
        WITH cohort AS (
        {_indent(cohort_source(cohort_table, unit))}
        ),
        experiment_meta AS (
        {_indent(run.experiment_meta_cte())}
        ),
        source_rows AS (
        {_indent(source_cte(source_definition, run, unit))}
        ),
        bucket_totals AS (
        {_indent(bucket_totals_cte(metrics, run))}
        ),
        unit_buckets AS (
        {_indent(unit_buckets_cte(run))}
        ),
        unit_windows AS (
        {_indent(unit_windows_cte(metrics))}
        )
        {rollup_select(metrics)}
    """).strip()


def cohort_source(cohort_table, unit):
    """Where the source queries read the cohort from, restricted to one analysis unit.

    The cohort holds every experiment in the run, and `unit_id` means a different thing in the rows
    of each unit, so a source query reads only the rows whose ids its join key can match.

    When only validating the SQL the cohort table has deliberately not been created, so
    referencing it would fail validation for a reason that says nothing about the query being
    validated. An empty literal of the same shape lets BigQuery check everything downstream of it
    instead.
    """
    if cohort_table is None:
        return (
            "SELECT * FROM UNNEST(ARRAY<STRUCT<slug STRING, unit_id STRING, branch STRING, "
            "enrollment_date DATE>>[])"
        )
    return (
        f"SELECT slug, unit_id, branch, enrollment_date FROM `{cohort_table}`\n"
        f"WHERE unit_kind = '{unit.kind}'"
    )


def source_cte(source_definition, run, unit):
    """Select the source rows for one unit, over the union of its experiments' date ranges.

    One scan serving every experiment analysed at this unit. It starts `covariate_days` before the
    EARLIEST of their enrollments, because the covariate window precedes assignment, and ends at the
    run date. The range reaches back to the oldest of those experiments' starts however short the
    windows are, because windows are anchored to each unit's own enrollment rather than to the
    calendar: a unit that enrolled on day one needs its own first week, which is months ago. That is
    why an old recipe is expensive no matter how the window horizon is set, and why age is handled
    by declining to analyse it.

    Column selection is explicit, so the scan is the columns this source's metrics name plus
    whatever a source-level restriction reads. `desktop_active_users` is the case where those
    differ: its `is_desktop` is a computed column, so filtering on it also reads the ISP,
    distribution and version columns behind it.
    """
    columns = ",\n  ".join(source_definition["columns"])
    restriction = (
        f"\n  AND {source_definition['where']}"
        if source_definition.get("where")
        else ""
    )
    return textwrap.dedent(f"""
        SELECT
          {unit.source_column} AS unit_id,
          submission_date,
          {columns}
        FROM `{source_definition['table']}`
        WHERE submission_date
          BETWEEN DATE '{run.first_source_date()}'
              AND DATE '{run.as_of}'{restriction}{source_sample(run, unit)}
    """).strip()


def source_sample(run, unit):
    """Restrict a source scan to the run's sample, in whichever form this unit allows.

    The bare clustered column where the unit is the id it is hashed from, since only that form lets
    BigQuery skip the blocks rather than read and discard them. A hash of the unit otherwise, which
    reads every block but keeps whole units together.
    """
    if run.sample_percent is None:
        return ""
    if unit.clustered_sample_id:
        return f"\n  AND sample_id < {run.sample_percent}"
    return sample_clause(unit.source_column, run.sample_percent, prefix="\n  AND ")


# ---------------------------------------------------------------------------- the stages ----


def bucket_totals_cte(metrics, run):
    """STAGE A: one row per (slug, unit, bucket), carrying one RAW aggregate per metric.

    The only stage that reads source rows, and it reads each of them once: a row's bucket is
    arithmetic on its tenure day, so nothing is replicated per window. Metrics are columns here
    rather than rows, which is safe because the metric set bounds them and they never multiply by
    the window count.

    Raw means before any threshold. A 0/1 metric carries its underlying SUM, COUNTIF or MIN through
    this stage so the threshold can be applied once to the combined value in stage B.

    The covariate rides along as one extra bucket rather than a second pass, so the pre-enrollment
    window costs nothing beyond the rows it reads.

    LEFT JOIN, and the direction is load-bearing rather than stylistic. The predicate on
    `source_rows.submission_date` below already discards units with no matching row, so this returns
    exactly what an inner join would; measured on one source, both forms produce identical output.
    But an inner join lets BigQuery reorder the two sides, and it was measured choosing a plan that
    read orders of magnitude more records than this one, at several times the slot cost. Writing it
    as an outer join drove the planner to hash-join from the cohort instead. Treat that as an
    observation rather than a guarantee: BigQuery can rewrite an outer join to an inner one when a
    predicate rejects the null-extended rows, as this one does, so the plan is not pinned by the
    syntax.
    """
    aggregates = ",\n  ".join(
        f"{metric.reducer.bucket_aggregate} AS {metric.name}__raw" for metric in metrics
    )
    tenure_day = "DATE_DIFF(source_rows.submission_date, cohort.enrollment_date, DAY)"
    return textwrap.dedent(f"""
        SELECT
          cohort.slug,
          cohort.unit_id,
          IF(tenure_day < 0, {PRE_BUCKET}, DIV(tenure_day, {run.length})) AS bucket,
          {aggregates}
        FROM cohort
        JOIN experiment_meta
          ON experiment_meta.slug = cohort.slug
        LEFT JOIN source_rows
          ON source_rows.unit_id = cohort.unit_id
        , UNNEST([{tenure_day}]) AS tenure_day
        WHERE tenure_day BETWEEN -{run.covariate_days}
                    AND {run.matured_buckets()} * {run.length} - 1
        GROUP BY cohort.slug, cohort.unit_id, bucket
    """).strip()


def unit_buckets_cte(run):
    """Every (slug, unit, bucket) a unit has matured, whether or not it reported anything.

    Generated from the cohort rather than from the source rows so a unit that reported nothing still
    appears, with its metric values 0, and so counts in `n`, which is what makes the denominator
    the cohort rather than the reporting subset of it.

    A unit younger than one bucket gets only the covariate row, because `matured_buckets` is 0 for
    it and the array then runs from the covariate bucket to -1. The rollup drops that row with
    `bucket >= 0`, which is where the maturity gate actually bites: a unit enters a window exactly
    once, when it completes it.
    """
    return textwrap.dedent(f"""
        SELECT
          cohort.slug,
          cohort.unit_id,
          cohort.branch,
          bucket
        FROM cohort
        JOIN experiment_meta
          ON experiment_meta.slug = cohort.slug
        CROSS JOIN UNNEST(GENERATE_ARRAY({PRE_BUCKET}, {run.matured_buckets()} - 1)) AS bucket
    """).strip()


def unit_windows_cte(metrics):
    """STAGE B: each bucket's row carries the value of every window that ends on it.

    A cumulative window is the running total to this bucket, which is a window function over a
    units-by-buckets table rather than a second pass over source rows. The table is smaller than the
    source rows it came from, so the whole window series costs one sort.

    The window LABEL is arithmetic on the bucket index rather than a lookup, which is what lets one
    query serve experiments of different ages: the window ending on bucket b is always the (b+1)th
    of its family. Experiments differ only in how many buckets they reach, and that is already
    enforced upstream by `matured_buckets`.

    The metric's threshold is applied HERE, to the combined raw value, and only here. The covariate
    is the pre bucket's raw value read across the partition, so it is one value per unit rather than
    one per unit per window.
    """
    kinds = sorted({rule["kind"] for metric in metrics for rule in metric.window_rules})
    labels = "".join(
        f"  IF(grid.bucket >= 0, CONCAT('{_PREFIX[kind]}:', grid.bucket + 1), NULL)"
        f" AS {kind}_window,\n"
        for kind in kinds
    )
    values = ",\n  ".join(
        [
            *(
                f"{window_value(metric, kind)} AS {metric.name}__{kind}"
                for metric in metrics
                for kind in sorted({rule["kind"] for rule in metric.window_rules})
            ),
            *(f"{covariate_value(metric)} AS {metric.name}__pre" for metric in metrics),
        ]
    )
    return textwrap.dedent(f"""
        SELECT
          grid.slug,
          grid.branch,
          grid.bucket,
        {labels}  {values}
        FROM unit_buckets AS grid
        LEFT JOIN bucket_totals AS bucket_total
          ON bucket_total.slug = grid.slug
         AND bucket_total.unit_id = grid.unit_id
         AND bucket_total.bucket = grid.bucket
        WINDOW
          unit_to_date AS (PARTITION BY grid.slug, grid.unit_id ORDER BY grid.bucket
                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
          unit_all AS (PARTITION BY grid.slug, grid.unit_id)
    """).strip()


# The label a window of each family carries. Kept beside the arithmetic that builds it, since the
# two have to agree with `discovery.generate_windows` for a cell to be found by the statistics.
_PREFIX = {"cumulative": "cumu", "disjoint": "week"}


def window_value(metric, kind):
    """One unit's value for the window of `kind` ending on this bucket.

    Cumulative combines every bucket to date, excluding the covariate bucket, which sorts first and
    would otherwise be counted as post-enrollment data. Disjoint is the bucket itself.
    """
    reducer = metric.reducer
    if kind == "cumulative":
        combined = (
            f"{reducer.combine}"
            f"(IF(grid.bucket >= 0, bucket_total.{metric.name}__raw, NULL))"
            f" OVER unit_to_date"
        )
    else:
        combined = f"bucket_total.{metric.name}__raw"
    return reducer.finalize(f"COALESCE({combined}, {reducer.no_rows})")


def covariate_value(metric):
    """One unit's pre-enrollment value, read off the covariate bucket across the partition."""
    reducer = metric.reducer
    combined = (
        f"MAX(IF(grid.bucket = {PRE_BUCKET}, bucket_total.{metric.name}__raw, NULL))"
        f" OVER unit_all"
    )
    return reducer.finalize(f"COALESCE({combined}, {reducer.no_rows})")


def rollup_select(metrics):
    """STAGE C: per-branch sufficient statistics, one row per (slug, branch, window, metric).

    The six aggregates are everything a covariate-adjusted mean comparison needs, which is the
    reason no per-unit row has to travel. Metrics are unpivoted through a struct array so the metric
    name becomes a row key, which is what makes the schema independent of the metric set. The array
    is applied to the units-by-buckets table rather than to source rows, so it multiplies the small
    table and not the large one.

    A NULL label is a bucket that completes no window of that metric's kind, and the covariate
    bucket is every metric's NULL, so the filter drops both.
    """
    structs = ",\n         ".join(
        f"STRUCT('{metric.name}' AS metric, {rule['kind']}_window AS window_label, "
        f"{metric.name}__{rule['kind']} AS post, {metric.name}__pre AS pre)"
        for metric in metrics
        for rule in metric.window_rules
    )
    return textwrap.dedent(f"""
        SELECT
          slug,
          branch,
          metric_row.window_label,
          metric_row.metric,
          COUNT(*) AS n,
          SUM(metric_row.post) AS sum,
          SUM(POW(metric_row.post, 2)) AS sum_squares,
          SUM(metric_row.pre) AS pre_sum,
          SUM(POW(metric_row.pre, 2)) AS pre_sum_squares,
          SUM(CAST(metric_row.post AS FLOAT64) * metric_row.pre) AS sum_x_pre
        FROM unit_windows,
             UNNEST([{structs}]) AS metric_row
        WHERE bucket >= 0
          AND metric_row.window_label IS NOT NULL
        GROUP BY slug, branch, metric_row.window_label, metric_row.metric
    """).strip()


def _indent(block, spaces=2):
    return textwrap.indent(block, " " * spaces)
