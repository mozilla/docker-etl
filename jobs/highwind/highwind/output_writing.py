"""SECTION 5: OUTPUT WRITING.

Two destinations, for two different readers. BigQuery keeps the corpus queryable across experiments,
which is what makes cross-experiment context and meta-analysis possible and lets a result be
inspected without opening a blob. GCS is the seam Experimenter reads, matching where the enrollment
funnel already writes from this same Airflow.

A third table holds what the run logged. A cell that failed is already queryable, because the
results grid is written whole and carries a state and an error, but an experiment that failed before
producing any cell has no row to carry one, and neither has a refused recipe or a run-level event.
Those reach stdout, which under Airflow is the account nobody reads.
"""

import datetime
import logging
import math
import pathlib
import traceback
from dataclasses import dataclass

from google.cloud import bigquery
from mozilla_nimbus_schemas.highwind import (
    HighwindAnalysis,
    HighwindAnalysisUnit,
    HighwindBranchValue,
    HighwindCellState,
    HighwindComparison,
    HighwindDirection,
    HighwindError,
    HighwindInterval,
    HighwindLogLevel,
    HighwindMetadata,
    HighwindMetricResult,
    HighwindSegment,
    HighwindSegmentBranch,
    HighwindSegmentResult,
    HighwindWindow,
    HighwindWindowKind,
    HighwindWindowResult,
)

from . import discovery, gbstats_compute

# Production targets, the defaults an `Outputs` takes when nothing overrides them. Their own
# dataset, so this job's artifacts are separable from the production analysis tables around them.
RESULTS_TABLE = "moz-fx-data-experiments.highwind_poc.highwind_statistics_v1"
SUFFICIENT_STATS_TABLE = (
    "moz-fx-data-experiments.highwind_poc.highwind_sufficient_stats_v1"
)
LOG_TABLE = "moz-fx-data-experiments.highwind_poc.highwind_logs_v1"
BLOB_PREFIX = "gs://mozanalysis/highwind"
PIPELINE_VERSION = "poc-1"
SCHEMA_VERSION = 1

SETTLED_STATES = {HighwindCellState.FORMING, HighwindCellState.CONFIDENT}

# What every log row is attributed to. Jetstream's log handler carries the same column so one table
# can hold several producers' logs, and it is kept here for the same reason the column names below
# are: adopting that handler in place of this one should be a swap rather than a migration.
LOG_SOURCE = "highwind"

# The column every table is partitioned on, and the run date every row carries.
PARTITION_FIELD = "as_of_date"

# The column every table is clustered on. Reading one experiment's results is the common query, so
# clustering on the slug lets it prune blocks rather than scan the whole partition. It matters more
# as the corpus grows, since a partition holds every experiment analysed that day.
CLUSTERING_FIELDS = ["experiment_slug"]

# The results table, one row per (slug, metric, window, comparison). The descriptions are the
# documentation of these columns, so they are carried into the table itself rather than kept here.
RESULTS_SCHEMA = [
    bigquery.SchemaField(
        "as_of_date",
        "DATE",
        description="Run date the analysis was computed for. Partition column.",
    ),
    bigquery.SchemaField(
        "experiment_slug", "STRING", description="Experiment slug."
    ),
    bigquery.SchemaField(
        "metric",
        "STRING",
        description="Metric name, from the hard-coded desktop guardrail set.",
    ),
    bigquery.SchemaField(
        "window",
        "STRING",
        description=(
            "Window label, relative to each unit's own enrollment rather than the calendar. "
            "cumu:N is the cumulative window covering a unit's first N weeks; week:N is the Nth "
            "disjoint week."
        ),
    ),
    bigquery.SchemaField(
        "window_kind", "STRING", description="cumulative or disjoint."
    ),
    bigquery.SchemaField(
        "window_start",
        "INT64",
        description=(
            "First tenure day the window covers, inclusive, counted from the unit's enrollment."
        ),
    ),
    bigquery.SchemaField(
        "window_end",
        "INT64",
        description="Last tenure day the window covers, inclusive.",
    ),
    bigquery.SchemaField(
        "branch",
        "STRING",
        description="Treatment branch this row compares against the reference.",
    ),
    bigquery.SchemaField(
        "reference_branch",
        "STRING",
        description="Branch the comparison is made against, from the Experimenter mirror.",
    ),
    bigquery.SchemaField(
        "state",
        "STRING",
        description=(
            "What the cell is, and whether waiting will help. error, computation raised and "
            "needs a fix; not_started, the window has not matured; insufficient_data, matured "
            "but below the minimum units or no interval could be produced; forming, an interval "
            "that still includes zero; confident, an interval that excludes zero. Every declared "
            "cell gets a row in one of these states, including cells that produced nothing, so "
            "that a failed run is visible as errors rather than as absent rows."
        ),
    ),
    bigquery.SchemaField(
        "point",
        "FLOAT64",
        description=(
            "Relative difference against the reference branch, as a percentage. NULL unless the "
            "cell produced an estimate."
        ),
    ),
    bigquery.SchemaField(
        "lower",
        "FLOAT64",
        description=(
            "Lower bound of the always-valid confidence interval, as a percentage."
        ),
    ),
    bigquery.SchemaField(
        "upper",
        "FLOAT64",
        description=(
            "Upper bound of the always-valid confidence interval, as a percentage."
        ),
    ),
    bigquery.SchemaField(
        "theta",
        "FLOAT64",
        description=(
            "CUPED coefficient, cov(pre, post) / var(pre), pooled across the two branches being "
            "compared so the adjustment is identical on each side of the contrast. Zero when the "
            "covariate had no variance, which disables the adjustment rather than dividing by "
            "zero."
        ),
    ),
    bigquery.SchemaField(
        "n_reference",
        "INT64",
        description=(
            "Analysis units of the reference branch that had matured this window. After branch "
            "balancing: where one branch is more than a few times the size of the smallest, it is "
            "down-sampled on a hash of the analysis unit, so this is the analysed population "
            "rather than the enrolled one and will not match an enrollment count for a lopsided "
            "experiment."
        ),
    ),
    bigquery.SchemaField(
        "n_treatment",
        "INT64",
        description=(
            "Analysis units of the treatment branch that had matured this window, after the same "
            "balancing described on n_reference."
        ),
    ),
    bigquery.SchemaField(
        "error",
        "STRING",
        description=(
            "Exception that produced an error state, truncated. NULL in every other state."
        ),
    ),
    bigquery.SchemaField(
        "pipeline_version",
        "STRING",
        description=(
            "Version of the analysis that produced the row, for provenance across changes."
        ),
    ),
]

# The aggregates the results above were computed from, one row per (slug, metric, window, branch).
SUFFICIENT_STATS_SCHEMA = [
    bigquery.SchemaField(
        "as_of_date",
        "DATE",
        description="Run date the aggregates were computed for. Partition column.",
    ),
    bigquery.SchemaField(
        "experiment_slug", "STRING", description="Experiment slug."
    ),
    bigquery.SchemaField(
        "metric",
        "STRING",
        description="Metric name, from the hard-coded desktop guardrail set.",
    ),
    bigquery.SchemaField(
        "window",
        "STRING",
        description=(
            "Window label, relative to each unit's own enrollment rather than the calendar. "
            "cumu:N is the cumulative window covering a unit's first N weeks; week:N is the Nth "
            "disjoint week. Joins to highwind_statistics_v1.window."
        ),
    ),
    bigquery.SchemaField(
        "branch", "STRING", description="Branch these aggregates are for."
    ),
    bigquery.SchemaField(
        "n",
        "INT64",
        description=(
            "Analysis units of this branch that had matured this window. After branch balancing, "
            "so on a lopsided experiment this is the analysed population rather than the "
            "enrolled one."
        ),
    ),
    bigquery.SchemaField(
        "sum",
        "FLOAT64",
        description=(
            "Sum of the metric over those units, each unit reduced to one value for the window."
        ),
    ),
    bigquery.SchemaField(
        "sum_squares",
        "FLOAT64",
        description=(
            "Sum of squares of the same per-unit values, which supplies the variance."
        ),
    ),
    bigquery.SchemaField(
        "pre_sum",
        "FLOAT64",
        description=(
            "Sum of the covariate, the same metric measured over a fixed pre-enrollment window. "
            "Always pre-enrollment regardless of which window the row is for."
        ),
    ),
    bigquery.SchemaField(
        "pre_sum_squares",
        "FLOAT64",
        description="Sum of squares of the covariate, which supplies its variance.",
    ),
    bigquery.SchemaField(
        "sum_x_pre",
        "FLOAT64",
        description=(
            "Sum of products of the post-enrollment value and the covariate, which supplies "
            "their covariance. Together with the four sums above this is what the CUPED "
            "coefficient is fitted from."
        ),
    ),
]

# One row per log record the run emitted. The columns after the first three are the ones
# Jetstream's own BigQuery log handler writes, names included, so that adopting that handler here
# later is a swap rather than a rewrite of everything reading this table.
LOG_SCHEMA = [
    bigquery.SchemaField(
        "as_of_date",
        "DATE",
        description="Run date the record was logged for. Partition column.",
    ),
    bigquery.SchemaField(
        "run_started_at",
        "TIMESTAMP",
        description=(
            "When the run that logged this record began, in UTC. Constant across every row one "
            "run writes, and the only thing separating one attempt at a date from another, since "
            "this table is appended to rather than replaced. Group by it to read a single "
            "attempt, and take the greatest value in a date to read the most recent one."
        ),
    ),
    bigquery.SchemaField(
        "experiment_slug",
        "STRING",
        description=(
            "Experiment the record is about, NULL for a record about the run as a whole. "
            "Jetstream's column of this concept is named experiment; it is named for the slug "
            "here to match the two tables beside it, which is also what lets it be the clustering "
            "column they cluster on."
        ),
    ),
    bigquery.SchemaField(
        "timestamp", "TIMESTAMP", description="When the record was logged, in UTC."
    ),
    bigquery.SchemaField(
        "log_level",
        "STRING",
        description=(
            "INFO for progress and for what the run consumed, WARNING for a recipe the run "
            "refused to analyse and for a result that ran but does not look like an analysis, "
            "ERROR for a failure."
        ),
    ),
    bigquery.SchemaField("message", "STRING", description="The record's message."),
    bigquery.SchemaField(
        "exception",
        "STRING",
        description=(
            "Formatted traceback of the exception the record carries, NULL when it carries none. "
            "The traceback rather than the exception's text, because the line that raised is the "
            "diagnosis and the message does not carry it."
        ),
    ),
    bigquery.SchemaField(
        "exception_type",
        "STRING",
        description="Class name of that exception, which is what an error rate groups by.",
    ),
    bigquery.SchemaField(
        "filename", "STRING", description="File the record was logged from."
    ),
    bigquery.SchemaField(
        "func_name", "STRING", description="Function the record was logged from."
    ),
    bigquery.SchemaField(
        "source",
        "STRING",
        description=(
            "Which producer wrote the row, so one table can hold more than this job's logs."
        ),
    ),
    bigquery.SchemaField(
        "metric",
        "STRING",
        description="Metric the record is about, NULL when it is about none.",
    ),
    bigquery.SchemaField(
        "statistic",
        "STRING",
        description="Statistic the record is about, NULL when it is about none.",
    ),
    bigquery.SchemaField(
        "analysis_basis",
        "STRING",
        description=(
            "Always NULL. Carried by the handler this schema follows and unimplemented here, "
            "since this job analyses on enrollment alone; declared so that implementing it is a "
            "value to fill rather than a column to add."
        ),
    ),
    bigquery.SchemaField(
        "segment",
        "STRING",
        description=(
            "Always NULL, for the same reason as analysis_basis: this job analyses no segments."
        ),
    ),
    bigquery.SchemaField(
        "analysis_period",
        "STRING",
        description=(
            "Always NULL, for the same reason again. The window is this job's equivalent, and it "
            "is a property of a cell, which the results table already carries one of per row."
        ),
    ),
]


@dataclass(frozen=True)
class Outputs:
    """Where one run's outputs go.

    An argument rather than module state, so a writer's behaviour is a function of what it was
    called with. A local run overrides the tables and sends blobs to a directory instead of GCS.
    """

    results_table: str = RESULTS_TABLE
    sufficient_stats_table: str = SUFFICIENT_STATS_TABLE
    log_table: str = LOG_TABLE
    blob_prefix: str = BLOB_PREFIX
    # Set for a local run: blobs go to this directory instead of to GCS.
    local_blob_dir: str | None = None


def write_blob(storage, analysis, outputs):
    metadata = analysis.metadata
    name = blob_name(metadata.experiment_slug)
    if outputs.local_blob_dir:
        path = pathlib.Path(outputs.local_blob_dir) / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(analysis.model_dump_json(indent=2))
        return str(path)
    blob = blob_for(storage, metadata.experiment_slug, outputs.blob_prefix)
    blob.metadata = {
        "as_of_date": metadata.as_of_date.isoformat(),
        "generated_at": metadata.generated_at.isoformat(),
        "pipeline_version": metadata.pipeline_version,
    }
    blob.upload_from_string(analysis.model_dump_json(), content_type="application/json")
    return f"{outputs.blob_prefix}/{name}"


def build_analysis(experiment, as_of, metrics, results, cells, units, problems):
    results_by_cell = {
        (result["metric"], result["window"], result["branch"]): result
        for result in results
    }
    return HighwindAnalysis(
        metadata=build_metadata(experiment, as_of),
        segments=[build_segment(experiment, units)],
        metrics=[
            build_metric(experiment, as_of, metric, results_by_cell, cells)
            for metric in metrics
        ],
        errors=[build_error(experiment, problem) for problem in problems],
    )


def build_refusal(experiment, as_of, problems):
    return HighwindAnalysis(
        metadata=build_metadata(experiment, as_of),
        segments=[],
        metrics=[],
        errors=[build_error(experiment, problem) for problem in problems],
    )


def build_metadata(experiment, as_of):
    return HighwindMetadata(
        schema_version=SCHEMA_VERSION,
        experiment_slug=experiment.slug,
        as_of_date=as_of,
        generated_at=datetime.datetime.now(datetime.timezone.utc),
        pipeline_version=PIPELINE_VERSION,
        start_date=experiment.start_date,
        end_date=experiment.end_date,
        analysis_unit=HighwindAnalysisUnit(experiment.unit.kind),
        reference_branch=experiment.reference_branch,
        branches=list(experiment.branches),
    )


def build_segment(experiment, units):
    return HighwindSegment(
        slug=discovery.ALL_ENROLLED,
        friendly_name=discovery.ALL_ENROLLED_NAME,
        branches=[
            HighwindSegmentBranch(branch=branch, units=units.get(branch, 0))
            for branch in experiment.branches
        ],
    )


def build_metric(experiment, as_of, metric, results_by_cell, cells):
    return HighwindMetricResult(
        slug=metric.name,
        friendly_name=metric.friendly_name or metric.name,
        description=metric.description,
        segments=[
            build_segment_result(experiment, as_of, metric, results_by_cell, cells)
        ],
    )


def build_segment_result(experiment, as_of, metric, results_by_cell, cells):
    rows = []
    for window in discovery.reported_windows(metric, experiment, as_of):
        comparisons = build_comparisons(
            experiment, as_of, metric, window, results_by_cell, cells
        )
        branches = build_branches(experiment, metric, window, cells, comparisons)
        rows.append((window, branches, comparisons))
    summary = summary_position(metric, rows)
    return HighwindSegmentResult(
        segment=discovery.ALL_ENROLLED,
        windows=[
            HighwindWindowResult(
                window=schema_window(experiment, window),
                is_summary=position == summary,
                branches=branches,
                comparisons=comparisons,
            )
            for position, (window, branches, comparisons) in enumerate(rows)
        ],
    )


def summary_position(metric, rows):
    settled = [
        position
        for position, (_, _, comparisons) in enumerate(rows)
        if any(comparison.state in SETTLED_STATES for comparison in comparisons)
    ]
    cumulative = [position for position in settled if rows[position][0].kind == "cumulative"]
    only_disjoint = all(rule["kind"] == "disjoint" for rule in metric.window_rules)
    candidates = cumulative or (settled if only_disjoint else [])
    if candidates:
        return max(candidates, key=lambda position: rows[position][0].end)
    return min(range(len(rows)), key=lambda position: rows[position][0].end, default=None)


def build_branches(experiment, metric, window, cells, comparisons):
    means = branch_means(experiment, metric, window, cells)
    return [
        HighwindBranchValue(
            branch=branch,
            n=cell_units(cells, metric, window, branch),
            value=empty_interval() if errored(experiment, branch, comparisons) else means[branch],
        )
        for branch in experiment.branches
    ]


def branch_means(experiment, metric, window, cells):
    reported = [
        cells[(metric.name, window.label, branch)]
        for branch in experiment.branches
        if (metric.name, window.label, branch) in cells
    ]
    try:
        theta = gbstats_compute.window_theta(experiment, metric, window, cells)
        pooled_pre_mean = sum(cell["pre_sum"] for cell in reported) / sum(
            cell["n"] for cell in reported
        )
    except Exception:
        return {branch: empty_interval() for branch in experiment.branches}
    return {
        branch: branch_mean(
            cells.get((metric.name, window.label, branch)), theta, pooled_pre_mean
        )
        for branch in experiment.branches
    }


def branch_mean(cell, theta, pooled_pre_mean):
    if cell is None or cell["n"] < gbstats_compute.MIN_UNITS:
        return empty_interval()
    try:
        mean = cell["sum"] / cell["n"] - theta * (cell["pre_sum"] / cell["n"] - pooled_pre_mean)
        halfwidth = gbstats_compute.mean_halfwidth(cell, theta)
    except Exception:
        return empty_interval()
    if not (math.isfinite(mean) and math.isfinite(halfwidth)):
        return empty_interval()
    return HighwindInterval(point=mean, lower=mean - halfwidth, upper=mean + halfwidth)


def empty_interval():
    return HighwindInterval(point=None, lower=None, upper=None)


def interval_of(values):
    return HighwindInterval(
        point=values.get("point"), lower=values.get("lower"), upper=values.get("upper")
    )


def errored(experiment, branch, comparisons):
    involved = [
        comparison
        for comparison in comparisons
        if branch in (comparison.branch, experiment.reference_branch)
    ]
    return all(comparison.state == HighwindCellState.ERROR for comparison in involved)


def cell_units(cells, metric, window, branch):
    return cells.get((metric.name, window.label, branch), {}).get("n", 0)


def build_comparisons(experiment, as_of, metric, window, results_by_cell, cells):
    if not discovery.has_matured(experiment, window, as_of):
        return [
            HighwindComparison(
                branch=treatment,
                reference_branch=experiment.reference_branch,
                state=HighwindCellState.NOT_STARTED,
                direction=HighwindDirection.NEUTRAL,
                relative=empty_interval(),
                absolute=empty_interval(),
                n_reference=0,
                n_treatment=0,
                error=None,
            )
            for treatment in experiment.treatment_branches
        ]
    return [
        build_comparison(
            experiment,
            metric,
            window,
            treatment,
            results_by_cell.get((metric.name, window.label, treatment)),
            cells,
        )
        for treatment in experiment.treatment_branches
    ]


def build_comparison(experiment, metric, window, treatment, result, cells):
    if result is None:
        return HighwindComparison(
            branch=treatment,
            reference_branch=experiment.reference_branch,
            state=HighwindCellState.ERROR,
            direction=HighwindDirection.NEUTRAL,
            relative=empty_interval(),
            absolute=empty_interval(),
            n_reference=None,
            n_treatment=None,
            error="no result was recorded for this cell",
        )
    state = HighwindCellState(result["state"])
    n_reference, n_treatment = result.get("n_reference"), result.get("n_treatment")
    if state == HighwindCellState.NOT_STARTED:
        state = HighwindCellState.INSUFFICIENT_DATA
        n_reference = cell_units(cells, metric, window, experiment.reference_branch)
        n_treatment = cell_units(cells, metric, window, treatment)
    return HighwindComparison(
        branch=treatment,
        reference_branch=experiment.reference_branch,
        state=state,
        direction=direction_of(state, result.get("point")),
        relative=interval_of(result),
        absolute=interval_of(result.get("absolute") or {}),
        n_reference=n_reference,
        n_treatment=n_treatment,
        error=result.get("error"),
    )


def direction_of(state, relative_shift):
    if state != HighwindCellState.CONFIDENT or relative_shift is None:
        return HighwindDirection.NEUTRAL
    if relative_shift > 0:
        return HighwindDirection.POSITIVE
    if relative_shift < 0:
        return HighwindDirection.NEGATIVE
    return HighwindDirection.NEUTRAL


def schema_window(experiment, window):
    return HighwindWindow(
        kind=HighwindWindowKind(window.kind),
        start_day=window.start,
        end_day=window.end,
        matures_on=discovery.matures_on(experiment, window),
    )


def build_error(experiment, problem):
    window = problem["window"]
    return HighwindError(
        timestamp=problem["timestamp"],
        log_level=HighwindLogLevel(problem["log_level"]),
        message=problem["message"],
        exception_type=problem["exception_type"],
        exception=problem["exception"],
        filename=problem["filename"],
        func_name=problem["func_name"],
        metric=problem["metric"],
        segment=problem["segment"],
        window=None if window is None else schema_window(experiment, window),
    )


def write_tables(client, as_of, results_by_slug, cells_by_slug, outputs):
    """Both BigQuery tables for the whole run, in one write each.

    Once per run rather than once per experiment, and that is what makes a rerun safe. Every
    experiment's rows land in the same daily partition, so a per-experiment write can only append,
    and appending means a retried or backfilled run duplicates every row it already wrote. Airflow
    retries as a matter of course, so this would not be a rare case. Writing the run's rows together
    lets the load replace the partition instead, which is idempotent by construction.

    It also turns two load jobs per experiment into two for the whole run.
    """
    ensure_table(client, outputs.results_table, RESULTS_SCHEMA)
    ensure_table(client, outputs.sufficient_stats_table, SUFFICIENT_STATS_SCHEMA)
    results_rows = [
        dict(
            table_columns(result),
            experiment_slug=slug,
            as_of_date=as_of.isoformat(),
            pipeline_version=PIPELINE_VERSION,
        )
        for slug, results in results_by_slug.items()
        for result in results
    ]
    stats_rows = [
        dict(
            stats,
            experiment_slug=slug,
            as_of_date=as_of.isoformat(),
            metric=metric,
            window=window,
            branch=branch,
        )
        for slug, cells in cells_by_slug.items()
        for (metric, window, branch), stats in cells.items()
    ]
    replace_partition(client, outputs.results_table, RESULTS_SCHEMA, results_rows, as_of)
    replace_partition(
        client, outputs.sufficient_stats_table, SUFFICIENT_STATS_SCHEMA, stats_rows, as_of
    )
    return len(results_rows), len(stats_rows)


def table_columns(result):
    return {key: value for key, value in result.items() if key != "absolute"}


class RunLog(logging.Handler):
    """Collect the records a run logs, so the run can write them as one load job at the end.

    A handler rather than a collector passed down through the analysis, so that anything which logs
    is recorded without having been handed somewhere to record it. That matters for the failures
    this table exists for, which are caught in places that have no reason to know a table is being
    written.

    A record becomes a row on arrival rather than at the write, because it holds the live traceback
    of whatever raised and the write can come long after that frame is gone. Nothing flushes on a
    capacity, unlike the buffering handler this is otherwise shaped like: one run's records are one
    attempt, and a flush part way through would split that attempt across two loads with no way to
    tell it was ever one.

    The start time is taken here, once, rather than per record, because it identifies the attempt
    rather than the record. Every row this handler makes carries the same value.
    """

    def __init__(self, as_of, source=LOG_SOURCE):
        super().__init__(level=logging.INFO)
        self.as_of = as_of
        self.source = source
        self.run_started_at = datetime.datetime.now(datetime.timezone.utc)
        self.rows = []
        self.problems = []

    def emit(self, record):
        try:
            row = log_row(record, self.as_of, self.run_started_at, self.source)
            self.rows.append(row)
            if record.levelno >= logging.WARNING:
                self.problems.append(problem_of(record, row))
        # A handler that raises reports the fault at the site of the log rather than at the site of
        # the fault, so a record this cannot represent would read as a bug in whatever logged it.
        except Exception:
            self.handleError(record)

    def problems_for(self, slug):
        return [
            problem
            for problem in self.problems
            if problem["experiment_slug"] in (slug, None)
        ]


def problem_of(record, row):
    fields = vars(record)
    return dict(
        experiment_slug=row["experiment_slug"],
        timestamp=datetime.datetime.fromtimestamp(record.created, datetime.timezone.utc),
        log_level=(
            HighwindLogLevel.ERROR
            if record.levelno >= logging.ERROR
            else HighwindLogLevel.WARNING
        ),
        message=row["message"],
        exception_type=row["exception_type"],
        exception=row["exception"],
        filename=row["filename"],
        func_name=row["func_name"],
        metric=row["metric"],
        segment=fields.get("segment"),
        window=fields.get("window"),
    )


def log_row(record, as_of, run_started_at, source):
    """One log record as a row of the log table.

    What a record is about beyond its message, the experiment and the metric, arrives as logging's
    `extra` and so lands in the record's own namespace, which is where these are read from. Most
    records carry none of it, which is why each is optional rather than an argument.
    """
    fields = vars(record)
    exception_type, formatted = exception_columns(record.exc_info)
    return dict(
        as_of_date=as_of.isoformat(),
        run_started_at=run_started_at.isoformat(),
        experiment_slug=fields.get("experiment_slug"),
        timestamp=datetime.datetime.fromtimestamp(
            record.created, datetime.timezone.utc
        ).isoformat(),
        log_level=record.levelname,
        message=record.getMessage(),
        exception=formatted,
        exception_type=exception_type,
        filename=record.filename,
        func_name=record.funcName,
        source=source,
        metric=fields.get("metric"),
        statistic=fields.get("statistic"),
        # Declared and unfilled, so that implementing either is a value to write rather than a
        # column to add and a reader to change. See their descriptions in LOG_SCHEMA.
        analysis_basis=None,
        segment=None,
        analysis_period=None,
    )


def exception_columns(exc_info):
    """The exception a record carries, as its class name and its formatted traceback.

    The absent case is tested on the exception rather than on the tuple, because logging leaves a
    record's `exc_info` a tuple of Nones when it is asked to log an exception with nothing in
    flight, and that tuple is as truthy as a real one.
    """
    if not exc_info or exc_info[0] is None:
        return None, None
    return exc_info[0].__name__, "".join(traceback.format_exception(*exc_info))


def write_log_table(client, rows, outputs):
    """Append what the run logged to the log table.

    Appended rather than replacing the date's partition, which is the one place this job differs
    from the tables above. Those hold results, so a rerun's output supersedes the attempt it
    retried and replacing is what makes the rerun idempotent. This holds the account of what
    happened, and the attempt that failed is usually the reason a rerun exists at all, so replacing
    would delete the explanation on the way to producing the fix. Every attempt at a date is kept
    and `run_started_at` is what separates them.

    The cost of keeping them is that a date accumulates rows across attempts, so anything reading
    this table for the state of a date has to pick an attempt rather than assume there is one.
    """
    ensure_table(client, outputs.log_table, LOG_SCHEMA)
    append_rows(client, outputs.log_table, LOG_SCHEMA, rows)
    return len(rows)


def ensure_table(client, table, schema):
    """Create `table` if it is not there yet, and leave it alone if it is.

    The job declares its own destinations because nothing else does, and because the load below
    writes a single partition, which needs a table to write a partition of. Declaring the schema
    here rather than inferring it also means a column's type and description are properties of the
    code that fills them.
    """
    definition = bigquery.Table(table, schema=schema)
    definition.time_partitioning = bigquery.TimePartitioning(field=PARTITION_FIELD)
    definition.clustering_fields = CLUSTERING_FIELDS
    client.create_table(definition, exists_ok=True)


def append_rows(client, table, schema, rows):
    """Add `rows` to `table`, leaving what is already there alone.

    No partition decorator on the target: the rows carry their own `as_of_date` and BigQuery files
    each one into the partition that column names, where a decorator would instead assert that
    every row belongs to one date.

    The schema is passed explicitly and inference switched off for the same reason as below, and
    with an added one: an appending load that inferred its schema would reconcile it against the
    live table on every run, so a column absent from one attempt's records is a schema change
    rather than an empty column.
    """
    if not rows:
        return
    config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=False,
        schema=schema,
    )
    client.load_table_from_json(rows, table, job_config=config).result()


def replace_partition(client, table, schema, rows, as_of):
    """Write `rows` as the entire contents of this run date's partition.

    Replacing the partition rather than appending to it is what makes the job safe to rerun: the
    second run of a date produces the same table as the first, where appending would double it. A
    run with no rows leaves the partition untouched rather than emptying it, so a failed rerun
    cannot destroy a good result.

    The schema is passed explicitly and inference switched off. The client turns inference on by
    itself for a WRITE_TRUNCATE with no schema, and inferring a schema per run is how a column
    changes type between days. That is a live risk here rather than a theoretical one, because a
    cell in the `error` or `not_started` state omits `point`, `lower`, `upper` and `theta`
    entirely, so which columns appear at all varies with the day's mix of states.
    """
    if not rows:
        return
    config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=False,
        schema=schema,
    )
    target = f"{table}${as_of.strftime('%Y%m%d')}"
    client.load_table_from_json(rows, target, job_config=config).result()


def blob_name(slug):
    """The object name one experiment's results are written under.

    Underscores rather than the hyphens a slug carries, matching the naming of the other artifacts
    this bucket holds. The name is otherwise the slug itself, since it is the key the ingest looks
    an experiment up by.
    """
    return f"{slug.replace('-', '_')}.json"


def blob_for(storage, slug, blob_prefix):
    """Resolve one experiment's blob from a gs:// prefix, with or without a path component."""
    bucket_name, _, prefix = blob_prefix.removeprefix("gs://").partition("/")
    name = blob_name(slug)
    key = f"{prefix}/{name}" if prefix else name
    return storage.bucket(bucket_name).blob(key)


def state_counts(results):
    """How many cells landed in each state, which is the operational-health signal per run."""
    counts = {}
    for result in results:
        counts[result["state"]] = counts.get(result["state"], 0) + 1
    return counts
