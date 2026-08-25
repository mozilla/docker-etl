"""SECTION 5: OUTPUT WRITING.

Two destinations, for two different readers. BigQuery keeps the corpus queryable across experiments,
which is what makes cross-experiment context and meta-analysis possible and lets a result be
inspected without opening a blob. GCS is the seam Experimenter reads, matching where the enrollment
funnel already writes from this same Airflow.
"""

import datetime
import json
import pathlib
from dataclasses import dataclass

from google.cloud import bigquery

# Production targets, the defaults an `Outputs` takes when nothing overrides them.
RESULTS_TABLE = "moz-fx-data-experiments.monitoring.highwind_statistics_v1"
SUFFICIENT_STATS_TABLE = (
    "moz-fx-data-experiments.monitoring.highwind_sufficient_stats_v1"
)
BLOB_PREFIX = "gs://mozanalysis/highwind"
PIPELINE_VERSION = "poc-1"

# The column both tables are partitioned on, and the run date every row carries.
PARTITION_FIELD = "as_of_date"

# The results table, one row per (slug, metric, window, comparison). The descriptions are the
# documentation of these columns, so they are carried into the table itself rather than kept here.
RESULTS_SCHEMA = [
    bigquery.SchemaField(
        "as_of_date",
        "DATE",
        description="Run date the analysis was computed for. Partition column.",
    ),
    bigquery.SchemaField("slug", "STRING", description="Experiment slug."),
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
            "balancing: where one arm is more than a few times the size of the smallest, it is "
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
    bigquery.SchemaField("slug", "STRING", description="Experiment slug."),
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
        "sumsq",
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
        "pre_sumsq",
        "FLOAT64",
        description="Sum of squares of the covariate, which supplies its variance.",
    ),
    bigquery.SchemaField(
        "xp",
        "FLOAT64",
        description=(
            "Sum of products of the post-enrollment value and the covariate, which supplies "
            "their covariance. Together with the four sums above this is what the CUPED "
            "coefficient is fitted from."
        ),
    ),
]


@dataclass(frozen=True)
class Outputs:
    """Where one run's three outputs go.

    An argument rather than module state, so a writer's behaviour is a function of what it was
    called with. A local run overrides the tables and sends blobs to a directory instead of GCS.
    """

    results_table: str = RESULTS_TABLE
    sufficient_stats_table: str = SUFFICIENT_STATS_TABLE
    blob_prefix: str = BLOB_PREFIX
    # Set for a local run: blobs go to this directory instead of to GCS.
    local_blob_dir: str | None = None


def write_blob(storage, experiment, as_of, results, outputs):
    """Write the per-experiment JSON Experimenter ingests.

    Written per experiment, unlike the tables below, because the blob IS per experiment and its name
    is the key: rewriting `<slug>.json` replaces the previous run's answer, so this leg is
    idempotent without any extra machinery.

    Carries `generated_at` and `pipeline_version` in the header and as object metadata, so the
    ingest can tell a new run from an old one without downloading the blob.
    """
    # When this ran, not the date it analysed: two runs of the same date, a retry or a backfill,
    # have to be distinguishable, which is the whole purpose of the field.
    generated_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    payload = {
        "metrics_meta": {
            "slug": experiment.slug,
            "as_of_date": as_of.isoformat(),
            "generated_at": generated_at,
            "pipeline_version": PIPELINE_VERSION,
            "reference_branch": experiment.reference_branch,
            "branches": list(experiment.branches),
        },
        "statistics": results,
        "errors": [result for result in results if result["state"] == "error"],
    }
    if outputs.local_blob_dir:
        path = pathlib.Path(outputs.local_blob_dir) / f"{experiment.slug}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2))
        return str(path)
    blob = blob_for(storage, experiment.slug, outputs.blob_prefix)
    blob.metadata = {
        "as_of_date": as_of.isoformat(),
        "generated_at": generated_at,
        "pipeline_version": PIPELINE_VERSION,
    }
    blob.upload_from_string(json.dumps(payload), content_type="application/json")
    return f"{outputs.blob_prefix}/{experiment.slug}.json"


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
            result,
            slug=slug,
            as_of_date=as_of.isoformat(),
            pipeline_version=PIPELINE_VERSION,
        )
        for slug, results in results_by_slug.items()
        for result in results
    ]
    stats_rows = [
        dict(
            stats,
            slug=slug,
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


def ensure_table(client, table, schema):
    """Create `table` if it is not there yet, and leave it alone if it is.

    The job declares its own destinations because nothing else does, and because the load below
    writes a single partition, which needs a table to write a partition of. Declaring the schema
    here rather than inferring it also means a column's type and description are properties of the
    code that fills them.
    """
    definition = bigquery.Table(table, schema=schema)
    definition.time_partitioning = bigquery.TimePartitioning(field=PARTITION_FIELD)
    client.create_table(definition, exists_ok=True)


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


def blob_for(storage, slug, blob_prefix):
    """Resolve one experiment's blob from a gs:// prefix, with or without a path component."""
    bucket_name, _, prefix = blob_prefix.removeprefix("gs://").partition("/")
    key = f"{prefix}/{slug}.json" if prefix else f"{slug}.json"
    return storage.bucket(bucket_name).blob(key)


def state_counts(results):
    """How many cells landed in each state, which is the operational-health signal per run."""
    counts = {}
    for result in results:
        counts[result["state"]] = counts.get(result["state"], 0) + 1
    return counts
