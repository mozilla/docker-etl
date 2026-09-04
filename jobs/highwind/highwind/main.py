"""Run the Highwind analysis for one date.

A thin entry point. Everything it calls lives in the `highwind` package, because the analysis is
several modules rather than one script. See `analysis.py` for the flow.

One run writes four things:

    highwind_statistics_v1        one row per (slug, metric, window, comparison), the results
    highwind_sufficient_stats_v1  one row per (slug, metric, window, branch), what they came from
    highwind_logs_v1              one row per log record, why a run or an experiment went wrong
    gs://mozanalysis/highwind/<slug>.json   per-experiment results, the seam Experimenter reads,
                                            named with the slug's hyphens as underscores

All four come from one computation, which is why they are one job rather than four.
"""

import logging
import sys

import click

from highwind.analysis import (
    DEFAULT_BILLING_PROJECT,
    run_daily_job,
    systemic_failure,
)
from highwind.output_writing import Outputs

DEFAULT_WORKERS = 8
DEFAULT_LOCAL_OUTPUT = "test_output"

# Level and message only. The run then reads as the progress report it was before any of it went
# through a logger, and the timestamp a reader wants against a line is on that line's row in the log
# table rather than in front of it here.
LOG_FORMAT = "%(levelname)-7s %(message)s"


@click.command()
@click.option(
    "--date",
    "run_date",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Run date. Windows mature against this, and it is the partition written.",
)
@click.option(
    "--workers",
    default=DEFAULT_WORKERS,
    show_default=True,
    help=(
        "Experiments whose statistics are computed concurrently. The BigQuery scan is shared "
        "across all experiments and does not depend on this."
    ),
)
@click.option(
    "--slug",
    "slugs",
    multiple=True,
    help=(
        "Analyse only this experiment. Repeat the flag for several. A filtered run writes blobs "
        "but no tables, so it cannot replace a full run's partition with a subset of it."
    ),
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help=(
        "Analyse only the first N experiments, shortest-running first. Filtered like --slug, so "
        "it writes blobs but no tables."
    ),
)
@click.option(
    "--dry-run",
    is_flag=True,
    help=(
        "Write the per-experiment JSON to --local-output instead of GCS, and skip the tables. The "
        "queries still run, so this costs what a real run costs."
    ),
)
@click.option(
    "--local-output",
    default=DEFAULT_LOCAL_OUTPUT,
    show_default=True,
    help="Directory --dry-run writes the per-experiment JSON to.",
)
@click.option(
    "--validate-sql",
    is_flag=True,
    help=(
        "Submit every query to BigQuery for validation without executing it, and write nothing. "
        "Checks that the generated SQL is well formed and typed, at no scan cost."
    ),
)
@click.option(
    "--sample-percent",
    type=int,
    default=None,
    help=(
        "Restrict the cohort to this percent of analysis units, so a run costs roughly that share "
        "of a full one. For development: the intervals it produces are wider than the real ones, "
        "so it writes no tables and no GCS blobs. Pair it with --dry-run to read its output."
    ),
)
@click.option(
    "--billing-project",
    default=DEFAULT_BILLING_PROJECT,
    show_default=True,
    help=(
        "Project the queries run and bill in. They read moz-fx-data-shared-prod and "
        "moz-fx-data-experiments tables by fully-qualified name regardless."
    ),
)
def main(
    run_date,
    workers,
    slugs,
    limit,
    dry_run,
    local_output,
    validate_sql,
    sample_percent,
    billing_project,
):
    """Analyse every live Firefox Desktop experiment for the run date."""
    configure_logging()
    summaries = run_daily_job(
        as_of=run_date.date(),
        only_slugs=list(slugs) or None,
        limit=limit,
        validate_only=validate_sql,
        workers=workers,
        sample_percent=sample_percent,
        billing_project=billing_project,
        outputs=Outputs(local_blob_dir=local_output) if dry_run else None,
    )
    # A run where nothing succeeded must not exit zero. Under Airflow that is a green task that did
    # nothing, which is the failure mode hardest to notice and slowest to diagnose.
    if systemic_failure(summaries):
        raise SystemExit("Highwind produced no results for any experiment")


def configure_logging():
    """Send the run's log to stdout, which is what a container's logs are.

    Configured once, here, rather than in the analysis: the analysis decides what to say and at what
    level, and where that goes is a property of how the job was started.
    """
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, stream=sys.stdout)


if __name__ == "__main__":
    sys.exit(main())
