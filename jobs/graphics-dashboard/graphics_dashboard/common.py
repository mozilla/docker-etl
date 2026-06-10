"""Shared helpers for the dashboard and trends jobs.

Both jobs run a BigQuery query against the Glean `metrics` ping, reshape the
rows into JSON, and either upload to GCS or (under --dry-run) write to a local
directory. This module holds the pieces common to both: SQL loading, query
execution, GCS/local writing, shared constants, and the click options they both
expose.
"""

import datetime
import json
import pathlib

import click
from google.cloud import bigquery, storage

# GCS location the dashboard frontend reads from.
DEFAULT_OUTPUT_BUCKET = "moz-fx-data-static-websit-8565-analysis-output"
DEFAULT_OUTPUT_PREFIX = "gfx/telemetry-data/"
# Where --dry-run writes files instead of uploading.
DEFAULT_TEST_OUTPUT_DIR = "test_output"

# Project queries run/bill in. The query reads moz-fx-data-shared-prod tables by
# fully-qualified name regardless; this only attributes on-demand billing.
DEFAULT_BILLING_PROJECT = "mozdata"

# The queries sample on Glean `sample_id` (0-99, each bucket = 1% of clients),
# selecting buckets [0, sample_id_count).
SAMPLE_ID_SPACE = 100
DEFAULT_SAMPLE_ID_COUNT = 1

_SQL_DIR = pathlib.Path(__file__).resolve().parent / "sql"


def sample_fraction(sample_id_count):
    """Fraction of clients sampled, for the dashboard sessions.metadata label."""
    return sample_id_count / SAMPLE_ID_SPACE


def load_sql(filename):
    """Read a query from the packaged sql/ directory."""
    return (_SQL_DIR / filename).read_text()


def default_end_date():
    """Yesterday (UTC) — the default inclusive end of every window."""
    return datetime.datetime.utcnow().date() - datetime.timedelta(days=1)


def run_query(billing_project, sql, **params):
    """Run `sql` in `billing_project` with named scalar params, return rows.

    Param types are inferred: datetime.date -> DATE, int -> INT64.
    """
    query_params = []
    for name, value in params.items():
        if isinstance(value, datetime.date):
            query_params.append(bigquery.ScalarQueryParameter(name, "DATE", value))
        elif isinstance(value, int):
            query_params.append(bigquery.ScalarQueryParameter(name, "INT64", value))
        else:
            raise TypeError(f"Unsupported query param type for {name!r}: {type(value)}")
    job_config = bigquery.QueryJobConfig(
        query_parameters=query_params,
        # the queries in these jobs use a lot of slots relative to their data scanned
        # so run with on-demand billing
        reservation="none",
    )
    client = bigquery.Client(project=billing_project)
    job = client.query(sql, job_config=job_config)
    print(f"Running query: {job.project}.{job.location}.{job.job_id}")
    return list(job.result())


def upload_json(bucket_name, prefix, name, payload):
    """Upload `payload` as JSON to gs://<bucket>/<prefix><name>."""
    bucket = storage.Client().bucket(bucket_name)
    blob = bucket.blob(f"{prefix}{name}")
    blob.upload_from_string(json.dumps(payload), content_type="application/json")
    print(f"Wrote gs://{bucket_name}/{prefix}{name}")


def read_json(bucket_name, prefix, name):
    """Read gs://<bucket>/<prefix><name> as JSON, or None if it doesn't exist."""
    blob = storage.Client().bucket(bucket_name).blob(f"{prefix}{name}")
    if not blob.exists():
        return None
    return json.loads(blob.download_as_text())


def write_local_json(output_dir, name, payload):
    """Write `payload` as pretty JSON to <output_dir>/<name>."""
    out_dir = pathlib.Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / name
    path.write_text(json.dumps(payload, indent=2))
    return path


# ---- shared click options (stack these decorators on a command) ----


def billing_project_option(fn):
    return click.option(
        "--billing-project",
        default=DEFAULT_BILLING_PROJECT,
        show_default=True,
        help="GCP project the BigQuery query runs and bills in (on-demand "
        "billing). The query reads moz-fx-data-shared-prod tables by their "
        "fully-qualified names regardless of this.",
    )(fn)


def sample_id_count_option(fn):
    return click.option(
        "--sample-id-count",
        type=int,
        default=DEFAULT_SAMPLE_ID_COUNT,
        show_default=True,
        help="Number of Glean sample_id buckets to scan, [0, N). Each bucket is "
        "1% of clients.",
    )(fn)


def output_location_options(fn):
    """--output-bucket / --output-prefix / --test-output-dir."""
    fn = click.option(
        "--test-output-dir",
        default=DEFAULT_TEST_OUTPUT_DIR,
        show_default=True,
        help="Directory for --dry-run output.",
    )(fn)
    fn = click.option(
        "--output-prefix", default=DEFAULT_OUTPUT_PREFIX, show_default=True
    )(fn)
    fn = click.option(
        "--output-bucket", default=DEFAULT_OUTPUT_BUCKET, show_default=True
    )(fn)
    return fn
