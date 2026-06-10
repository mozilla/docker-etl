"""Build the Firefox Application Update Out Of Date dashboard JSON file.

Replacement for the legacy Spark job
mozilla/telemetry-airflow:jobs/update_orphaning_dashboard_etl.py, which powers
https://telemetry.mozilla.org/update-orphaning/.

The legacy job ran on a 20-node Dataproc cluster: it aggregated the 1% sample of
`telemetry.main` in BigQuery, dumped the result to AVRO in GCS, loaded it into
Spark to fix histogram shapes, then ran an RDD map/filter pipeline. None of that
needs Spark — the working set after filtering is ~10k-15k clients. This job does
the histogram densification and out-of-date filtering in BigQuery (see
sql/out_of_date_details.sql) and the per-client categorization in plain python
(see processing.py).

Output is a single JSON file `<report_filename>.json` written to
gs://<output-bucket>/<output-prefix>, identical in shape to the legacy output.
"""

import datetime as dt
import json
import pathlib

import click
from google.cloud import bigquery, bigquery_storage, storage

from update_orphaning_dashboard import processing

# Defaults match the legacy Airflow DAG's py_args.
DEFAULT_OUTPUT_BUCKET = "moz-fx-data-static-websit-8565-analysis-output"
DEFAULT_OUTPUT_PREFIX = "app-update/data/out-of-date/"
DEFAULT_BILLING_PROJECT = "mozdata"
DEFAULT_TEST_OUTPUT_DIR = "test_output"

# Report constants, carried over verbatim from the legacy job.
CHANNEL_TO_PROCESS = "release"
MIN_VERSION = 42
UP_TO_DATE_RELEASES = 2
WEEKS_OF_SUBSESSION_DATA = 12
MIN_UPDATE_PING_COUNT = 4
MIN_SUBSESSION_HOURS = 2
MIN_SUBSESSION_SECONDS = MIN_SUBSESSION_HOURS * 60 * 60

MAJOR_RELEASES_URL = (
    "https://product-details.mozilla.org/1.0/firefox_history_major_releases.json"
)

_SQL_DIR = pathlib.Path(__file__).resolve().parent / "sql"


def load_sql(filename):
    return (_SQL_DIR / filename).read_text()


class ReportDates:
    """All the derived dates the report needs, computed from the run date.

    Mirrors the date math at the top of the legacy job. `run_date` is the
    Airflow `ds_nodash` (a Monday, by schedule).
    """

    def __init__(self, run_date):
        today = run_date
        # MON=0..SUN=6 -> SUN=0, MON=1, ..., SAT=6
        day_index = (today.weekday() + 1) % 7
        # Filename used to save the report's JSON (the report week's Sunday).
        self.report_filename = (today - dt.timedelta(day_index)).strftime("%Y%m%d")
        # Maximum report date: the previous Saturday.
        self.max_report_date = today - dt.timedelta(7 + day_index - 6)
        # The Sunday prior to that Saturday.
        self.min_report_date = self.max_report_date - dt.timedelta(days=6)
        # Subsession data lower bound.
        self.min_subsession_date = self.max_report_date - dt.timedelta(
            weeks=WEEKS_OF_SUBSESSION_DATA
        )
        # Date used to compute the latest version from the major-releases file.
        self.latest_ver_date_str = (
            self.max_report_date - dt.timedelta(days=7)
        ).strftime("%Y-%m-%d")
        # Submission-date window for the BigQuery aggregation: ~6 months.
        self.aggregation_to = self.max_report_date
        self.aggregation_from = self.max_report_date - dt.timedelta(days=6 * 31)
        # The SQL date-diff anchors. The legacy job compared subsession_start_date
        # against the day AFTER the previous Saturday (with `< 0`) and the
        # min_report_date (with `>= 0`).
        self.max_report_date_sql = self.max_report_date + dt.timedelta(days=1)


def latest_version_on_date(date_str, major_releases):
    """Latest Firefox major version released on or before `date_str`.

    Verbatim port of the legacy `latest_version_on_date`.
    """
    latest_date = "1900-01-01"
    latest_ver = 0
    for version, release_date in major_releases.items():
        version_int = int(version.split(".")[0])
        if date_str >= release_date >= latest_date and version_int >= latest_ver:
            latest_date = release_date
            latest_ver = version_int
    return latest_ver


def fetch_latest_version(latest_ver_date_str):
    # Imported lazily so unit tests don't reach the network.
    from urllib.request import urlopen

    major_releases = json.loads(urlopen(MAJOR_RELEASES_URL).read())
    return latest_version_on_date(latest_ver_date_str, major_releases)


def _start_query(billing_project, sql, **params):
    """Submit `sql` with named scalar params and return the finished job."""
    query_params = []
    for name, value in params.items():
        if isinstance(value, dt.date):
            query_params.append(bigquery.ScalarQueryParameter(name, "DATE", value))
        elif isinstance(value, int):
            query_params.append(bigquery.ScalarQueryParameter(name, "INT64", value))
        elif isinstance(value, str):
            query_params.append(bigquery.ScalarQueryParameter(name, "STRING", value))
        else:
            raise TypeError(f"Unsupported query param type for {name!r}: {type(value)}")
    job_config = bigquery.QueryJobConfig(
        query_parameters=query_params,
        # These queries use a lot of slots relative to bytes scanned; run on
        # on-demand billing like the other migrated dashboard jobs.
        reservation="none",
    )
    client = bigquery.Client(project=billing_project)
    job = client.query(sql, job_config=job_config)
    print(f"Running query: {job.project}.{job.location}.{job.job_id}")
    return job


def run_query(billing_project, sql, **params):
    """Run `sql` and return all rows as a list of dicts (for small results)."""
    job = _start_query(billing_project, sql, **params)
    bqstorage_client = bigquery_storage.BigQueryReadClient()
    return job.result().to_arrow(bqstorage_client=bqstorage_client).to_pylist()


def iter_query_rows(billing_project, sql, **params):
    """Run `sql` and yield result rows as dicts, one Arrow batch at a time.

    Reads results via the BigQuery Storage API (Arrow). The details query returns
    ~100k rows whose histogram columns are large JSON strings; the default REST
    row iterator deserializes those one row at a time (~15 min), whereas the
    Storage/Arrow path streams them in well under a minute. The flat all-STRING
    output schema (see out_of_date_details.sql) is what makes the Storage API
    usable here -- it rejects the nested-record schema the legacy job needed.

    Unlike :func:`run_query`, this never materializes the whole result set: it
    pulls one record batch at a time and yields its rows as dicts, so peak memory
    is bounded by a single batch plus whatever the caller keeps. This is what
    keeps the ~100k-client details pass within the GKE memory budget.
    """
    job = _start_query(billing_project, sql, **params)
    bqstorage_client = bigquery_storage.BigQueryReadClient()
    for batch in job.result().to_arrow_iterable(bqstorage_client=bqstorage_client):
        # to_pylist() on a single batch -> one short-lived list of row dicts.
        for row in batch.to_pylist():
            yield row


def build_results(dates, latest_version, summary_row, counts):
    """Assemble the report dict, matching the legacy results_dict exactly."""
    report_details = {
        "latestVersion": latest_version,
        "upToDateReleases": UP_TO_DATE_RELEASES,
        "minReportDate": dates.min_report_date.strftime("%Y-%m-%d"),
        "maxReportDate": dates.max_report_date.strftime("%Y-%m-%d"),
        "weeksOfSubsessionData": WEEKS_OF_SUBSESSION_DATA,
        "minSubsessionDate": dates.min_subsession_date.strftime("%Y-%m-%d"),
        "minSubsessionHours": MIN_SUBSESSION_HOURS,
        "minSubsessionSeconds": MIN_SUBSESSION_SECONDS,
        "minUpdatePingCount": MIN_UPDATE_PING_COUNT,
    }
    summary = {
        "versionUpToDate": summary_row["versionUpToDate"],
        "versionOutOfDate": summary_row["versionOutOfDate"],
        "versionTooLow": summary_row["versionTooLow"],
        "versionTooHigh": summary_row["versionTooHigh"],
        "versionMissing": summary_row["versionMissing"],
    }
    results = {"reportDetails": report_details, "summary": summary}
    results.update(counts)
    return results


def to_json(results):
    """Serialize, reproducing the legacy boolean-key compatibility hack.

    The dict-keyed counts have python `True`/`False` keys, which json renders
    as `"true"`/`"false"`. The legacy job (originally python2) emitted them
    capitalized; the dashboard frontend reads the capitalized form.
    """
    results_json = json.dumps(results, ensure_ascii=False)
    return results_json.replace('"true"', '"True"').replace('"false"', '"False"')


def upload_json(bucket_name, prefix, name, payload):
    bucket = storage.Client().get_bucket(bucket_name)
    blob = bucket.blob(f"{prefix}{name}")
    blob.upload_from_string(payload)
    print(f"Output file saved to: gs://{bucket_name}/{prefix}{name}")


def write_local_json(output_dir, name, payload):
    out_dir = pathlib.Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / name
    path.write_text(payload)
    return path


@click.command()
@click.option(
    "--run-date",
    "-d",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d", "%Y%m%d"]),
    help="Date of the processing run (Airflow ds_nodash). E.g. 2026-06-08.",
)
@click.option(
    "--billing-project",
    default=DEFAULT_BILLING_PROJECT,
    show_default=True,
    help="GCP project the BigQuery queries run and bill in.",
)
@click.option("--output-bucket", default=DEFAULT_OUTPUT_BUCKET, show_default=True)
@click.option("--output-prefix", default=DEFAULT_OUTPUT_PREFIX, show_default=True)
@click.option(
    "--test-output-dir",
    default=DEFAULT_TEST_OUTPUT_DIR,
    show_default=True,
    help="Directory for --dry-run output.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Write the JSON to --test-output-dir instead of uploading to GCS.",
)
def main(
    run_date, billing_project, output_bucket, output_prefix, test_output_dir, dry_run
):
    start_time = dt.datetime.now()
    print("Start: " + start_time.strftime("%Y-%m-%d %H:%M:%S"))

    dates = ReportDates(run_date.date())
    print(f"max_report_date     : {dates.max_report_date:%Y%m%d}")
    print(f"min_report_date     : {dates.min_report_date:%Y%m%d}")
    print(f"min_subsession_date : {dates.min_subsession_date:%Y%m%d}")
    print(f"report_filename     : {dates.report_filename}")
    print(f"latest_ver_date_str : {dates.latest_ver_date_str}")

    print("\n[1/5] Fetching latest Firefox version from product-details")
    latest_version = fetch_latest_version(dates.latest_ver_date_str)
    earliest_up_to_date_version = str(latest_version - UP_TO_DATE_RELEASES)
    max_up_to_date_ver = latest_version - UP_TO_DATE_RELEASES
    print(f"Latest Version: {latest_version}")

    print("\n[2/5] Running summary query (version counts)")
    summary_rows = run_query(
        billing_project,
        load_sql("summary.sql"),
        date_from=dates.aggregation_from,
        date_to=dates.aggregation_to,
        min_report_date=dates.min_report_date,
        max_report_date=dates.max_report_date_sql,
        channel=CHANNEL_TO_PROCESS,
        min_version=MIN_VERSION,
        up_to_date_low=max_up_to_date_ver,
        up_to_date_high=latest_version + 2,
    )
    summary_row = summary_rows[0]
    print(f"Summary counts: {dict(summary_row)}")

    print("\n[3/5] Running out-of-date details query (candidate clients)")
    # Streamed one Arrow batch at a time (not materialized) so the ~100k-client
    # details pass stays within the GKE memory budget; categorize() consumes the
    # iterator client-by-client.
    detail_rows = iter_query_rows(
        billing_project,
        load_sql("out_of_date_details.sql"),
        date_from=dates.aggregation_from,
        date_to=dates.aggregation_to,
        min_report_date=dates.min_report_date,
        max_report_date=dates.max_report_date_sql,
        channel=CHANNEL_TO_PROCESS,
        min_version=MIN_VERSION,
        max_up_to_date_ver=max_up_to_date_ver,
    )

    print("\n[4/5] Categorizing clients (streaming funnel)")
    counts = processing.categorize(
        detail_rows,
        min_subsession_date=dates.min_subsession_date,
        min_subsession_seconds=MIN_SUBSESSION_SECONDS,
        min_update_ping_count=MIN_UPDATE_PING_COUNT,
        earliest_up_to_date_version=earliest_up_to_date_version,
    )

    results = build_results(dates, latest_version, summary_row, counts)
    payload = to_json(results)

    name = f"{dates.report_filename}.json"
    if dry_run:
        print(f"\n[5/5] Writing report locally (--dry-run): {test_output_dir}/{name}")
        path = write_local_json(test_output_dir, name, payload)
        print(f"Wrote {path}")
    else:
        print(f"\n[5/5] Uploading report to gs://{output_bucket}/{output_prefix}{name}")
        upload_json(output_bucket, output_prefix, name, payload)

    end_time = dt.datetime.now()
    print("End: " + end_time.strftime("%Y-%m-%d %H:%M:%S"))
    print("Elapsed Seconds: " + str(int((end_time - start_time).total_seconds())))


if __name__ == "__main__":
    main()
