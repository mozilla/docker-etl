"""Weekly report of modules with missing symbols in Firefox crash reports.

Ported from the PySpark job in python_mozetl (mozetl/symbolication). The Spark
work was one explode, one dedupe and one group-by, which is now the query in
sql/modules_with_missing_symbols.sql. Everything else (the symbols server
lookups, the HTML, the SES send) was already plain Python.
"""

import dataclasses
import datetime
import pathlib
import sys

import click
import requests
from google.cloud import bigquery

from crash_missing_symbols import report, symbols
from crash_missing_symbols.module_lists import fetch_module_lists

DEFAULT_BILLING_PROJECT = "mozdata"

# Modules under this many crash reports in the window aren't worth reporting.
DEFAULT_MIN_CRASH_COUNT = 70
DEFAULT_WINDOW_DAYS = 3

# How a module repeated within one crash report is counted. Defaults to what
# Spark did, because the migration is validated by diffing against the old job.
# See the SQL and the README for what the two modes mean.
DEDUPE_MODULE_STRUCT = "module-struct"
DEDUPE_CRASH_REPORT = "crash-report"
DEFAULT_DEDUPE_KEY = DEDUPE_MODULE_STRUCT

DEFAULT_EMAIL_SENDER = "telemetry-alerts@mozilla.com"
DEFAULT_EMAIL_RECIPIENTS = (
    "mcastelluccio@mozilla.com",
    "release-mgmt@mozilla.com",
    "stability@mozilla.org",
)
SES_REGION = "us-west-2"

_SQL_PATH = (
    pathlib.Path(__file__).resolve().parent
    / "sql"
    / "modules_with_missing_symbols.sql"
)


@dataclasses.dataclass
class Module:
    name: str
    version: str
    debug_id: str
    debug_file: str
    crash_count: int
    symbols_available: bool = False


def query_modules(
    billing_project,
    end_date,
    window_days,
    min_crash_count,
    dedupe_key=DEFAULT_DEDUPE_KEY,
):
    """Modules with missing symbols over the window, most frequent first."""
    client = bigquery.Client(project=billing_project)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
            bigquery.ScalarQueryParameter("window_days", "INT64", window_days),
            bigquery.ScalarQueryParameter(
                "min_crash_count", "INT64", min_crash_count
            ),
            bigquery.ScalarQueryParameter("dedupe_key", "STRING", dedupe_key),
        ]
    )
    rows = client.query(
        _SQL_PATH.read_text(), job_config=job_config
    ).result()
    return [
        Module(
            name=row["name"],
            version=row["version"],
            debug_id=row["debug_id"],
            debug_file=row["debug_file"],
            crash_count=row["crash_count"],
        )
        for row in rows
    ]


def parse_recipients(values):
    """Clean up repeated --recipient flags.

    Order is preserved and duplicates dropped.
    """
    recipients = []
    for value in values:
        address = value.strip()
        if address and address not in recipients:
            recipients.append(address)
    if not recipients:
        raise click.UsageError("--recipient cannot be empty")
    return recipients


def send_email(subject, body, sender, recipients):
    import boto3

    boto3.client("ses", region_name=SES_REGION).send_email(
        Source=sender,
        Destination={"ToAddresses": list(recipients), "CcAddresses": []},
        Message={
            "Subject": {"Data": subject, "Charset": "UTF-8"},
            "Body": {"Html": {"Data": body, "Charset": "UTF-8"}},
        },
    )


@click.command()
@click.option(
    "--run-on-days",
    multiple=True,
    type=int,
    help=(
        "Only send email on these days of the week (0 is Sunday). The report is "
        "still built every day and printed. Works around Airflow not supporting "
        "per-task schedules. Omit to never send, so a run that forgets the flag "
        "can't mail the distribution lists."
    ),
)
@click.option(
    "--date",
    "run_date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="Run date. Defaults to today (UTC).",
)
@click.option("--window-days", default=DEFAULT_WINDOW_DAYS, show_default=True)
@click.option(
    "--min-crash-count", default=DEFAULT_MIN_CRASH_COUNT, show_default=True
)
@click.option("--billing-project", default=DEFAULT_BILLING_PROJECT)
@click.option(
    "--recipient",
    "recipients",
    multiple=True,
    default=DEFAULT_EMAIL_RECIPIENTS,
    show_default=True,
    help=(
        "Address to send the report to. Repeat the flag for several. Replaces "
        "the defaults rather than adding to them."
    ),
)
@click.option(
    "--sender",
    default=DEFAULT_EMAIL_SENDER,
    show_default=True,
    help="From address. Must be verified in SES.",
)
@click.option(
    "--dedupe-key",
    type=click.Choice([DEDUPE_MODULE_STRUCT, DEDUPE_CRASH_REPORT]),
    default=DEFAULT_DEDUPE_KEY,
    show_default=True,
    help=(
        "How to count a module repeated within one crash report. "
        f"'{DEDUPE_MODULE_STRUCT}' reproduces the Spark job, counting one per "
        f"mapped address. '{DEDUPE_CRASH_REPORT}' counts each report once, "
        "which is what the report's column header claims."
    ),
)
@click.option(
    "--fix-availability-args",
    is_flag=True,
    help=(
        "Look up symbols at the URL that actually exists. The Spark job passed "
        "debug_file and debug_id transposed, so nothing was ever marked "
        "available; off by default to reproduce that."
    ),
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Print the email to stdout instead of sending it.",
)
def main(
    run_on_days,
    run_date,
    window_days,
    min_crash_count,
    billing_project,
    recipients,
    sender,
    dedupe_key,
    fix_availability_args,
    dry_run,
):
    run_date = (
        run_date.date()
        if run_date
        else datetime.datetime.now(datetime.UTC).date()
    )
    recipients = parse_recipients(recipients)

    # The job runs in full every day so failures surface on the day they break
    # rather than once a week. Only the send is weekly.
    # isoweekday() is 1-7 Mon-Sun; % 7 makes Sunday 0 to match --run-on-days.
    weekday = run_date.isoweekday() % 7
    off_schedule = weekday not in run_on_days
    if off_schedule:
        reason = (
            f"{run_date} is day {weekday}, not in {sorted(run_on_days)}"
            if run_on_days
            else "no --run-on-days given, so there are no send days"
        )
        click.echo(f"{reason}. Building the report but not sending it.", err=True)

    session = requests.Session()
    known_modules, firefox_modules, windows_modules = fetch_module_lists(session)
    # Pinned to run_date, not the clock, so a backfill uses the expiry cutoff
    # that applied for the window it's reporting on.
    old_firefox_versions = symbols.fetch_old_firefox_versions(
        session, datetime.datetime.combine(run_date, datetime.time())
    )

    modules = query_modules(
        billing_project, run_date, window_days, min_crash_count, dedupe_key
    )
    click.echo(
        f"modules from query: {len(modules)} (dedupe key: {dedupe_key})", err=True
    )

    # Suppress modules we expect to have no symbols for, and Firefox modules old
    # enough that their symbols have expired off the server. The known_modules
    # filter is applied here rather than in SQL because the list is small and
    # this keeps the repo the single place people edit to suppress a module.
    modules = [
        module
        for module in modules
        if module.name.lower() not in known_modules
        and not symbols.is_old_firefox_module(
            module, firefox_modules, old_firefox_versions
        )
    ]
    click.echo(f"modules after filtering: {len(modules)}", err=True)

    for module in modules:
        module.symbols_available = symbols.are_symbols_available(
            session,
            module.debug_file,
            module.debug_id,
            fix_arg_order=fix_availability_args,
        )

    subject = report.subject(run_date)
    body = report.build_body(
        modules,
        firefox_modules,
        windows_modules,
        window_days,
        min_crash_count,
    )

    if dry_run or off_schedule:
        # Recipients go to stderr so stdout is just the email, and piping it to
        # a file or a browser gives you the report on its own.
        reason = "Dry run" if dry_run else "Not a send day"
        click.echo(
            f"{reason}, not sending. Would have mailed {', '.join(recipients)} "
            f"from {sender}.",
            err=True,
        )
        click.echo(subject)
        click.echo(body)
        return
    else:
        send_email(subject, body, sender, recipients)
        click.echo(f"Sent report to {', '.join(recipients)}", err=True)


if __name__ == "__main__":
    sys.exit(main())
