"""SECTION 3: SQL RUNNING.

Executes the per-source queries and returns the sufficient statistics keyed by slug and cell.
Records what each query consumed, because on a shared slot reservation wall time reflects how many
slots happened to be free, and slot-hours is the portable number.
"""

import datetime
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import bigquery

# The cohort holds one row per analysis unit, so it goes in this job's own dataset alongside its
# results rather than in a dataset anything else writes to.
COHORT_DATASET = "moz-fx-data-experiments.highwind_poc"

# How long a run's cohort table lives. It is an intermediate of one run and nothing reads it once
# the source queries have finished, so it is expired rather than kept. An expiry rather than a
# delete at the end of the run, because a run that fails between writing the table and deleting it
# would otherwise leave a table nothing will ever clean up.
COHORT_TABLE_TTL = datetime.timedelta(days=1)


def cohort_table_name(as_of):
    """Name this run's cohort table, uniquely to the run rather than to the date.

    Two runs of one date can overlap: Airflow retries as a matter of course, and a retry can start
    while the attempt it replaces is still running. On a shared name each run truncates the other's
    cohort out from under the source queries already reading it. A name per run makes the runs
    independent, and keeps the date in it so a table is still attributable to the run date it was
    built for.
    """
    date_part = as_of.isoformat().replace("-", "_")
    return f"{COHORT_DATASET}.highwind_cohort_{date_part}_{uuid.uuid4().hex[:8]}"


def materialize_cohort(client, sql, as_of, validate_only=False):
    """Write the run's cohort to its own table and return where it went.

    One table for every experiment in the run, keyed by slug and carrying the unit each was
    randomized on. Materialized rather than inlined because every source query needs it, and a CTE
    would be re-scanned once per query. The assignment scan is a large share of the job's bytes, so
    paying it once rather than once per source per experiment is the difference between a viable
    daily run and an unaffordable one.
    """
    table = cohort_table_name(as_of)
    started = time.time()
    if validate_only:
        # Validate the cohort query itself, but hand back None so the source queries validate
        # against an inline stub rather than a table this run is not going to create.
        job = client.query(sql, job_config=bigquery.QueryJobConfig(dry_run=True))
        return None, query_timing("cohort", job, [], time.time() - started)
    # Past the early return above, so this path always executes.
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            destination=table,
            write_disposition="WRITE_TRUNCATE",
            use_query_cache=False,
        ),
    )
    job.result()
    expire_cohort_table(client, table)
    return table, query_timing("cohort", job, [], time.time() - started)


def expire_cohort_table(client, table):
    """Give the cohort table an expiry, set after the write because a query job cannot carry one."""
    definition = client.get_table(table)
    definition.expires = datetime.datetime.now(datetime.timezone.utc) + COHORT_TABLE_TTL
    client.update_table(definition, ["expires"])


def run_queries(client, queries, validate_only=False):
    """Run every source query, returning (cells_by_slug, timings).

    The queries cover every experiment at once, so the rows come back carrying a slug and are split
    here. `cells_by_slug[slug][(metric, window, branch)]` is that cell's six sufficient statistics,
    which is the shape the statistics section asks for: one comparison needs two branches of one
    (metric, window) for one experiment, and nothing else.

    Run concurrently. The sources are independent tables, so serialising them would make the run's
    wall time their sum rather than the longest of them.
    """
    cells_by_slug, timings = {}, []
    with ThreadPoolExecutor(max_workers=len(queries) or 1) as pool:
        futures = {
            pool.submit(run_one_query, client, source, sql, validate_only): source
            for source, sql in queries.items()
        }
        for future in as_completed(futures):
            rows, timing = future.result()
            timings.append(timing)
            for row in rows:
                cells_by_slug.setdefault(row["slug"], {})[
                    (row["metric"], row["window_label"], row["branch"])
                ] = sufficient_stats(row)
    return cells_by_slug, sorted(timings, key=lambda t: t["source"])


def run_one_query(client, source, sql, validate_only):
    """Execute one source query and report what it consumed."""
    started = time.time()
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            dry_run=validate_only, use_query_cache=False
        ),
    )
    rows = [] if validate_only else [dict(row) for row in job.result()]
    return rows, query_timing(source, job, rows, time.time() - started)


def query_timing(source, job, rows, wall_seconds):
    """Report what the query consumed: wall time, bytes scanned, slot time and rows returned.

    Consumption rather than a price. Under a shared slot reservation the slots are already paid for,
    so slot-hours is what one query took from every other query rather than an amount of money, and
    wall time reflects how many slots happened to be free at the time.
    """
    slot_millis = job.slot_millis
    return dict(
        source=source,
        wall_s=round(wall_seconds, 1),
        gb_scanned=round((job.total_bytes_processed or 0) / 1e9, 1),
        slot_hours=None if slot_millis is None else round(slot_millis / 3_600_000, 2),
        rows=len(rows),
    )


def sufficient_stats(row):
    """One cell's six aggregates, as floats with an integer count."""
    return {
        "n": int(row["n"]),
        "sum": float(row["sum"] or 0.0),
        "sumsq": float(row["sumsq"] or 0.0),
        "pre_sum": float(row["pre_sum"] or 0.0),
        "pre_sumsq": float(row["pre_sumsq"] or 0.0),
        "xp": float(row["xp"] or 0.0),
    }
