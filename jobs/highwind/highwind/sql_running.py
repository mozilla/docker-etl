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

# Each run's intermediate tables (the cohort, the unit segments) hold one row per analysis unit, so
# they go in this job's own dataset alongside its results rather than in a dataset anything else
# writes to.
SCRATCH_DATASET = "moz-fx-data-experiments.highwind_poc"

# How long one of this job's intermediate tables lives. Nothing reads it once the source queries
# have finished, so it is expired rather than kept. An expiry rather than a delete at the end of the
# run, because a run that fails between writing the table and deleting it would otherwise leave a
# table nothing will ever clean up.
SCRATCH_TABLE_LIFETIME = datetime.timedelta(days=1)


def cohort_table_name(as_of):
    """Name this run's cohort table, uniquely to the run rather than to the date.

    Two runs of one date can overlap: Airflow retries as a matter of course, and a retry can start
    while the attempt it replaces is still running. On a shared name each run truncates the other's
    cohort out from under the source queries already reading it. A name per run makes the runs
    independent, and keeps the date in it so a table is still attributable to the run date it was
    built for.
    """
    date_part = as_of.isoformat().replace("-", "_")
    return f"{SCRATCH_DATASET}.highwind_cohort_{date_part}_{uuid.uuid4().hex[:8]}"


def unit_segments_table_name(as_of):
    """Name this run's unit-segments table, the same way `cohort_table_name` names the cohort."""
    date_part = as_of.isoformat().replace("-", "_")
    return f"{SCRATCH_DATASET}.highwind_unit_segments_{date_part}_{uuid.uuid4().hex[:8]}"


def materialize_scratch_table(
    client, sql, table, source, validate_only=False, write_disposition="WRITE_TRUNCATE"
):
    """Write one query's results into a uniquely named scratch table, under the run's scratch
    expiry, or just validate the SQL when `validate_only`.

    Shared by every intermediate table this job materializes: the cohort, written once with
    `WRITE_TRUNCATE`, and each segment source's own membership query, several of which are
    `WRITE_APPEND`ed in turn into the same destination. `source` is the timing's own label; the
    caller picks it since one query may share a destination with several others.
    """
    started = time.time()
    if validate_only:
        job = client.query(sql, job_config=bigquery.QueryJobConfig(dry_run=True))
        return None, query_timing(source, job, [], time.time() - started)
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            destination=table,
            write_disposition=write_disposition,
            use_query_cache=False,
        ),
    )
    job.result()
    expire_scratch_table(client, table)
    return table, query_timing(source, job, [], time.time() - started)


def materialize_cohort(client, sql, as_of, validate_only=False):
    """Write the run's cohort to its own table and return where it went.

    One table for every experiment in the run, keyed by slug and carrying the unit each was
    randomized on. Materialized rather than inlined because every source query needs it, and a CTE
    would be re-scanned once per query. The assignment scan is a large share of the job's bytes, so
    paying it once rather than once per source per experiment is the difference between a viable
    daily run and an unaffordable one.
    """
    table = cohort_table_name(as_of)
    return materialize_scratch_table(client, sql, table, "cohort", validate_only)


def materialize_unit_segments(client, queries, as_of, validate_only=False):
    """Write the run's resolved, non-`all_enrolled` segment memberships to their own table, one
    segment source's own membership query at a time.

    `queries` is keyed by `(source, analysis unit)`, as `sql_generation.segment_source_queries`
    builds it. Each runs as its own job and is `WRITE_APPEND`ed into the same freshly named table
    in turn, so one bad `select_expression` costs only the segments its own source defines rather
    than every segment the run resolved; a run's table name is unique, so appending the first query
    into it is no different from truncating. `queries` is empty whenever no experiment in the run
    resolved a segment (every run until a parser with `segment_plan()` is installed), in which case
    nothing is materialized and nothing downstream reads this table at all. Returns the table
    (`None` if nothing was written), the timings, and any failed query's error by its key.
    """
    if not queries:
        return None, [], {}
    table = unit_segments_table_name(as_of)
    timings, failures = [], {}
    wrote_anything = False
    for (source, unit_kind), sql in queries.items():
        try:
            _, timing = materialize_scratch_table(
                client,
                sql,
                table,
                f"unit_segments/{source}/{unit_kind}",
                validate_only=validate_only,
                write_disposition="WRITE_APPEND",
            )
            timings.append(timing)
            wrote_anything = wrote_anything or not validate_only
        except Exception as error:
            failures[(source, unit_kind)] = error
    return (table if wrote_anything else None), timings, failures


def cohort_enrollment_bounds(client, sql):
    """Each slug's own (earliest, latest) enrollment date, from
    `sql_generation.cohort_enrollment_bounds_query`."""
    return {row["slug"]: (row["earliest"], row["latest"]) for row in client.query(sql).result()}


def count_branch_units(client, sql):
    units_by_slug = {}
    for row in client.query(sql).result():
        units_by_slug.setdefault(row["slug"], {})[row["branch"]] = int(row["units"])
    return units_by_slug


def count_segment_branch_units(client, sql):
    """Each run's resolved, non-`all_enrolled` segments' own unit counts, by slug then segment.

    Mirrors `count_branch_units`'s shape one level deeper, since a segment beyond `all_enrolled`
    needs its own count per branch rather than the plain cohort count every slug already gets.
    """
    units_by_slug = {}
    for row in client.query(sql).result():
        by_segment = units_by_slug.setdefault(row["slug"], {})
        by_segment.setdefault(row["segment"], {})[row["branch"]] = int(row["units"])
    return units_by_slug


def expire_scratch_table(client, table):
    """Give this run's intermediate table an expiry, set after the write because a query job
    cannot carry one."""
    definition = client.get_table(table)
    definition.expires = datetime.datetime.now(datetime.timezone.utc) + SCRATCH_TABLE_LIFETIME
    client.update_table(definition, ["expires"])


def run_queries(client, queries, validate_only=False):
    """Run every source query, returning (cells_by_slug, timings).

    The queries cover every experiment at once, so the rows come back carrying a slug and are split
    here. `cells_by_slug[slug][(metric, window, segment, branch)]` is that cell's six sufficient
    statistics, which is the shape the statistics section asks for: one comparison needs two
    branches of one (metric, window, segment) for one experiment, and nothing else.

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
                    (row["metric"], row["window_label"], row["segment"], row["branch"])
                ] = sufficient_stats(row)
    return cells_by_slug, sorted(timings, key=lambda timing: timing["source"])


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
    slot_milliseconds = job.slot_millis
    return dict(
        source=source,
        wall_seconds=round(wall_seconds, 1),
        gigabytes_scanned=round((job.total_bytes_processed or 0) / 1e9, 1),
        slot_hours=(
            None if slot_milliseconds is None else round(slot_milliseconds / 3_600_000, 2)
        ),
        rows=len(rows),
    )


def sufficient_stats(row):
    """One cell's six aggregates, as floats with an integer count, plus how it was winsorized.

    The winsorize columns are nullable rather than defaulted like the six aggregates above: NULL
    is itself meaningful here (no threshold applied, for an OFF metric or a skipped-too-few
    window), where a NULL aggregate would only ever mean a BigQuery NULL arithmetic result this
    job already treats as zero.
    """
    return {
        "n": int(row["n"]),
        "sum": float(row["sum"] or 0.0),
        "sum_squares": float(row["sum_squares"] or 0.0),
        "pre_sum": float(row["pre_sum"] or 0.0),
        "pre_sum_squares": float(row["pre_sum_squares"] or 0.0),
        "sum_x_pre": float(row["sum_x_pre"] or 0.0),
        "winsorize_mode": row["winsorize_mode"],
        "post_winsorize_threshold": row["post_winsorize_threshold"],
        "pre_winsorize_threshold": row["pre_winsorize_threshold"],
        "clipped_share": row["clipped_share"],
    }
