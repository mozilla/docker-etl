"""Run history for bigquery-etl tables, from BigQuery's job history.

lineage.py says which tables carry a metric; this says whether anything still
writes them and who reads them, over the same windows the other sources use
(common.WINDOWS).
A table nobody refreshes is not a consumer anybody maintains.

The source is monitoring_derived.jobs_by_organization_v1, the org-wide copy of
INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION without query text, readable with
data-viewers access. monitoring.bigquery_usage is built on it and adds what
it parses out of query text (Redash's query_id and username), at the cost of a
row per referenced table, and bqetl jobs have neither.

A run is a successful job that wrote the table: a query with it as destination
(the `$YYYYMMDD` partition decorator stripped), or a load or copy job, which is
how query.py files that build rows in Python write. DDL is not a run, and every
view is rewritten weekly by the view deploy, so views get no history. A read is a
job whose referenced_tables include the table, other than one writing it. A read
through a view is recorded against its base tables, so a view itself is never
read here.

Left out of both: shredder (it rewrites tables to delete rows, and runs as the
Airflow accounts, so only its reservation gives it away), Bigeye's checks,
Google's @system jobs and datahub's catalog crawl. A script's parent job is left
out too, since its statements are jobs of their own.

jobs_by_organization_v1 keeps a job only if it runs in, reads or writes one of
the projects listed in its query.py. moz-fx-data-experiments is not one of them,
so tables there show no history.
"""

from .common import LONGEST, WINDOWS, log, query, window_end, window_sql, window_start

JOBS = "moz-fx-data-shared-prod.monitoring_derived.jobs_by_organization_v1"

# Statements that put rows in their destination. CREATE_TABLE, CREATE_VIEW,
# ALTER and DROP define rather than run; DELETE on its own is cleanup.
WRITE_STATEMENTS = ("SELECT", "INSERT", "MERGE", "UPDATE", "CREATE_TABLE_AS_SELECT")
NOT_USE_RE = r"bigeye|@system\.|datahub"


def runs_sql(table_ids, end):
    start = window_start(LONGEST, end)
    ids = ", ".join(f"'{t}'" for t in sorted(table_ids))
    writes = ", ".join(f"'{s}'" for s in WRITE_STATEMENTS)
    return f"""
WITH tables AS (
  SELECT
    table_id,
    SPLIT(table_id, '.')[OFFSET(0)] AS p,
    SPLIT(table_id, '.')[OFFSET(1)] AS d,
    SPLIT(table_id, '.')[OFFSET(2)] AS t,
  FROM UNNEST([{ids}]) AS table_id
),
jobs AS (
  SELECT
    job_id,
    DATE(creation_time) AS day,
    LOWER(user_email) AS user_email,
    job_type IN ('LOAD', 'COPY') OR statement_type IN ({writes}) AS is_write,
    destination_table.project_id AS dp,
    destination_table.dataset_id AS dd,
    REGEXP_REPLACE(destination_table.table_id, r'\\$.*', '') AS dt,
    IF(job_type = 'QUERY', referenced_tables, []) AS referenced_tables,
  FROM `{JOBS}`
  WHERE DATE(creation_time) BETWEEN '{start}' AND '{end}'
    AND error_result IS NULL
    AND IFNULL(statement_type, '') != 'SCRIPT'
    AND NOT REGEXP_CONTAINS(IFNULL(reservation_id, ''), r'(?i)shredder')
    AND NOT REGEXP_CONTAINS(LOWER(user_email), r'{NOT_USE_RE}')
),
writes AS (
  SELECT
    tables.table_id,
    MAX(day) AS last_run,
    LOGICAL_OR(STRPOS(user_email, 'airflow') > 0) AS is_scheduled,
  FROM jobs
  JOIN tables ON tables.p = jobs.dp AND tables.d = jobs.dd AND tables.t = jobs.dt
  WHERE is_write
  GROUP BY table_id
),
reads AS (
  SELECT
    tables.table_id,
    {window_sql("day", end, n_reads="COUNT(DISTINCT IF({c}, job_id, NULL))",
                n_readers="COUNT(DISTINCT IF({c}, user_email, NULL))")},
  FROM jobs
  CROSS JOIN UNNEST(referenced_tables) AS r
  JOIN tables ON tables.p = r.project_id AND tables.d = r.dataset_id AND tables.t = r.table_id
  -- a table's own incremental runs read it too
  WHERE NOT (is_write AND IFNULL(jobs.dp = r.project_id AND jobs.dd = r.dataset_id
                               AND jobs.dt = r.table_id, FALSE))
  GROUP BY table_id
)
SELECT
  table_id, last_run, is_scheduled,
  {", ".join(f"n_reads_{w}d, n_readers_{w}d" for w in WINDOWS)}
FROM writes FULL OUTER JOIN reads USING (table_id)
"""


def table_runs(table_ids, end=None):
    """{table_id: {last_run, is_scheduled, views_<n>d, users_<n>d}} for the
    tables written or read in the longest window, with reads and readers per
    window. is_scheduled is an Airflow write anywhere in the longest window.
    The query names the window's end, so its cached result holds for the day."""
    end = end or window_end()
    rows = query(runs_sql(table_ids, end), name="bqetl runs")
    out = {}
    for r in rows:
        out[r["table_id"]] = {"last_run": r["last_run"], "is_scheduled": bool(r["is_scheduled"])}
        for w in WINDOWS:
            out[r["table_id"]][f"views_{w}d"] = r[f"n_reads_{w}d"] or 0
            out[r["table_id"]][f"users_{w}d"] = r[f"n_readers_{w}d"] or 0
    return out


NO_RUNS = {
    "last_run": None,
    "is_scheduled": False,
    **{f"{k}_{w}d": 0 for w in WINDOWS for k in ("views", "users")},
}


def add_runs(arts):
    """Fill run history into bqetl_table artifact rows, in place. A table with
    no write in the window keeps last_run null and is_scheduled false."""
    tables = [a for a in arts if a["kind"] == "bqetl_table"]
    got = table_runs({a["artifact_id"] for a in tables})
    for a in tables:
        a.update(got.get(a["artifact_id"]) or NO_RUNS)
    written = sum(1 for a in tables if a["last_run"])
    log(
        f"  run history: {written:,} of {len(tables):,} bqetl tables written in "
        f"{LONGEST} days ({sum(1 for a in tables if a['is_scheduled']):,} by Airflow), "
        f"{sum(1 for a in tables if a[f'views_{LONGEST}d']):,} read"
    )
