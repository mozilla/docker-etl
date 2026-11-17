"""Metric references in ad hoc BigQuery queries, read from job history.

    python -m metric_usage.adhoc                          # 180 days of jobs, write tables
    python -m metric_usage.adhoc --dry-run                # everything but writing
    python -m metric_usage.adhoc --project mozdata --days 7 --dry-run   # a quick look

Writes refs_adhoc and artifacts_adhoc, one artifact per account that ran a
query naming a metric: people in the console, notebooks and the bq CLI, and
service accounts such as Deepnote and Grafana. Jetstream's queries are
credited to the experiment they compute metrics for, read from the
mozanalysis table each one writes (artifact_of), rather than to its one
service account: which experiments used a metric, and on which days, is what
a metric owner needs from it. A metric is named for an experiment when its
metric-hub config chose it, and not named when only the platform's default
metrics compute it (metric_hub.Model.experiment_refs). Its later reads of
those tables, to compute statistics, are not read at all, since they only
touch mozanalysis (see job_rows). Needs lineage.py to
have run first; each query goes through the same resolver as a Redash query
(redash.analyse), with the job's own project for unqualified table names.

Reads INFORMATION_SCHEMA.JOBS_BY_PROJECT in each project in PROJECTS, which
needs bigquery.jobs.listAll there; a project the account cannot read is logged
and skipped. Nothing here is tied to a person, so a service account with that
access can run it. JOBS_BY_PROJECT keeps 180 days, which is the longest
window (common.WINDOWS) and caps --days; its first day is cut at the time of
day the run starts. Each ref has its days per window and seen_within, the
shortest window with a query naming the metric, and each account and
experiment its jobs per window.

Left out, because another source covers them or because they are not somebody
using a metric: Redash and Looker (redash.py and looker.py), Airflow (its jobs
are bigquery-etl's own queries, checks, backfills and copy_deduplicate), any
job writing a bigquery-etl table (lineage.py has its SQL), Bigeye's checks,
Google's @system jobs, datahub's catalog crawl, shredder's reservations, DDL
(a CREATE VIEW names every column of the view without reading any), failed
jobs, and this script's own jobs, which carry MARKER. A script is read whole
from its parent job, since its statements share temp tables; the child jobs
are skipped.

Each distinct query is resolved once. A person's query is keyed on its exact
text, since two that differ only in an event name are different questions. A
service account's is keyed on BigQuery's literal-normalized hash: its SQL is
generated, and the literals that vary are parameters such as a Grafana panel's
time range, while an event a panel filters on stays the same from run to run.

Query text stays in memory. These are people's own queries, whose literals can
hold client ids and emails, so unlike other sources' text it is not written to
.cache either, and its rows never go through common.query(), which
caches what it returns. What is cached is the job list, which has accounts and
query keys but no text, and the resolved refs per fingerprint until lineage
is rebuilt, so a rerun fetches text only for queries it has not resolved.
Only metric ids from the inventory reach the tables, never a literal, a column
alias or a table name from the text.

Emails are stored and shown, since
monitoring_derived.jobs_by_organization_v1 already publishes user_email to the
same audience, and knowing who reads a metric is what lets somebody ask before
changing it.
"""

import argparse
import collections
import datetime
import hashlib
import json
import multiprocessing
import re
import signal
import sys
import time

from .common import (CACHE, DEST, LONGEST, PREFIX, WINDOWS, artifact_schema, client,
                     days_within, log, query, ref_schema, seen_within, union_views,
                     window_end, window_start, write_table, write_atomic)

from . import metric_hub as MH
from . import redash as R

# The projects whose jobs touch stable tables, chosen from
# jobs_by_organization_v1. bq-batch-prod, shredder and glam-prod are left out,
# since their jobs are pipelines rather than somebody using a metric.
PROJECTS = [
    "mozdata",
    "moz-fx-data-shared-prod",
    "moz-fx-data-airflow-gke-prod",
    "moz-fx-data-experiments",
    "moz-fx-data-bq-data-science",
    "moz-fx-data-marketing-prod",
    "moz-fx-data-backfill-2",
    "moz-fx-data-backfill-3",
    "moz-fx-data-backfill-4",
]

EXCLUDE_ACCOUNTS_RE = r"redash|looker|airflow|bigeye|@system\.|datahub"
DDL_TYPES = ("CREATE_VIEW", "CREATE_MATERIALIZED_VIEW", "CREATE_FUNCTION",
             "CREATE_TABLE_FUNCTION", "CREATE_PROCEDURE", "CREATE_SCHEMA",
             "CREATE_TABLE", "ALTER_TABLE", "ALTER_VIEW", "DROP_TABLE", "DROP_VIEW",
             "CREATE_ROW_ACCESS_POLICY")
# In every query this script runs, so a later run does not read its own jobs
MARKER = "bigquery-metric-lineage:adhoc"

RESOLVE_TIMEOUT = 60
BATCH_DAYS = 7
# Generated SQL past this (a pasted list of ids, a machine-built query) is
# left unparsed rather than given a worker for a minute
MAX_TEXT = 1_000_000

# Bumped when what a cached result means changes
RESOLVED_VERSION = 1

# An account's ref takes the strongest role any of its queries gave it;
# unknown is a query only read by name (regex)
ROLE_RANK = {"unknown": 0, "filter": 1, "output": 2}


# What a query is resolved once per; see the module docstring
KEY_SQL = """IF(
    ENDS_WITH(LOWER(user_email), 'gserviceaccount.com')
    AND query_info.query_hashes.normalized_literals IS NOT NULL,
    CONCAT('n', query_info.query_hashes.normalized_literals),
    CONCAT('t', CAST(FARM_FINGERPRINT(query) AS STRING)))"""


def jobs_where(start, end):
    ddl = ", ".join(f"'{t}'" for t in DDL_TYPES)
    stop = end + datetime.timedelta(days=1)
    return f"""
  creation_time >= TIMESTAMP('{start}') AND creation_time < TIMESTAMP('{stop}')
  AND job_type = 'QUERY'
  AND state = 'DONE'
  AND error_result IS NULL
  AND parent_job_id IS NULL
  AND query IS NOT NULL
  AND IFNULL(statement_type, '') NOT IN ({ddl})
  AND NOT REGEXP_CONTAINS(IFNULL(reservation_id, ''), r'(?i)shredder')
  AND NOT REGEXP_CONTAINS(LOWER(user_email), r'{EXCLUDE_ACCOUNTS_RE}')
  AND STRPOS(query, '{MARKER}') = 0"""


def job_list_sql(project, start, end, known):
    """Jobs grouped by query key, account and day, without the text, for the
    jobs that read a table in known (see job_rows)."""
    tables = ", ".join(f"'{t}'" for t in sorted(known))
    return f"""/* {MARKER} */
SELECT
  {KEY_SQL} AS key,
  LOWER(user_email) AS account,
  DATE(creation_time) AS day,
  IF(
    STARTS_WITH(destination_table.dataset_id, '_'),
    NULL,
    FORMAT('%s.%s.%s', destination_table.project_id, destination_table.dataset_id,
           REGEXP_REPLACE(destination_table.table_id, r'\\$.*', ''))
  ) AS wrote,
  COUNT(*) AS jobs,
FROM `{project}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE {jobs_where(start, end)}
  AND (
    ARRAY_LENGTH(referenced_tables) = 0
    OR EXISTS (
      SELECT 1 FROM UNNEST(referenced_tables) AS r
      WHERE FORMAT('%s.%s.%s', r.project_id, r.dataset_id, r.table_id) IN UNNEST([{tables}])
    )
  )
GROUP BY ALL
"""


def texts_sql(project, start, end):
    return f"""/* {MARKER} */
SELECT {KEY_SQL} AS key, ANY_VALUE(query) AS query
FROM `{project}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE {jobs_where(start, end)}
  AND {KEY_SQL} IN UNNEST(@keys)
GROUP BY key
"""


def texts_reading(project, start, end, tables):
    """{key: text} for the queries over the whole window whose jobs read one
    of tables, or recorded no tables (a cache hit, a script's parent), in
    memory only. For accuracy.py, whose text search only needs the queries
    that could mention its sampled metrics: few enough to fetch at once."""
    from google.cloud import bigquery

    sql = f"""/* {MARKER} */
SELECT {KEY_SQL} AS key, ANY_VALUE(query) AS query
FROM `{project}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE {jobs_where(start, end)}
  AND (
    ARRAY_LENGTH(referenced_tables) = 0
    OR EXISTS (
      SELECT 1 FROM UNNEST(referenced_tables) AS r
      WHERE FORMAT('%s.%s.%s', r.project_id, r.dataset_id, r.table_id) IN UNNEST(@tables)
    )
  )
GROUP BY key
"""
    config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter("tables", "STRING", sorted(tables))])
    job = client().query(sql, job_config=config)
    out = {r["key"]: r["query"] for r in job.result(page_size=2000)}
    fetch_texts.bytes += job.total_bytes_processed or 0
    return out


def fetch_texts(project, start, end, keys):
    """{key: text} for the given query keys, in memory only."""
    from google.cloud import bigquery

    config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter("keys", "STRING", sorted(keys))])
    job = client().query(texts_sql(project, start, end), job_config=config)
    out = {r["key"]: r["query"] for r in job.result(page_size=2000)}
    fetch_texts.bytes += job.total_bytes_processed or 0
    return out


fetch_texts.bytes = 0


# ------------------------------------------------------------------ resolving


class _Timeout(BaseException):
    # not an Exception, so parse and resolve error handling cannot swallow it
    pass


def _alarm(signum, frame):
    raise _Timeout()


def _init():
    sys.setrecursionlimit(20000)
    signal.signal(signal.SIGALRM, _alarm)


def _work(item):
    key, project, text = item
    if len(text) > MAX_TEXT:
        return key, ({}, [], "too long")
    signal.alarm(RESOLVE_TIMEOUT)
    try:
        named, stars, note = R.analyse(text, project)
    except _Timeout:
        named, stars, note = {}, [], "timeout"
    except RecursionError:
        named, stars, note = {}, [], "resolve failed: RecursionError"
    finally:
        signal.alarm(0)
    return key, (named, stars, note)


def snapshot():
    """Resolved results hold while the metrics and carries tables are the ones
    they were resolved against."""
    return "|".join([str(RESOLVED_VERSION)]
                    + [str(client().get_table(f"{DEST}.{PREFIX}{t}").modified)
                       for t in ("metrics", "carries")])


def resolve_all(rows, start, end):
    """{(project, key): (named, stars, note)} for every query in rows, reusing
    what was resolved against the same lineage.

    The rest are fetched and resolved a week at a time, each from the week it
    first ran, so one week's text is in memory at once and the job history
    is scanned once. What is resolved is saved after each week, so an
    interrupted run picks up where it stopped.
    """
    path = CACHE / f"adhoc_resolved_{hashlib.sha256(snapshot().encode()).hexdigest()[:16]}.json"
    held = json.loads(path.read_text()) if path.exists() else {}
    for old in CACHE.glob("adhoc_resolved_*.json"):
        if old != path:
            old.unlink()
    out, need = {}, {}
    for k, day in first_days(rows).items():
        h = held.get(f"{k[0]}:{k[1]}")
        # a timeout says how fast the resolver was, not what the SQL reads, so
        # it is tried again rather than reused
        if h is None or h[2] == "timeout":
            need[k] = day
        else:
            out[k] = ({int(m): tuple(v) for m, v in h[0].items()}, h[1], h[2])
    log(f"  resolving {len(need):,} queries ({len(out):,} reused from this lineage snapshot)")
    if not need:
        return out
    R.L.persistent_udfs()  # parsed once here for the forked workers
    ctx = multiprocessing.get_context("fork")
    started, done = time.monotonic(), 0
    with ctx.Pool(initializer=_init) as pool:
        for project, b_start, n, texts in text_batches(need, start, end):
            items = [(f"{project}:{key}", project, t) for key, t in texts.items()]
            del texts
            for key, res in pool.imap_unordered(_work, items, chunksize=16):
                out[tuple(key.split(":", 1))] = res
                held[key] = [{str(m): list(v) for m, v in res[0].items()}, res[1], res[2]]
            del items
            write_atomic(path, json.dumps(held).encode())
            done += n
            log(f"    {project} from {b_start}: {n:,} queries; {done:,} of {len(need):,} "
                f"after {(time.monotonic() - started) / 60:.1f} minutes")
    return out


def first_days(rows):
    """{(project, key): the first day it ran} over the job list."""
    first = {}
    for r in rows:
        k = (r["project"], r["key"])
        if k not in first or r["day"] < first[k]:
            first[k] = r["day"]
    return first


def text_batches(first, start, end):
    """(project, week start, number of queries, {key: text}) a week at a time,
    each query's text from the first week it ran. first is first_days(), or
    the part of it to fetch."""
    batches = collections.defaultdict(set)
    for (project, key), day in first.items():
        week = (datetime.date.fromisoformat(day) - start).days // BATCH_DAYS
        batches[(project, week)].add(key)
    for (project, week), keys in sorted(batches.items()):
        b_start = start + datetime.timedelta(days=week * BATCH_DAYS)
        b_end = min(end, b_start + datetime.timedelta(days=BATCH_DAYS - 1))
        yield project, b_start, len(keys), fetch_texts(project, b_start, b_end, keys)


# ---------------------------------------------------------------------- main


# Jetstream writes each experiment's metrics to mozanalysis tables named for
# it: enrollments_<slug>, and <slug>_enrollments_<data source>_<period>_<n>
# (or _exposures_), with the slug's punctuation turned into underscores.
JETSTREAM_TABLE_RE = re.compile(r"^moz-fx-data-experiments\.mozanalysis\.(.+)$")
SLUG_END_RE = re.compile(r"_(?:enrollments|exposures)_")
experiments = MH.experiments


def experiment_of(wrote, known):
    """The table-name slug of the experiment a Jetstream job computed metrics
    for, from the table it wrote, or None. A slug is checked against known
    where the name is ambiguous, as in a slug with _exposures_ in it."""
    m = JETSTREAM_TABLE_RE.match(wrote or "")
    if not m:
        return None
    rest = m.group(1).lower()
    if rest.startswith("enrollments_"):
        return rest[len("enrollments_"):]
    cuts = [x.start() for x in SLUG_END_RE.finditer(rest)]
    if not cuts:
        return None
    return next((rest[:c] for c in reversed(cuts) if rest[:c] in known), rest[:cuts[0]])


def artifact_of(row, known):
    """(artifact id, kind, name) a job's refs are credited to: the experiment
    for a Jetstream job, otherwise the account that ran it."""
    slug = experiment_of(row["wrote"], known)
    if slug:
        return f"jetstream:{slug}", "jetstream_experiment", slug
    account = row["account"]
    kind = "service_account" if account.endswith("gserviceaccount.com") else "person"
    return f"adhoc:{account}", kind, account


def window(days=LONGEST):
    """(start, end) of the days ending yesterday, the last whole day."""
    end = window_end()
    return window_start(days, end), end


def job_rows(projects, start, end):
    """The job list, one row per (project, query key, account, day, table
    written), without jobs writing a bigquery-etl table.

    Only jobs reading a table lineage has a shape for can name a metric, so
    the rest are dropped before any text is fetched: Jetstream reading its
    own tables in mozanalysis, metadata lookups, logs. That is most distinct
    queries. referenced_tables lists the base tables under any view, which is
    what the shapes are keyed on. A job with none recorded is kept, since that
    is also how a cache hit and a script's parent job look.
    """
    known = R.known_tables()
    bqetl = {r["artifact_id"] for r in query(
        f"SELECT artifact_id FROM `{DEST}.{PREFIX}artifacts_bqetl` WHERE kind = 'bqetl_table'",
        name="bqetl tables", tables=["artifacts_bqetl"])}

    rows = []
    for project in projects:
        try:
            got = query(job_list_sql(project, start, end, known), name=f"jobs in {project}")
        except Exception as e:
            log(f"  skipped {project}: {getattr(e, 'message', None) or str(e).splitlines()[0]}")
            continue
        for r in got:
            r["project"] = project
        rows += got
    # a job writing a bigquery-etl table is that table's own query
    return [r for r in rows if r["wrote"] not in bqetl]


def run(projects, days):
    start, end = window(days)
    log(f"  jobs from {start} to {end}")
    inventory = R.load_shapes()
    rows = job_rows(projects, start, end)
    log(f"  {sum(r['jobs'] for r in rows):,} jobs, "
        f"{len({(r['project'], r['key']) for r in rows}):,} distinct queries, "
        f"{len({r['account'] for r in rows}):,} accounts")

    results = resolve_all(rows, start, end)
    if fetch_texts.bytes:
        log(f"  query text read: {fetch_texts.bytes / 2**30:.1f} GiB processed")

    notes = collections.Counter(v[2].split(":")[0] for v in results.values())
    exps = experiments(end)
    # (artifact, metric) -> days, role, hops, via, transformed, evidence
    refs = {}
    arts = {}
    for r in rows:
        named, _, note = results.get((r["project"], r["key"]), ({}, [], "missing"))
        if not named:
            continue
        aid, kind, name = artifact_of(r, exps)
        a = arts.setdefault(aid, {"kind": kind, "name": name, "jobs": collections.Counter(),
                                  "days": set(), "metrics": set(), "accounts": set()})
        a["jobs"][r["day"]] += r["jobs"]
        a["days"].add(r["day"])
        a["accounts"].add(r["account"])
        for m, (role, h, via, t) in named.items():
            a["metrics"].add(m)
            held = refs.get((aid, m))
            if held is None:
                refs[(aid, m)] = held = {
                    "days": set(), "role": role, "hops": h, "via": via, "transformed": t,
                    "evidence": "regex" if role == "unknown" else "parsed"}
            held["days"].add(r["day"])
            if ROLE_RANK[role] > ROLE_RANK[held["role"]]:
                held["role"] = role
                held["evidence"] = "parsed"
            if h < held["hops"]:
                held["hops"], held["via"] = h, via
            held["transformed"] = held["transformed"] or t

    # A Jetstream experiment names a metric when its metric-hub config chose
    # it; one computed only by its platform's defaults, or by Jetstream's own
    # enrollment queries, is not named. The via becomes the metric-hub metric.
    chosen = {}
    for aid, a in arts.items():
        if a["kind"] == "jetstream_experiment":
            chosen[aid] = MH.model().experiment_refs(a["name"], exps.get(a["name"]))

    ref_rows = []
    for (aid, m), v in sorted(refs.items()):
        r = inventory[m]
        named, hops, via = True, v["hops"] + 1, v["via"]
        if aid in chosen:
            c = chosen[aid].get(m)
            named = bool(c and c[0])
            if c and c[2]:
                hops, via = c[1] + 1, c[2]
        ref_rows.append({
            "app": r["app"], "metric": r["metric"], "artifact_id": aid,
            "artifact_kind": arts[aid]["kind"], "role": v["role"], "hops": hops,
            "named": named, "transformed": v["transformed"], "via": via,
            "output_columns": [], "n_output_columns": 0, "evidence": v["evidence"],
            **days_within(v["days"], end), "seen_within": seen_within(max(v["days"]), end),
            "last_observed": max(v["days"]),
        })
    n_named = collections.Counter(r["artifact_id"] for r in ref_rows if r["named"])
    art_rows = []
    for aid, a in sorted(arts.items()):
        row = {
            "artifact_id": aid, "kind": a["kind"], "name": a["name"],
            "owners": [a["name"]] if a["kind"] != "jetstream_experiment" else [],
            "n_metrics_named": n_named[aid], "n_metrics_total": len(a["metrics"]),
            "last_run": max(a["days"]),
            **{f"views_{w}d": sum(n for d, n in a["jobs"].items()
                                  if d >= str(window_start(w, end))) for w in WINDOWS},
            "note": f"{len(a['days'])} days with a query naming a metric",
        }
        e = exps.get(a["name"]) if a["kind"] == "jetstream_experiment" else None
        if e:
            row["name"] = e["normandy_slug"]
            row["url"] = MH.EXPERIMENTER.format(slug=e["experimenter_slug"] or e["normandy_slug"])
            row["friendly_name"] = (f"{e['app_name']}, {e['start_date'] or '?'} to "
                                    f"{e['end_date'] or 'now'}") if e["app_name"] else None
        art_rows.append(row)

    log("\nqueries by outcome:")
    for k, n in notes.most_common():
        log(f"  {n:7,}  {k}")
    by_kind = collections.Counter(a["kind"] for a in art_rows)
    unknown = sum(1 for a in art_rows if a["kind"] == "jetstream_experiment" and not a.get("url"))
    log(f"\n{len(ref_rows):,} refs over {len({(r['app'], r['metric']) for r in ref_rows}):,} "
        f"metrics from {by_kind['person']:,} people, "
        f"{by_kind['service_account']:,} service accounts and "
        f"{by_kind['jetstream_experiment']:,} Jetstream experiments "
        f"({unknown:,} not found in Experimenter)")
    return ref_rows, art_rows


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--project", action="append", help="read only this project (repeatable)")
    ap.add_argument("--days", type=int, default=LONGEST)
    args = ap.parse_args()
    if args.days > 180:
        raise SystemExit("JOBS_BY_PROJECT keeps 180 days")
    refs, arts = run(args.project or PROJECTS, args.days)
    if args.dry_run:
        return
    if args.project or args.days != LONGEST:
        raise SystemExit("--project and --days are for --dry-run; the tables hold "
                         f"every project over {LONGEST} days")
    suffix = ("Prototype table maintained by the docker-etl metric-usage job, metric_usage/adhoc.py; "
              "replaced on each run.")
    write_table("refs_adhoc", refs, ref_schema(),
                f"Metrics named by ad hoc BigQuery queries over {LONGEST} days, per "
                "account, from job history, with days per window. " + suffix,
                clustering=["app", "metric"])
    write_table("artifacts_adhoc", arts, artifact_schema(),
                "People, service accounts and Jetstream experiments whose ad hoc queries "
                "name a metric; views_<n>d is their jobs naming one. " + suffix,
                clustering=["artifact_id"])
    union_views()


if __name__ == "__main__":
    main()
