"""Metric references in ad hoc BigQuery queries, read from job history.

    python -m metric_usage.adhoc                          # capture new days, write tables
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
access can run it. JOBS_BY_PROJECT keeps JOBS_KEPT days, less than the longest
window (common.WINDOWS), so the job list is kept in the adhoc_jobs table, a
partition a day: each run adds the days JOBS_BY_PROJECT has whole that the
table does not (capture()), the first one all of them, and the refs are built
from the table over the longest window. Its partitions are kept for the
longest window and then dropped, since they hold accounts. A query whose last
run is older than JOBS_BY_PROJECT keeps the result it was resolved to while
its text was there, frozen: it cannot be resolved again. Each ref has its days
per window and seen_within, the shortest window with a query naming the
metric, and each account and experiment its jobs per window.

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
caches what it returns. The job list in adhoc_jobs has accounts and query keys
but no text. Each query's resolved refs are kept between runs in the
resolved_adhoc table (common.ResolvedStore), by query key and with the digest
of the columns it mentions, so a run fetches and resolves only the queries
that are new or whose columns changed.
The refs hold only metric ids from the inventory. resolved_adhoc also keeps
the tables and columns each query mentions (redash.dependencies()), limited
to those lineage has a shape for, so nothing there comes from the text alone.

Emails are stored and shown, since
monitoring_derived.jobs_by_organization_v1 already publishes user_email to the
same audience, and knowing who reads a metric is what lets somebody ask before
changing it.
"""

import argparse
import collections
import datetime
import re
import sys
import time

from .common import (
    LONGEST,
    WINDOWS,
    ResolvedStore,
    ResolveTimeout,
    append_table,
    artifact_schema,
    client,
    days_within,
    dest_table,
    log,
    pool_map,
    query,
    ref_schema,
    seen_within,
    time_limit,
    union_views,
    window_end,
    window_start,
    write_table,
)

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
DDL_TYPES = (
    "CREATE_VIEW",
    "CREATE_MATERIALIZED_VIEW",
    "CREATE_FUNCTION",
    "CREATE_TABLE_FUNCTION",
    "CREATE_PROCEDURE",
    "CREATE_SCHEMA",
    "CREATE_TABLE",
    "ALTER_TABLE",
    "ALTER_VIEW",
    "DROP_TABLE",
    "DROP_VIEW",
    "CREATE_ROW_ACCESS_POLICY",
)
# In every query this script runs, so a later run does not read its own jobs
MARKER = "bigquery-metric-lineage:adhoc"

# Days INFORMATION_SCHEMA.JOBS_BY_PROJECT keeps, back from the time of the query
JOBS_KEPT = 180
HISTORY = "adhoc_jobs"

RESOLVE_TIMEOUT = 60
BATCH_DAYS = 7
# Generated SQL past this (a pasted list of ids, a machine-built query) is
# left unparsed rather than given a worker for a minute
MAX_TEXT = 1_000_000

# Bumped when what a kept result means changes outside the resolver, which
# lineage.resolver_digest() covers
RESOLVED_VERSION = 2

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
    config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("tables", "STRING", sorted(tables))]
    )
    job = client().query(sql, job_config=config)
    out = {r["key"]: r["query"] for r in job.result(page_size=2000)}
    fetch_texts.bytes += job.total_bytes_processed or 0
    return out


def fetch_texts(project, start, end, keys):
    """{key: text} for the given query keys, in memory only."""
    from google.cloud import bigquery

    config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("keys", "STRING", sorted(keys))]
    )
    job = client().query(texts_sql(project, start, end), job_config=config)
    out = {r["key"]: r["query"] for r in job.result(page_size=2000)}
    fetch_texts.bytes += job.total_bytes_processed or 0
    return out


fetch_texts.bytes = 0


# ------------------------------------------------------------------ resolving


def _crashed(item):
    """_work()'s result for a query that killed its worker. It is kept like
    any other, so the query is not resolved again until the entry expires."""
    key, project, text = item
    return key, ({}, [], "resolve failed: worker died"), R.dependencies(text, project)


def _init():
    sys.setrecursionlimit(20000)


def _work(item):
    key, project, text = item
    deps = R.dependencies(text, project)
    if len(text) > MAX_TEXT:
        return key, ({}, [], "too long"), deps
    try:
        with time_limit(RESOLVE_TIMEOUT):
            named, stars, note = R.analyse(text, project)
    except ResolveTimeout:
        named, stars, note = {}, [], "timeout"
    except RecursionError:
        named, stars, note = {}, [], "resolve failed: RecursionError"
    return key, (named, stars, note), deps


def resolve_all(rows, end, store):
    """{(project, key): (named, stars, note)} for every query in rows, reusing
    what store holds for it.

    The rest are fetched and resolved a week at a time (text_batches()), so
    one week's text is in memory at once and the job history is scanned once.
    Only a query that ran in the newest week is redone for its age, since its
    text is read from that week anyway: one that stopped running keeps its
    result until it leaves the window or a column it names changes, rather
    than costing a scan of an older week. One whose last run JOBS_BY_PROJECT
    no longer has keeps what store holds, frozen, and is left out if it holds
    nothing.
    """
    first, _ = available(end)
    out, need, gone = {}, {}, 0
    newest = week_of(end.isoformat(), first)
    for k, day in last_days(rows).items():
        live = day >= first.isoformat()
        got = store.get(
            f"{k[0]}:{k[1]}", may_expire=live and week_of(day, first) == newest, frozen=not live
        )
        if got is not None:
            out[k] = got
        elif live:
            need[k] = day
        else:
            gone += 1
    log(
        f"  resolving {len(need):,} queries ({len(out):,} reused"
        + (f"; {gone:,} with no result and no text left to resolve" if gone else "")
        + ")"
    )
    if not need:
        return out
    R.L.persistent_udfs()  # parsed once here for the forked workers
    started, done = time.monotonic(), 0
    for project, b_start, keys, texts in text_batches(need, first, end):
        n = len(keys)
        if texts is None:
            # the text can no longer be read, so these keep what they were
            # resolved to, as a query whose text has rotated out does
            for key in keys:
                got = store.get(f"{project}:{key}", frozen=True)
                if got is not None:
                    out[(project, key)] = got
            done += n
            continue
        items = [(f"{project}:{key}", project, t) for key, t in texts.items()]
        del texts
        for key, res, deps in pool_map(
            _work, items, chunksize=16, initializer=_init, on_crash=_crashed
        ):
            out[tuple(key.split(":", 1))] = res
            # a timeout says how fast the resolver was, not what the SQL reads,
            # so it is tried again next run rather than kept
            if res[2] != "timeout":
                store.put(key, res, deps)
        del items
        done += n
        log(
            f"    {project} from {b_start}: {n:,} queries; {done:,} of {len(need):,} "
            f"after {(time.monotonic() - started) / 60:.1f} minutes"
        )
    return out


def last_days(rows):
    """{(project, key): the last day it ran} over the job list."""
    last = {}
    for r in rows:
        k = (r["project"], r["key"])
        if k not in last or r["day"] > last[k]:
            last[k] = r["day"]
    return last


def week_of(day, start):
    return (datetime.date.fromisoformat(day) - start).days // BATCH_DAYS


def text_batches(last, start, end):
    """(project, week start, query keys, {key: text}) a week at a time, each
    query's text from the last week it ran, so that a query still running is
    read from the newest week, which most of the queries a run needs share.
    The texts are None for a project the account can no longer read. last is
    last_days(), or the part of it to fetch."""
    from google.api_core.exceptions import Forbidden

    batches = collections.defaultdict(set)
    for (project, key), day in last.items():
        batches[(project, week_of(day, start))].add(key)
    for (project, week), keys in sorted(batches.items()):
        b_start = start + datetime.timedelta(days=week * BATCH_DAYS)
        b_end = min(end, b_start + datetime.timedelta(days=BATCH_DAYS - 1))
        try:
            texts = fetch_texts(project, b_start, b_end, keys)
        except Forbidden as e:
            log(
                f"    skipped the text of {project} from {b_start}: "
                f"{getattr(e, 'message', None) or str(e).splitlines()[0]}"
            )
            texts = None
        yield project, b_start, keys, texts


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
        return rest[len("enrollments_") :]
    cuts = [x.start() for x in SLUG_END_RE.finditer(rest)]
    if not cuts:
        return None
    return next((rest[:c] for c in reversed(cuts) if rest[:c] in known), rest[: cuts[0]])


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


def available(end):
    """(first, last) of the days JOBS_BY_PROJECT has whole: it keeps JOBS_KEPT
    days back from the time of the query, so the oldest of them is cut."""
    return end - datetime.timedelta(days=JOBS_KEPT - 2), end


def history_schema():
    from google.cloud.bigquery import SchemaField as F

    return [
        F("day", "DATE", description="Day the jobs ran, UTC"),
        F("project", "STRING", description="Project whose JOBS_BY_PROJECT listed them"),
        F(
            "key",
            "STRING",
            description="Query key (KEY_SQL); null marks a day captured "
            "with no jobs, so it is not fetched again",
        ),
        F("account", "STRING", description="Account that ran them, lowercased"),
        F(
            "wrote",
            "STRING",
            description="Destination table, project.dataset.table, unless it was an "
            "anonymous results table",
        ),
        F("jobs", "INT64", description="Jobs with this key, account and destination that day"),
    ]


def read_history(sql):
    from google.api_core.exceptions import NotFound

    try:
        return [dict(r) for r in client().query(sql).result()]
    except NotFound:
        return []


def capture(projects, known, save):
    """The job list for the days JOBS_BY_PROJECT has whole that adhoc_jobs
    does not, per project, appended to it when save. Rows as in job_rows()."""
    from google.api_core.exceptions import Forbidden

    first, last = available(window_end())
    have = collections.defaultdict(set)
    for r in read_history(
        f"SELECT DISTINCT project, day FROM `{dest_table(HISTORY)}` " f"WHERE day >= '{first}'"
    ):
        have[r["project"]].add(r["day"])
    days = [first + datetime.timedelta(days=i) for i in range((last - first).days + 1)]
    new = []
    for project in projects:
        missing = [d for d in days if d not in have[project]]
        # contiguous runs of missing days, one query each
        runs = []
        for d in missing:
            if runs and d == runs[-1][1] + datetime.timedelta(days=1):
                runs[-1][1] = d
            else:
                runs.append([d, d])
        for a, b in runs:
            try:
                got = [dict(r) for r in client().query(job_list_sql(project, a, b, known)).result()]
            except Forbidden as e:
                # only a project the account cannot read is skipped; any other
                # error stops the step, since a day not captured within
                # JOBS_BY_PROJECT's 180 days is lost for good
                log(f"  skipped {project}: {getattr(e, 'message', None) or str(e).splitlines()[0]}")
                break
            seen = {r["day"] for r in got}
            new += [dict(r, project=project, day=r["day"].isoformat()) for r in got]
            new += [
                {
                    "day": d.isoformat(),
                    "project": project,
                    "key": None,
                    "account": None,
                    "wrote": None,
                    "jobs": 0,
                }
                for d in (a + datetime.timedelta(days=i) for i in range((b - a).days + 1))
                if d not in seen
            ]
    n_days = len({(r["project"], r["day"]) for r in new})
    log(f"  captured {n_days:,} project-days of jobs from JOBS_BY_PROJECT")
    if save and new:
        append_table(
            HISTORY,
            new,
            history_schema(),
            "Ad hoc BigQuery jobs per (day, project, query key, account), kept "
            f"{LONGEST} days; no query text. Maintained by the docker-etl "
            "metric-usage job, metric_usage/adhoc.py.",
            partition="day",
            keep_days=LONGEST + 1,
        )
    return new


def job_rows(projects, start, end, save=False):
    """The job list, one row per (project, query key, account, day, table
    written), without jobs writing a bigquery-etl table: adhoc_jobs over the
    days from start to end, after capture() adds the days it lacks.

    Only jobs reading a table lineage has a shape for can name a metric, so
    the rest are dropped before any text is fetched: Jetstream reading its
    own tables in mozanalysis, metadata lookups, logs. That is most distinct
    queries. referenced_tables lists the base tables under any view, which is
    what the shapes are keyed on. A job with none recorded is kept, since that
    is also how a cache hit and a script's parent job look. A table lineage
    gets a shape for later is read only from the day it has one.
    """
    known = R.known_tables()
    bqetl = {
        r["artifact_id"]
        for r in query(
            f"SELECT artifact_id FROM `{dest_table('artifacts_bqetl')}` WHERE kind = 'bqetl_table'",
            name="bqetl tables",
            tables=["artifacts_bqetl"],
        )
    }
    listed = ", ".join(f"'{p}'" for p in projects)
    rows = [dict(r, day=r["day"].isoformat()) for r in read_history(f"""
SELECT day, project, key, account, wrote, jobs FROM `{dest_table(HISTORY)}`
WHERE key IS NOT NULL AND day BETWEEN '{start}' AND '{end}' AND project IN ({listed})""")]
    log(f"  {len(rows):,} job rows held in {dest_table(HISTORY)}")
    rows += [
        r
        for r in capture(projects, known, save)
        if r["key"] is not None and start.isoformat() <= r["day"] <= end.isoformat()
    ]
    # a job writing a bigquery-etl table is that table's own query
    return [r for r in rows if r["wrote"] not in bqetl]


def run(projects, days, save):
    """(refs, artifacts, store). save says whether adhoc_jobs and the store may
    be written: on a full run only. The store is written here only if resolving
    stops early."""
    start, end = window(days)
    log(f"  jobs from {start} to {end}")
    inventory = R.load_shapes()
    R.digests()  # computed once here for the forked workers
    store = ResolvedStore(
        "resolved_adhoc",
        f"{RESOLVED_VERSION}:{R.L.resolver_digest()}",
        R.dependency_digest,
        inventory,
        # only tables lineage knows, so no dotted name from people's text is stored
        keep_table=R.known_tables().__contains__,
    )
    rows = job_rows(projects, start, end, save)
    log(
        f"  {sum(r['jobs'] for r in rows):,} jobs, "
        f"{len({(r['project'], r['key']) for r in rows}):,} distinct queries, "
        f"{len({r['account'] for r in rows}):,} accounts"
    )

    try:
        results = resolve_all(rows, end, store)
    except BaseException:
        if save:
            store.save(complete=False)
        raise
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
        a = arts.setdefault(
            aid,
            {
                "kind": kind,
                "name": name,
                "jobs": collections.Counter(),
                "days": set(),
                "metrics": set(),
                "accounts": set(),
            },
        )
        a["jobs"][r["day"]] += r["jobs"]
        a["days"].add(r["day"])
        a["accounts"].add(r["account"])
        for m, (role, h, via, t) in named.items():
            a["metrics"].add(m)
            held = refs.get((aid, m))
            if held is None:
                refs[(aid, m)] = held = {
                    "days": set(),
                    "role": role,
                    "hops": h,
                    "via": via,
                    "transformed": t,
                    "evidence": "regex" if role == "unknown" else "parsed",
                }
            held["days"].add(r["day"])
            if ROLE_RANK[role] > ROLE_RANK[held["role"]]:
                held["role"] = role
                held["evidence"] = "parsed"
            # ties on hops take the smallest via, so a run picks the same one
            # whatever order BigQuery returns the job list in
            if (h, via or "") < (held["hops"], held["via"] or ""):
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
        ref_rows.append(
            {
                "app": r["app"],
                "metric": r["metric"],
                "artifact_id": aid,
                "artifact_kind": arts[aid]["kind"],
                "role": v["role"],
                "hops": hops,
                "named": named,
                "transformed": v["transformed"],
                "via": via,
                "output_columns": [],
                "n_output_columns": 0,
                "evidence": v["evidence"],
                **days_within(v["days"], end),
                "seen_within": seen_within(max(v["days"]), end),
                "last_observed": max(v["days"]),
            }
        )
    n_named = collections.Counter(r["artifact_id"] for r in ref_rows if r["named"])
    art_rows = []
    for aid, a in sorted(arts.items()):
        row = {
            "artifact_id": aid,
            "kind": a["kind"],
            "name": a["name"],
            "owners": [a["name"]] if a["kind"] != "jetstream_experiment" else [],
            "n_metrics_named": n_named[aid],
            "n_metrics_total": len(a["metrics"]),
            "last_run": max(a["days"]),
            **{
                f"views_{w}d": sum(
                    n for d, n in a["jobs"].items() if d >= str(window_start(w, end))
                )
                for w in WINDOWS
            },
            "note": f"{len(a['days'])} days with a query naming a metric",
        }
        e = exps.get(a["name"]) if a["kind"] == "jetstream_experiment" else None
        if e:
            row["name"] = e["normandy_slug"]
            row["url"] = MH.EXPERIMENTER.format(slug=e["experimenter_slug"] or e["normandy_slug"])
            row["friendly_name"] = (
                (f"{e['app_name']}, {e['start_date'] or '?'} to " f"{e['end_date'] or 'now'}")
                if e["app_name"]
                else None
            )
        art_rows.append(row)

    log("\nqueries by outcome:")
    for k, n in notes.most_common():
        log(f"  {n:7,}  {k}")
    by_kind = collections.Counter(a["kind"] for a in art_rows)
    unknown = sum(1 for a in art_rows if a["kind"] == "jetstream_experiment" and not a.get("url"))
    log(
        f"\n{len(ref_rows):,} refs over {len({(r['app'], r['metric']) for r in ref_rows}):,} "
        f"metrics from {by_kind['person']:,} people, "
        f"{by_kind['service_account']:,} service accounts and "
        f"{by_kind['jetstream_experiment']:,} Jetstream experiments "
        f"({unknown:,} not found in Experimenter)"
    )
    return ref_rows, art_rows, store


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--project", action="append", help="read only this project (repeatable)")
    ap.add_argument("--days", type=int, default=LONGEST)
    args = ap.parse_args()
    if args.days > LONGEST:
        raise SystemExit(f"the longest window is {LONGEST} days")
    if (args.project or args.days != LONGEST) and not args.dry_run:
        raise SystemExit(
            "--project and --days are for --dry-run; the tables hold "
            f"every project over {LONGEST} days"
        )
    # checked here rather than when the experiments are credited, after the
    # queries are resolved
    MH.require_hub()
    refs, arts, store = run(args.project or PROJECTS, args.days, save=not args.dry_run)
    if args.dry_run:
        return
    suffix = (
        "Maintained by the docker-etl metric-usage job, metric_usage/adhoc.py; "
        "replaced on each run."
    )
    write_table(
        "refs_adhoc",
        refs,
        ref_schema(),
        f"Metrics named by ad hoc BigQuery queries over {LONGEST} days, per "
        "account, from job history, with days per window. " + suffix,
        clustering=["app", "metric"],
    )
    write_table(
        "artifacts_adhoc",
        arts,
        artifact_schema(),
        "People, service accounts and Jetstream experiments whose ad hoc queries "
        "name a metric; views_<n>d is their jobs naming one. " + suffix,
        clustering=["artifact_id"],
    )
    union_views()
    store.save()


if __name__ == "__main__":
    main()
