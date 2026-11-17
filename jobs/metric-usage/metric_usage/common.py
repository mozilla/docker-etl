"""Shared helpers for the metric usage pipeline.

Everything is cached under .cache so a re-run only refetches what it is
told to. HTTP responses are keyed on the URL and query results on the SQL, so
editing a query refetches only that query. A query of this app's own tables
names them, and is cached until one of them is written again. Pass --force to a
script to ignore the cache, which is needed after the upstream data changes
underneath the same key.
"""

import collections
import contextlib
import csv
import datetime
import decimal
import hashlib
import json
import multiprocessing
import os
import pathlib
import re
import signal
import sys
import urllib.error
import urllib.request

# The job's directory, which holds the cache and the labels, outside the package
ROOT = pathlib.Path(__file__).resolve().parent.parent
CACHE = ROOT / ".cache"
# METRIC_USAGE_CACHE=0 keeps the cache in memory for the process instead of
# on disk, for a container that runs once, where it would only take up disk:
# within a step a value is still fetched once, but a later step fetches it
# again. The generated-sql and metric-hub clones live under .cache either way.
DISK_CACHE = os.environ.get("METRIC_USAGE_CACHE", "1") != "0"
_MEMORY = {}
# Hand-labelled accuracy samples. Not in git, since they hold snippets of query
# text; see write_labels().
LABELS = ROOT / "labels"

# Where the app's tables live, project.dataset. quick.query can read mozdata, and
# so can anybody with warehouse access, so this is also where the tables are
# useful outside the app. METRIC_USAGE_DATASET moves them.
DEST = os.environ.get("METRIC_USAGE_DATASET", "mozdata.analysis")
# METRIC_USAGE_PREFIX gives a working copy its own set of tables, so a branch
# can be rebuilt without touching the ones the deployed page reads. In the
# shared analysis dataset it has to be this job's own.
PREFIX = os.environ.get("METRIC_USAGE_PREFIX", "benwu_metrics_usage_")
if DEST == "mozdata.analysis" and not PREFIX.startswith("benwu_metrics_usage_"):
    raise SystemExit("METRIC_USAGE_PREFIX has to start with benwu_metrics_usage_ "
                     "in mozdata.analysis")

# The windows of use the page offers, in days, each ending yesterday, the last
# whole day. Sources read the longest once and count each window within it, so
# switching windows on the page needs no rebuild. Looker's System Activity keeps
# a year, which bounds the longest. Ad hoc queries go back only as far as
# adhoc.py's history does, since JOBS_BY_PROJECT keeps 180 days.
WINDOWS = (30, 90, 180, 365)
LONGEST = max(WINDOWS)

PROBEINFO = "https://probeinfo.telemetry.mozilla.org"


def log(*args):
    print(*args, file=sys.stderr)


def run_deep(main):
    """Run main in a thread with a 1 GiB stack and a raised recursion limit.

    sqlglot trees for the larger queries (clients_daily_v6 and its long AND and
    CONCAT chains) are deeper than the default recursion limit allows, and a
    raised limit needs a thread with a bigger stack to go with it. An exception
    in a thread only prints, and a SystemExit there is dropped, so either is
    raised again here for the process to exit with it.
    """
    import threading

    sys.setrecursionlimit(200000)
    threading.stack_size(1024 * 1024 * 1024)
    failed = []

    def target():
        try:
            main()
        except BaseException as e:
            failed.append(e)

    t = threading.Thread(target=target)
    t.start()
    t.join()
    if failed:
        raise failed[0]


class ResolveTimeout(BaseException):
    # not an Exception, so parse and resolve error handling cannot swallow it
    pass


def _alarm(signum, frame):
    raise ResolveTimeout()


@contextlib.contextmanager
def time_limit(seconds):
    """Raise ResolveTimeout in the block after seconds. Uses SIGALRM, so only
    in a process's main thread, as in a pool_map() worker."""
    signal.signal(signal.SIGALRM, _alarm)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)


def _run_chunk(work, chunk):
    return [work(item) for item in chunk]


def pool_map(work, items, chunksize=1, initializer=None):
    """work(item) for each of items in forked worker processes, yielded as each
    chunk finishes.

    Forked, so the workers share the shapes loaded before. multiprocessing.Pool
    waits forever on a task whose worker died (killed for memory, a crash in
    C), so this uses ProcessPoolExecutor, which raises BrokenProcessPool.
    """
    from concurrent.futures import ProcessPoolExecutor, as_completed

    ctx = multiprocessing.get_context("fork")
    with ProcessPoolExecutor(mp_context=ctx, initializer=initializer) as pool:
        futures = [pool.submit(_run_chunk, work, items[i:i + chunksize])
                   for i in range(0, len(items), chunksize)]
        for f in as_completed(futures):
            yield from f.result()


def utc_today():
    """Today in UTC, the day BigQuery's DATE(timestamp) and CURRENT_DATE()
    use, so a run on a machine in another timezone ends its windows on the
    same day as one in the container."""
    return datetime.datetime.now(datetime.timezone.utc).date()


def window_end():
    return utc_today() - datetime.timedelta(days=1)


def window_start(days, end):
    return end - datetime.timedelta(days=days - 1)


def seen_within(last, end):
    """The shortest window that includes the day last, or None."""
    if last is None:
        return None
    last = datetime.date.fromisoformat(str(last))
    return next((w for w in WINDOWS if last >= window_start(w, end)), None)


def days_within(days, end):
    """{observed_days_<n>d: how many of days fall in each window}."""
    days = [datetime.date.fromisoformat(str(d)) for d in days]
    return {f"observed_days_{w}d": sum(1 for d in days if d >= window_start(w, end))
            for w in WINDOWS}


def window_sql(day, end, **aggs):
    """Select-list items with one column per window and aggregate, over rows
    whose DATE expression day is in the longest window. Each aggregate is a
    template with {c} for the window's condition, as in
    views="SUM(IF({c}, views, 0))", which gives views_30d, views_90d and so on."""
    conds = {w: f"{day} >= '{window_start(w, end)}'" for w in WINDOWS}
    return ",\n  ".join(f"{agg.format(c=conds[w])} AS {name}_{w}d"
                        for name, agg in aggs.items() for w in WINDOWS)


def _cache_file(kind, key):
    digest = hashlib.sha256(key.encode()).hexdigest()[:16]
    return CACHE / f"{kind}_{digest}.json"


def write_atomic(path, data):
    """Write through a temporary file, so an interrupted run never leaves a
    half-written cache entry that later runs would trust."""
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(data)
    tmp.replace(path)


def read_cache(path):
    """The bytes cached at path, or None. In memory with METRIC_USAGE_CACHE=0,
    where bytes rather than values are kept, so each read is a fresh copy as
    it is from disk."""
    if not DISK_CACHE:
        return _MEMORY.get(path)
    return path.read_bytes() if path.exists() else None


def write_cache(path, data):
    """Cache bytes at path, atomically on disk or in memory."""
    if not DISK_CACHE:
        _MEMORY[path] = data
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    write_atomic(path, data)


def get_json(url, force=False):
    """GET a URL as JSON, cached by URL. A body that is not JSON raises before
    it is cached."""
    path = _cache_file("http", url)
    held = None if force else read_cache(path)
    if held is not None:
        return json.loads(held)
    req = urllib.request.Request(url, headers={"Accept-Encoding": "identity"})
    with urllib.request.urlopen(req, timeout=120) as r:
        body = r.read()
    value = json.loads(body)
    write_cache(path, body)
    return value


def get_json_if_found(url, force=False):
    """get_json(), or None when the URL is not found. Any other error raises,
    so a step does not replace its tables with what it could not fetch."""
    try:
        return get_json(url, force)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def label_copies(name):
    """labels/<name>.csv and its timestamped copies, oldest first, with the
    working copy last since that is the one edited by hand."""
    copies = sorted(p for p in LABELS.glob(f"{name}_*.csv")
                    if re.fullmatch(rf"{re.escape(name)}_\d{{14}}", p.stem))
    path = LABELS / f"{name}.csv"
    return copies + ([path] if path.exists() else [])


def held_labels(name, key):
    """{key(row): (verdict, note)} from every copy of labels/<name>.csv.

    A later copy wins, a cleared verdict included, so a label holds until it is
    changed, and a row that drops out of a run keeps its label in the copies
    for when it comes back.
    """
    held = {}
    for path in label_copies(name):
        with open(path, newline="") as f:
            for r in csv.DictReader(f):
                held[key(r)] = (r.get("verdict") or "", r.get("note") or "")
    return {k: v for k, v in held.items() if v[0]}


def write_labels(name, rows, fieldnames):
    """Write labels/<name>.csv, the working copy to label, and the same rows to
    labels/<name>_<YYYYMMDDHHMMSS>.csv, so no run can lose a label.

    A working copy labelled since the last run is kept first under the time it
    was saved, or labels on rows this run leaves out would go with it.
    """
    LABELS.mkdir(parents=True, exist_ok=True)
    path = LABELS / f"{name}.csv"
    copies = label_copies(name)[:-1] if path.exists() else []
    if path.exists() and (not copies or copies[-1].read_bytes() != path.read_bytes()):
        saved = datetime.datetime.fromtimestamp(path.stat().st_mtime)
        kept = LABELS / f"{name}_{saved.strftime('%Y%m%d%H%M%S')}.csv"
        if not kept.exists():
            kept.write_bytes(path.read_bytes())
    stamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    for p in (LABELS / f"{name}_{stamp}.csv", path):
        with open(p, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(rows)
    return path


_CLIENT = None


def client():
    global _CLIENT
    if _CLIENT is None:
        from google.cloud import bigquery

        _CLIENT = bigquery.Client()
    return _CLIENT


def _jsonable(v):
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, datetime.datetime):
        return v.isoformat(sep=" ")
    if isinstance(v, datetime.date):
        return v.isoformat()
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    if isinstance(v, dict):
        return {k: _jsonable(x) for k, x in v.items()}
    return str(v)


def table_versions(tables):
    """When each DEST.PREFIX<table> was last written, as one string. A view
    counts too: every source recreates the refs and artifacts views after it
    writes its tables."""
    return "|".join(f"{t}@{client().get_table(f'{DEST}.{PREFIX}{t}').modified.isoformat()}"
                    for t in tables)


def query(sql, force=False, name="query", tables=()):
    """Rows of a query as dicts, cached by the SQL text.

    tables names the DEST.PREFIX tables the query reads, which are rewritten
    in place under the same SQL: the result is then cached until one of them
    is written again, and the copy for an older version is removed.
    """
    if tables:
        stem = f"bq_{hashlib.sha256(sql.encode()).hexdigest()[:16]}"
        version = hashlib.sha256(table_versions(tables).encode()).hexdigest()[:8]
        path = CACHE / f"{stem}_{version}.json"
    else:
        path = _cache_file("bq", sql)
    held = None if force else read_cache(path)
    if held is not None:
        rows = json.loads(held)
        log(f"  {name}: {len(rows):,} rows (cached)")
        return rows
    rows = [{k: _jsonable(v) for k, v in dict(r).items()}
            for r in client().query(sql).result()]
    write_cache(path, json.dumps(rows).encode())
    if tables and DISK_CACHE:
        for old in CACHE.glob(f"{stem}_*.json"):
            if old != path:
                old.unlink()
    log(f"  {name}: {len(rows):,} rows")
    return rows


def write_table(name, rows, schema, description, clustering=None):
    """Replace DEST.PREFIX<name> with rows.

    schema is a list of bigquery.SchemaField. The table is replaced wholesale on
    every run. One created in a dataset with a default expiration (analysis has
    180 days) takes it from when it was created, and a rewrite does not renew it.
    """
    from google.cloud import bigquery

    table_id = f"{DEST}.{PREFIX}{name}"
    config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        clustering_fields=clustering,
    )
    job = client().load_table_from_json(rows, table_id, job_config=config)
    job.result()
    table = client().get_table(table_id)
    table.description = description
    client().update_table(table, ["description"])
    log(f"  wrote {table_id}: {table.num_rows:,} rows")


def append_table(name, rows, schema, description, partition, keep_days):
    """Append rows to DEST.PREFIX<name>, creating it partitioned by day on the
    DATE field partition, with each partition kept keep_days days."""
    from google.cloud import bigquery

    table_id = f"{DEST}.{PREFIX}{name}"
    config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        time_partitioning=bigquery.TimePartitioning(
            field=partition, expiration_ms=keep_days * 24 * 3600 * 1000),
    )
    client().load_table_from_json(rows, table_id, job_config=config).result()
    table = client().get_table(table_id)
    if table.description != description:
        table.description = description
        client().update_table(table, ["description"])
    log(f"  appended {len(rows):,} rows to {table_id}")


class ResolvedStore:
    """Resolved queries kept between runs in DEST.PREFIX<name>, since the job
    runs with no disk that outlives it.

    An entry is a query's named refs, by app and metric, with what they were
    resolved against: the resolver's digest (lineage.resolver_digest()) and,
    for every table the query names, the columns of it the query mentions and
    their digest (redash.dependencies()). It is reused while those match, so a
    run resolves only the queries that are new or whose columns changed. An
    entry is also redone after MIN_AGE to MAX_AGE days, spread by its key, to
    bound what a missed dependency could leave stale. A frozen entry is the
    exception: its text is gone, so it is kept whatever resolver version it
    was resolved with, and a change to the resolver does not drop it. Only a
    query's key and schema names are kept, never its text. save() keeps the
    entries this run used, so queries that left the window drop out.
    """

    MIN_AGE, MAX_AGE = 15, 30

    def __init__(self, name, version, digest_of, inventory):
        """digest_of(table, names) is the current digest of a table's
        columns, redash.dependency_digest()."""
        self.name, self.version, self.digest_of = name, version, digest_of
        self.today = utc_today()
        self.inventory = inventory
        self.ids = {(r["app"], r["metric"]): i for i, r in enumerate(inventory)}
        self.held, self.kept = {}, {}
        from google.api_core.exceptions import NotFound

        # every version is read, since a frozen entry is kept across versions;
        # get() checks the version of the rest
        try:
            for r in client().query(f"SELECT * FROM `{DEST}.{PREFIX}{name}`").result():
                self.held[r["key"]] = dict(r)
        except NotFound:
            pass
        old = sum(r["version"] != version for r in self.held.values())
        log(f"  {len(self.held):,} resolved queries held in {PREFIX}{name}, "
            f"{old:,} from an earlier resolver version")

    def get(self, key, may_expire=True, frozen=False):
        """(named refs by metric id, star tables, note) for key, or None when
        it has to be resolved again. may_expire=False keeps an entry past its
        age, for a caller that would pay more than a resolve to redo it.
        frozen=True is for a query that cannot be resolved again, its text
        being gone: the entry is kept as it was resolved, whatever changed
        since (the resolver included), less any metric no longer in the
        inventory."""
        r = self.held.get(key)
        if r is None:
            return None
        if not frozen:
            if r["version"] != self.version:
                return None
            lifetime = self.MIN_AGE + int(hashlib.sha256(key.encode()).hexdigest(), 16) % (
                self.MAX_AGE - self.MIN_AGE + 1)
            age = (self.today - datetime.date.fromisoformat(str(r["resolved_on"]))).days
            if may_expire and age >= lifetime:
                return None
            if any(self.digest_of(d["table"], d["names"]) != d["digest"] for d in r["deps"]):
                return None
        named = {}
        for x in r["refs"]:
            m = self.ids.get((x["app"], x["metric"]))
            if m is None:
                if frozen:
                    continue
                return None
            named[m] = (x["role"], x["hops"], x["via"], x["transformed"])
        self.kept[key] = r
        return named, list(r["stars"]), r["note"]

    def put(self, key, result, deps):
        """Keep a result just resolved, with deps, {table: column names} for
        the tables its query names."""
        named, stars, note = result
        self.kept[key] = {
            "key": key, "version": self.version, "resolved_on": self.today.isoformat(),
            "deps": [{"table": t, "names": names, "digest": self.digest_of(t, names)}
                     for t, names in sorted(deps.items())],
            "refs": [{"app": self.inventory[m]["app"], "metric": self.inventory[m]["metric"],
                      "role": role, "hops": h, "via": via, "transformed": t}
                     for m, (role, h, via, t) in named.items()],
            "stars": list(stars), "note": note,
        }

    def save(self, complete=True):
        """Write the entries back. After a run that stopped early, those it did
        not get to are kept too, for the rerun to check."""
        from google.cloud.bigquery import SchemaField as F

        rows = self.kept if complete else {**self.held, **self.kept}
        for r in rows.values():
            r["resolved_on"] = str(r["resolved_on"])
        s, rep = "STRING", "REPEATED"
        schema = [
            F("key", s), F("version", s, description="lineage.resolver_digest() it was resolved with"),
            F("resolved_on", "DATE"),
            F("deps", "RECORD", mode=rep, fields=[F("table", s), F("names", s, mode=rep),
                                                  F("digest", s)],
              description="Each table the query names, the columns of it the query "
              "mentions, and their digest then"),
            F("refs", "RECORD", mode=rep, fields=[
                F("app", s), F("metric", s), F("role", s), F("hops", "INT64"), F("via", s),
                F("transformed", "BOOL")]),
            F("stars", s, mode=rep), F("note", s),
        ]
        write_table(self.name, list(rows.values()), schema,
                    "Resolved queries the docker-etl metric-usage job keeps between runs, by "
                    "query key; no query text. Prototype table, replaced on each run.",
                    clustering=["key"])


def snakecase(s):
    """stringcase.snakecase, which is what the Glean Dictionary uses.

    Separators become underscores and an uppercase letter after the first
    character gets an underscore in front of it. Glean identifiers are almost
    always lowercase already, so in practice this is "dots to underscores".
    """
    s = re.sub(r"[\-\.\s]", "_", str(s))
    if not s:
        return s
    return s[0].lower() + re.sub(r"[A-Z]", lambda m: "_" + m.group(0).lower(), s[1:])


# ------------------------------------------------------------- shared schemas
#
# Every source writes refs_<source> and artifacts_<source> with these schemas,
# and union_views() puts a refs and an artifacts view over all of them, which is
# what the page reads. A field a source has nothing for is left null.


def ref_schema():
    from google.cloud.bigquery import SchemaField as F

    s, b, i = "STRING", "BOOL", "INT64"
    return [
        F("app", s), F("metric", s),
        F("artifact_id", s), F("artifact_kind", s),
        F("role", s, description="output: reaches an output column; filter: named "
          "but only used in WHERE, JOIN, GROUP BY or an aggregate that drops it; "
          "viewed: opened in a tool such as GLAM"),
        F("hops", i, description="Tables between the stable table and this artifact, plus one"),
        F("named", b, description="The SQL names the metric, rather than carrying it "
          "through a star or a whole struct"),
        F("transformed", b),
        F("via", s, description="table:column the artifact read it from"),
        F("output_columns", s, mode="REPEATED"),
        F("n_output_columns", i),
        F("evidence", s, description="parsed, regex, or observed (from use, not code)"),
        F("observed_days", i, description="Days in the source's own window with a use, "
          "for GLAM, whose window is a year; windowed sources use observed_days_<n>d"),
        *[F(f"observed_days_{w}d", i, description=f"Days in the {w} days ending on the "
            "window end with a use") for w in WINDOWS],
        F("seen_within", i, description="For a ref that exists only because of use (an "
          "ad hoc query, an experiment, a Looker query, an OpMon project), the shortest "
          f"window of {', '.join(map(str, WINDOWS))} days that has it; null for refs from "
          "code or saved content, which hold in every window"),
        F("last_observed", "DATE"),
        F("top_percent", "FLOAT64", description="Percent of the metrics used in the same "
          "place (a GLAM product) that were used on at least as many days; 1 is the top 1%"),
    ]


def artifact_schema():
    from google.cloud.bigquery import SchemaField as F

    s, b, i, rep = "STRING", "BOOL", "INT64", "REPEATED"
    return [
        F("artifact_id", s), F("kind", s), F("name", s), F("url", s),
        F("owners", s, mode=rep), F("friendly_name", s), F("generated_by", s),
        F("note", s), F("n_metrics_named", i), F("n_metrics_total", i),
        F("is_draft", b), F("is_scheduled", b),
        F("last_run", "DATE"), F("updated_at", "DATE"),
        F("last_used", "DATE", description="Last day in the longest window somebody "
          "viewed or ran it: a Redash view, a Looker run"),
        *[f for w in WINDOWS for f in (
            F(f"views_{w}d", i, description=f"Views, runs or jobs in the {w} days ending on "
              "the window end"),
            F(f"users_{w}d", i, description=f"Distinct people or accounts behind views_{w}d"))],
        F("parent_ids", s, mode=rep, description="For a Redash query, the dashboards showing it"),
        F("child_ids", s, mode=rep, description="For a Redash dashboard, its queries on BigQuery"),
        F("star_tables", s, mode=rep, description="Tables it reads with SELECT *"),
    ]


def union_views():
    """refs and artifacts views over every refs_* and artifacts_* table.

    A column added to a schema is null for the sources not rebuilt since, so
    adding one does not mean rebuilding every source first.
    """
    columns = collections.defaultdict(set)
    for r in client().query(f"""
SELECT table_name, column_name FROM `{DEST}`.INFORMATION_SCHEMA.COLUMNS
WHERE table_name IN (
  SELECT table_name FROM `{DEST}`.INFORMATION_SCHEMA.TABLES
  WHERE table_type = 'BASE TABLE'
    AND (STARTS_WITH(table_name, '{PREFIX}refs_')
         OR STARTS_WITH(table_name, '{PREFIX}artifacts_')))
""").result():
        columns[r["table_name"]].add(r["column_name"])
    for kind, fields in (("refs", ref_schema()), ("artifacts", artifact_schema())):
        # exactly refs_<source> / artifacts_<source>, so a scratch copy such
        # as refs_bqetl_prev is not read
        parts = sorted(t for t in columns
                       if re.fullmatch(rf"{PREFIX}{kind}_[a-z0-9]+", t))
        if not parts:
            continue

        def col(f, have):
            if f.name in have:
                return f"`{f.name}`"
            t = f"ARRAY<{f.field_type}>" if f.mode == "REPEATED" else f.field_type
            return f"CAST(NULL AS {t}) AS `{f.name}`"

        body = "\nUNION ALL\n".join(
            f"SELECT {', '.join(col(f, columns[t]) for f in fields)} FROM `{DEST}.{t}`"
            for t in parts)
        client().query(f"CREATE OR REPLACE VIEW `{DEST}.{PREFIX}{kind}` AS\n{body}").result()
        log(f"  view {DEST}.{PREFIX}{kind} over {', '.join(parts)}")
