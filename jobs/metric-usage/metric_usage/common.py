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
import math
import multiprocessing
import os
import pathlib
import re
import signal
import sys
import tempfile
import time
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

# Where the tables live, project.dataset. The deployed ones are in a
# bigquery-etl dataset, which defines them (metadata.yaml and schema.yaml) and
# the views over them, among them the user-facing views in metric_usage_tracking
# that the page reads through mozdata. bigquery-etl does not create or alter
# tables without a query, so this job creates them on its first run and its
# schemas are the ones that hold; bigquery-etl's schema.yaml has to match them.
# METRIC_USAGE_DATASET moves the tables, for a working copy.
BQETL_DATASET = "moz-fx-data-shared-prod.metric_usage_tracking_derived"
DEST = os.environ.get("METRIC_USAGE_DATASET", BQETL_DATASET)
# In bigquery-etl's dataset, its metadata.yaml sets each table's description and
# its view.sql files define the refs and artifacts views, so the job sets
# neither there.
MANAGED = DEST == BQETL_DATASET
# METRIC_USAGE_PREFIX gives a working copy its own set of tables, so a branch
# can be rebuilt without touching the ones the deployed page reads. In the
# shared analysis dataset it has to be this job's own.
PREFIX = os.environ.get("METRIC_USAGE_PREFIX", "")
if MANAGED and PREFIX:
    raise SystemExit(f"METRIC_USAGE_PREFIX has to be empty in {BQETL_DATASET}")
if DEST == "mozdata.analysis" and not PREFIX.startswith("benwu_metrics_usage_"):
    raise SystemExit(
        "METRIC_USAGE_PREFIX has to start with benwu_metrics_usage_ in mozdata.analysis"
    )


def dest_table(name):
    """The full id of one of the job's tables or views, by its short name such
    as refs_redash. The version suffix is bigquery-etl's naming."""
    return f"{DEST}.{PREFIX}{name}_v1"


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


def worker_count():
    """Worker processes for pool_map(): METRIC_USAGE_WORKERS if set, else the
    CPUs this process may run on, capped by the cgroup CPU quota. os.cpu_count()
    counts the node's cores, which in a pod with a CPU limit is far too many."""
    if os.environ.get("METRIC_USAGE_WORKERS"):
        return max(1, int(os.environ["METRIC_USAGE_WORKERS"]))
    n = len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else os.cpu_count()
    n = n or 1
    # cgroup v2, then v1; "max" or -1 is no quota
    for quota_file, period_file in (
        ("/sys/fs/cgroup/cpu.max", None),
        ("/sys/fs/cgroup/cpu/cpu.cfs_quota_us", "/sys/fs/cgroup/cpu/cpu.cfs_period_us"),
    ):
        try:
            if period_file is None:
                quota, period = pathlib.Path(quota_file).read_text().split()[:2]
            else:
                quota = pathlib.Path(quota_file).read_text().strip()
                period = pathlib.Path(period_file).read_text().strip()
        except (OSError, ValueError):
            continue
        if quota not in ("max", "-1"):
            n = min(n, max(1, math.ceil(int(quota) / int(period))))
        break
    return n


def _run_chunk(work, chunk, marks):
    """[(index, work(item))] for a chunk of (index, item), noting in marks the
    index of the item this worker is on, so that pool_map() can tell which
    items were running when a worker died."""
    mark = os.path.join(marks, str(os.getpid()))
    out = []
    for i, item in chunk:
        # written aside and renamed into place, since a worker the pool kills
        # after another died could otherwise leave the mark empty
        with open(f"{mark}.tmp", "w") as f:
            f.write(str(i))
        os.replace(f"{mark}.tmp", mark)
        out.append((i, work(item)))
    return out


def _marks(marks):
    """The item index in each worker's mark. A worker killed mid-write leaves
    only a .tmp file; its item is run again with the rest."""
    out = set()
    for p in pathlib.Path(marks).iterdir():
        if p.suffix != ".tmp" and p.read_text().isdigit():
            out.add(int(p.read_text()))
    return out


def pool_map(work, items, chunksize=1, initializer=None, on_crash=None):
    """work(item) for each of items in forked worker processes, yielded as each
    chunk finishes, in no particular order.

    Forked, so the workers share the shapes loaded before. multiprocessing.Pool
    waits forever on a task whose worker died (killed for memory, a crash in
    C), so this uses ProcessPoolExecutor, which raises BrokenProcessPool.

    When a worker dies, the items that were running then are each run again
    alone, and the rest again in a new pool. An item that kills its worker
    when run alone yields on_crash(item), so one bad query does not fail every
    run that meets it; with no on_crash, BrokenProcessPool is raised.
    """
    from concurrent.futures import ProcessPoolExecutor, as_completed
    from concurrent.futures.process import BrokenProcessPool

    ctx = multiprocessing.get_context("fork")
    todo = list(enumerate(items))
    while todo:
        done = set()
        with tempfile.TemporaryDirectory() as marks:
            broke = False
            with ProcessPoolExecutor(
                max_workers=worker_count(), mp_context=ctx, initializer=initializer
            ) as pool:
                futures = [
                    pool.submit(_run_chunk, work, todo[i : i + chunksize], marks)
                    for i in range(0, len(todo), chunksize)
                ]
                for f in as_completed(futures):
                    try:
                        got = f.result()
                    except BrokenProcessPool:
                        broke = True
                        continue
                    for i, res in got:
                        done.add(i)
                        yield res
            if not broke:
                return
            running = _marks(marks) - done
        if not done and not running:
            # a worker died before starting any item, as in its initializer;
            # running the pool again would do the same
            raise BrokenProcessPool("a worker died before starting any item")
        log(
            f"  a worker died; running {len(running)} items alone, then the other "
            f"{len(todo) - len(done) - len(running):,} again"
        )
        for i in sorted(running):
            with ProcessPoolExecutor(1, mp_context=ctx, initializer=initializer) as one:
                try:
                    res = one.submit(work, items[i]).result()
                except BrokenProcessPool:
                    if on_crash is None:
                        raise
                    log(f"  item {i} killed its worker when run alone; recorded as failed")
                    res = on_crash(items[i])
            done.add(i)
            yield res
        todo = [(i, item) for i, item in todo if i not in done]


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
    return {
        f"observed_days_{w}d": sum(1 for d in days if d >= window_start(w, end)) for w in WINDOWS
    }


def window_sql(day, end, **aggs):
    """Select-list items with one column per window and aggregate, over rows
    whose DATE expression day is in the longest window. Each aggregate is a
    template with {c} for the window's condition, as in
    views="SUM(IF({c}, views, 0))", which gives views_30d, views_90d and so on."""
    conds = {w: f"{day} >= '{window_start(w, end)}'" for w in WINDOWS}
    return ",\n  ".join(
        f"{agg.format(c=conds[w])} AS {name}_{w}d" for name, agg in aggs.items() for w in WINDOWS
    )


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


# seconds before the second and third attempts at a URL
GET_RETRY_DELAYS = (5, 20)


def _transient(e):
    """Whether a failed GET may succeed if tried again: a timeout, a dropped
    connection, a 429 or a 5xx. A 404 or other 4xx will not."""
    if isinstance(e, urllib.error.HTTPError):
        return e.code == 429 or e.code >= 500
    return isinstance(e, (urllib.error.URLError, TimeoutError, ConnectionError))


def get_json(url, force=False):
    """GET a URL as JSON, cached by URL, retrying a transient failure. A body
    that is not JSON raises before it is cached."""
    path = _cache_file("http", url)
    held = None if force else read_cache(path)
    if held is not None:
        return json.loads(held)
    req = urllib.request.Request(url, headers={"Accept-Encoding": "identity"})
    for delay in GET_RETRY_DELAYS + (None,):
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                body = r.read()
            break
        except Exception as e:
            if delay is None or not _transient(e):
                raise
            log(f"  {url}: {type(e).__name__}, trying again in {delay}s")
            time.sleep(delay)
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
    copies = sorted(
        p
        for p in LABELS.glob(f"{name}_*.csv")
        if re.fullmatch(rf"{re.escape(name)}_\d{{14}}", p.stem)
    )
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
    """When each of the job's tables was last written, as one string. The refs
    and artifacts views stand for the source tables under them, since a view
    bigquery-etl defines is not recreated when they are written. A working copy
    can lack some sources."""
    from google.api_core.exceptions import NotFound

    parts = []
    for t in tables:
        if t not in ("refs", "artifacts"):
            parts.append(f"{t}@{client().get_table(dest_table(t)).modified.isoformat()}")
            continue
        for s in SOURCES:
            try:
                parts.append(
                    f"{t}_{s}@{client().get_table(dest_table(f'{t}_{s}')).modified.isoformat()}"
                )
            except NotFound:
                pass
    return "|".join(parts)


def query(sql, force=False, name="query", tables=()):
    """Rows of a query as dicts, cached by the SQL text.

    tables names the job's tables the query reads, by short name (see
    dest_table()), which are rewritten in place under the same SQL: the result
    is then cached until one of them is written again, and the copy for an
    older version is removed.
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
    rows = [{k: _jsonable(v) for k, v in dict(r).items()} for r in client().query(sql).result()]
    write_cache(path, json.dumps(rows).encode())
    if tables and DISK_CACHE:
        for old in CACHE.glob(f"{stem}_*.json"):
            if old != path:
                old.unlink()
    log(f"  {name}: {len(rows):,} rows")
    return rows


def describe(table_id, description):
    """Set a table's description, outside bigquery-etl's dataset, where its
    metadata.yaml does. Returns the table."""
    t = client().get_table(table_id)
    if not MANAGED and t.description != description:
        t.description = description
        t = client().update_table(t, ["description"])
    return t


def write_table(name, rows, schema, description, clustering=None):
    """Replace dest_table(name) with rows.

    schema is a list of bigquery.SchemaField. The table is replaced wholesale on
    every run, keeping its labels and description. One created in a dataset with
    a default expiration (analysis has 180 days) takes it from when it was
    created, and a rewrite does not renew it.
    """
    from google.cloud import bigquery

    table_id = dest_table(name)
    config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        clustering_fields=clustering,
    )
    job = client().load_table_from_json(rows, table_id, job_config=config)
    job.result()
    t = describe(table_id, description)
    log(f"  wrote {table_id}: {t.num_rows:,} rows")


def write_query(name, sql, description, columns, clustering=None):
    """Replace dest_table(name) with the result of sql. A query job writing to the
    table rather than CREATE OR REPLACE TABLE, which would drop the labels and
    description bigquery-etl gives it.

    columns is {column name: description}. A query result has none, and
    bigquery-etl does not deploy the schema of a table without a query, so they
    are set here in every dataset, after each write.
    """
    from google.cloud import bigquery

    table_id = dest_table(name)
    config = bigquery.QueryJobConfig(
        destination=table_id,
        write_disposition="WRITE_TRUNCATE",
        clustering_fields=clustering,
    )
    client().query(sql, job_config=config).result()
    t = describe(table_id, description)
    missing = [f.name for f in t.schema if f.name not in columns]
    if missing:
        log(f"  no description for {', '.join(missing)} in {table_id}")
    t.schema = [
        bigquery.SchemaField.from_api_repr(
            {**f.to_api_repr(), "description": columns.get(f.name, f.description)}
        )
        for f in t.schema
    ]
    t = client().update_table(t, ["schema"])
    log(f"  wrote {table_id}: {t.num_rows:,} rows")


def append_table(name, rows, schema, description, partition, keep_days):
    """Append rows to dest_table(name), creating it partitioned by day on the
    DATE field partition, with each partition kept keep_days days."""
    from google.cloud import bigquery

    table_id = dest_table(name)
    config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        time_partitioning=bigquery.TimePartitioning(
            field=partition, expiration_ms=keep_days * 24 * 3600 * 1000
        ),
    )
    client().load_table_from_json(rows, table_id, job_config=config).result()
    describe(table_id, description)
    log(f"  appended {len(rows):,} rows to {table_id}")


class ResolvedStore:
    """Resolved queries kept between runs in dest_table(name), since the job
    runs with no disk that outlives it.

    An entry is a query's named refs, by app and metric, with what they were
    resolved against: the resolver's digest (lineage.resolver_digest()) and,
    for every known table the query names, the columns of it the query
    mentions and their digest (redash.dependencies()). It is reused while those match, so a
    run resolves only the queries that are new or whose columns changed. An
    entry is also redone after MIN_AGE to MAX_AGE days, spread by its key, to
    bound what a missed dependency could leave stale. A frozen entry is the
    exception: its text is gone, so it is kept whatever resolver version it
    was resolved with, and a change to the resolver does not drop it. A
    query's key and the known tables and columns it mentions are kept, never
    its text.
    save() keeps the entries this run used, so queries that left the window
    drop out.
    """

    MIN_AGE, MAX_AGE = 15, 30

    def __init__(self, name, version, digest_of, inventory, keep_table=None):
        """digest_of(table, names) is the current digest of a table's
        columns, redash.dependency_digest(). keep_table(table), if given, says
        which deps may be stored: the others are dropped from what is held and
        what is put, so a table written before a dep was filtered loses it on
        the next complete save, frozen entries included."""
        self.name, self.version, self.digest_of = name, version, digest_of
        self.keep_table = keep_table or (lambda t: True)
        self.today = utc_today()
        self.inventory = inventory
        self.ids = {(r["app"], r["metric"]): i for i, r in enumerate(inventory)}
        self.held, self.kept = {}, {}
        from google.api_core.exceptions import NotFound

        # every version is read, since a frozen entry is kept across versions;
        # get() checks the version of the rest
        try:
            for r in client().query(f"SELECT * FROM `{dest_table(name)}`").result():
                r = dict(r)
                r["deps"] = [d for d in r["deps"] if self.keep_table(d["table"])]
                self.held[r["key"]] = r
        except NotFound:
            pass
        old = sum(r["version"] != version for r in self.held.values())
        log(
            f"  {len(self.held):,} resolved queries held in {dest_table(name)}, "
            f"{old:,} from an earlier resolver version"
        )

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
                self.MAX_AGE - self.MIN_AGE + 1
            )
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
            "key": key,
            "version": self.version,
            "resolved_on": self.today.isoformat(),
            "deps": [
                {"table": t, "names": names, "digest": self.digest_of(t, names)}
                for t, names in sorted(deps.items())
                if self.keep_table(t)
            ],
            "refs": [
                {
                    "app": self.inventory[m]["app"],
                    "metric": self.inventory[m]["metric"],
                    "role": role,
                    "hops": h,
                    "via": via,
                    "transformed": t,
                }
                for m, (role, h, via, t) in named.items()
            ],
            "stars": list(stars),
            "note": note,
        }

    def save(self, complete=True):
        """Write the entries back. After a run that stopped early, those it did
        not get to are kept too, for the rerun to check."""
        rows = self.kept if complete else {**self.held, **self.kept}
        for r in rows.values():
            r["resolved_on"] = str(r["resolved_on"])
        write_table(
            self.name,
            list(rows.values()),
            resolved_schema(),
            "Resolved queries the docker-etl metric-usage job keeps between runs, by "
            "query key; no query text. Replaced on each run.",
            clustering=["key"],
        )


def resolved_schema():
    from google.cloud.bigquery import SchemaField as F

    s, rep = "STRING", "REPEATED"
    return [
        F(
            "key",
            s,
            description="Query key (adhoc.KEY_SQL): BigQuery's literal-normalized hash for a "
            "service account's query, else a fingerprint of the query text",
        ),
        F("version", s, description="lineage.resolver_digest() it was resolved with"),
        F("resolved_on", "DATE", description="Day the query was last resolved"),
        F(
            "deps",
            "RECORD",
            mode=rep,
            fields=[
                F("table", s, description="A known table the query names, project.dataset.table"),
                F("names", s, mode=rep, description="Columns of the table the query mentions"),
                F(
                    "digest",
                    s,
                    description="Digest of those columns' lineage when the query was resolved",
                ),
            ],
            description="Each table the query names, the columns of it the query "
            "mentions, and their digest then",
        ),
        F(
            "refs",
            "RECORD",
            mode=rep,
            fields=[
                F("app", s, description="App of a metric the query names"),
                F("metric", s, description="Metric name"),
                F("role", s, description="As refs.role"),
                F("hops", "INT64", description="As refs.hops"),
                F("via", s, description="table:column the query read it from"),
                F("transformed", "BOOL", description="As refs.transformed"),
            ],
            description="The metrics the query names",
        ),
        F("stars", s, mode=rep, description="Tables the query reads with SELECT *"),
        F("note", s, description="How the query was read, as artifacts.note"),
    ]


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
# and a refs and an artifacts view over all of them is what the page reads. In
# bigquery-etl's dataset the views are its refs_v1 and artifacts_v1, which name
# each source's table; elsewhere union_views() makes them. A field a source has
# nothing for is left null.

# The sources writing refs_<source> and artifacts_<source>. A new one has to be
# added to bigquery-etl's refs_v1 and artifacts_v1 views as well.
SOURCES = ("bqetl", "redash", "looker", "glam", "metrichub", "adhoc")


def ref_schema():
    from google.cloud.bigquery import SchemaField as F

    s, b, i = "STRING", "BOOL", "INT64"
    return [
        F("app", s, description="App of the metric, as in metrics"),
        F("metric", s, description="Metric name, as in metrics"),
        F("artifact_id", s, description="The artifact using the metric; joins to artifacts"),
        F("artifact_kind", s, description="The artifact's kind, as artifacts.kind"),
        F(
            "role",
            s,
            description="output: reaches an output column; filter: named "
            "but only used in WHERE, JOIN, GROUP BY, ORDER BY, a window or an aggregate "
            "that drops it; "
            "viewed: opened in a tool such as GLAM",
        ),
        F("hops", i, description="Tables between the stable table and this artifact, plus one"),
        F(
            "named",
            b,
            description="The SQL names the metric, rather than carrying it "
            "through a star or a whole struct",
        ),
        F(
            "transformed",
            b,
            description="The value was computed from, cast or aggregated on the way here "
            "rather than copied as it is",
        ),
        F("via", s, description="table:column the artifact read it from"),
        F(
            "output_columns",
            s,
            mode="REPEATED",
            description="Up to 20 of the artifact's output columns carrying the metric",
        ),
        F("n_output_columns", i, description="How many of its output columns carry the metric"),
        F("evidence", s, description="parsed, regex, or observed (from use, not code)"),
        F(
            "observed_days",
            i,
            description="Days in the source's own window with a use, "
            "for GLAM, whose window is a year; windowed sources use observed_days_<n>d",
        ),
        *[
            F(
                f"observed_days_{w}d",
                i,
                description=f"Days in the {w} days ending on the window end with a use",
            )
            for w in WINDOWS
        ],
        F(
            "seen_within",
            i,
            description="For a ref that exists only because of use (an "
            "ad hoc query, an experiment, a Looker query, an OpMon project), the shortest "
            f"window of {', '.join(map(str, WINDOWS))} days that has it; null for refs from "
            "code or saved content, which hold in every window",
        ),
        F("last_observed", "DATE", description="Last day with a use, for refs from use"),
        F(
            "top_percent",
            "FLOAT64",
            description="Percent of the metrics used in the same "
            "place (a GLAM product) that were used on at least as many days; 1 is the top 1%",
        ),
    ]


def artifact_schema():
    from google.cloud.bigquery import SchemaField as F

    s, b, i, rep = "STRING", "BOOL", "INT64", "REPEATED"
    return [
        F(
            "artifact_id",
            s,
            description="project.dataset.table for bigquery-etl tables and views; otherwise "
            "prefixed by source, as in redash:query:<id>, looker:dashboard:<id>, "
            "looker:explore:<model>.<explore>, glam:<product>:<probe>, "
            "metric-hub:<platform>.<metric>, opmon:<slug>, jetstream:<slug> or adhoc:<account>",
        ),
        F(
            "kind",
            s,
            description="bqetl_table, bqetl_view, glean_ping_view, redash_query, "
            "redash_dashboard, looker_dashboard, looker_look, looker_explore, glam_metric, "
            "metric_hub_metric, opmon_project, jetstream_experiment, person or service_account",
        ),
        F("name", s, description="Title, dataset.table, slug or account, by kind"),
        F("url", s, description="Where to see it: the code, the dashboard or the experiment"),
        F("owners", s, mode=rep, description="Owner emails, or the account itself for ad hoc"),
        F(
            "friendly_name",
            s,
            description="bigquery-etl's friendly_name, metric-hub's, or an experiment's app "
            "and dates",
        ),
        F(
            "generated_by",
            s,
            description="For a bigquery-etl artifact, the bqetl generator that wrote it",
        ),
        F(
            "note",
            s,
            description="How its SQL was read (ok, partial: N scopes unread, resolve failed, "
            "or the fallback used), or a short note on the artifact",
        ),
        F("n_metrics_named", i, description="Metrics it names"),
        F("n_metrics_total", i, description="Metrics it names or carries in bulk"),
        F("is_draft", b, description="For Redash, the query or dashboard is a draft"),
        F(
            "is_scheduled",
            b,
            description="Runs on a schedule: a Redash refresh, a Looker schedule, or an "
            "Airflow write to a bigquery-etl table",
        ),
        F("last_run", "DATE", description="Last day it ran or was written"),
        F("updated_at", "DATE", description="For a Redash query, the last day it was edited"),
        F(
            "last_used",
            "DATE",
            description="Last day in the longest window somebody "
            "viewed or ran it: a Redash view, a Looker run",
        ),
        *[
            f
            for w in WINDOWS
            for f in (
                F(
                    f"views_{w}d",
                    i,
                    description=f"Views, runs or jobs in the {w} days ending on the window end",
                ),
                F(f"users_{w}d", i, description=f"Distinct people or accounts behind views_{w}d"),
            )
        ],
        F("parent_ids", s, mode=rep, description="For a Redash query, the dashboards showing it"),
        F("child_ids", s, mode=rep, description="For a Redash dashboard, its queries on BigQuery"),
        F("star_tables", s, mode=rep, description="Tables it reads with SELECT *"),
    ]


def union_views():
    """refs and artifacts views over every refs_* and artifacts_* table, for a
    working copy; bigquery-etl defines them in its dataset.

    A column added to a schema is null for the sources not rebuilt since, so
    adding one does not mean rebuilding every source first.
    """
    if MANAGED:
        return
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
        # exactly refs_<source>_v1 / artifacts_<source>_v1, so a scratch copy
        # such as refs_bqetl_prev_v1 is not read
        parts = sorted(t for t in columns if re.fullmatch(rf"{PREFIX}{kind}_[a-z0-9]+_v1", t))
        if not parts:
            continue

        def col(f, have):
            if f.name in have:
                return f"`{f.name}`"
            t = f"ARRAY<{f.field_type}>" if f.mode == "REPEATED" else f.field_type
            return f"CAST(NULL AS {t}) AS `{f.name}`"

        body = "\nUNION ALL\n".join(
            f"SELECT {', '.join(col(f, columns[t]) for f in fields)} FROM `{DEST}.{t}`"
            for t in parts
        )
        client().query(f"CREATE OR REPLACE VIEW `{dest_table(kind)}` AS\n{body}").result()
        log(f"  view {dest_table(kind)} over {', '.join(parts)}")
