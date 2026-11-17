"""Metric references in Looker dashboards, Looks and explores, through the API.

    python -m metric_usage.looker              # fetch, generate SQL, resolve, write tables
    python -m metric_usage.looker --dry-run    # everything but writing
    python -m metric_usage.looker --measure    # fetch content and usage and report sizes only
    python -m metric_usage.looker --no-exposure  # skip the per-explore exposure pass

Content and usage come from the Looker API and System
Activity, read as the API key's user with the key from the system keyring
(looker_api.py); only read-only calls are made (run_inline_query is a POST
but changes nothing). The SQL Looker would run for each query is
generated without running it and read by the same resolver as Redash queries,
so a Looker field reaches a metric through lineage like any other query.

Queries are grouped by what decides their columns: model, explore, fields,
the fields they filter on, and custom fields. Filter values are left out of
the group and the SQL, since they change with every dashboard load, except
that a filter on an event identity column (events_stream.event_category) is
an event literal: the group's SQL selects its filter fields too, so their
shapes say which ones carry event identity, and each query's own values on
those are matched as events.

API responses and generated SQL are cached under .cache/looker_api,
System Activity for the day and SQL by group until --refresh-sql. They hold
filter values people typed, so like Redash text they are never written to
BigQuery or the site.
"""

import argparse
import collections
import concurrent.futures
import datetime
import hashlib
import json
import re
import sys
import threading
import time

from .common import (
    CACHE,
    DEST,
    LONGEST,
    PREFIX,
    WINDOWS,
    ResolveTimeout,
    artifact_schema,
    client,
    log,
    pool_map,
    read_cache,
    ref_schema,
    seen_within,
    time_limit,
    union_views,
    utc_today,
    window_end,
    window_start,
    write_cache,
    write_table,
)

from . import lineage as L
from . import looker_api
from . import metric_hub as MH

API_CACHE = CACHE / "looker_api"
LOOKER = looker_api.BASE_URL
PROJECT = "mozdata"
SQL_THREADS = 8
# Seconds one statement may take to resolve. A few Looker statements send the
# resolver into exponential work; they are recorded as timeouts and listed.
RESOLVE_TIMEOUT = 60

# Looker's own usage data, and projects that hold no data models
SKIP_MODELS = {"system__activity"}
# lookml-generator's model for OpMon projects; see opmon_query_refs()
OPMON_MODEL = "operational_monitoring"
SKIP_PROJECT_RE = re.compile(r"^(marketplace_extension_|private-test$)")

# Runs that are somebody exploring rather than a saved dashboard or Look
ADHOC_SOURCES = {"explore", "api4", "api3", "suggest", "drill_modal", "sql_runner"}

# Errors about the query itself, so a group or explore that gets one is not
# asked again. The SDK turns every failure into the same SDKError with no
# status code, so anything else (a timeout, a reset connection, a 429 or 5xx
# page) is taken as passing: retried, and left out of the cache if it persists.
PERMANENT_ERROR_RE = re.compile(
    "|".join(
        [
            r"^Model Not Found",
            r"^View Not Found",
            r"^Not found$",
            r"^Must query at least one dimension or measure",
            r"Period-over-period measure",
            r"incompatible with the selected period-over-period",
            r"^The filter .* is not allowed",
            r"compute distinct value",
            r"without a primary key",
            r"cannot be calculated",
            r'^Field "[^"]+" for custom',
            r"^Invalid expression",
            r'^Invalid "filter_expression"',
            r"^Variable not found",
        ]
    )
)
RETRY_DELAYS = (5, 20)  # seconds before the second and third attempts
# Share of groups, or of exposure chunks, whose SQL may fail for passing
# reasons before the step stops rather than replace its tables. Past this the
# Looker API was likely down for part of the run, and the refs it would write
# would show metrics as unused that are not.
MAX_TRANSIENT = 0.01

_LOCAL = threading.local()


def sdk():
    # one client per thread; looker_sdk's session is not shared across threads
    if getattr(_LOCAL, "sdk", None) is None:
        _LOCAL.sdk = looker_api.sdk()
    return _LOCAL.sdk


def _cached(kind, key, fetch, daily=True, refresh=False):
    """fetch() cached by key (common.read_cache()); daily keys expire at
    midnight. A value marked transient is returned but not cached."""
    if daily:
        key = f"{utc_today()}:{key}"
    path = API_CACHE / f"{kind}_{hashlib.sha256(key.encode()).hexdigest()[:16]}.json"
    held = None if refresh else read_cache(path)
    if held is not None:
        return json.loads(held)
    value = fetch()
    if not (isinstance(value, dict) and value.get("transient")):
        write_cache(path, json.dumps(value).encode())
    return value


def check_transient(n_failed, n_total, what):
    """Stop the step when more than MAX_TRANSIENT of n_total failed for
    passing reasons, so the tables keep the last complete run."""
    if n_total and n_failed / n_total > MAX_TRANSIENT:
        raise SystemExit(
            f"{n_failed:,} of {n_total:,} {what} failed for passing reasons, "
            f"over {MAX_TRANSIENT:.0%}; not writing. Rerun once Looker recovers."
        )


def first_line(e, width):
    return str(e).strip().splitlines()[0][:width] if str(e).strip() else type(e).__name__


def is_permanent(error):
    return bool(PERMANENT_ERROR_RE.search(re.sub(r"^message:\s*", "", error)))


def with_retries(call, errors, width):
    """{"value": call()}, or {"error": ...} once an error is permanent or the
    retries run out, then marked transient so it is not cached."""
    for delay in RETRY_DELAYS + (None,):
        try:
            return {"value": call()}
        except errors as e:
            err = first_line(e, width)
        if is_permanent(err):
            return {"error": err}
        if delay is not None:
            time.sleep(delay)
    return {"error": err, "transient": True}


def api_user_id():
    if getattr(_LOCAL, "me", None) is None:
        _LOCAL.me = sdk().me(fields="id").id
    return _LOCAL.me


def system_activity(view, fields, filters=None, sorts=None, limit=500000):
    """Rows of a System Activity query, cached for the day.

    History leaves out API runs by the user whose key this is: the SQL this
    pipeline asks Looker for is logged as an api4 run, and would come back on
    the next run as ad hoc queries naming every metric an explore exposes.
    Bigeye's lineage crawl is left out for the same reason, as in adhoc.py:
    it runs queries selecting every field of the metrics and use_counters
    explores, whose field lists System Activity cuts off.
    """
    from looker_sdk import error as E
    from looker_sdk import models40 as M

    expr = None
    if view == "history":
        expr = (
            f'NOT (${{user.id}} = {api_user_id()} AND ${{history.source}} = "api4") '
            'AND NOT contains(${user.email}, "bigeye")'
        )

    def run():
        q = M.WriteQuery(
            model="system__activity",
            view=view,
            fields=fields,
            filters=filters or {},
            filter_expression=expr,
            sorts=sorts,
            limit=str(limit),
        )
        return json.loads(sdk().run_inline_query("json", q))

    def fetch():
        # retried like generated SQL, but nothing here can be done without it,
        # so an error that persists stops the step
        got = with_retries(run, E.SDKError, 200)
        if "error" in got:
            raise RuntimeError(f"System Activity {view}: {got['error']}")
        return got["value"]

    rows = _cached(
        "sa", json.dumps([view, fields, filters, expr, sorts, limit], sort_keys=True), fetch
    )
    if len(rows) >= limit:
        raise RuntimeError(f"System Activity {view} hit the row limit {limit}")
    return rows


def model_projects():
    def fetch():
        return {m.name: m.project_name for m in sdk().all_lookml_models(fields="name,project_name")}

    return _cached("models", "all", fetch)


# ------------------------------------------------------------------ fetching

QUERY_FIELDS = [
    "query.id",
    "query.model",
    "query.view",
    "query.fields",
    "query.filters",
    "query.dynamic_fields",
]


def window(days, end):
    """System Activity's filter for the days ending on end; Looker's "to"
    leaves out the day it ends on."""
    stop = end + datetime.timedelta(days=1)
    return {"history.created_date": f"{window_start(days, end)} to {stop}"}


def history(end=None):
    """Query runs over the longest window, one row per (query, dashboard, Look,
    source), with the last day each ran there."""
    return system_activity(
        "history",
        QUERY_FIELDS
        + [
            "history.dashboard_id",
            "history.look_id",
            "history.source",
            "history.query_run_count",
            "user.count",
            "history.most_recent_query_date",
        ],
        dict(window(LONGEST, end or window_end()), **{"query.id": "NOT NULL"}),
    )


def content():
    """(dashboard tiles, Looks) that exist now, whether or not they ran."""
    tiles = system_activity(
        "dashboard",
        QUERY_FIELDS
        + [
            "dashboard.id",
            "dashboard.title",
            "dashboard.lookml_link_id",
            "user.email",
            "dashboard_element.id",
            "look.id",
        ],
        {"dashboard.deleted_date": "NULL"},
    )
    looks = system_activity(
        "look",
        QUERY_FIELDS
        + [
            "look.id",
            "look.title",
            "user.email",
        ],
        {"look.deleted_date": "NULL"},
    )
    return tiles, looks


def usage(days, end):
    """Runs, users and last use per dashboard, Look and ad hoc explore in one
    window."""
    when = window(days, end)
    # history.dashboard_id is a string field, which a NOT NULL filter matches
    # nothing on; dashboard.id is the number
    dash = system_activity(
        "history",
        [
            "dashboard.id",
            "history.dashboard_run_count",
            "history.query_run_count",
            "user.count",
            "history.most_recent_query_date",
        ],
        dict(when, **{"dashboard.id": "NOT NULL"}),
    )
    # LookML dashboards run as history.dashboard_id 'model::name'
    lookml_dash = system_activity(
        "history",
        [
            "history.dashboard_id",
            "history.dashboard_run_count",
            "history.query_run_count",
            "user.count",
            "history.most_recent_query_date",
        ],
        dict(when, **{"history.dashboard_id": "%::%"}),
    )
    look = system_activity(
        "history",
        [
            "history.look_id",
            "history.query_run_count",
            "user.count",
            "history.most_recent_query_date",
        ],
        dict(when, **{"history.look_id": "NOT NULL"}),
    )
    explore = system_activity(
        "history",
        [
            "query.model",
            "query.view",
            "history.query_run_count",
            "user.count",
            "history.most_recent_query_date",
        ],
        dict(
            when,
            **{
                "history.source": ",".join(sorted(ADHOC_SOURCES)),
                "history.dashboard_id": "NULL",
                "history.look_id": "NULL",
            },
        ),
    )
    for r in lookml_dash:
        r["dashboard.id"] = r.pop("history.dashboard_id")
    return dash + lookml_dash, look, explore


def lookml_dashboards():
    """{id: title} for LookML dashboards ('model::name'), which System Activity's
    dashboard explore does not list."""

    def fetch():
        return {
            d.id: d.title
            for d in sdk().all_dashboards(fields="id,title")
            if d.id and "::" in str(d.id)
        }

    return _cached("lookml_dashboards", "all", fetch)


# ------------------------------------------------------------------ exposure
#
# What each explore lets somebody query, whether or not anybody has: the SQL
# for a query selecting every visible field, in chunks, resolved like any
# other. A metric a field names directly is exposed; a field that is a whole
# struct or JSON blob exposes nothing in it, as everywhere else.

EXPOSURE_CHUNK = 400


def explores_in_scope():
    """[(model, explore)] for visible explores in models in scope."""

    def fetch():
        return [
            {
                "name": m.name,
                "project": m.project_name,
                "explores": [
                    {"name": e.name, "hidden": bool(e.hidden)} for e in (m.explores or [])
                ],
            }
            for m in sdk().all_lookml_models(fields="name,project_name,explores")
        ]

    out = []
    for m in _cached("models_explores", "all", fetch):
        if m["name"] in SKIP_MODELS or SKIP_PROJECT_RE.search(m["project"] or ""):
            continue
        out += [(m["name"], e["name"]) for e in m["explores"] if not e["hidden"]]
    return out


def explore_fields(model, explore):
    """Visible [name, "dimension" | "measure"] of an explore, or {"error": ...}."""

    def fetch():
        got = with_retries(
            lambda: sdk()
            .lookml_model_explore(
                model,
                explore,
                fields="fields(dimensions(name,hidden,category),measures(name,hidden,category))",
            )
            .fields,
            Exception,
            120,
        )
        if "error" in got:
            return got
        f = got["value"]
        # category is an enum in looker_sdk
        out = []
        for d in (f.dimensions or []) + (f.measures or []):
            cat = getattr(d.category, "value", d.category)
            if not d.hidden and cat in ("dimension", "measure"):
                out.append([d.name, cat])
        return out

    return _cached("explore_fields_v4", f"{model}.{explore}", fetch)


def chunk_sql(model, explore, fields, failed, skipped):
    """[(key, sql)] covering fields, splitting a chunk Looker rejects in half
    until the field it cannot select is found; that field goes in failed. A
    chunk that fails for a passing reason is not split but left for the next
    run, in skipped."""
    key = json.dumps([model, explore, sorted(fields), ""])
    got = generate_sql(key)
    if "sql" in got:
        return [(key, got["sql"])]
    if got.get("transient"):
        skipped.append((model, explore, len(fields), got["error"]))
        return []
    if len(fields) == 1:
        failed.append((model, explore, fields[0], got["error"]))
        return []
    mid = len(fields) // 2
    return chunk_sql(model, explore, fields[:mid], failed, skipped) + chunk_sql(
        model, explore, fields[mid:], failed, skipped
    )


def exposure():
    """({(model, explore): [group keys]}, [(key, sql)], failed fields)."""
    explores = explores_in_scope()
    with concurrent.futures.ThreadPoolExecutor(SQL_THREADS) as pool:
        fields = dict(zip(explores, pool.map(lambda me: explore_fields(*me), explores)))
    errors = [me for me, f in fields.items() if isinstance(f, dict)]
    transient = [me for me in errors if fields[me].get("transient")]
    jobs = []
    for me, fs in fields.items():
        if isinstance(fs, dict) or not fs:
            continue
        # Dimensions go together. Measures go one view at a time: measures from
        # several fanned-out joins (the hub's unnested labeled counters) make
        # Looker write a multi-grain query, 35M characters for 400 fields.
        dims = sorted(n for n, c in fs if c == "dimension")
        jobs += [(me, dims[i : i + EXPOSURE_CHUNK]) for i in range(0, len(dims), EXPOSURE_CHUNK)]
        by_view = collections.defaultdict(list)
        for n, c in fs:
            if c == "measure":
                by_view[n.split(".", 1)[0]].append(n)
        for ms in by_view.values():
            ms.sort()
            jobs += [(me, ms[i : i + EXPOSURE_CHUNK]) for i in range(0, len(ms), EXPOSURE_CHUNK)]
    failed, skipped, keys, items = [], [], collections.defaultdict(list), []
    with concurrent.futures.ThreadPoolExecutor(SQL_THREADS) as pool:
        futures = {pool.submit(chunk_sql, me[0], me[1], fs, failed, skipped): me for me, fs in jobs}
        for f in concurrent.futures.as_completed(futures):
            for key, sql in f.result():
                keys[futures[f]].append(key)
                items.append((key, sql))
    log(
        f"  exposure: {len(explores):,} visible explores, {len(errors)} without metadata, "
        f"{sum(len(f) for f in fields.values() if isinstance(f, list)):,} fields in "
        f"{len(items):,} chunks, {len(failed):,} fields Looker would not select"
    )
    if skipped or transient:
        log(
            f"  exposure: {len(transient)} explores' metadata and {len(skipped)} chunks "
            f"({sum(n for _, _, n, _ in skipped):,} fields) failed for passing reasons; "
            f"asked again next run"
        )
    check_transient(len(transient), len(explores), "explores' metadata")
    check_transient(len(skipped), len(jobs), "exposure chunks")
    return keys, items, failed


# ------------------------------------------------------------------ grouping


def _json(v, default):
    if v in (None, ""):
        return default
    return json.loads(v) if isinstance(v, str) else v


def query_of(r):
    """The query on a System Activity row, or None when System Activity cut
    off its field or filter list, which leaves JSON that does not parse."""
    try:
        fields, filters = _json(r["query.fields"], []), _json(r["query.filters"], {})
    except json.JSONDecodeError:
        return None
    return {
        "id": r["query.id"],
        "model": r["query.model"],
        "view": r["query.view"],
        "fields": fields,
        "filters": filters,
        "dynamic_fields": r["query.dynamic_fields"] or "",
    }


def group_key(q):
    """What decides a query's SQL columns. Filter fields are selected too, so
    the SQL shows what each filter is applied to."""
    fields = sorted(set(q["fields"]) | set(q["filters"]))
    return json.dumps([q["model"], q["view"], fields, q["dynamic_fields"]])


def alias(field):
    """The column Looker gives a field in its SQL: view.field becomes view_field."""
    return field.replace(".", "_").lower()


# ------------------------------------------------------------- SQL from Looker


def generate_sql(key, refresh=False):
    """{"sql": ...} or {"error": ...} for a group, generated by Looker without
    running it. Errors about the query are kept, so a group that fails (a
    model or explore since removed, a query of table calculations only) is not
    asked again; other errors are retried, and asked again next run."""
    from looker_sdk import error as E
    from looker_sdk import models40 as M

    model, view, fields, dyn = json.loads(key)

    def run(fs):
        q = M.WriteQuery(model=model, view=view, fields=fs, dynamic_fields=dyn or None, limit="1")
        return sdk().run_inline_query("sql", q)

    def fetch():
        got = with_retries(lambda: run(fields), E.SDKError, 200)
        return {"sql": got.pop("value")} if "value" in got else got

    return _cached("sql", key, fetch, daily=False, refresh=refresh)


# Looker answers a query from an aggregate table when one holds its fields,
# and says so in the SQL: `-- use existing <explore>::<rollup> in
# `mozdata.looker_tmp.LR_...``. The table name changes with every rebuild.
ROLLUP_RE = re.compile(r"--\s*use existing \S+::\S+ in `[^`]*looker_tmp\.", re.I)
# extra dimensions to try per group before giving up on reaching its base tables
MAX_BREAKER_TRIES = 20


def ensure_shapes():
    global _SHAPES, _EVENTS
    if _SHAPES is None:
        inventory, _SHAPES = L.shapes_from_tables()
        _EVENTS = L.event_index(inventory)


def unroll(sqls, refresh=False):
    """{group key: SQL against the base tables} for the groups Looker answers
    from an aggregate table.

    An aggregate table is a query on the same explore, built ahead of time, so
    a group reading one depends on what its fields read in the base tables.
    The API cannot turn aggregate awareness off, and the aggregate table's own
    SQL is not in the generated SQL, but a query with a dimension the table
    does not hold cannot use it. So each such group's SQL is generated again
    with one more dimension, picked so that alone it names no metric and adds
    nothing to the group's refs. The group's filter fields are among its
    fields, so filters resolve against the base tables as well.
    """
    ensure_shapes()
    out, good = {}, {}
    for key, v in sorted(sqls.items()):
        if "sql" not in v or not ROLLUP_RE.search(v["sql"]):
            continue
        model, view, fields, dyn = json.loads(key)
        meta = explore_fields(model, view)
        if not isinstance(meta, list):
            continue
        dims = sorted(n for n, c in meta if c == "dimension" and n not in fields)
        if good.get((model, view)) in dims:
            dims.remove(good[(model, view)])
            dims.insert(0, good[(model, view)])
        for extra in dims[:MAX_BREAKER_TRIES]:
            alone = generate_sql(json.dumps([model, view, [extra], ""]), refresh)
            if "sql" not in alone or "looker_tmp" in alone["sql"].lower():
                continue
            named, _, note = resolve_group(alone["sql"])
            if named or note != "ok":
                continue
            got = generate_sql(json.dumps([model, view, sorted(fields + [extra]), dyn]), refresh)
            if "sql" in got and "looker_tmp" not in got["sql"].lower():
                out[key] = got["sql"]
                good[(model, view)] = extra
                break
    return out


def effective_sqls(sqls, refresh=False):
    """{group key: the SQL to read for it}: the generated SQL, or for a group
    answered from an aggregate table, the same fields against the base tables.
    looker.py and accuracy.py both read groups through this."""
    unrolled = unroll(sqls, refresh)
    return {k: unrolled.get(k, v["sql"]) for k, v in sqls.items() if "sql" in v}, len(unrolled)


def generate_all(keys, refresh=False):
    out = {}
    with concurrent.futures.ThreadPoolExecutor(SQL_THREADS) as pool:
        futures = {pool.submit(generate_sql, k, refresh): k for k in keys}
        for n, f in enumerate(concurrent.futures.as_completed(futures), 1):
            out[futures[f]] = f.result()
            if n % 500 == 0:
                log(f"    SQL for {n:,} of {len(keys):,} groups")
    return out


# ----------------------------------------------------------------- resolving
#
# Forked workers read the shapes built before the fork, as in redash.py.

_SHAPES = None
_EVENTS = None


def resolve_group(sql):
    """(named refs, filter field idents, note) for one group's SQL.

    named maps a metric id to (role, hops, via, transformed). idents maps an
    output column to {app: [(kind, hops, via)]} for the event identity it holds.
    """
    prelude, tree, note = L.parse_script(sql)
    if tree is None:
        return {}, {}, note or "unparsed"
    try:
        out, touched, _, unread = L.resolve_script(
            prelude, tree, _SHAPES, PROJECT, all_selects=True
        )
    except Exception as e:
        return {}, {}, f"resolve failed: {type(e).__name__}"
    idents = {}
    for name, shape in out.items():
        found = collections.defaultdict(list)
        for k, (h, via, t, n) in L.flatten(shape).items():
            if isinstance(k, tuple) and k[0] == "ident":
                found[k[1]].append((k[2], h, via))
        if found:
            idents[name.lower()] = dict(found)
    out = L.resolve_events_in(out, _EVENTS)
    touched = L.resolve_events(touched, _EVENTS)
    in_output = {m for _, leaf in L.output_paths(out) for m in leaf}
    named = {
        m: ("output" if m in in_output else "filter", h, via, t)
        for m, (h, via, t, n) in touched.items()
        if isinstance(m, int) and n
    }
    return named, idents, L.partial_note(unread)


def _work(item):
    key, sql = item
    start = time.monotonic()
    try:
        with time_limit(RESOLVE_TIMEOUT):
            res = resolve_group(sql)
    except ResolveTimeout:
        res = ({}, {}, "timeout")
    except RecursionError:
        res = ({}, {}, "resolve failed: RecursionError")
    return key, res, time.monotonic() - start


# Bumped when what a cached result means changes. 2: metric ids are positions
# in the inventory ordered by app and metric, which it was not before.
RESOLVED_VERSION = 2


def snapshot():
    """Identifies the lineage the shapes come from: resolved results are only
    reused while the metrics and carries tables are the ones they were made with."""
    return "|".join(
        [str(RESOLVED_VERSION)]
        + [str(client().get_table(f"{DEST}.{PREFIX}{t}").modified) for t in ("metrics", "carries")]
    )


def resolve_all(items):
    """{group key: (named, idents, note)}, reusing results for SQL already
    resolved against the same lineage snapshot."""
    fp = snapshot()
    path = API_CACHE / f"resolved_{hashlib.sha256(fp.encode()).hexdigest()[:16]}.json"
    held = json.loads(read_cache(path) or b"{}")
    digest = {k: hashlib.sha256(sql.encode()).hexdigest() for k, sql in items}
    out, todo = {}, []
    for k, sql in items:
        h = held.get(digest[k])
        # a timeout says how fast the resolver was, not what the SQL reads,
        # so it is tried again rather than reused
        if h is None or h[2] == "timeout":
            todo.append((k, sql))
        else:
            out[k] = ({int(m): tuple(v) for m, v in h[0].items()}, h[1], h[2])
    log(f"  resolving {len(todo):,} groups ({len(out):,} reused from this lineage snapshot)")
    timings = []
    # parsed once here for the forked workers, rather than in each one inside
    # its first item's time limit
    L.persistent_udfs()
    # what was resolved is kept even if a worker dies, so a rerun picks up there
    try:
        for n, (key, res, secs) in enumerate(pool_map(_work, todo, initializer=_init), 1):
            out[key] = res
            held[digest[key]] = [{str(m): list(v) for m, v in res[0].items()}, res[1], res[2]]
            timings.append((secs, key))
            if n % 1000 == 0:
                log(f"    resolved {n:,} of {len(todo):,}")
    finally:
        write_cache(path, json.dumps(held).encode())
    timings.sort(reverse=True)
    if timings:
        log(f"  slowest groups (total {sum(t for t, _ in timings) / 60:.1f} CPU minutes):")
        sql_of = dict(items)
        for secs, key in timings[:10]:
            model, view, fields, _ = json.loads(key)
            log(
                f"    {secs:6.1f}s  {model}.{view}  {len(fields)} fields, "
                f"{len(sql_of[key]):,} chars"
                f"  {out[key][2]}"
            )
    slow = [k for k, v in out.items() if v[2] == "timeout"]
    (API_CACHE / "timeouts.json").unlink(missing_ok=True)
    if slow:
        API_CACHE.mkdir(parents=True, exist_ok=True)
        (API_CACHE / "timeouts.json").write_text(json.dumps({k: dict(items)[k] for k in slow}))
        log(
            f"  {len(slow)} groups timed out after {RESOLVE_TIMEOUT}s; SQL in "
            f"{(API_CACHE / 'timeouts.json').relative_to(CACHE.parent)}"
        )
    return out


def _init():
    sys.setrecursionlimit(20000)


# a comma that separates values: not ^-escaped and not inside double quotes
FILTER_SPLIT_RE = re.compile(r'(?<!\^),(?=(?:[^"]*"[^"]*")*[^"]*$)')


def filter_values(value):
    """(literal, mode) for each value in a Looker string filter expression:
    comma separated, ^ escapes, % wildcards, and double quotes around a value
    taken as written ("smart_window", "a,b"), which is how the UI writes most
    of them; exclusions and NULL / EMPTY skipped."""
    out = []
    for part in FILTER_SPLIT_RE.split(str(value)):
        part = part.strip()
        if not part or part.startswith("-") or part.upper() in ("NULL", "EMPTY"):
            continue
        if len(part) >= 2 and part[0] == part[-1] == '"':
            part = part[1:-1]
        part = re.sub(r"\^(.)", r"\1", part)
        if part:
            out.append((part.lower(), "like" if "%" in part else "eq"))
    return out


def query_refs(q, group):
    """Named refs for one query: its group's, plus events its filter values
    pick out on event identity columns."""
    named, idents, _ = group
    refs = dict(named)
    lits = L.Leaf()
    for field, value in q["filters"].items():
        held = idents.get(alias(field))
        if not held or not value:
            continue
        for lit, mode in filter_values(value):
            for app, kinds in held.items():
                ks = {k for k, _, _ in kinds}
                _, h, via = kinds[0]
                kind = "full" if len(ks) >= 2 else next(iter(ks))
                # a query's filters hold together, so no branches
                lits[("lit", app, kind, lit, mode, frozenset())] = (h, via, True, True)
    for m, (h, via, t, n) in L.resolve_events(lits, _EVENTS).items():
        if isinstance(m, int) and n and m not in refs:
            refs[m] = ("filter", h, via, True)
    return refs


# ---------------------------------------------------------------------- main


def opmon_query_refs(queries, end):
    """{query id: named refs} for queries on the generated OpMon explores.

    Their tables have a row per metric-hub metric name, so the SQL names no
    metric and resolves to nothing. A query is credited instead with what its
    OpMon project's metrics are built from (metric_hub.py), narrowed to the
    ones its `metric` filter names, with the project as via.
    """
    om = {qid: q for qid, q in queries.items() if q.get("model") == OPMON_MODEL}
    if not om:
        return {}
    mdl = MH.model()
    projects = mdl.opmon_projects(end)
    exps = MH.experiments(end)
    out, unknown = {}, set()
    for qid, q in om.items():
        slug = re.sub(r"_alerts$", "", q["view"] or "")
        p = projects.get(slug)
        if p is None:
            unknown.add(slug)
            continue
        names = {
            lit
            for f, v in q["filters"].items()
            if f.endswith(".metric") and v
            for lit, _ in filter_values(v)
        }
        names = (names & set(p["metrics"])) or set(p["metrics"])
        refs = mdl.opmon_refs(
            slug, names, (exps.get(slug) or {}).get("app_name") or "firefox_desktop"
        )
        out[qid] = {m: ("output", h + 1, f"opmon:{slug}", None) for m, (_, h, _) in refs.items()}
    log(
        f"  {len(out):,} OpMon queries credited through their project's metric-hub metrics"
        + (f"; {len(unknown)} on projects not active in the window" if unknown else "")
    )
    return out


def merge_ref(acc, refs):
    for m, (role, h, via, t) in refs.items():
        held = acc.get(m)
        if held is None:
            acc[m] = (role, h, via, t)
        else:
            acc[m] = (
                "output" if "output" in (role, held[0]) else "filter",
                min(h, held[1]),
                via if h < held[1] else held[2],
                t or held[3],
            )


def load(end=None):
    """(history, tiles, Looks, linked, queries, groups): the content and
    usage rows, LookML dashboard id -> the UDD linked to it, the queries in
    scope by id, and group key -> the ids of the queries in it."""
    projects = model_projects()

    def in_scope(model):
        return (
            model
            and model not in SKIP_MODELS
            and model in projects
            and not SKIP_PROJECT_RE.search(projects[model] or "")
        )

    hist = history(end)
    tiles, looks = content()
    # Dashboard ids are kept as strings: history has '1245' for a dashboard
    # built in the UI and 'model::name' for a LookML one, the dashboard explore
    # has numbers
    for t in tiles:
        t["dashboard.id"] = str(t["dashboard.id"])
    linked = {
        t["dashboard.lookml_link_id"]: t["dashboard.id"]
        for t in tiles
        if t.get("dashboard.lookml_link_id")
    }
    for h in hist:
        d = h["history.dashboard_id"]
        h["history.dashboard_id"] = linked.get(d, d) if d not in (None, "") else None
    log(f"  {len(hist):,} history rows, {len(tiles):,} dashboard tiles, {len(looks):,} Looks")

    queries, cut = {}, set()
    for r in hist + tiles + looks:
        qid = r.get("query.id")
        if qid and in_scope(r["query.model"]) and qid not in queries and qid not in cut:
            q = query_of(r)
            if q is None:
                cut.add(qid)
            else:
                queries[qid] = q
    groups = collections.defaultdict(list)
    for q in queries.values():
        groups[group_key(q)].append(q["id"])
    log(f"  {len(queries):,} queries in scope, {len(groups):,} groups")
    if cut:
        log(f"  {len(cut):,} queries left out: System Activity cut off their field lists")
    return hist, tiles, looks, linked, queries, groups


def members(hist, tiles, looks):
    """(artifact id -> the ids of the queries it is made of, dashboard info,
    Look info, Look id -> the dashboards it is on, (artifact id, query id) ->
    the last day the query ran there).

    A dashboard is made of its tiles' queries, the queries run on it and those
    of the Looks on it; a Look of its own query and those run on it; an
    explore of the ad hoc runs on no dashboard or Look. accuracy.py reads
    artifacts through this too, so both sides of the check group queries alike.
    A tile's or a saved Look's query is part of it whether or not it ran, so
    its last day is None.
    """
    of = collections.defaultdict(set)
    last = {}

    def add(aid, qid, day=None):
        of[aid].add(qid)
        if (aid, qid) in last and last[(aid, qid)] is None:
            return
        last[(aid, qid)] = None if day is None else max(day, last.get((aid, qid)) or "")

    dash_info, look_info = {}, {}
    look_dashboards = collections.defaultdict(set)
    for t in tiles:
        d = t["dashboard.id"]
        dash_info.setdefault(d, t)
        if t.get("look.id"):
            look_dashboards[t["look.id"]].add(d)
        if t.get("query.id"):
            add(f"looker:dashboard:{d}", t["query.id"])
    for lk in looks:
        look_info[lk["look.id"]] = lk
        if lk.get("query.id"):
            add(f"looker:look:{lk['look.id']}", lk["query.id"])
    lookml_titles = lookml_dashboards()
    for h in hist:
        d = h["history.dashboard_id"]
        if d and "::" in d and d not in dash_info:
            dash_info[d] = {
                "dashboard.title": lookml_titles.get(d, d),
                "user.email": None,
                "dashboard.lookml_link_id": d,
            }
    for h in hist:
        if not h.get("query.id"):
            continue
        day = h["history.most_recent_query_date"]
        if h["history.dashboard_id"] in dash_info:
            add(f"looker:dashboard:{h['history.dashboard_id']}", h["query.id"], day)
        elif h["history.look_id"] in look_info:
            add(f"looker:look:{h['history.look_id']}", h["query.id"], day)
        elif h["history.source"] in ADHOC_SOURCES:
            add(f"looker:explore:{h['query.model']}.{h['query.view']}", h["query.id"], day)
    # a Look on a dashboard is part of it
    for lk, ds in look_dashboards.items():
        for d in ds:
            for qid in of.get(f"looker:look:{lk}", ()):
                add(f"looker:dashboard:{d}", qid, last[(f"looker:look:{lk}", qid)])
    return dict(of), dash_info, look_info, look_dashboards, last


def usage_by_window(end, linked):
    """({window: (dashboard use, Look use, explore use)}, scheduled dashboards,
    scheduled Looks), each use keyed by artifact. A LookML dashboard's runs
    count toward the UDD linked to it, like its history and schedules. Users
    are summed, so a person who used both counts twice. Scheduled is a
    scheduled run anywhere in the longest window."""
    out = {}
    for w in WINDOWS:
        dash_use, look_use, explore_use = usage(w, end)
        dash_u = {}
        for r in dash_use:
            d = linked.get(str(r["dashboard.id"]), str(r["dashboard.id"]))
            held = dash_u.get(d)
            if held is None:
                dash_u[d] = dict(r)
                continue
            for f in ("history.dashboard_run_count", "history.query_run_count", "user.count"):
                held[f] = int(held.get(f) or 0) + int(r.get(f) or 0)
            held["history.most_recent_query_date"] = (
                max(
                    held.get("history.most_recent_query_date") or "",
                    r.get("history.most_recent_query_date") or "",
                )
                or None
            )
        out[w] = (
            dash_u,
            {r["history.look_id"]: r for r in look_use},
            {(r["query.model"], r["query.view"]): r for r in explore_use},
        )
    scheduled = system_activity(
        "history",
        ["history.dashboard_id", "history.look_id"],
        dict(window(LONGEST, end), **{"history.source": "scheduled_task"}),
    )
    sched_d = {
        linked.get(r["history.dashboard_id"], r["history.dashboard_id"])
        for r in scheduled
        if r["history.dashboard_id"]
    }
    sched_l = {r["history.look_id"] for r in scheduled if r["history.look_id"]}
    return out, sched_d, sched_l


def run(refresh_sql=False, measure_only=False, with_exposure=True):
    global _SHAPES, _EVENTS
    end = window_end()
    hist, tiles, looks, linked, queries, groups = load(end)
    use, sched_d, sched_l = usage_by_window(end, linked)
    if measure_only:
        return None

    sqls = generate_all(list(groups), refresh_sql)
    errors = collections.Counter(v["error"][:80] for v in sqls.values() if "error" in v)
    passing = sum(1 for v in sqls.values() if v.get("transient"))
    log(
        f"  SQL for {len(sqls) - sum(errors.values()):,} groups, "
        f"{sum(errors.values()):,} failed"
        + (f" ({passing:,} for passing reasons, asked again next run)" if passing else "")
    )
    for e, n in errors.most_common(5):
        log(f"    {n:5,}  {e}")
    check_transient(passing, len(sqls), "groups")

    inventory, _SHAPES = L.shapes_from_tables()
    _EVENTS = L.event_index(inventory)
    rolled = sum(1 for v in sqls.values() if "sql" in v and ROLLUP_RE.search(v["sql"]))
    effective, n_unrolled = effective_sqls(sqls, refresh_sql)
    log(
        f"  {rolled:,} groups are answered from an aggregate table; {n_unrolled:,} of them "
        f"read through to the base tables"
    )
    items = sorted(effective.items())
    exposed_keys, exposed_items, exposed_failed = ({}, [], [])
    if with_exposure:
        exposed_keys, exposed_items, exposed_failed = exposure()
        if exposed_failed:
            API_CACHE.mkdir(parents=True, exist_ok=True)
            (API_CACHE / "exposure_failed.json").write_text(json.dumps(exposed_failed))
    resolved = resolve_all(list(dict(items + exposed_items).items()))
    notes = collections.Counter(r[2].split(":")[0] for r in resolved.values())
    log("  groups by outcome: " + ", ".join(f"{k} {n:,}" for k, n in notes.most_common()))
    pdt = sum(1 for _, s in items if "looker_tmp" in s.lower())
    log(f"  {pdt:,} groups still read a table in looker_tmp")

    per_query = {}
    for key, ids in groups.items():
        g = resolved.get(key)
        if g is None:
            continue
        for qid in ids:
            per_query[qid] = query_refs(queries[qid], g)
    per_query.update(opmon_query_refs(queries, end))

    # artifacts and the refs each one gathers from its queries, in query id
    # order so ties on hops pick the same via every run. A ref's seen_within
    # is the shortest window with a run of a query naming the metric there,
    # or None (0 here) when a saved tile or Look names it.
    of, dash_info, look_info, look_dashboards, last = members(hist, tiles, looks)
    refs_of = collections.defaultdict(dict)
    seen = collections.defaultdict(dict)
    for aid, qids in of.items():
        for qid in sorted(qids, key=str):
            if qid in per_query:
                merge_ref(refs_of[aid], per_query[qid])
                day = last[(aid, qid)]
                w = 0 if day is None else seen_within(day, end) or LONGEST
                for m in per_query[qid]:
                    seen[aid][m] = min(w, seen[aid].get(m, w))
    explore_refs = {
        tuple(aid.split(":", 2)[2].split(".", 1)): refs
        for aid, refs in refs_of.items()
        if aid.startswith("looker:explore:")
    }

    def n(v):
        return int(v) if v not in (None, "") else 0

    def counts(key, which, runs):
        """views_<n>d and users_<n>d of one artifact, which picks its use."""
        out = {}
        for w in WINDOWS:
            u = use[w][which].get(key, {})
            out[f"views_{w}d"] = n(u.get(runs))
            out[f"users_{w}d"] = n(u.get("user.count"))
        return out

    def last_run(key, which):
        return use[LONGEST][which].get(key, {}).get("history.most_recent_query_date")

    ref_rows, art_rows = [], []

    def add(aid, kind, refs, named=True):
        for m, (role, h, via, t) in refs.items():
            r = inventory[m]
            ref_rows.append(
                {
                    "app": r["app"],
                    "metric": r["metric"],
                    "artifact_id": aid,
                    "artifact_kind": kind,
                    "role": role,
                    "hops": h + 1,
                    "named": named,
                    "transformed": t,
                    "via": via,
                    "output_columns": [],
                    "n_output_columns": 0,
                    "evidence": "parsed",
                    "seen_within": (seen[aid].get(m) or None) if named else None,
                }
            )

    for d, info in dash_info.items():
        aid = f"looker:dashboard:{d}"
        refs = refs_of.get(aid, {})
        add(aid, "looker_dashboard", refs)
        art_rows.append(
            {
                "artifact_id": aid,
                "kind": "looker_dashboard",
                "name": info["dashboard.title"],
                "url": f"{LOOKER}/dashboards/{d}",
                "owners": [info["user.email"]] if info.get("user.email") else [],
                "note": (
                    f"LookML dashboard {info['dashboard.lookml_link_id']}"
                    if info.get("dashboard.lookml_link_id")
                    else None
                ),
                "n_metrics_named": len(refs),
                "n_metrics_total": len(refs),
                "is_scheduled": d in sched_d,
                "last_run": last_run(d, 0),
                "last_used": last_run(d, 0),
                **counts(d, 0, "history.dashboard_run_count"),
                "child_ids": [f"looker:look:{lk}" for lk, ds in look_dashboards.items() if d in ds],
            }
        )
    for lk, info in look_info.items():
        aid = f"looker:look:{lk}"
        refs = refs_of.get(aid, {})
        add(aid, "looker_look", refs)
        art_rows.append(
            {
                "artifact_id": aid,
                "kind": "looker_look",
                "name": info["look.title"],
                "url": f"{LOOKER}/looks/{lk}",
                "owners": [info["user.email"]] if info.get("user.email") else [],
                "n_metrics_named": len(refs),
                "n_metrics_total": len(refs),
                "is_scheduled": lk in sched_l,
                "last_run": last_run(lk, 1),
                "last_used": last_run(lk, 1),
                **counts(lk, 1, "history.query_run_count"),
                "parent_ids": [
                    f"looker:dashboard:{d}" for d in sorted(look_dashboards.get(lk, ()))
                ],
            }
        )
    # What each visible explore exposes, as refs with named false. A metric
    # its queries name is exposed too, for the windows with no such query, so
    # the page leaves out an exposed ref where the same explore has a named
    # one in the window.
    exposed = collections.defaultdict(dict)
    for me, keys in exposed_keys.items():
        for k in keys:
            if k in resolved:
                merge_ref(exposed[me], resolved[k][0])
    for model, view in set(explore_refs) | set(exposed):
        refs = explore_refs.get((model, view), {})
        aid = f"looker:explore:{model}.{view}"
        add(aid, "looker_explore", refs)
        shows = exposed.get((model, view), {})
        add(aid, "looker_explore", {m: ("output",) + v[1:] for m, v in shows.items()}, named=False)
        art_rows.append(
            {
                "artifact_id": aid,
                "kind": "looker_explore",
                "name": f"{model} / {view}",
                "url": f"{LOOKER}/explore/{model}/{view}",
                "note": "exposes it as a field; named refs are ad hoc queries in the explore "
                "and through the API",
                "n_metrics_named": len(refs),
                "n_metrics_total": len(set(refs) | set(shows)),
                "last_run": last_run((model, view), 2),
                "last_used": last_run((model, view), 2),
                **counts((model, view), 2, "history.query_run_count"),
            }
        )

    by_kind = collections.Counter(
        r["artifact_kind"] + ("" if r["named"] else " (exposed)") for r in ref_rows
    )
    by_window = collections.Counter(r["seen_within"] for r in ref_rows if r["named"])
    log(
        "named refs by the shortest window with a run: "
        + ", ".join(
            f"{k or 'saved content'} {v:,}"
            for k, v in sorted(by_window.items(), key=lambda x: x[0] or 0)
        )
    )
    with_refs = collections.Counter(a["kind"] for a in art_rows if a["n_metrics_named"])
    log(
        f"\n{len(ref_rows):,} refs over "
        f"{len({(r['app'], r['metric']) for r in ref_rows}):,} metrics: "
        + ", ".join(f"{k} {v:,}" for k, v in by_kind.most_common())
    )
    log(
        "artifacts naming a metric: "
        + ", ".join(
            f"{k} {with_refs[k]:,} of {sum(1 for a in art_rows if a['kind'] == k):,}"
            for k in ("looker_dashboard", "looker_look", "looker_explore")
        )
    )
    return ref_rows, art_rows


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--measure", action="store_true")
    ap.add_argument(
        "--refresh-sql",
        action="store_true",
        help="generate SQL again for every group, after LookML changes",
    )
    ap.add_argument("--no-exposure", action="store_true")
    args = ap.parse_args()
    result = run(args.refresh_sql, args.measure, not args.no_exposure)
    if result is None or args.dry_run:
        return
    refs, arts = result
    suffix = (
        "Prototype table maintained by the docker-etl metric-usage job, metric_usage/looker.py; "
        "replaced on each run."
    )
    write_table(
        "refs_looker",
        refs,
        ref_schema(),
        "Metrics named by Looker dashboards, Looks and ad hoc explore queries, "
        "from the SQL Looker generates for them. " + suffix,
        clustering=["app", "metric"],
    )
    write_table(
        "artifacts_looker",
        arts,
        artifact_schema(),
        "Looker dashboards, Looks and explores, with owners and use per window "
        f"of {', '.join(map(str, WINDOWS))} days from System Activity. " + suffix,
        clustering=["artifact_id"],
    )
    union_views()


if __name__ == "__main__":
    main()
