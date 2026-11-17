"""Metric references in saved Redash (sql.telemetry.mozilla.org) queries.

    python -m metric_usage.redash              # parse every live BigQuery query, write tables
    python -m metric_usage.redash --dry-run    # parse and summarize, write nothing
    python -m metric_usage.redash --limit 2000 # first N queries only, for a quick look

Writes refs_redash and artifacts_redash, in the shared schemas the refs and
artifacts views read. Needs lineage.py to have run first: a query is resolved
against the stable tables and every bigquery-etl table's carried columns, so a
query reading clients_daily.total_uri_count is credited to the legacy scalar
browser.engagement.total_uri_count.

Source is stmo_external, the copy of Redash's metadata database. Every
non-archived query on a BigQuery data source is read, drafts included and
flagged, since a draft is somebody's scratch query and still a place the metric
is used. Query text is read into memory and dropped; it is not written to the
local cache or anywhere else. Only references are kept, and only named ones:
nothing reads a Redash query's output, so a SELECT * from telemetry.main is not
a use of each of its 3,240 probes. Which tables a query star-selects is kept
instead, for the page's blind spots.

Dashboards, owners and view counts come from bigquery-etl's deployed stmo
views, query_dashboards and object_views_daily, with views and viewers counted
per window (common.WINDOWS). object_views_daily starts on 2025-09-17.
"""

import argparse
import collections
import hashlib
import re
import sys

from .common import (
    LONGEST,
    WINDOWS,
    ResolveTimeout,
    artifact_schema,
    client,
    log,
    pool_map,
    ref_schema,
    time_limit,
    union_views,
    window_end,
    window_sql,
    window_start,
    write_table,
)

from . import lineage as L

STMO = "moz-fx-data-shared-prod.stmo"
REDASH = "https://sql.telemetry.mozilla.org"

# Redash's own project for unqualified dataset.table references
REDASH_PROJECT = "mozdata"

# A query the resolver takes longer than this on is left unresolved
RESOLVE_TIMEOUT = 60

PARAM_RE = re.compile(r"\{\{\s*[^{}]*?\s*\}\}")


QUERIES_SQL = """
SELECT
  q.id,
  q.name,
  q.query,
  q.is_draft,
  DATE(q.updated_at) AS updated_at,
  owners.email AS owner,
  JSON_VALUE(q.schedule, '$.interval') IS NOT NULL
  AND COALESCE(
    SAFE_CAST(JSON_VALUE(q.schedule, '$.until') AS DATE) >= CURRENT_DATE(),
    TRUE
  ) AS is_scheduled,
  DATE(results.retrieved_at) AS last_run,
FROM
  `moz-fx-data-shared-prod.stmo_external.queries_v1` AS q
INNER JOIN
  `moz-fx-data-shared-prod.stmo_external.data_sources_v1` AS ds
  ON ds.id = q.data_source_id
LEFT JOIN
  `moz-fx-data-shared-prod.stmo_external.users_v1` AS owners
  ON owners.id = q.user_id
LEFT JOIN
  `moz-fx-data-shared-prod.stmo_external.query_results_v1` AS results
  ON results.id = q.latest_query_data_id
WHERE
  ds.type = 'bigquery'
  AND NOT q.is_archived
ORDER BY
  q.id
"""


def views_sql(end):
    return (
        window_sql(
            "submission_date",
            end,
            views="SUM(IF({c}, views, 0))",
            users="COUNT(DISTINCT IF({c}, user_email, NULL))",
        )
        + ",\n  MAX(IF(views > 0, submission_date, NULL)) AS last_used"
    )


def in_window(column, end):
    return f"{column} BETWEEN '{window_start(LONGEST, end)}' AND '{end}'"


def dashboards_sql(end):
    return f"""
WITH dashboard_views AS (
  SELECT
    object_id AS dashboard_id,
    {views_sql(end)},
  FROM `{STMO}.object_views_daily`
  WHERE object_type = 'dashboard'
    AND {in_window("submission_date", end)}
  GROUP BY dashboard_id
)
SELECT
  qd.query_id,
  qd.dashboard_id,
  qd.dashboard_name,
  qd.dashboard_url,
  qd.dashboard_owner_email,
  qd.dashboard_is_draft,
  dv.* EXCEPT (dashboard_id),
FROM `{STMO}.query_dashboards` AS qd
LEFT JOIN dashboard_views AS dv USING (dashboard_id)
WHERE NOT qd.dashboard_is_archived
  AND NOT qd.query_is_archived
"""


def query_views_sql(end):
    """Views per query in each window: its visualizations on dashboards plus
    its own page, the same accounting as object_views_daily."""
    return f"""
SELECT
  v.query_id,
  {views_sql(end)},
FROM `{STMO}.object_views_daily` AS ov
INNER JOIN `moz-fx-data-shared-prod.stmo_external.visualizations_v1` AS v
  ON v.id = ov.object_id
WHERE ov.object_type = 'visualization'
  AND {in_window("ov.submission_date", end)}
GROUP BY v.query_id
"""


def usage(r):
    """views_<n>d, users_<n>d and last_used from a row of either query, 0
    and None for none."""
    out = {
        f"{k}_{w}d": (r or {}).get(f"{k}_{w}d") or 0 for w in WINDOWS for k in ("views", "users")
    }
    last = (r or {}).get("last_used")
    out["last_used"] = last.isoformat() if last else None
    return out


def rows(sql):
    return [dict(r) for r in client().query(sql).result()]


# ------------------------------------------------------------------ parsing
#
# Workers are forked after the shapes are built, so each one reads the same
# shapes without copying them.

_INVENTORY = None
_SHAPES = None
_TABLES = None
_EVENTS = None
_DIGESTS = None


def load_shapes():
    """Load lineage's shapes for analyse(), before workers fork. Returns the
    inventory, whose positions are the metric ids analyse() hands back."""
    global _INVENTORY, _SHAPES, _TABLES, _EVENTS, _DIGESTS
    _INVENTORY, _SHAPES = L.shapes_from_tables()
    _TABLES = set(_SHAPES)
    _EVENTS = L.event_index(_INVENTORY)
    _DIGESTS = None
    log(f"  shapes for {len(_SHAPES):,} tables")
    return _INVENTORY


def digests():
    """lineage.shape_digests() for the loaded shapes, computed once, before
    workers fork so that dependencies() can read them."""
    global _DIGESTS
    if _DIGESTS is None:
        _DIGESTS = L.shape_digests(_INVENTORY, _SHAPES)
    return _DIGESTS


WORD_RE = re.compile(r"[a-z0-9_]+")


def dependencies(text, project=REDASH_PROJECT):
    """{table: column names} for every table a query names, the names being
    those of the table's columns the text mentions: what its analyse() result
    holds for (see lineage.shape_digests()). Only schema names are kept, never
    a word that is the author's own."""
    low = PARAM_RE.sub("0", text).lower()
    words = set(WORD_RE.findall(low))
    return {t: sorted(words & (_DIGESTS.get(t) or {}).keys()) for t in table_refs(low, project)}


def dependency_digest(table, names):
    """One digest for what dependencies() lists for a table. Whether lineage
    has a shape for it at all counts too, so a result is redone once a table
    it names gets one."""
    cols = _DIGESTS.get(table)
    material = (
        ["absent"] if cols is None else ["present"] + [f"{n}={cols.get(n, '')}" for n in names]
    )
    return hashlib.sha256("|".join(material).encode()).hexdigest()[:16]


def known_tables():
    """Every table lineage has a shape for, loading the shapes if need be."""
    if _TABLES is None:
        load_shapes()
    return _TABLES


def table_refs(text, project=REDASH_PROJECT):
    out = set()
    for m in L.TABLE_TEXT_RE.finditer(L.merge_quoted(text)):
        parts = m.group(1).split(".")
        if len(parts) == 3:
            out.add(L.norm_table(tuple(parts), project))
        elif len(parts) == 2:
            out.add(L.norm_table((None, parts[0], parts[1]), project))
    return out


def analyse(text, project=REDASH_PROJECT):
    """(named refs, star tables, note) for one query's text. project is where
    an unqualified dataset.table lives: mozdata for Redash, the job's own
    project for adhoc.py.

    named refs maps a metric id to (role, hops, via, transformed). The output
    columns are the author's own aliases, so they are not kept.
    """
    text = PARAM_RE.sub("0", text)
    refs = table_refs(text.lower(), project)
    if not refs & _TABLES:
        return {}, [], "reads nothing carrying a metric"
    prelude, tree, note = L.parse_script(text)
    if tree is None:
        # unparseable (legacy SQL, scripting, a template in table position):
        # dotted names, credited only against tables the text names
        art = type("A", (), {"sql": text, "refs": refs})()
        found = L.regex_refs(art, _EVENTS, _SHAPES)
        return (
            {m: ("unknown", h, v, True) for m, (h, v, t, n) in found.items() if isinstance(m, int)},
            [],
            note or "regex",
        )
    try:
        out, touched, star_tables, unread = L.resolve_script(
            prelude, tree, _SHAPES, project, all_selects=True
        )
    except Exception as e:
        return {}, [], f"resolve failed: {type(e).__name__}"
    out = L.resolve_events_in(out, _EVENTS)
    touched = L.resolve_events(touched, _EVENTS)
    outputs = collections.defaultdict(list)
    for _, leaf in L.output_paths(out):
        for m, val in leaf.items():
            outputs[m].append(val)
    named = {}
    for m, (h, via, t, n) in touched.items():
        if isinstance(m, tuple):
            continue
        outs = outputs.get(m, [])
        if not n:
            continue
        named[m] = ("output" if outs else "filter", h, via, t or any(v[2] for v in outs))
    return named, sorted(star_tables & _TABLES), L.partial_note(unread)


def _work(item):
    key, text = item
    try:
        with time_limit(RESOLVE_TIMEOUT):
            return key, analyse(text)
    except ResolveTimeout:
        return key, ({}, [], "timeout")
    except RecursionError:
        return key, ({}, [], "resolve failed: RecursionError")


def _crashed(item):
    """_work()'s result for a query that killed its worker."""
    return item[0], ({}, [], "resolve failed: worker died")


def _init():
    sys.setrecursionlimit(20000)


# ---------------------------------------------------------------------- main


def run(limit=None):
    inventory = load_shapes()

    queries = rows(QUERIES_SQL)
    if limit:
        queries = queries[:limit]
    log(f"  {len(queries):,} live BigQuery queries")

    # Forks of a query are common, so identical text is parsed once.
    by_text = collections.defaultdict(list)
    for q in queries:
        by_text[q.pop("query") or ""].append(q["id"])
    log(f"  {len(by_text):,} distinct query texts")

    results = {}
    L.persistent_udfs()  # parsed once here for the forked workers
    items = list(enumerate(by_text))
    for n, (i, res) in enumerate(
        pool_map(_work, items, chunksize=64, initializer=_init, on_crash=_crashed)
    ):
        results[items[i][1]] = res
        if n and n % 5000 == 0:
            log(f"    parsed {n:,}")
    text_of = {qid: t for t, ids in by_text.items() for qid in ids}
    del by_text

    end = window_end()
    dashboards = rows(dashboards_sql(end))
    query_views = {r["query_id"]: r for r in rows(query_views_sql(end))}
    dash_of = collections.defaultdict(list)
    dash_info = {}
    for d in dashboards:
        dash_of[d["query_id"]].append(d["dashboard_id"])
        dash_info[d["dashboard_id"]] = d
    log(f"  {len(dash_info):,} live dashboards over {len(dash_of):,} queries")

    notes = collections.Counter()
    ref_rows, art_rows = [], []
    dash_refs = collections.defaultdict(dict)
    for q in queries:
        named, stars, note = results[text_of[q["id"]]]
        notes[note.split(":")[0]] += 1
        aid = f"redash:query:{q['id']}"
        for m, (role, h, via, t) in named.items():
            r = inventory[m]
            ref_rows.append(
                {
                    "app": r["app"],
                    "metric": r["metric"],
                    "artifact_id": aid,
                    "artifact_kind": "redash_query",
                    "role": role,
                    "hops": h + 1,
                    "named": True,
                    "transformed": t,
                    "via": via,
                    "output_columns": [],
                    "n_output_columns": 0,
                    "evidence": "parsed" if role != "unknown" else "regex",
                }
            )
            for d in dash_of.get(q["id"], []):
                held = dash_refs[d].get(m)
                if held is None or h < held[0]:
                    dash_refs[d][m] = (h, aid)
        qv = query_views.get(q["id"], {})
        art_rows.append(
            {
                "artifact_id": aid,
                "kind": "redash_query",
                "name": q["name"],
                "url": f"{REDASH}/queries/{q['id']}",
                "owners": [q["owner"]] if q["owner"] else [],
                "note": note,
                "n_metrics_named": len(named),
                "n_metrics_total": len(named),
                "is_draft": q["is_draft"],
                "is_scheduled": q["is_scheduled"],
                "last_run": q["last_run"].isoformat() if q["last_run"] else None,
                "updated_at": q["updated_at"].isoformat() if q["updated_at"] else None,
                **usage(qv),
                "parent_ids": [f"redash:dashboard:{d}" for d in dash_of.get(q["id"], [])],
                "star_tables": stars,
            }
        )

    # A dashboard depends on every metric its queries name. Its child_ids are
    # the queries that have an artifact here; queries on other data sources
    # (and past --limit) are left out.
    query_ids = {q["id"] for q in queries}
    for d, info in dash_info.items():
        did = f"redash:dashboard:{d}"
        for m, (h, via_query) in dash_refs.get(d, {}).items():
            r = inventory[m]
            ref_rows.append(
                {
                    "app": r["app"],
                    "metric": r["metric"],
                    "artifact_id": did,
                    "artifact_kind": "redash_dashboard",
                    "role": "output",
                    "hops": h + 2,
                    "named": True,
                    "transformed": None,
                    "via": via_query,
                    "output_columns": [],
                    "n_output_columns": 0,
                    "evidence": "parsed",
                }
            )
        art_rows.append(
            {
                "artifact_id": did,
                "kind": "redash_dashboard",
                "name": info["dashboard_name"],
                "url": info["dashboard_url"],
                "owners": [info["dashboard_owner_email"]] if info["dashboard_owner_email"] else [],
                "n_metrics_named": len(dash_refs.get(d, {})),
                "n_metrics_total": len(dash_refs.get(d, {})),
                "is_draft": info["dashboard_is_draft"],
                **usage(info),
                "child_ids": [
                    f"redash:query:{q}" for q, ds in dash_of.items() if d in ds and q in query_ids
                ],
            }
        )

    log("\nqueries by outcome:")
    for k, n in notes.most_common():
        log(f"  {n:7,}  {k}")
    qrefs = [r for r in ref_rows if r["artifact_kind"] == "redash_query"]
    log(
        f"\n{len(qrefs):,} query refs over "
        f"{len({r['artifact_id'] for r in qrefs}):,} queries and "
        f"{len({(r['app'], r['metric']) for r in qrefs}):,} metrics; "
        f"{len(ref_rows) - len(qrefs):,} dashboard refs"
    )
    return ref_rows, art_rows


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int)
    args = ap.parse_args()
    refs, arts = run(args.limit)
    if args.dry_run:
        return
    suffix = (
        "Prototype table maintained by the docker-etl metric-usage job, metric_usage/redash.py; "
        "replaced on each run."
    )
    write_table(
        "refs_redash",
        refs,
        ref_schema(),
        "Metrics named by saved Redash queries and, through them, " "dashboards. " + suffix,
        clustering=["app", "metric"],
    )
    write_table(
        "artifacts_redash",
        arts,
        artifact_schema(),
        "Redash queries and dashboards, with owners, schedule, last run "
        f"and human views per window of {', '.join(map(str, WINDOWS))} days. " + suffix,
        clustering=["artifact_id"],
    )
    union_views()


if __name__ == "__main__":
    main()
