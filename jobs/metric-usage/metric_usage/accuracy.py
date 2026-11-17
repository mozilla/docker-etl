"""Compare the parsed refs against a plain text search, for a sample of metrics.

    python -m metric_usage.accuracy                # sample, compare, write the report
    python -m metric_usage.accuracy --seed 7       # a different sample
    python -m metric_usage.accuracy --metric telemetry:GC_MS   # add a metric, repeatable

Writes to labels/ (sample.csv, disagreements.csv) and .cache/accuracy/
(summary.csv):

  sample.csv         the sampled metrics, drawn once and reused so that labels
                     survive reruns; --resample draws a new one
  summary.csv        one row per sampled metric: how many artifacts the parser
                     and the text search each found, and how many both did
  disagreements.csv  one row per (metric, artifact) the two disagree on, with
                     the metric's ping-table column (or event filter), the
                     matching line and an empty `verdict` column to fill in
                     with `ref` or `not_ref`; check.py scores against it.
                     A note starting `first pass:` is a label waiting for review.
                     Each run also writes a timestamped copy, and labels are
                     read back from every copy, so a row that leaves the
                     review set keeps its label for when it returns.

The text search counts an artifact as referencing a metric when its text names
a table holding the metric and, as a whole word, the name of the column holding
it in that table. The tables are the ping tables and every bigquery-etl table
whose named columns carries says hold the metric, so the search follows the
same hops as the parser, but it checks them by reading the text rather than by
resolving the query. Comments are removed first. Events are searched by their
category and name as quoted strings.

Only named refs are compared. A SELECT * forwards a metric without naming it,
and a text search cannot see that by construction, so bulk refs are counted per
metric and left out of the comparison, as are the Looker explores that only
expose a metric as a field.

Looker dashboards, Looks and explores are searched through the SQL Looker
generates for each query they are made of (looker.members()), with the query's
filter values added as quoted strings, since a filter on an event's category
and name is how Looker names an event and the generated SQL leaves values out.
A group Looker answers from an aggregate table is read against the base
tables on both sides (looker.unroll()). The two persistent derived tables in
looker_tmp carry no metric. Looker is a source of its own in summary.csv and
in the review groups, and check.py scores it on its own line.

Ad hoc queries are searched per account and Jetstream experiment, over the
text of the queries in adhoc.py's job list that read a table the search looks
at (adhoc.texts_reading(), one query per project, in memory only), and are a
source of their own like Looker. metric-hub metrics are searched over the SQL
each definition stands for (metric_hub.Model.sqls), as a fourth source.

The disagreements file holds short snippets of Redash and ad hoc query text so
they can be reviewed. labels/ stays on the machine that runs it (it is not in git), and nothing
in it is written to BigQuery or deployed.
"""

import argparse
import collections
import csv
import random
import re

from .common import (CACHE, DEST, LABELS, PREFIX, held_labels, log, query, run_deep,
                     write_labels)

from . import adhoc as AH
from . import lineage as L
from . import looker as LK
from . import metric_hub as MH
from . import redash as R

OUT = CACHE / "accuracy"

# Review groups and summary rows are per source: Looker, ad hoc queries,
# metric-hub, or the warehouse SQL of bigquery-etl and Redash.
SOURCES = ("sql", "looker", "adhoc", "metrichub")


def source_of(artifact_id):
    if artifact_id.startswith("looker:"):
        return "looker"
    if artifact_id.startswith("metric-hub:"):
        return "metrichub"
    if artifact_id.startswith(("adhoc:", "jetstream:")):
        return "adhoc"
    return "sql"

# (source, column_status, bucket) -> how many to sample. A bucket is the number
# of named bigquery-etl and Redash refs, from the summary table.
STRATA = {
    ("glean", "column", "0"): 6,
    ("glean", "column", "1-5"): 8,
    ("glean", "column", "6-50"): 8,
    ("glean", "column", ">50"): 4,
    ("legacy", "column", "0"): 4,
    ("legacy", "column", "1-5"): 5,
    ("legacy", "column", "6-50"): 5,
    ("legacy", "column", ">50"): 2,
    ("glean", "event", "0"): 3,
    ("legacy", "event", "0"): 1,
    # events have refs since they are matched by literal
    ("glean", "event", "1-5"): 4,
    ("glean", "event", "6-50"): 4,
    ("glean", "event", ">50"): 2,
    # legacy events, matched by category and method
    ("legacy", "event", "1-5"): 3,
    ("legacy", "event", "6-50"): 3,
    ("legacy", "event", ">50"): 1,
}

# Known paths through several hops, always included
ALWAYS = [
    ("telemetry", "browser.engagement.total_uri_count"),
    ("firefox_desktop", "browser.engagement.tab_open_event_count"),
    ("fenix", "metrics.default_browser"),
]

# Above this many refs a metric is all generic client_info fields, whose
# disagreements are too many to review and say little about the parser
MAX_REFS = 400

# Glean events in the sample at least. The strata are by how many refs a
# metric has, which says nothing for events before they could be matched, so
# events are topped up at random to this many.
EVENT_SAMPLE = 12

# Disagreements kept for review per (metric, side). The rest are counted, and
# check.py applies each group's labelled share to all of them.
REVIEW_PER_GROUP = 8

DIS_FIELDS = ["app", "metric", "column", "artifact_id", "side", "stands_for", "url",
              "evidence", "verdict", "note"]


def column_of(p):
    """What a query names to read the metric, for reading a disagreement: its
    column paths in the ping tables (the same path in each ping's table), or
    for an event the category and name or methods it is filtered on."""
    paths = sorted({c.split(":", 1)[1] for c in p["cols"] or []})
    if paths:
        return ", ".join(paths)
    if not p["event_category"]:
        return ""
    if p["source"] == "legacy":
        # recorded by category, one of its methods and one of its objects
        methods = ", ".join(sorted(p.get("event_methods") or []))
        objects = ", ".join(sorted(p.get("event_objects") or []))
        return (f"event_category = '{p['event_category']}', event_method in ({methods})"
                + (f", event_object in ({objects})" if objects else ""))
    return f"event_category = '{p['event_category']}', event_name = '{p['event_name']}'"


def save_sample(path, picked):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["app", "metric", "stratum"])
        w.writeheader()
        w.writerows({k: p[k] for k in ("app", "metric", "stratum")} for p in picked)


def sample(seed, extra, resample=False):
    rows = query(f"""
SELECT
  s.app, s.metric, s.source, s.column_status,
  s.n_tables + s.n_views + s.n_redash_all AS n,
  m.event_category, m.event_name, m.event_methods, m.event_objects,
  ARRAY(SELECT CONCAT(c.table, ':', c.path) FROM UNNEST(m.columns) AS c) AS cols
FROM `{DEST}.{PREFIX}summary` AS s
JOIN `{DEST}.{PREFIX}metrics` AS m USING (app, metric)
WHERE NOT s.app_deprecated AND s.in_source
""", tables=["summary", "metrics"], name="candidates")
    index = {(r["app"], r["metric"]): r for r in rows}
    path = LABELS / "sample.csv"
    if path.exists() and not resample:
        held = list(csv.DictReader(open(path)))
        picked = [dict(index[(h["app"], h["metric"])], stratum=h["stratum"])
                  for h in held if (h["app"], h["metric"]) in index]
        log(f"  reusing {path.name} ({len(picked)} metrics)")
        # strata added to STRATA after the sample was drawn are drawn now and
        # appended, so the existing metrics and their labels stay
        missing = [k for k in STRATA if "/".join(k) not in {p["stratum"] for p in picked}]
        if missing:
            have = {(p["app"], p["metric"]) for p in picked}
            by = collections.defaultdict(list)
            for r in rows:
                n = r["n"]
                b = "0" if n == 0 else "1-5" if n <= 5 else "6-50" if n <= 50 else ">50"
                if n <= MAX_REFS and (r["app"], r["metric"]) not in have:
                    by[(r["source"], r["column_status"], b)].append(r)
            rng = random.Random(seed + 2)
            for key in missing:
                pool = sorted(by.get(key, []), key=lambda r: (r["app"], r["metric"]))
                picked += [dict(r, stratum="/".join(key))
                           for r in rng.sample(pool, min(STRATA[key], len(pool)))]
            save_sample(path, picked)
            log(f"  added strata {', '.join('/'.join(k) for k in missing)} to {path.name}")
    else:
        picked = draw(rows, seed)
        LABELS.mkdir(parents=True, exist_ok=True)
        save_sample(path, picked)
    have = {(p["app"], p["metric"]) for p in picked}
    n_events = sum(1 for p in picked if p["column_status"] == "event" and p["source"] == "glean")
    if n_events < EVENT_SAMPLE:
        pool = sorted((r for r in rows if r["column_status"] == "event" and r["source"] == "glean"
                       and (r["app"], r["metric"]) not in have),
                      key=lambda r: (r["app"], r["metric"]))
        more = random.Random(seed + 1).sample(pool, min(EVENT_SAMPLE - n_events, len(pool)))
        picked += [dict(r, stratum="glean/event/any") for r in more]
        save_sample(path, picked)
        log(f"  added {len(more)} Glean events to {path.name}")
    for app, metric in extra:
        r = index.get((app, metric))
        if r and all((p["app"], p["metric"]) != (app, metric) for p in picked):
            picked.append(dict(r, stratum="chosen"))
    return picked


def draw(rows, seed):
    """A stratified sample, plus ALWAYS."""
    rng = random.Random(seed)
    by = collections.defaultdict(list)
    for r in rows:
        n = r["n"]
        b = "0" if n == 0 else "1-5" if n <= 5 else "6-50" if n <= 50 else ">50"
        if n <= MAX_REFS:
            by[(r["source"], r["column_status"], b)].append(r)
    picked = []
    for key, k in STRATA.items():
        pool = sorted(by.get(key, []), key=lambda r: (r["app"], r["metric"]))
        picked += [dict(r, stratum="/".join(key)) for r in rng.sample(pool, min(k, len(pool)))]
    index = {(r["app"], r["metric"]): r for r in rows}
    for app, metric in ALWAYS:
        r = index.get((app, metric))
        if r and all((p["app"], p["metric"]) != (app, metric) for p in picked):
            picked.append(dict(r, stratum="chosen"))
    return picked


def terms_for(picked):
    """table -> [(metric key, column name)] for the text search.

    The ping tables come from the inventory. Derived tables come from carries,
    for columns holding only a few metrics: a pass-through view's columns keep
    the metric's own name and are good search terms, but a column that rolls
    many metrics into one (`addons`, `scalar_aggregates`) is named after the
    rollup and says nothing about this metric.
    """
    keys = {(p["app"], p["metric"]) for p in picked}
    terms = collections.defaultdict(set)
    for p in picked:
        for c in p["cols"] or []:
            table, path = c.split(":", 1)
            terms[f"{L.DEFAULT_PROJECT}.{table}"].add(((p["app"], p["metric"]), path.split(".")[-1]))
    where = " OR ".join(f"(app = '{a}' AND metric = '{m}')" for a, m in keys)
    for r in query(f"""
SELECT `table`, path, app, metric
FROM (
  SELECT *, COUNT(*) OVER (PARTITION BY `table`, path) AS n_metrics
  FROM `{DEST}.{PREFIX}carries`
)
WHERE n_metrics < {L.NARROW_MIN} AND ({where})
""", tables=["carries"], name="carried columns"):
        leaf = r["path"].split(".")[-1]
        terms[r["table"]].add(((r["app"], r["metric"]), leaf))
    # the live tables share their stable twin's columns
    for t in list(terms):
        if "_stable." in t:
            terms[t.replace("_stable.", "_live.")] |= terms[t]
    return terms


def events_for(picked):
    """metric key -> (category, name, tables holding that app's events).

    A Glean event is found by its category and name. A legacy event has no
    single name and a category alone matches far too much, so it is found by
    its category and any one of its methods. Two apps can have
    an event of the same name (fenix and desktop both have ipprotection.paused),
    so a hit only counts if the text reads a table holding this app's events:
    its ping tables, or a table carries says holds its event identity.
    """
    keys = {(p["app"], p["metric"]): (p["event_category"], p["event_name"])
            for p in picked if p["column_status"] == "event" and p["source"] == "glean"
            and p["event_category"] and p["event_name"]}
    # a legacy event is found by its category and any of its methods
    keys.update({(p["app"], p["metric"]): (p["event_category"], tuple(p.get("event_methods") or []))
                 for p in picked if p["column_status"] == "event" and p["source"] == "legacy"
                 and p["event_category"] and p.get("event_methods")})
    apps = {k[0] for k in keys}
    if not apps:
        return {}
    tables = collections.defaultdict(set)
    where = ", ".join(f"'{a}'" for a in apps)
    for r in query(f"""
SELECT DISTINCT app, `table` FROM `{DEST}.{PREFIX}carries`
WHERE metric = '(event category)' AND app IN ({where})
""", tables=["carries"], name="event identity tables"):
        tables[r["app"]].add(r["table"])
    from .inventory import glean_apps
    app_of = {f"{v['bq_dataset_family']}_stable": name
              for name, app in glean_apps(False).items() for v in app["variants"]}
    app_of["telemetry_stable"] = "telemetry"
    for r in query(L.EVENT_FIELDS_SQL, name="event fields"):
        app = app_of.get(r["dataset_id"])
        if app in apps:
            tables[app].add(f"{L.DEFAULT_PROJECT}.{r['dataset_id']}.{r['table_id']}")
            tables[app].add(f"{L.DEFAULT_PROJECT}.{r['dataset_id'][:-len('_stable')]}_live.{r['table_id']}")
    return {k: (c, n, tables[k[0]]) for k, (c, n) in keys.items()}


def strip_comments(text):
    """The text with /* block */, -- and # comments removed.

    Block comments keep their newlines so line numbers stay put. Comment
    markers inside string literals are not special-cased; they are rare enough
    in SQL to accept.
    """
    return L.COMMENT_RE.sub(lambda m: "\n" * m.group(0).count("\n"), text)


def text_hits(text, project, terms, events):
    """{metric key: matching line} for one artifact's text, comments excluded."""
    low = strip_comments(text.lower())
    tables = set()
    for m in L.TABLE_TEXT_RE.finditer(L.merge_quoted(low)):
        parts = m.group(1).split(".")
        if len(parts) == 3:
            tables.add(L.norm_table(tuple(parts), project))
        elif len(parts) == 2:
            tables.add(L.norm_table((None, parts[0], parts[1]), project))
    hits = {}
    lines = low.splitlines()
    for t in tables & terms.keys():
        for key, col in terms[t]:
            if key in hits:
                continue
            pat = re.compile(rf"\b{re.escape(col)}\b")
            for ln in lines:
                if pat.search(ln):
                    hits[key] = ln.strip()[:200]
                    break
    for key, (cat, name, event_tables) in events.items():
        if key in hits or not tables & event_tables:
            continue
        cq = re.compile(rf"['\"]{re.escape(cat.lower())}['\"]")
        names = name if isinstance(name, tuple) else (name,)
        nqs = [re.compile(rf"['\"]{re.escape(n.lower())}['\"]") for n in names]
        if cq.search(low) and any(nq.search(low) for nq in nqs):
            ln = next((x for x in lines if any(nq.search(x) for nq in nqs)), "")
            hits[key] = ln.strip()[:200]
    return hits


def search_all(items, terms, events):
    """{key: text_hits()} for (key, text, project) items."""
    return {key: text_hits(text, project, terms, events) for key, text, project in items}


def looker_url(aid):
    kind, rest = aid.split(":", 2)[1:]
    if kind == "dashboard":
        return f"{LK.LOOKER}/dashboards/{rest}"
    if kind == "look":
        return f"{LK.LOOKER}/looks/{rest}"
    return f"{LK.LOOKER}/explore/{rest.replace('.', '/', 1)}"


def looker_hits(terms, events):
    """{metric key + (artifact id,): matching line} for every Looker artifact,
    from the generated SQL of its queries plus their filter values. The SQL
    comes from looker.py's cache; a group not in it is generated now."""
    hist, tiles, looks, _, queries, groups = LK.load()
    of = LK.members(hist, tiles, looks)[0]
    # an aggregate table's groups are read against the base tables, as looker.py does
    sqls, _ = LK.effective_sqls(LK.generate_all(list(groups)))
    combo_of, texts = {}, {}
    for key, qids in groups.items():
        sql = sqls.get(key)
        if not sql:
            continue
        for qid in qids:
            values = sorted({lit for v in queries[qid]["filters"].values() if v
                             for lit, _ in LK.filter_values(v)})
            k = (key, tuple(values))
            if k not in texts:
                texts[k] = sql + "".join(f"\n'{v}'" for v in values)
            combo_of[qid] = k
    done = search_all([(k, t, LK.PROJECT) for k, t in texts.items()], terms, events)
    per_query = {qid: done[k] for qid, k in combo_of.items()}
    hits = {}
    for aid, qids in of.items():
        for qid in sorted(qids, key=str):
            for k, line in per_query.get(qid, {}).items():
                hits.setdefault(k + (aid,), line)
    log(f"  searched Looker: {len(done):,} SQL and filter combinations over "
        f"{len(of):,} artifacts")
    return hits


def adhoc_hits(terms, events):
    """{metric key + (artifact id,): matching line} for every account and
    Jetstream experiment in adhoc.py's job list, from the text of each query
    credited to it (adhoc.artifact_of). A query run for several counts for
    each of them.

    Only the queries reading a table the search looks at are fetched, in one
    query per project (adhoc.texts_reading): a hit needs the text to name one
    of those tables, and reading through a view records its base tables, which
    carry the metric too.
    """
    start, end = AH.window()
    rows = AH.job_rows(AH.PROJECTS, start, end)
    exps = AH.experiments(end)
    accounts = collections.defaultdict(set)
    for r in rows:
        accounts[(r["project"], r["key"])].add(AH.artifact_of(r, exps)[0])
    tables = set(terms) | {t for _, _, ts in events.values() for t in ts}
    hits, n = {}, 0
    for project in AH.PROJECTS:
        texts = AH.texts_reading(project, start, end, tables)
        items = [((project, key), t, project) for key, t in texts.items()
                 if (project, key) in accounts]
        del texts
        for pk, found in sorted(search_all(items, terms, events).items()):
            for aid in sorted(accounts[pk]):
                for k, line in found.items():
                    hits.setdefault(k + (aid,), line)
        n += len(items)
        del items
    log(f"  searched {n:,} ad hoc queries over {len({a for v in accounts.values() for a in v}):,} "
        "accounts and experiments")
    return hits


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seed", type=int, default=20261013)
    ap.add_argument("--metric", action="append", default=[],
                    help="app:metric to include as well, repeatable")
    ap.add_argument("--resample", action="store_true",
                    help="draw a new sample; existing labels for metrics in it are kept")
    args = ap.parse_args()
    extra = [tuple(m.split(":", 1)) for m in args.metric]

    picked = sample(args.seed, extra, args.resample)
    keys = {(p["app"], p["metric"]) for p in picked}
    log(f"  sampled {len(picked)} metrics")
    terms = terms_for(picked)
    events = events_for(picked)
    log(f"  text search over {len(terms):,} tables")

    # parser side
    where = " OR ".join(f"(r.app = '{a}' AND r.metric = '{m}')" for a, m in keys)
    refs = query(f"""
SELECT r.app, r.metric, r.artifact_id, r.artifact_kind, r.named, r.role, r.via,
  r.output_columns, r.evidence, a.url, a.generated_by
FROM `{DEST}.{PREFIX}refs` AS r
LEFT JOIN `{DEST}.{PREFIX}artifacts` AS a USING (artifact_id)
WHERE r.artifact_kind IN ('bqetl_table', 'bqetl_view', 'glean_ping_view', 'redash_query',
                          'looker_dashboard', 'looker_look', 'looker_explore',
                          'person', 'service_account', 'jetstream_experiment',
                          'metric_hub_metric')
  AND ({where})
""", tables=["refs", "artifacts"], name="parsed refs")
    # A Jetstream experiment's ref is not named when only its platform's
    # defaults compute the metric, which says nothing about whether the SQL
    # read it, so here it counts either way.
    parsed = {(r["app"], r["metric"], r["artifact_id"]): r for r in refs
              if r["named"] or r["artifact_kind"] == "jetstream_experiment"}
    # for Looker, the explores that only expose the metric as a field; an
    # explore whose queries name it has an exposed ref too, for the page's
    # shorter windows, which is not counted here
    bulk = collections.Counter((r["app"], r["metric"], source_of(r["artifact_id"]))
                               for r in refs if not r["named"]
                               and r["artifact_kind"] != "jetstream_experiment"
                               and (r["app"], r["metric"], r["artifact_id"]) not in parsed)

    # text side: bigquery-etl
    text = {}
    urls = {}
    for a in L.corpus():
        a.load()
        for key, hit in text_hits(a.sql or "", a.project, terms, events).items():
            text[key + (a.id,)] = hit
        urls[a.id] = a.url
    log("  searched generated-sql")
    # text side: Redash, held in memory only
    items = []
    for q in R.rows(R.QUERIES_SQL):
        aid = f"redash:query:{q['id']}"
        items.append((aid, R.PARAM_RE.sub("0", q["query"] or ""), R.REDASH_PROJECT))
        urls[aid] = f"{R.REDASH}/queries/{q['id']}"
    for aid, found in search_all(items, terms, events).items():
        for key, hit in found.items():
            text[key + (aid,)] = hit
    del items
    log("  searched Redash")
    # text side: Looker, also in memory only
    for key, hit in looker_hits(terms, events).items():
        text[key] = hit
        urls[key[2]] = looker_url(key[2])
    # text side: ad hoc queries, in memory only
    for key, hit in adhoc_hits(terms, events).items():
        text[key] = hit
    # text side: metric-hub, the SQL each definition stands for
    items = [(aid, info["sql"], MH.PROJECT) for aid, info in MH.model().metrics.items() if info["sql"]]
    for aid, found in search_all(items, terms, events).items():
        for key, hit in found.items():
            text[key + (aid,)] = hit
        urls[aid] = MH.model().metrics[aid]["url"]
    log(f"  searched {len(items):,} metric-hub metrics")

    OUT.mkdir(parents=True, exist_ok=True)
    # Labels from earlier runs, keyed on what they judged. A labelled row that
    # is still a disagreement stays in the review set, so reviewing is never
    # undone by a rerun.
    held = held_labels("disagreements",
                       lambda r: (r["app"], r["metric"], r["artifact_id"], r["side"]))
    summary, dis = [], []
    # one random stream per source, so Looker's draws leave the others' alone
    rngs = {"sql": random.Random(args.seed), "looker": random.Random(args.seed + 3),
            "adhoc": random.Random(args.seed + 5), "metrichub": random.Random(args.seed + 7)}

    def pick(arts, side, k, rng):
        """Labelled ones first, then a seeded sample of the rest."""
        done = [a for a in arts if k + (a, side) in held]
        rest = [a for a in arts if k + (a, side) not in held]
        n = max(0, min(REVIEW_PER_GROUP, len(arts)) - len(done))
        return done + rng.sample(rest, min(n, len(rest)))
    for p, source in ((p, s) for p in picked for s in SOURCES):
        k = (p["app"], p["metric"])
        column = column_of(p)
        ps = {a for (ap_, m, a) in parsed if (ap_, m) == k and source_of(a) == source}
        ts = {a for (ap_, m, a) in text if (ap_, m) == k and source_of(a) == source}
        summary.append({
            "app": p["app"], "metric": p["metric"], "source": source, "stratum": p["stratum"],
            "parsed": len(ps), "text": len(ts), "both": len(ps & ts),
            "parser_only": len(ps - ts), "text_only": len(ts - ps),
            "bulk": bulk[k + (source,)],
        })
        po, to = sorted(ps - ts), sorted(ts - ps)
        po_pick = pick(po, "parser_only", k, rngs[source])
        to_pick = pick(to, "text_only", k, rngs[source])
        for a in po_pick:
            r = parsed[k + (a,)]
            v, note = held.get(k + (a, "parser_only"), ("", ""))
            dis.append({
                "app": k[0], "metric": k[1], "column": column, "artifact_id": a, "side": "parser_only",
                "stands_for": round(len(po) / len(po_pick), 2),
                "url": r["url"] or urls.get(a, ""),
                "evidence": f"role={r['role']} via={r['via']} "
                            f"cols={','.join((r['output_columns'] or [])[:3])} {r['evidence']}",
                "verdict": v, "note": note,
            })
        for a in to_pick:
            v, note = held.get(k + (a, "text_only"), ("", ""))
            dis.append({
                "app": k[0], "metric": k[1], "column": column, "artifact_id": a, "side": "text_only",
                "stands_for": round(len(to) / len(to_pick), 2),
                "url": urls.get(a, ""), "evidence": text[k + (a,)],
                "verdict": v, "note": note,
            })

    with open(OUT / "summary.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(summary[0]) if summary else ["empty"])
        w.writeheader()
        w.writerows(summary)
    dis_path = write_labels("disagreements", dis, DIS_FIELDS)

    for source in SOURCES:
        tot = collections.Counter()
        for s in summary:
            if s["source"] == source:
                for c in ("parsed", "text", "both", "parser_only", "text_only"):
                    tot[c] += s[c]
        log(f"\n{len(picked)} metrics, {source}: parser {tot['parsed']:,} artifacts, "
            f"text {tot['text']:,}, both {tot['both']:,}, parser only {tot['parser_only']:,}, "
            f"text only {tot['text_only']:,}")
    log(f"wrote {OUT / 'summary.csv'} and {dis_path} (and a timestamped copy)")


if __name__ == "__main__":
    run_deep(main)
