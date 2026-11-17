"""Shared helpers for the metric usage pipeline.

Everything is cached under .cache so a re-run only refetches what it is
told to. HTTP responses are keyed on the URL and query results on the SQL, so
editing a query refetches only that query. A query of this app's own tables
names them, and is cached until one of them is written again. Pass --force to a
script to ignore the cache, which is needed after the upstream data changes
underneath the same key.
"""

import collections
import csv
import datetime
import decimal
import hashlib
import json
import os
import pathlib
import re
import sys
import urllib.request

# The job's directory, which holds the cache and the labels, outside the package
ROOT = pathlib.Path(__file__).resolve().parent.parent
CACHE = ROOT / ".cache"
# Hand-labelled accuracy samples. Not in git, since they hold snippets of query
# text; see write_labels().
LABELS = ROOT / "labels"

# Where the app's tables live. quick.query can read mozdata, and so can anybody
# with warehouse access, so this is also where the tables are useful outside the
# app.
DEST = "mozdata.analysis"
# METRIC_USAGE_PREFIX gives a working copy its own set of tables, so a branch
# can be rebuilt without touching the ones the deployed page reads.
PREFIX = os.environ.get("METRIC_USAGE_PREFIX", "benwu_metrics_usage_")
if not PREFIX.startswith("benwu_metrics_usage_"):
    raise SystemExit("METRIC_USAGE_PREFIX has to start with benwu_metrics_usage_")

# The windows of use the page offers, in days, each ending yesterday, the last
# whole day. Sources read the longest once and count each window within it, so
# switching windows on the page needs no rebuild. The longest is capped by
# JOBS_BY_PROJECT, which keeps 180 days of job history (adhoc.py).
WINDOWS = (30, 90, 180)
LONGEST = max(WINDOWS)

PROBEINFO = "https://probeinfo.telemetry.mozilla.org"


def log(*args):
    print(*args, file=sys.stderr)


def window_end():
    return datetime.date.today() - datetime.timedelta(days=1)


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
    CACHE.mkdir(exist_ok=True)
    digest = hashlib.sha256(key.encode()).hexdigest()[:16]
    return CACHE / f"{kind}_{digest}.json"


def write_atomic(path, data):
    """Write through a temporary file, so an interrupted run never leaves a
    half-written cache entry that later runs would trust."""
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(data)
    tmp.replace(path)


def get_json(url, force=False):
    """GET a URL as JSON, cached on disk by URL. A body that is not JSON
    raises before it is cached."""
    path = _cache_file("http", url)
    if path.exists() and not force:
        return json.loads(path.read_text())
    req = urllib.request.Request(url, headers={"Accept-Encoding": "identity"})
    with urllib.request.urlopen(req, timeout=120) as r:
        body = r.read()
    value = json.loads(body)
    write_atomic(path, body)
    return value


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
    """Rows of a query as dicts, cached on disk by the SQL text.

    tables names the DEST.PREFIX tables the query reads, which are rewritten
    in place under the same SQL: the result is then cached until one of them
    is written again, and the copy for an older version is removed.
    """
    if tables:
        CACHE.mkdir(exist_ok=True)
        stem = f"bq_{hashlib.sha256(sql.encode()).hexdigest()[:16]}"
        version = hashlib.sha256(table_versions(tables).encode()).hexdigest()[:8]
        path = CACHE / f"{stem}_{version}.json"
    else:
        path = _cache_file("bq", sql)
    if path.exists() and not force:
        rows = json.loads(path.read_text())
        log(f"  {name}: {len(rows):,} rows (cached)")
        return rows
    rows = [{k: _jsonable(v) for k, v in dict(r).items()}
            for r in client().query(sql).result()]
    write_atomic(path, json.dumps(rows).encode())
    if tables:
        for old in CACHE.glob(f"{stem}_*.json"):
            if old != path:
                old.unlink()
    log(f"  {name}: {len(rows):,} rows")
    return rows


def write_table(name, rows, schema, description, clustering=None):
    """Replace DEST.PREFIX<name> with rows.

    schema is a list of bigquery.SchemaField. The table is replaced wholesale on
    every run, and the dataset's 180 day default expiration is left in place.
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
