"""Build the metric inventory: every Glean metric and legacy probe, per app.

    python -m metric_usage.inventory            # fetch (cached), resolve, write the table
    python -m metric_usage.inventory --force    # refetch probeinfo and the field paths
    python -m metric_usage.inventory --dry-run  # resolve and print a summary, write nothing

Writes metrics_v1, one row per (app, metric).

The definitions come from probeinfo, the same source and the same merge rules the
Glean Dictionary uses, so a metric here and its Dictionary page agree on what it
is. An app is the Dictionary's app_name, which spans channels: fenix is five
probeinfo repositories and five stable datasets. Library metrics (glean-core,
gecko, the android-components libraries) are listed under every app that depends
on the library, because a fenix query reading a gecko metric is about fenix.

The warehouse side comes from INFORMATION_SCHEMA.COLUMN_FIELD_PATHS over the
*_stable datasets, which lists every nested field that exists. column_size_v1 is
incomplete for this, because it only measures fields that had data.

Each metric is resolved to the columns that hold it. Three outcomes:

  column    one or more stable-table fields hold it
  event     a Glean or legacy event, which is a row in an events array rather
            than a column, so queries name it by string literal
  missing   no field holds it: expired before it reached a schema, not sent in
            any ping we have a table for, or a naming rule we do not know yet

Legacy probes are listed under app "telemetry", the bq dataset family of the
legacy main ping.

Environment fields (environment.system.cpu.cores) are listed there too, with
type "environment". probeinfo does not know them, so they come from the main
ping's own schema, telemetry_stable.main_v5, with the description the schema
gives (about half have one) and no owners or expiry. A column is never dropped
from a stable table, so a field Firefox no longer sends is still listed, and
in_source is left null. A
repeated field (active_addons, user_prefs, gfx.adapters) is one entry, as a
labeled metric is: its elements are records keyed by add-on or pref, not fields
of their own.
"""

import argparse
import collections
import re
import sys

from .common import (
    APP_DESCRIPTION,
    METRIC_DESCRIPTION,
    PROBEINFO,
    get_json,
    get_json_if_found,
    log,
    query,
    snakecase,
    write_table,
)

GLEAN_ALL_PINGS = ("all-pings", "all_pings", "glean_client_info", "glean_internal_info")

FIELD_PATHS_SQL = """
SELECT
  table_schema AS dataset_id,
  table_name AS table_id,
  field_path,
  REGEXP_EXTRACT(data_type, r'^[A-Z0-9]+') AS kind,
  IF(STARTS_WITH(field_path, 'environment.'), description, NULL) AS description
FROM
  `moz-fx-data-shared-prod.region-us.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
WHERE
  ENDS_WITH(table_schema, '_stable')
  AND (
    STARTS_WITH(field_path, 'metrics.')
    OR STARTS_WITH(field_path, 'client_info.')
    OR STARTS_WITH(field_path, 'ping_info.')
    OR (table_schema = 'telemetry_stable' AND STARTS_WITH(field_path, 'payload.'))
    OR (table_schema = 'telemetry_stable' AND STARTS_WITH(field_path, 'environment.'))
  )
"""

LEGACY_PATH_RE = re.compile(
    r"^payload\.(?:processes\.[a-z_]+\.)?(?:keyed_)?(?:histograms|scalars)\.([a-z0-9_]+)$"
)
TABLE_VERSION_RE = re.compile(r"^(.*)_v\d+$")
# the table whose environment fields make the inventory
ENVIRONMENT_TABLE = "telemetry_stable.main_v5"

DICTIONARY = "https://dictionary.telemetry.mozilla.org/apps/{app}/metrics/{metric}"


# ----------------------------------------------------------------- warehouse


def field_index(force):
    """Index the stable-table field paths four ways.

    glean[(dataset, ping_table)][leaf] -> metrics.<type>.<leaf> paths
    info[(dataset, ping_table)][rest]  -> client_info / ping_info paths, keyed on
                                          the path after the prefix, which can
                                          be nested (attribution.campaign)
    legacy[leaf]                       -> (table, path) in telemetry_stable
    environment[path]                  -> (table, kind, description) in
                                          telemetry_stable, kind being the
                                          type's first word (STRUCT, ARRAY,
                                          STRING)

    ping_table is the table name without its version suffix, so a ping that moved
    from _v1 to _v2 still resolves; every version found is kept.
    """
    rows = query(FIELD_PATHS_SQL, force=force, name="stable field paths")
    glean = collections.defaultdict(lambda: collections.defaultdict(list))
    info = collections.defaultdict(lambda: collections.defaultdict(list))
    legacy = collections.defaultdict(list)
    environment = collections.defaultdict(list)
    for r in rows:
        ds, tbl, path = r["dataset_id"], r["table_id"], r["field_path"]
        m = TABLE_VERSION_RE.match(tbl)
        base = m.group(1) if m else tbl
        parts = path.split(".")
        table = f"{ds}.{tbl}"
        if parts[0] == "metrics" and len(parts) == 3:
            glean[(ds, base)][parts[2]].append((table, path))
        elif parts[0] in ("client_info", "ping_info"):
            info[(ds, base)][".".join(parts[1:])].append((table, path))
        elif ds == "telemetry_stable" and parts[0] == "environment":
            environment[path].append((table, r["kind"], r["description"]))
        elif ds == "telemetry_stable":
            m = LEGACY_PATH_RE.match(path)
            if m:
                legacy[m.group(1)].append((table, path))
    return glean, info, legacy, environment


def spellings(leaf):
    """Ways the schema may spell a column, most literal first.

    The schema transpiler collapses runs of underscores, so the use counter
    external__add_search_provider is the column external_add_search_provider.
    """
    collapsed = re.sub(r"_+", "_", leaf)
    return [leaf] if collapsed == leaf else [leaf, collapsed]


def lookup(index, keys):
    """The first spelling the index holds, as (spelling, hits)."""
    for k in keys:
        if k in index:
            return k, index[k]
    return keys[0], []


# --------------------------------------------------------------------- glean


def latest(history):
    """The current definition: the history entry most recently seen."""
    return max(history, key=lambda h: h["dates"]["last"])


def glean_apps(force):
    """app_name -> {label, deprecated, variants: [listing], dependencies: set}."""
    listings = get_json(f"{PROBEINFO}/v2/glean/app-listings", force)
    apps = {}
    for a in listings:
        app = apps.setdefault(
            a["app_name"],
            {
                "label": a.get("canonical_app_name") or a["app_name"],
                "deprecated": True,
                "variants": [],
                "dependencies": set(),
            },
        )
        app["variants"].append(a)
        # an app is deprecated only if every channel of it is
        app["deprecated"] = app["deprecated"] and bool(a.get("deprecated"))
        app["dependencies"].update(a.get("dependencies") or [])
    return apps


def glean_inventory(force, glean_idx, info_idx):
    libraries = {
        lib["dependency_name"]: lib
        for lib in get_json(f"{PROBEINFO}/v2/glean/library-variants", force)
    }
    out = []
    for app_name, app in sorted(glean_apps(force).items()):
        # Gather definitions from every channel and every library, keeping the
        # most recently seen definition for each name, as the Dictionary does.
        defs, pings = {}, set()
        sources = [(v["v1_name"], app_name) for v in app["variants"]]
        sources += [
            (libraries[d]["v1_name"], libraries[d]["library_name"])
            for d in sorted(app["dependencies"])
            if d in libraries
        ]
        for v1_name, origin in sources:
            # a variant probeinfo does not list is skipped; any other error
            # stops the step rather than write metrics with some missing
            metrics = get_json_if_found(f"{PROBEINFO}/glean/{v1_name}/metrics", force)
            got = get_json_if_found(f"{PROBEINFO}/glean/{v1_name}/pings", force)
            if metrics is None or got is None:
                log(f"  {app_name}: skipped {v1_name}: not in probeinfo")
                continue
            pings |= set(got)
            for name, d in metrics.items():
                held = defs.get(name)
                if (
                    held is None
                    or latest(d["history"])["dates"]["last"]
                    > latest(held[0]["history"])["dates"]["last"]
                ):
                    defs[name] = (d, origin)

        datasets = sorted({f"{v['bq_dataset_family']}_stable" for v in app["variants"]})
        for name, (d, origin) in sorted(defs.items()):
            out.append(
                glean_row(app_name, app, name, d, origin, datasets, pings, glean_idx, info_idx)
            )
    return out


def glean_row(app_name, app, name, d, origin, datasets, app_pings, glean_idx, info_idx):
    h = latest(d["history"])
    mtype = h["type"]
    send_in = list(h.get("send_in_pings") or [])
    prefix = (
        "client_info"
        if "glean_client_info" in send_in
        else "ping_info" if "glean_internal_info" in send_in else None
    )
    # Every ping table the app has, not just the ones the current definition
    # sends in: a metric moved out of a ping keeps its column there, and old
    # queries still read it (fenix's play_store_attribution_campaign in
    # first_session). A metric's name is unique within its app, so a column of
    # that name in any of the app's ping tables is this metric.
    pings = sorted(app_pings | set(send_in))
    leaf = snakecase(name)

    # client_info and ping_info metrics are named glean.internal.metrics.<x>,
    # and their column is <prefix>.<x>, which can be nested.
    if prefix:
        rest = name.split("glean.internal.metrics.", 1)[-1]
        keys = [".".join(snakecase(p) for p in rest.split("."))]
    else:
        keys = spellings(leaf)

    columns = []
    if mtype != "event":
        idx = info_idx if prefix else glean_idx
        for ds in datasets:
            for ping in pings:
                spelt, hits = lookup(idx.get((ds, snakecase(ping)), {}), keys)
                columns += hits
                if hits:
                    leaf = spelt.split(".")[-1]
    status = "event" if mtype == "event" else ("column" if columns else "missing")

    category, _, event_name = name.rpartition(".")
    mirror = h.get("telemetry_mirror")
    return {
        "app": app_name,
        "app_label": app["label"],
        "app_deprecated": app["deprecated"],
        "source": "glean",
        "metric": name,
        "origin": origin,
        "type": mtype,
        "description": (h.get("description") or "").strip(),
        "owners": list(h.get("notification_emails") or []),
        "expires": str(h.get("expires")) if h.get("expires") is not None else None,
        "in_source": bool(d.get("in-source")),
        "first_seen": min(x["dates"]["first"] for x in d["history"])[:10],
        "last_change": max(x["dates"]["last"] for x in d["history"])[:10],
        "send_in_pings": send_in,
        "tags": list((h.get("metadata") or {}).get("tags") or []),
        "data_sensitivity": list(h.get("data_sensitivity") or []),
        "column_leaf": leaf if mtype != "event" else None,
        "column_status": status,
        "columns": [{"table": t, "path": p} for t, p in sorted(set(columns))],
        "event_category": category if mtype == "event" else None,
        "event_name": event_name if mtype == "event" else None,
        "event_methods": [],
        "event_objects": [],
        "telemetry_mirror": mirror,
        "glean_mirrors": [],
        # the Dictionary's slug is the name, not the column spelling found above
        "dictionary_url": DICTIONARY.format(app=app_name, metric=snakecase(name)),
    }


# -------------------------------------------------------------------- legacy


def legacy_leaves(probe_id, name):
    """Candidate column leaves for a legacy probe, best guess first.

    The schema transpiler lowercases, flattens dots, splits camelCase into words
    (BackgroundRepeatX is background_repeat_x) and collapses repeated
    underscores, but older columns predate some of that, so each spelling is
    tried and the one that exists in the schema wins.
    """
    plain = name.lower().replace(".", "_")
    split = re.sub(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", "_", name)
    split = split.lower().replace(".", "_")
    out = []
    for cand in (plain, split):
        for s in spellings(cand):
            if s not in out:
                out.append(s)
    return out


def legacy_inventory(force, legacy_idx):
    probes = get_json(f"{PROBEINFO}/firefox/all/main/all_probes", force)

    # A probe still in the code is one the newest nightly still carries.
    def nightly_last(p):
        v = (p["history"].get("nightly") or [{}])[0].get("versions", {}).get("last")
        return int(v) if v and v.isdigit() else 0

    newest = max(nightly_last(p) for p in probes.values())
    out = []
    for probe_id, p in sorted(probes.items()):
        kind = p["type"]
        hist = (
            p["history"].get("nightly")
            or p["history"].get("beta")
            or next(iter(p["history"].values()))
        )
        h = hist[0]
        columns, leaf = [], None
        if kind in ("histogram", "scalar"):
            for cand in legacy_leaves(probe_id, p["name"]):
                if cand in legacy_idx:
                    leaf, columns = cand, legacy_idx[cand]
                    break
            leaf = leaf or legacy_leaves(probe_id, p["name"])[0]
        status = "event" if kind == "event" else ("column" if columns else "missing")
        # A legacy event probe is named <category>.<first method>#<event name>,
        # and its details list every method and object it can be recorded with.
        # Categories can contain dots, so the category is everything before
        # the last dot of the part before '#'.
        category = event_name = None
        methods, objects = [], []
        if kind == "event":
            head, _, event_name = p["name"].partition("#")
            category = head.rpartition(".")[0] or head
            det = h.get("details") or {}
            methods = list(det.get("methods") or [head.rpartition(".")[2]])
            objects = list(det.get("objects") or [])
        out.append(
            {
                "app": "telemetry",
                "app_label": "Legacy telemetry",
                "app_deprecated": False,
                "source": "legacy",
                "metric": p["name"],
                "origin": "telemetry",
                "type": kind + ("_keyed" if (h.get("details") or {}).get("keyed") else ""),
                "description": (h.get("description") or "").strip(),
                "owners": list(h.get("notification_emails") or []),
                "expires": h.get("expiry_version"),
                "in_source": nightly_last(p) == newest,
                "first_seen": (p.get("first_added") or {}).get("nightly", "")[:10] or None,
                "last_change": None,
                "send_in_pings": [],
                "tags": [],
                "data_sensitivity": [],
                "column_leaf": leaf,
                "column_status": status,
                "columns": [{"table": t, "path": pth} for t, pth in sorted(set(columns))],
                "event_category": category,
                "event_name": event_name or None,
                "event_methods": methods,
                "event_objects": objects,
                "telemetry_mirror": None,
                "glean_mirrors": [],
                "dictionary_url": (
                    DICTIONARY.format(app="firefox_legacy", metric=leaf) if leaf else None
                ),
            }
        )
    return out


def environment_inventory(env_idx):
    """One row per environment field of ENVIRONMENT_TABLE.

    A field is a non-struct path with no repeated field above it, so a
    repeated field is one row and nothing under it is listed. The columns are
    that path in every telemetry_stable table that has it, since the other
    legacy pings (event, crash, first_shutdown) carry the same environment.
    """
    kinds, descriptions = {}, {}
    for path, hits in env_idx.items():
        for table, kind, description in hits:
            if table == ENVIRONMENT_TABLE:
                kinds[path], descriptions[path] = kind, description
    out = []
    for path, kind in sorted(kinds.items()):
        parts = path.split(".")
        if kind == "STRUCT" or any(
            kinds.get(".".join(parts[:i])) == "ARRAY" for i in range(2, len(parts))
        ):
            continue
        out.append(
            {
                "app": "telemetry",
                "app_label": "Legacy telemetry",
                "app_deprecated": False,
                "source": "legacy",
                "metric": path,
                "origin": "telemetry",
                "type": "environment",
                "description": (descriptions[path] or "").strip(),
                "owners": [],
                "expires": None,
                "in_source": None,
                "first_seen": None,
                "last_change": None,
                "send_in_pings": [],
                "tags": [],
                "data_sensitivity": [],
                "column_leaf": parts[-1],
                "column_status": "column",
                "columns": [
                    {"table": t, "path": path} for t in sorted({hit[0] for hit in env_idx[path]})
                ],
                "event_category": None,
                "event_name": None,
                "event_methods": [],
                "event_objects": [],
                "telemetry_mirror": None,
                "glean_mirrors": [],
                "dictionary_url": None,
            }
        )
    return out


def link_mirrors(glean_rows, legacy_rows):
    """Point legacy probes and their Glean mirrors at each other.

    telemetry_mirror holds the legacy probe's C++ enum name: the histogram id
    (GC_MS) or the scalar name uppercased with dots as underscores. Lowercased,
    that is the probe's column leaf. An event mirror is
    Category_Method_Object in CamelCase (Aboutpreferences_Show_Click), which is
    matched against the legacy events' categories, methods and objects with
    case and separators ignored.
    """
    by_leaf = {}
    for r in legacy_rows:
        if (
            r["source"] == "legacy"
            and r["type"] != "environment"
            and not r["type"].startswith("event")
        ):
            for leaf in legacy_leaves(None, r["metric"]):
                by_leaf.setdefault(leaf, r)

    def flat(x):
        return re.sub(r"[^a-z0-9]", "", x.lower())

    by_event = {}
    for r in legacy_rows:
        if r["type"].startswith("event") and r.get("event_category"):
            for m in r.get("event_methods") or []:
                for o in r.get("event_objects") or []:
                    by_event.setdefault((flat(r["event_category"]), flat(m), flat(o)), r)
    linked = 0
    for g in glean_rows:
        mirror = g["telemetry_mirror"]
        if not mirror:
            continue
        if g["type"] == "event":
            parts = mirror.split("_")
            legacy = by_event.get(tuple(flat(x) for x in parts)) if len(parts) == 3 else None
        else:
            legacy = next((by_leaf[k] for k in spellings(mirror.lower()) if k in by_leaf), None)
        if legacy is None:
            continue
        g["telemetry_mirror"] = legacy["metric"]
        legacy["glean_mirrors"].append(f"{g['app']}:{g['metric']}")
        linked += 1
    return linked


# ---------------------------------------------------------------------- main


def schema():
    from google.cloud.bigquery import SchemaField as F

    s, b, rep = "STRING", "BOOL", "REPEATED"
    return [
        F("app", s, description=APP_DESCRIPTION),
        F("app_label", s, description="App name for display, from probeinfo"),
        F("app_deprecated", b, description="Every channel of the app is deprecated"),
        F("source", s, description="glean or legacy"),
        F("metric", s, description=METRIC_DESCRIPTION),
        F("origin", s, description="App or library that defines the metric"),
        F(
            "type",
            s,
            description="Glean metric type, legacy probe kind (histogram, scalar, event, with "
            "_keyed when keyed), or environment",
        ),
        F("description", s, description="The definition's description"),
        F("owners", s, mode=rep, description="Notification emails from the definition"),
        F(
            "expires",
            s,
            description="Expiry as defined: a date, a version, never, expired, or null",
        ),
        F(
            "in_source",
            b,
            description="Still in the code; null for environment fields, where it is not known",
        ),
        F(
            "first_seen",
            "DATE",
            description="First day probeinfo saw the definition; for a legacy probe, when it "
            "was added to Nightly",
        ),
        F(
            "last_change",
            "DATE",
            description="Last day the Glean definition changed; null for legacy",
        ),
        F("send_in_pings", s, mode=rep, description="Pings a Glean metric is sent in"),
        F("tags", s, mode=rep, description="Glean metadata tags"),
        F(
            "data_sensitivity",
            s,
            mode=rep,
            description="Glean data sensitivity categories",
        ),
        F(
            "column_leaf",
            s,
            description="Last segment of the column path; what a bare token would match",
        ),
        F("column_status", s, description="column, event, or missing"),
        F(
            "columns",
            "RECORD",
            mode=rep,
            fields=[
                F("table", s, description="dataset.table of a stable table"),
                F("path", s, description="Dotted path of the field in that table"),
            ],
            description=(
                "Stable-table fields holding the metric, table as dataset.table in "
                "moz-fx-data-shared-prod"
            ),
        ),
        F("event_category", s, description="For an event, its category"),
        F("event_name", s, description="For an event, its name within the category"),
        F(
            "event_methods",
            s,
            mode=rep,
            description="Legacy events: the methods it can be recorded with",
        ),
        F(
            "event_objects",
            s,
            mode=rep,
            description="Legacy events: the objects it can be recorded with",
        ),
        F("telemetry_mirror", s, description="For a Glean metric, the legacy probe it mirrors"),
        F(
            "glean_mirrors",
            s,
            mode=rep,
            description="For a legacy probe, app:metric of its Glean mirrors",
        ),
        F("dictionary_url", s, description="The metric's Glean Dictionary page"),
    ]


DESCRIPTION = (
    "Every Glean metric and legacy telemetry probe, one row per (app, metric), from "
    "probeinfo, with the stable-table fields that hold each one resolved against "
    "INFORMATION_SCHEMA.COLUMN_FIELD_PATHS. Library metrics are listed under every "
    "app that depends on the library. Maintained by "
    "the docker-etl metric-usage job, metric_usage/inventory.py; replaced on each run."
)


def summarize(rows):
    by = collections.Counter(
        ("environment" if r["type"] == "environment" else r["source"], r["column_status"])
        for r in rows
    )
    log(f"\n{len(rows):,} (app, metric) rows across " f"{len({r['app'] for r in rows})} apps")
    for (src, status), n in sorted(by.items()):
        log(f"  {src:11} {status:8} {n:7,}")
    live = [
        r
        for r in rows
        if r["source"] == "glean"
        and not r["app_deprecated"]
        and r["in_source"]
        and r["column_status"] == "missing"
    ]
    log(f"  in-source Glean metrics of live apps with no column: {len(live):,}")
    for t, n in collections.Counter(r["type"] for r in live).most_common(8):
        log(f"    {t:28} {n:6,}")


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--force", action="store_true", help="ignore the cache")
    ap.add_argument("--dry-run", action="store_true", help="write nothing")
    args = ap.parse_args()

    glean_idx, info_idx, legacy_idx, env_idx = field_index(args.force)
    glean = glean_inventory(args.force, glean_idx, info_idx)
    legacy = legacy_inventory(args.force, legacy_idx) + environment_inventory(env_idx)
    linked = link_mirrors(glean, legacy)
    rows = glean + legacy
    summarize(rows)
    log(f"  Glean metrics linked to a legacy mirror: {linked:,}")
    if not args.dry_run:
        write_table("metrics", rows, schema(), DESCRIPTION, clustering=["app", "metric"])


if __name__ == "__main__":
    sys.exit(main())
