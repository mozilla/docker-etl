"""metric-hub: metrics defined by name, and the OpMon projects that use them.

    python -m metric_usage.metric_hub                 # parse, resolve, write the tables
    python -m metric_usage.metric_hub --dry-run       # everything but writing
    python -m metric_usage.metric_hub --dry-run --show firefox_desktop.active_hours

metric-hub (github.com/mozilla/metric-hub, a shallow clone in
.cache/metric_hub kept fresh by script/fetch_metric_hub.sh) defines metrics for
Jetstream, OpMon and Looker's generated explores: a select_expression over a
data source's from_expression, with Jinja aggregation helpers from
definitions/functions.toml. Each definition here becomes the SQL
`SELECT <select_expression> FROM <from_expression>` and goes through the same
resolver as a Redash query, so a metric-hub metric is credited with the Glean
metrics and probes it is built from. A depends_on metric (a ratio of others)
takes the union of what it depends on. `{dataset}` takes the data source's
default_dataset, and `{{experiment.*}}` and `{{parameters.*}}` render as
placeholders, since only the columns matter here.

Writes refs_metrichub and artifacts_metrichub:

  metric_hub_metric  every metric in definitions/, and the tool-specific ones
                     in opmon/ and looker/definitions and the Jetstream and
                     OpMon defaults where their SQL differs from definitions/
  opmon_project      every OpMon project active in the longest window, from
                     OpMon's own registry (operational_monitoring_derived.projects_v1),
                     which also covers the rollouts OpMon monitors without a
                     config file. A metric is named when the project's own
                     config lists it, and not named when it only comes from
                     OpMon's defaults. Its refs' seen_within is the shortest
                     window its dates overlap.

Jetstream experiments are written by adhoc.py, from the queries Jetstream ran;
it calls experiment_refs() here to tell a metric the experiment's config chose
from one only its platform's defaults compute. looker.py calls opmon_refs() for
the OpMon dashboards, which pick metrics by their metric-hub name.

Resolved SQL is cached under .cache until lineage is rebuilt, like looker.py
and adhoc.py; metric-hub is public, so its SQL may be cached too.
"""

import argparse
import collections
import hashlib
import json
import re
import tomllib

import jinja2

from .common import (CACHE, DEST, DISK_CACHE, LONGEST, PREFIX, WINDOWS, artifact_schema, client,
                     log, query, read_cache, ref_schema, union_views, window_end, window_start,
                     write_cache, write_table)

from . import redash as R

HUB = CACHE / "metric_hub"
GITHUB = "https://github.com/mozilla/metric-hub/blob/main/{path}"
EXPERIMENTER = "https://experimenter.services.mozilla.com/nimbus/{slug}/summary"
OPMON_DASHBOARD = "https://mozilla.cloud.looker.com/dashboards/operational_monitoring::{slug}"
# where an unqualified dataset.table in metric-hub SQL lives
PROJECT = "mozdata"
RESOLVED_VERSION = 1


def norm(slug):
    """A slug as it appears in table and Looker names: punctuation to _."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", slug or "").lower()


class Source:
    """One metric-hub file."""

    def __init__(self, path):
        self.path = path
        self.rel = path.relative_to(HUB).as_posix()
        self.text = path.read_text()
        self.data = tomllib.loads(self.text)

    def section(self, kind):
        return {k: v for k, v in (self.data.get(kind) or {}).items() if isinstance(v, dict)}

    def url(self, kind, name):
        """The file on GitHub, at the line defining kind.name if there is one."""
        m = re.search(rf"^\[{kind}\.{re.escape(name)}\]", self.text, re.M)
        line = self.text.count("\n", 0, m.start()) + 1 if m else None
        return GITHUB.format(path=self.rel) + (f"#L{line}" if line else "")


class _Placeholder(jinja2.ChainableUndefined):
    """{{experiment.start_date_str}}, {{parameters.id}} and helpers no file
    defines: rendered as 0, or as the expression a helper was given."""

    def __str__(self):
        return "0"

    def __call__(self, *args, **kwargs):
        return str(args[0]) if args else "0"


class Hub:
    def __init__(self, root=HUB):
        self.files = {p.relative_to(root).as_posix(): Source(p)
                      for p in sorted(root.glob("**/*.toml"))
                      if "lib/" not in p.relative_to(root).as_posix()}
        self.functions = {}
        for src in self.files.values():
            for name, f in src.section("functions").items():
                if "definition" in f:
                    self.functions[name] = f["definition"]
        self.env = jinja2.Environment(undefined=_Placeholder)

    def get(self, rel):
        return self.files.get(rel)

    def platforms(self):
        return sorted(r.split("/", 1)[1][:-5] for r in self.files
                      if r.startswith("definitions/") and r != "definitions/functions.toml")

    def render(self, text, dataset=None):
        if dataset:
            text = text.replace("{dataset}", dataset)
        if "{{" not in text and "{%" not in text:
            return text
        helpers = {n: (lambda d: lambda expr="": d.replace("{select_expr}", str(expr)))(d)
                   for n, d in self.functions.items()}
        try:
            return self.env.from_string(text).render(**helpers)
        except jinja2.TemplateError:
            return re.sub(r"\{\{.*?\}\}", "0", text, flags=re.S)

    @staticmethod
    def find(contexts, kind, name):
        """(source, definition) for kind.name in the first context that defines
        it; for a metric, the first that says how to compute it."""
        for src in contexts:
            if src is None:
                continue
            d = src.section(kind).get(name)
            if d is None:
                continue
            if kind != "metrics" or "select_expression" in d or "depends_on" in d:
                return src, d
        return None, None

    def sql(self, metric, contexts):
        """SELECT <select_expression> FROM <from_expression>, or (None, why)."""
        if "select_expression" not in metric:
            return None, "no select_expression"
        _, ds = self.find(contexts, "data_sources", metric.get("data_source"))
        if ds is None or "from_expression" not in ds:
            return None, f"unknown data source {metric.get('data_source')}"
        dataset = ds.get("default_dataset")
        frm = self.render(ds["from_expression"], dataset).strip()
        if not frm.startswith("(") and " " not in frm and "`" not in frm:
            frm = f"`{frm}`"
        return f"SELECT {self.render(metric['select_expression'], dataset)} AS value\nFROM {frm}", None


def contexts_for(hub, platform, *first):
    """The files a metric name is looked up in, most specific first: the given
    ones (an experiment's or OpMon project's own config, tool defaults), then
    definitions/<platform>.toml, which every tool falls back to."""
    return [*first, hub.get(f"definitions/{platform}.toml")]


# ---------------------------------------------------------------- resolving


def snapshot():
    return "|".join([str(RESOLVED_VERSION)]
                    + [str(client().get_table(f"{DEST}.{PREFIX}{t}").modified)
                       for t in ("metrics", "carries")])


class Resolved:
    """{sql: (named refs, note)} for metric SQL, resolved against the lineage
    the shapes come from and cached until it is rebuilt."""

    def __init__(self):
        self.path = CACHE / f"metric_hub_resolved_{hashlib.sha256(snapshot().encode()).hexdigest()[:16]}.json"
        self.held = json.loads(read_cache(self.path) or b"{}")
        self.loaded = False
        self.new = 0

    def __call__(self, sql):
        h = hashlib.sha256(sql.encode()).hexdigest()
        if h not in self.held:
            if not self.loaded:
                R.load_shapes()
                self.loaded = True
            try:
                named, _, note = R.analyse(sql, PROJECT)
            except RecursionError:
                named, note = {}, "resolve failed: RecursionError"
            self.held[h] = [{str(m): list(v) for m, v in named.items()}, note]
            self.new += 1
        named, note = self.held[h]
        return {int(m): tuple(v) for m, v in named.items()}, note

    def save(self):
        if self.new:
            if DISK_CACHE:
                for old in CACHE.glob("metric_hub_resolved_*.json"):
                    if old != self.path:
                        old.unlink()
            write_cache(self.path, json.dumps(self.held).encode())


def resolve_metric(hub, resolved, name, contexts, seen=()):
    """(named refs {metric id: (role, hops, via, transformed)}, note, source,
    definition) for a metric name looked up in contexts."""
    src, d = hub.find(contexts, "metrics", name)
    if d is None:
        return {}, "not defined", None, None
    if "select_expression" not in d:
        refs = {}
        for dep in d.get("depends_on") or []:
            if dep not in seen:
                got = resolve_metric(hub, resolved, dep, contexts, seen + (name,))[0]
                for m, v in got.items():
                    refs.setdefault(m, v)
        return refs, "depends_on", src, d
    sql, why = hub.sql(d, contexts)
    if sql is None:
        return {}, why, src, d
    refs, note = resolved(sql)
    return refs, note, src, d


# ----------------------------------------------------------------- the model

# Files holding metric definitions shared beyond one experiment or project,
# besides definitions/<platform>.toml, with how the page names them
TOOL_FILES = (("jetstream/definitions", "Jetstream"), ("jetstream/defaults", "Jetstream defaults"),
              ("jetstream/outcomes", "Jetstream outcome"), ("opmon/definitions", "OpMon"),
              ("opmon/defaults", "OpMon defaults"), ("looker/definitions", "Looker"))


def experiments(end):
    """{slug as it appears in a table name: experiment} from Experimenter's
    copy in moz-fx-data-experiments.monitoring. The SQL names the day, so the
    cached result is refreshed daily."""
    rows = query(f"""-- as of {end}
SELECT normandy_slug, experimenter_slug, app_name, start_date, end_date, outcomes
FROM `moz-fx-data-experiments.monitoring.experimenter_experiments_v1`
""", name="experiments")
    return {norm(r["normandy_slug"]): r for r in rows if r["normandy_slug"]}


def metric_lists(src):
    """The metric names an experiment or OpMon config lists: every list under
    [metrics] (daily, weekly, overall, preenrollment_*) or [project].metrics."""
    if src is None:
        return set()
    out = set()
    for v in (src.data.get("metrics") or {}).values():
        if isinstance(v, list):
            out.update(x for x in v if isinstance(x, str))
    out.update(x for x in (src.data.get("project") or {}).get("metrics") or [] if isinstance(x, str))
    return out


class Model:
    """Every shared metric-hub metric resolved, and what the experiments and
    OpMon projects using them read."""

    def __init__(self):
        self.hub = Hub()
        self.resolved = Resolved()
        self.metrics = {}   # artifact id -> info
        self.ids = {}       # (file, metric name) -> artifact id
        for platform in self.hub.platforms():
            src = self.hub.get(f"definitions/{platform}.toml")
            for name in src.section("metrics"):
                self._add(f"metric-hub:{platform}.{name}", f"{platform}.{name}", src, name,
                          contexts_for(self.hub, platform))
        for prefix, label in TOOL_FILES:
            for rel, src in self.files_under(prefix):
                platform = self.platform_of(rel)
                base = self.hub.get(f"definitions/{platform}.toml")
                for name, d in src.section("metrics").items():
                    if "select_expression" not in d and "depends_on" not in d:
                        continue
                    ctx = contexts_for(self.hub, platform, src)
                    # the same SQL as definitions/ is the same metric
                    _, bd = self.hub.find([base], "metrics", name)
                    if bd is not None and self.hub.sql(bd, [base])[0] == self.hub.sql(d, ctx)[0]:
                        self.ids[(rel, name)] = f"metric-hub:{platform}.{name}"
                        continue
                    what = f"{label} {rel.split('/')[-1][:-5]}" if prefix == "jetstream/outcomes" else label
                    self._add(f"metric-hub:{rel[:-5]}.{name}", f"{platform}.{name} ({what})",
                              src, name, ctx)
        self.resolved.save()
        self.configs = {norm(p.split("/")[-1][:-5]): self.hub.get(p) for p in self.hub.files
                        if p.count("/") == 1 and p.split("/")[0] in ("jetstream", "opmon")}

    def files_under(self, prefix):
        return [(rel, src) for rel, src in self.hub.files.items() if rel.startswith(prefix + "/")]

    def platform_of(self, rel):
        if rel.startswith("jetstream/outcomes/"):
            return rel.split("/")[2]
        stem = rel.split("/")[-1][:-5]
        # defaults like cfr_metrics.toml and messaging.toml are desktop's
        return stem if self.hub.get(f"definitions/{stem}.toml") else "firefox_desktop"

    def _add(self, aid, name, src, metric, contexts):
        refs, note, _, d = resolve_metric(self.hub, self.resolved, metric, contexts)
        self.ids[(src.rel, metric)] = aid
        self.metrics[aid] = {"name": name, "refs": refs, "note": note, "def": d or {},
                             "url": src.url("metrics", metric),
                             "sql": "\n".join(self.sqls(metric, contexts))}

    def sqls(self, name, contexts, seen=()):
        """The SQL a metric stands for, through depends_on, for accuracy.py."""
        _, d = self.hub.find(contexts, "metrics", name)
        if d is None:
            return []
        if "select_expression" in d:
            return [s for s in [self.hub.sql(d, contexts)[0]] if s]
        return [s for dep in d.get("depends_on") or [] if dep not in seen
                for s in self.sqls(dep, contexts, seen + (name,))]

    def via(self, src, name):
        """The artifact id a name resolves to, or None for a metric an
        experiment or project defines for itself."""
        return self.ids.get((src.rel, name)) if src is not None else None

    def refs_for(self, names, contexts):
        """{metric id: (hops, via)} for metric names looked up in contexts."""
        out = {}
        for name in sorted(names):
            src, _ = self.hub.find(contexts, "metrics", name)
            aid = self.via(src, name)
            refs = self.metrics[aid]["refs"] if aid in self.metrics else \
                resolve_metric(self.hub, self.resolved, name, contexts)[0]
            for m, (role, h, via, t) in refs.items():
                if m not in out or h + 1 < out[m][0]:
                    out[m] = (h + 1 if aid else h, aid or via)
        return out

    def experiment_refs(self, slug, experiment):
        """{metric id: (named, hops, via)} for an experiment: named when the
        metrics its config lists or defines, the outcomes chosen for it in
        Experimenter, its exposure signal or its enrollment query read the
        Glean metric, not named when only its platform's default metrics do.
        via is the metric-hub metric when there is one."""
        platform = (experiment or {}).get("app_name") or "firefox_desktop"
        config = self.configs.get(slug)
        if config is not None and not config.rel.startswith("jetstream/"):
            config = None
        outcomes = [o for o in (self.hub.get(f"jetstream/outcomes/{platform}/{x}.toml")
                                for x in (experiment or {}).get("outcomes") or []) if o]
        defaults = self.hub.get(f"jetstream/defaults/{platform}.toml")
        ctx = contexts_for(self.hub, platform, config, *outcomes, defaults,
                           self.hub.get(f"jetstream/definitions/{platform}.toml"))
        names = set()
        if config is not None:
            names = metric_lists(config) | {n for n, d in config.section("metrics").items()
                                            if "select_expression" in d or "depends_on" in d}
        for o in outcomes:
            names |= set(o.section("metrics"))
        chosen = self.refs_for(names, ctx)
        if config is not None:
            exp = config.data.get("experiment") or {}
            extra = [self.hub.sql(exp["exposure_signal"], ctx)[0]] if isinstance(
                exp.get("exposure_signal"), dict) else []
            if exp.get("enrollment_query"):
                extra.append(self.hub.render(exp["enrollment_query"]))
            for sql in filter(None, extra):
                for m, (role, h, via, t) in self.resolved(sql)[0].items():
                    chosen.setdefault(m, (h, via))
        default = self.refs_for(metric_lists(defaults), ctx)
        out = {m: (False, h, via) for m, (h, via) in default.items()}
        out.update({m: (True, h, via) for m, (h, via) in chosen.items()})
        return out

    def opmon_config(self, slug):
        config = self.configs.get(slug)
        return config if config is not None and config.rel.startswith("opmon/") else None

    def opmon_projects(self, end):
        """OpMon projects whose dates overlap the longest window, from OpMon's
        registry, with the metric names each summarizes and seen_within, the
        shortest window its dates overlap."""
        start = window_start(LONGEST, end)
        rows = query(f"""-- as of {end}
SELECT slug, name, start_date, end_date,
  ARRAY(SELECT DISTINCT s.metric FROM UNNEST(summaries) AS s WHERE s.metric IS NOT NULL) AS metrics
FROM `moz-fx-data-shared-prod.operational_monitoring_derived.projects_v1`
WHERE (end_date IS NULL OR end_date >= '{start}') AND (start_date IS NULL OR start_date <= '{end}')
""", name="opmon projects")
        for r in rows:
            r["seen_within"] = next(w for w in WINDOWS if r["end_date"] is None
                                    or r["end_date"] >= str(window_start(w, end)))
        return {norm(r["slug"]): r for r in rows}

    def opmon_refs(self, slug, names, platform):
        """{metric id: (named, hops, via)} for the given metric names of an
        OpMon project: named when the project's own config lists or defines
        the metric, not named when it comes from OpMon's defaults."""
        config = self.opmon_config(slug)
        platform = ((config.data.get("project") or {}).get("platform") if config else None) or platform
        ctx = contexts_for(self.hub, platform, config, self.hub.get(f"opmon/defaults/{platform}.toml"),
                           self.hub.get("opmon/defaults/rollout.toml"),
                           self.hub.get(f"opmon/definitions/{platform}.toml"))
        own = metric_lists(config) | (set(config.section("metrics")) if config else set())
        out = {}
        for name in sorted(names):
            for m, (h, via) in self.refs_for([name], ctx).items():
                held = out.get(m)
                named = name in own
                if held is None or (named and not held[0]) or (named == held[0] and h < held[1]):
                    out[m] = (named, h, via)
        return out


_MODEL = None


def model():
    """The Model, built once per process."""
    global _MODEL
    if _MODEL is None:
        _MODEL = Model()
    return _MODEL


# ---------------------------------------------------------------------- main


def run(show=None):
    end = window_end()
    inventory = R.load_shapes()
    mdl = model()
    exps = experiments(end)
    ref_rows, art_rows = [], []

    def ref(aid, kind, m, named, role, h, via, t=None):
        r = inventory[m]
        return {"app": r["app"], "metric": r["metric"], "artifact_id": aid,
                "artifact_kind": kind, "role": role, "hops": h + 1, "named": named,
                "transformed": t, "via": via, "output_columns": [], "n_output_columns": 0,
                "evidence": "parsed"}

    notes = collections.Counter()
    for aid, info in sorted(mdl.metrics.items()):
        d = info["def"]
        notes[info["note"].split(":")[0] if info["refs"] or info["note"] != "ok" else "reads no metric"] += 1
        for m, (role, h, via, t) in sorted(info["refs"].items()):
            ref_rows.append(ref(aid, "metric_hub_metric", m, True, role, h, via, t))
        owners = d.get("owner") or []
        art_rows.append({
            "artifact_id": aid, "kind": "metric_hub_metric", "name": info["name"],
            "url": info["url"], "owners": [owners] if isinstance(owners, str) else list(owners),
            "friendly_name": d.get("friendly_name"),
            "note": "deprecated" if d.get("deprecated") else None,
            "n_metrics_named": len(info["refs"]), "n_metrics_total": len(info["refs"]),
        })

    projects = mdl.opmon_projects(end)
    for slug, p in sorted(projects.items()):
        e = exps.get(slug)
        refs = mdl.opmon_refs(slug, p["metrics"], (e or {}).get("app_name") or "firefox_desktop")
        aid = f"opmon:{slug}"
        for m, (named, h, via) in sorted(refs.items()):
            # last_observed is the project's last day in the window
            ref_rows.append(dict(ref(aid, "opmon_project", m, named, "output", h, via),
                                 seen_within=p["seen_within"],
                                 last_observed=min(p["end_date"] or str(end), str(end))))
        art_rows.append({
            "artifact_id": aid, "kind": "opmon_project", "name": p["name"] or p["slug"],
            "url": OPMON_DASHBOARD.format(slug=slug),
            "friendly_name": f"{p['start_date'] or '?'} to {p['end_date'] or 'now'}",
            "note": "config in metric-hub" if mdl.opmon_config(slug) else None,
            "n_metrics_named": sum(1 for v in refs.values() if v[0]),
            "n_metrics_total": len(refs),
        })

    if show:
        for aid, info in mdl.metrics.items():
            if show in aid:
                log(f"\n{aid}  [{info['note']}]  {info['url']}")
                for m, v in sorted(info["refs"].items()):
                    log(f"    {inventory[m]['app']}:{inventory[m]['metric']}  via {v[2]}")
    log("\nmetric-hub metrics by outcome:")
    for k, n in notes.most_common():
        log(f"  {n:6,}  {k}")
    log(f"\n{len(mdl.metrics):,} metric-hub metrics, {len(projects):,} OpMon projects; "
        f"{len(ref_rows):,} refs over {len({(r['app'], r['metric']) for r in ref_rows}):,} metrics")
    return ref_rows, art_rows


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--show", help="print the refs of metric-hub metrics whose id contains this")
    args = ap.parse_args()
    if not HUB.exists():
        raise SystemExit(f"{HUB} is missing; run script/fetch_metric_hub.sh first")
    refs, arts = run(args.show)
    if args.dry_run:
        return
    suffix = ("Prototype table maintained by the docker-etl metric-usage job, metric_usage/metric_hub.py; "
              "replaced on each run.")
    write_table("refs_metrichub", refs, ref_schema(),
                "Metrics metric-hub definitions and OpMon projects are built from. " + suffix,
                clustering=["app", "metric"])
    write_table("artifacts_metrichub", arts, artifact_schema(),
                "metric-hub metric definitions and active OpMon projects. " + suffix,
                clustering=["artifact_id"])
    union_views()


if __name__ == "__main__":
    import sys

    sys.setrecursionlimit(20000)
    main()
