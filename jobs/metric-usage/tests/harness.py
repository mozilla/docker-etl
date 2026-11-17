"""Helpers for resolver tests.

A test describes the tables a query reads by their column paths and the metric
each holds, resolves the query against them, and checks two things: what each
output column carries (Result.columns) and how the query uses each metric
(Result.uses). Metric ids are plain strings here; the pipeline uses inventory
row numbers, which the resolver treats the same way.
"""

import collections

from metric_usage import lineage as L

PROJECT = L.DEFAULT_PROJECT

# What one output column holds of one metric
Carried = collections.namedtuple("Carried", "named transformed via")
# How the query uses one metric: role is output or filter
Use = collections.namedtuple("Use", "role named transformed via")


def table_id(name):
    """Fully qualified, as the resolver normalizes table names."""
    return (name if name.count(".") == 2 else f"{PROJECT}.{name}").lower()


def table(name, columns=None, events_app=None, events_path="events", legacy_paths=()):
    """A stable table's shape from {column path: metric}, with the event
    identity of events_app seeded as stable_shapes() does: at events.category
    and events.name, or for legacy telemetry at the anonymous f1_ (category),
    f2_ (method) and f3_ (object) fields under each of legacy_paths."""
    tid = table_id(name)
    shape = {}
    for path, metric in (columns or {}).items():
        L.insert(shape, path.lower().split("."),
                 L.Leaf({metric: (0, f"{tid}:{path}", False, True)}))
    seeds = [(f"{events_path}.{kind}", kind) for kind in ("category", "name")] \
        if events_app and not legacy_paths else []
    seeds += [(f"{base}.{field}", L.LEGACY_EVENT_FIELD[field])
              for base in legacy_paths for field in ("f1_", "f2_", "f3_")]
    for path, kind in seeds:
        L.insert(shape, path.split("."),
                 L.Leaf({("ident", events_app, kind): (0, f"{tid}:{path}", False, True)}))
    return tid, shape


def events(app, *pairs):
    """An event index for resolve_events() over (category, name) pairs, and a
    map from its ids back to "category.name"."""
    inventory = [{"app": app, "type": "event", "event_category": c, "event_name": n}
                 for c, n in pairs]
    return L.event_index(inventory), {i: f"{c}.{n}" for i, (c, n) in enumerate(pairs)}


def legacy_events(app, *probes):
    """An event index over legacy probes given as (name, category, methods,
    objects), and a map from its ids back to the names."""
    inventory = [{"app": app, "source": "legacy", "event_category": c,
                  "event_methods": list(ms), "event_objects": list(os_)}
                 for _, c, ms, os_ in probes]
    return L.event_index(inventory), {i: p[0] for i, p in enumerate(probes)}


class Result:
    def __init__(self, out, touched, stars, names):
        self.out = out
        self.touched = touched
        self.stars = stars
        self._names = names

    def name(self, m):
        if isinstance(m, tuple):
            return f"{m[1]}:{L.IDENTITY_METRIC.get(m[2], m[2])}"
        return self._names.get(m, m)

    @property
    def columns(self):
        """{output column path: {metric: Carried}}"""
        return {".".join(path): {self.name(m): Carried(n, t, via)
                                 for m, (h, via, t, n) in leaf.items()}
                for path, leaf in L.output_paths(self.out)}

    def carried(self, path):
        """{metric: Carried} for one output column, {} if it holds nothing."""
        return self.columns.get(path, {})

    @property
    def reached(self):
        """{metric: named} for everything the query touched, including what it
        read in bulk or reached without naming, which uses leaves out."""
        return {self.name(m): v[3] for m, v in self.touched.items() if not isinstance(m, tuple)}

    @property
    def uses(self):
        """{metric: Use}, by the rule every source shares: a metric reaching an
        output column is an output, else a filter, and one read only in bulk
        and dropped is not a use. Event identity keys are left out."""
        reached = collections.defaultdict(list)
        for _, leaf in L.output_paths(self.out):
            for m, v in leaf.items():
                reached[m].append(v)
        found = {}
        for m, (h, via, t, n) in self.touched.items():
            if isinstance(m, tuple):
                continue
            outs = reached.get(m, [])
            if not outs and not n:
                continue
            found[self.name(m)] = Use("output" if outs else "filter", n,
                                      t or any(v[2] for v in outs), via)
        return found

    def shape_for(self, name):
        """This query's output as a table that later queries read."""
        tid = table_id(name)
        return tid, L.carried_shape(tid, self.out)


def regex(sql, *tables, events=None):
    """{metric: named} from the regex fallback, which reads SQL that does not
    parse: the tables come from the text as in Artifact, the metrics from
    column names."""
    art = type("Unparsed", (), {"sql": sql, "refs": L.text_table_refs(sql, PROJECT)})()
    idx, names = events if events is not None else (None, {})
    found = L.regex_refs(art, idx, dict(tables))
    return {names.get(m, m): v[3] for m, v in found.items()}


def resolve(sql, *tables, events=None, all_selects=False):
    """Resolve one query or script against tables from table(), shape_for()
    or a (table id, shape) pair. events is the pair from events()."""
    prelude, tree, note = L.parse_script(sql)
    assert tree is not None, f"did not parse: {note}"
    shapes = dict(tables)
    out, touched, stars, _ = L.resolve_script(prelude, tree, shapes, PROJECT,
                                              all_selects=all_selects)
    names = {}
    if events is not None:
        idx, names = events
        out = L.resolve_events_in(out, idx)
        touched = L.resolve_events(touched, idx)
    return Result(out, touched, stars, names)
