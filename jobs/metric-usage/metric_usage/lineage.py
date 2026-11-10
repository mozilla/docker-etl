"""Multi-hop metric lineage over every query and view bigquery-etl deploys.

    python -m metric_usage.lineage                 # resolve everything and write the tables
    python -m metric_usage.lineage --dry-run       # resolve and summarize, write nothing
    python -m metric_usage.lineage --dry-run --only clients_daily_v6 --show 40
                                          # also print the edges of matching targets

Everything is resolved on every run; --only only picks what --show prints.

Source SQL is the generated-sql branch, a shallow clone in .cache/generated_sql
kept fresh by script/fetch_generated_sql.sh. That branch holds what is deployed,
generated and hand-written alike, which sql/ on main does not: the stable views,
events_stream and most of glean_usage only exist after `bqetl generate`.

Writes three tables:

  carries     (table, path, app, metric): which columns of which warehouse
              tables hold a metric, with how many hops from the stable table
  artifacts   one row per bigquery-etl table or view, with each table's last
              write and its reads per window from runs.py
  refs        (app, metric, artifact): a query or view that reads the metric,
              whether it reaches an output column or is only used in a filter or
              join, and whether the SQL names it or carries it in bulk

How a query is read
-------------------

query_graph/lineage_pass.py resolves columns by name, which turns
metrics.counter.foo into "metrics" and every metric in the table into one node.
This keeps the shape instead. Every expression evaluates to either a Leaf, the
set of metrics it depends on, or a Struct, a mapping of field names to shapes. A
table's shape is a tree built from the columns known to carry a metric, so
`metrics.*`, `STRUCT(...)`, `SELECT AS STRUCT` and `* EXCEPT / REPLACE` forward
individual metrics with their paths intact, and anything else (a function, an
operator, an aggregate) collapses its arguments into one Leaf.

Tables are processed in dependency order, so a derived table's carries are known
before anything that reads it, and a metric follows clients_daily into everything
built on top of it. A dependency cycle, which is mostly a self-referencing
incremental table reading its own previous day, is resolved repeatedly until
what it carries stops changing, up to MAX_CYCLE_PASSES; the passes each cycle
needed are logged. Every table the resolver actually reads is recorded, and a
bigquery-etl table read before it was resolved is reported as a dependency the
ordering missed.

Each metric carried by a Leaf records the hops from its stable table, the column
it was read from in the upstream table, whether this hop transformed it, and
whether the SQL named it. "Named" means a column reference reached the metric
itself or something inside it; reaching it through `*` or a whole struct is bulk.
A generated view that forwards every metric is real lineage and the page shows it
as plumbing, but it is not somebody choosing to use that metric.

Known limits, counted rather than hidden:
  * query.py files that build their SQL in Python, and scripts with control
    flow, are read by name and literal matching only (column names and quoted
    event names of the tables the text reads), so their refs carry no output
    column.
  * a query that pivots metrics into rows (GLAM, the use counter aggregates)
    turns column identity into data; the pivoted output is one Leaf.
  * event extra keys are not matched, deliberately: a query filtering an
    event's extras is credited to the event it names, but the key itself is
    not recorded (see Scope in README.md).
"""

import argparse
import collections
import re
import sys

import yaml

import jinja2
import sqlglot
from sqlglot import exp
from sqlglot.optimizer.scope import ScopeType, build_scope

from .common import (CACHE, DEST, PREFIX, PROBEINFO, artifact_schema, get_json, log,
                     query, ref_schema, snakecase, union_views, write_table)

from . import runs

import logging

# sqlglot logs every JSON path it cannot parse ($.values.0), which is noise here
logging.getLogger("sqlglot").setLevel(logging.ERROR)

# Functions that return one of their inputs, or an array of them, rather than
# computing something new: ARRAY_AGG(STRUCT(...) ORDER BY ... LIMIT 1)[OFFSET(0)]
# picks a whole row, and collapsing it would make every field depend on every
# other. Matched on sqlglot's class name, and on the name for UDFs.
VALUE_FUNCS = {"arrayagg", "anyvalue", "argmax", "argmin", "firstvalue", "lastvalue",
               "nthvalue", "arrayreverse", "arrayconcat", "arrayconcatagg", "first", "last",
               # JSON keeps the struct it was made from: TO_JSON(metrics) still
               # has metrics.counter.foo, now at $.counter.foo
               "jsonformat", "jsonstripnulls", "parsejson"}
VALUE_UDF_RE = re.compile(
    r"mode_(last|first)|^(first|last)_value|any_value|^from_(nested_)?map$|^json_strip_nulls$",
    re.I)

# JSON functions that read a path out of a JSON value
JSON_EXTRACT_FUNCS = ("JSONExtract", "JSONExtractScalar", "JSONExtractArray",
                      "JSONValueArray", "JSONExtractQuote")

# Glean events have no column of their own: a query picks one out by comparing
# the events array's category and name to string literals. Lineage marks where
# an event's identity is held with keys ("ident", app, "category" | "name"),
# seeded at events.category / events.name in every ping table and carried
# through queries like a metric, so events_stream.event_category is known to
# hold desktop's event categories. A comparison of such a column to a literal
# adds ("lit", app, kind, literal, mode); resolve_events() turns those into
# event metrics once a whole expression or query has been read.
IDENTITY_METRIC = {"category": "(event category)", "name": "(event name)",
                   # legacy telemetry events are category, method and object
                   "method": "(event method)", "object": "(event object)"}
# Legacy event fields are anonymous: f1_ category, f2_ method, f3_ object
LEGACY_EVENT_FIELD = {"f1_": "category", "f2_": "method", "f3_": "object"}
IDENTITY_KIND = {v: k for k, v in IDENTITY_METRIC.items()}

# A dependency cycle (self-referencing incremental tables, and a few mutual
# ones) is resolved repeatedly until its outputs stop changing. This caps it.
MAX_CYCLE_PASSES = 10

# A column fed by this many metrics or more is a candidate for narrowing by name.
# See narrow().
NARROW_MIN = 5

SQL_ROOT = CACHE / "generated_sql" / "sql"
DEFAULT_PROJECT = "moz-fx-data-shared-prod"
GITHUB = "https://github.com/mozilla/bigquery-etl/blob/generated-sql/sql/{path}"

# user-facing views are published to mozdata under the same dataset and name
PROJECT_ALIASES = {"mozdata": DEFAULT_PROJECT}

# GLAM pivots every metric into rows by design, so its lineage would be one Leaf
# per table holding everything. What people open in GLAM is its own source
# (glam.py).
EXCLUDE_PATHS = ("glam-fenix-dev", "moz-fx-glam-prod", "/glam_etl/")
# Legacy desktop GLAM is decommissioned. Its tables are still in bigquery-etl
# but unscheduled, so they are left out too. Matched against
# <project>/<dataset>/<table>.
LEGACY_GLAM_RE = re.compile(
    r"moz-fx-data-shared-prod/(?:"
    r"telemetry(?:_derived)?/(?:client_probe_counts"
    r"|clients_(?:daily_)?(?:keyed_)?(?:histogram|scalar|boolean)_(?:aggregates|bucket_counts|probe_counts)(?:_[a-z]+)?(?:_v\d+)?"
    r"|glam_extract_firefox_[a-z]+_v\d+|glam_sample_counts_v\d+)"
    r"|glam_derived/client_probe_counts_firefox_desktop_[a-z]+_v\d+)$"
)

TABLE_TEXT_RE = re.compile(r"`?((?:[a-z0-9-]+\.)?[a-z0-9_]+\.[a-z0-9_*]+)`?")
# For text that could not be parsed: a table name in backticks, or right after
# FROM or JOIN, but not Python's `from google.cloud import bigquery`.
# TABLE_TEXT_RE takes any dotted name, which in a query.py turns google.cloud
# and parser.parse_args into tables.
TABLE_REF_RE = re.compile(
    r"`([a-z0-9_-]+(?:\.[a-z0-9_*-]+){1,2})`"
    r"|\b(?:from|join)\s+([a-z0-9_-]+(?:\.[a-z0-9_*-]+){1,2})(?![\w.*-])(?!\s+import\b)",
    re.I,
)
GENERATED_RE = re.compile(r"Generated (?:via|by) `?\.?/?bqetl generate ([a-z_]+)", re.I)


# --------------------------------------------------------------------- shapes
#
# A Leaf maps a metric id to (hops, via, transformed, named). hops counts from
# the stable table. via is "table:path" of the column it was read from at the
# previous hop. A Struct is a plain dict of field name to shape.


class Leaf(dict):
    pass


EMPTY = Leaf()


def merge_into(acc, leaf):
    for mid, val in leaf.items():
        held = acc.get(mid)
        if held is None:
            acc[mid] = val
        else:
            # fewest hops wins; named and transformed are sticky
            best = val if val[0] < held[0] else held
            acc[mid] = (best[0], best[1], held[2] or val[2], held[3] or val[3])
    return acc


def flatten(shape):
    """Every metric anywhere in a shape, as one Leaf."""
    if isinstance(shape, Leaf):
        return shape
    acc = Leaf()
    stack = [shape]
    while stack:
        s = stack.pop()
        if isinstance(s, Leaf):
            merge_into(acc, s)
        elif isinstance(s, dict):
            stack.extend(s.values())
    return acc


def mark(leaf, transformed=None, named=None):
    if not leaf:
        return leaf
    return Leaf({m: (h, v, t if transformed is None else (t or transformed),
                     n if named is None else named)
                 for m, (h, v, t, n) in leaf.items()})


def bulk(shape):
    """The same shape with every metric marked as not named."""
    if isinstance(shape, Leaf):
        return mark(shape, named=False)
    return {k: bulk(v) for k, v in shape.items()}


def access(shape, parts, named=True):
    """Field access into a shape.

    Past a Leaf the path is inside one metric (`.sum`, `.values`, `.key`), so the
    Leaf is the answer. A path that stops at a Struct returns the Struct, and
    whatever uses it whole has not named the metrics inside it.

    The value comes back as it is. Whether reaching it means the query named a
    metric is decided by referenced(), which is a fact about the query rather
    than about the value.
    """
    for p in parts:
        if isinstance(shape, Leaf):
            break
        if p not in shape:
            return EMPTY
        shape = shape[p]
    return shape


def referenced(got, name=None):
    """What a column reference that resolved to `got` names, as a Leaf.

    A Leaf of one metric is named outright. A Leaf of several keeps each
    metric's flag from where the column was built: naming sync_enabled, built as
    regexp_contains(to_json_string(metrics.boolean), ...), is not naming each of
    the booleans behind it. A whole struct is read as whole() says; name is
    the last name in the path that reached it.
    """
    if isinstance(got, Leaf):
        return mark(got, named=True) if len(got) == 1 else got
    return whole(got, name)


def whole(shape, name=None):
    """A struct used whole (passed to a function, compared, selected), as one
    Leaf.

    A container of different metrics (metrics.boolean) names none of them. A
    struct of two or more fields that all hold the same metrics is those
    metrics' column in parts (addons.id and addons.version both hold the addon
    metrics), so it is read like a column: see referenced(). A Redash query
    counting addon ids through a JavaScript function given the whole addons
    column once named nothing. A struct named for a Glean type stays a
    container even then: the crash ping's metrics.datetime holds only
    crash.time, as its value and raw string.
    """
    flat = flatten(shape)
    fields = [set(leaf) for _, leaf in output_paths(shape)]
    if name not in GLEAN_TYPES and len(fields) >= 2 and all(f == set(flat) for f in fields):
        return mark(flat, named=True) if len(flat) == 1 else flat
    return mark(flat, named=False)


def insert(tree, parts, leaf):
    node = tree
    for p in parts[:-1]:
        nxt = node.get(p)
        if isinstance(nxt, Leaf):
            # a metric whose own column has subfields carried separately; keep
            # the metric and drop the deeper entry rather than corrupt the tree
            return
        node = node.setdefault(p, {})
    last = parts[-1]
    if isinstance(node.get(last), dict) and not isinstance(node.get(last), Leaf):
        return
    node[last] = merge_into(node.get(last) or Leaf(), leaf)


# ------------------------------------------------------------------ resolving


class Resolver:
    """Evaluates the shapes of one query, given the shapes of its tables."""

    def __init__(self, tables, project, reads=None, udfs=None):
        self.tables = tables          # normalized table id -> Struct
        self.project = project
        self.udfs = udfs or {}        # SQL function name -> (params, body)
        self.frames = []              # per computation in progress: half-built keys it hit
        self.waiting = {}             # half-built key -> provisional results to drop
        self._orphans = []
        self.bindings = []            # parameter shapes while a UDF body is evaluated
        self.binding_ids = []         # a unique number per UDF call, for cache keys
        self.call_seq = 0
        self.reads = reads if reads is not None else set()   # table ids looked up
        self.memo = {}
        self.active = set()
        self.touched = Leaf()         # every metric any column reference reached
        self.star_tables = set()      # base tables read with SELECT *

    # sources

    def table_shape(self, t):
        name = norm_table(t, self.project)
        self.reads.add(name)
        return self.tables.get(name, {})

    def source_shape(self, src, scope):
        if isinstance(src, exp.Table):
            return self.table_shape(src)
        expr = getattr(src, "expression", None)
        if isinstance(expr, exp.Unnest):
            # The element of the array: a Struct for an array of structs, else
            # whatever the array's own expression depends on. An unqualified
            # column inside the UNNEST is looked up through the parent scope,
            # which lists this same UNNEST as a source, so without the guard
            # `UNNEST(events) AS e` resolves itself forever.
            def compute():
                parts = [self.shape(e, src) for e in expr.expressions]
                if len(parts) == 1:
                    return parts[0]
                return mark(flatten({str(i): p for i, p in enumerate(parts)}),
                            transformed=True)
            # inside a UDF body the same UNNEST node is evaluated once per call,
            # each with different arguments, so the call is part of the key
            # (otherwise every mode_last(...) got the first call's value). The
            # call is a sequence number: id() of the bindings dict was reused
            # once a call's dict was freed, which handed one call another's
            # result and made runs differ.
            frame = self.binding_ids[-1] if self.binding_ids else None
            return self._guarded(("unnest", id(expr), frame), compute)
        if expr is not None:
            return self.outputs(src)
        return {}

    def lookup(self, parts, scope):
        """A column path resolved against the scope's sources, then its parents."""
        if self.bindings and parts[0] in self.bindings[-1]:
            return access(self.bindings[-1][parts[0]], parts[1:])
        while scope is not None:
            sources = {k.lower(): v for k, v in scope.sources.items()}
            if parts[0] in sources:
                whole = self.source_shape(sources[parts[0]], scope)
                # A bare name that is both a table's alias and one of its
                # columns is the column: in `FROM firefox_desktop.events,
                # UNNEST(events) AS event` it is the events array, not the row.
                if len(parts) == 1 and isinstance(sources[parts[0]], exp.Table) \
                        and isinstance(whole, dict) and not isinstance(whole, Leaf) \
                        and parts[0] in whole:
                    return whole[parts[0]]
                got = access(whole, parts[1:])
                # `FROM firefox_desktop.metrics` gives the table the implicit
                # alias `metrics`, the same name as its metrics column, and
                # metrics.counter.x means the column; so does `UNNEST(events)
                # AS events` for the table's events. So when the alias reading
                # finds nothing, fall through to reading it as a column.
                if got or len(parts) == 1:
                    return got
                fell_through = True
            else:
                fell_through = False
            # An unqualified name can only come from what this SELECT reads.
            # scope.sources also lists every CTE in the query, including ones
            # still being resolved, and searching those caches empty results.
            # Sources in FROM order, stopping at the first that carries
            # something under the name. A CTE lists every output column, so an
            # earlier source can have the name with nothing in it while a later
            # one holds the metric; stopping at the first name, not the first
            # value, lost most of mobile_search_clients_daily_v2's columns.
            empty = None
            for alias, src in self.selected(scope):
                # A column an alias hides is a table's (or CTE's), never an
                # UNNEST element's. Evaluating every UNNEST here as well sent
                # each t.x that carries nothing through all of its siblings:
                # a Looker query with 60 UNNESTs over clients_daily recomputed
                # them past ten minutes.
                if fell_through and isinstance(getattr(src, "expression", None), exp.Unnest):
                    continue
                s = self.source_shape(src, scope)
                if isinstance(s, dict) and not isinstance(s, Leaf) and parts[0] in s:
                    got = access(s, parts)
                    if got:
                        return got
                    empty = got if empty is None else empty
            if empty is not None:
                return empty
            # Only a correlated subquery or an UNNEST can see the enclosing
            # query's sources. A CTE or a subquery in FROM cannot, and climbing
            # out of one into the main query's FROM evaluated that FROM's UNNEST
            # while the CTE it reads was still half built, caching it empty.
            if scope.scope_type in (ScopeType.CTE, ScopeType.DERIVED_TABLE):
                break
            scope = scope.parent
        return EMPTY

    def selected(self, scope):
        """(alias, source) for what this SELECT reads in its FROM and JOINs.

        scope.selected_sources does this, but raises when two sources share an
        alias, which every SELECT with more than one unaliased UNNEST does (both
        alias to ""). This falls back to reading the FROM and JOIN nodes.
        """
        key = ("selected", id(scope))
        if key in self.memo:
            return self.memo[key]
        try:
            out = [(a, v[1] if isinstance(v, tuple) else v)
                   for a, v in scope.selected_sources.items()]
        except Exception:
            out = []
            select = scope.expression
            nodes = []
            frm = select.args.get("from") or select.args.get("from_")
            if frm is not None:
                nodes.append(frm.this)
            nodes += [j.this for j in select.args.get("joins") or []]
            by_expr = {id(c.expression): c for c in
                       scope.derived_table_scopes + scope.udtf_scopes}
            for n in nodes:
                if id(n) in by_expr:
                    out.append((n.alias_or_name, by_expr[id(n)]))
                elif isinstance(n, exp.Table):
                    name = n.alias_or_name
                    out.append((name, scope.sources.get(name, n)))
        self.memo[key] = out
        return out

    # expressions

    def shape(self, node, scope):
        if node is None:
            return EMPTY
        # IGNORE NULLS can wrap a whole ARRAY_AGG rather than its argument
        if isinstance(node, (exp.Alias, exp.Paren, exp.IgnoreNulls, exp.RespectNulls)):
            return self.shape(node.this, scope)
        if isinstance(node, exp.Dot) and isinstance(node.expression, exp.Func):
            # mozfun.stats.mode_last(...) is the call, qualified; a persistent
            # SQL UDF is expanded under its qualified name
            if isinstance(node.expression, exp.Anonymous):
                qual = dotted_names(node.this)
                key = self.udf_key(qual + [node.expression.name]) if qual is not None else None
                if key:
                    return self.call_function(node.expression, scope, key)
            return self.shape(node.expression, scope)
        if isinstance(node, exp.Column) and isinstance(node.this, exp.Func):
            # inside ARRAY_AGG(... IGNORE NULLS) sqlglot parses
            # mozfun.hist.extract(x) as a Column whose name is the call, so it
            # read as a column named extract and the argument was never seen
            qual = [node.args[k].name for k in ("catalog", "db", "table") if node.args.get(k)]
            key = self.udf_key(qual + [node.this.name]) \
                if isinstance(node.this, exp.Anonymous) else None
            if key:
                return self.call_function(node.this, scope, key)
            return self.shape(node.this, scope)
        if isinstance(node, (exp.EQ, exp.In, exp.Like, exp.ILike)) or (
                isinstance(node, exp.Case) and node.this is not None):
            got = self.event_literals(node, scope)
            if got is not None:
                return got
        if type(node).__name__ in JSON_EXTRACT_FUNCS:
            # JSON_VALUE(metrics, '$.uuid.legacy_telemetry_client_id') is field
            # access into whatever the JSON was built from, and names it
            base = self.shape(node.this, scope)
            keys = json_path_keys(node.args.get("expression"))
            if keys is not None and isinstance(base, dict) and not isinstance(base, Leaf):
                got = access(base, keys)
                merge_into(self.touched, referenced(got, keys[-1] if keys else None))
                return got
        if isinstance(node, exp.Anonymous):
            key = self.udf_key((node.name or "").split("."))
            if key:
                return self.call_function(node, scope, key)
        if type(node).__name__ in ("JSONSet", "JSONRemove") or (
                isinstance(node, exp.Anonymous) and (node.name or "").lower() in ("json_set", "json_remove")):
            got = self.json_edit(node, scope)
            if got is not None:
                return got
        if isinstance(node, exp.Array) and len(node.expressions) > 1 and \
                isinstance(node.expressions[0], exp.Struct):
            # [STRUCT("content" AS p, x AS events), ("dynamic", y), ...]: the
            # elements are one struct type, named by the first; later tuples
            # line up with it by position
            first = self.shape(node.expressions[0], scope)
            names = list(first) if isinstance(first, dict) and not isinstance(first, Leaf) else []
            merged = first
            for e in node.expressions[1:]:
                if isinstance(e, exp.Tuple) and names:
                    s = {n: self.shape(x, scope) for n, x in zip(names, e.expressions)}
                else:
                    s = self.shape(e, scope)
                merged = merge_shapes(merged, s)
            return merged
        if isinstance(node, exp.Func):
            got = self.value_struct(node, scope)
            if got is not None:
                return got
        if isinstance(node, (exp.Column, exp.Dot)):
            parts = path_parts(node)
            if parts:
                got = self.lookup(parts, scope)
                merge_into(self.touched, referenced(got, parts[-1]))
                return got
            if isinstance(node, exp.Dot):
                base = self.shape(node.this, scope)
                field = node.expression
                if isinstance(field, exp.Identifier):
                    return access(base, [field.name]) if isinstance(base, dict) \
                        and not isinstance(base, Leaf) else base
        if isinstance(node, exp.Struct):
            out = {}
            for i, e in enumerate(node.expressions):
                if isinstance(e, exp.PropertyEQ):
                    out[e.this.name.lower()] = self.shape(e.expression, scope)
                elif isinstance(e, exp.Alias):
                    out[e.alias.lower()] = self.shape(e.this, scope)
                else:
                    name = e.name.lower() if isinstance(e, exp.Column) else f"_field_{i + 1}"
                    out[name] = self.shape(e, scope)
            return out
        if isinstance(node, exp.Subquery) or isinstance(node, exp.Select):
            sub = self.scope_for(node, scope)
            if sub is None and self.bindings:
                # a subquery in a UDF body is not in the calling query's scope
                # tree, so it gets a scope of its own; kept referenced so its id
                # cannot be reused while the memo holds it
                target = node.this if isinstance(node, exp.Subquery) else node
                if isinstance(target, (exp.Select, exp.Union)):
                    sub = build_scope(target)
                    self._orphans.append(sub)
            if sub is not None:
                out = self.outputs(sub)
                select = sub.expression
                if isinstance(select, exp.Select) and select.args.get("kind") == "STRUCT":
                    return out
                if len(out) == 1:
                    return next(iter(out.values()))
                return out
        arms = self.branches(node)
        if arms is not None:
            # IF(cond, _current, _previous) is a whole row either way. When
            # every branch is a struct, merge them and keep the shape; the
            # condition is still evaluated so what it names counts as touched.
            shapes = [self.shape(a, scope) for a in arms if a is not None]
            structs = [x for x in shapes if isinstance(x, dict) and not isinstance(x, Leaf)]
            if structs and all(x in structs or not x for x in shapes):
                cond = node.this if isinstance(node, exp.If) else None
                if cond is not None:
                    self.shape(cond, scope)
                for i in (node.args.get("ifs") or []) if isinstance(node, exp.Case) else []:
                    self.shape(i.this, scope)
                merged = None
                for x in structs:
                    merged = merge_shapes(merged, x)
                return merged
            # Otherwise the value combines every branch and condition, as for
            # any other expression below, but from the branch shapes already
            # in hand: walking the children again re-evaluated each branch at
            # every level, so nested CASEs cost exponential time (a Looker
            # query of 49 CASEs ran past ten minutes).
            others = [node.this] if isinstance(node, exp.If) else \
                [node.this] + [i.this for i in node.args.get("ifs") or []] \
                if isinstance(node, exp.Case) else []
            acc = Leaf()
            names = [field_name(a) for a in arms if a is not None] + \
                [field_name(o) for o in others if o is not None]
            for got, name in zip(shapes + [self.shape(o, scope) for o in others if o is not None],
                                 names):
                if isinstance(got, dict) and not isinstance(got, Leaf):
                    merge_into(acc, whole(got, name))
                else:
                    merge_into(acc, got)
            return mark(acc, transformed=True)
        if isinstance(node, exp.Bracket):
            base = self.shape(node.this, scope)
            keys = [e.this.lower() for e in node.expressions
                    if isinstance(e, exp.Literal) and e.is_string]
            if keys and len(keys) == len(node.expressions) and isinstance(base, dict) \
                    and not isinstance(base, Leaf):
                # metrics['uuid']['x'] on a JSON column
                got = access(base, keys)
                merge_into(self.touched, referenced(got, keys[-1] if keys else None))
                return got
            # arr[OFFSET(0)] is an element of arr; the index is not a metric
            return base
        if isinstance(node, exp.Cast):
            return self.shape(node.this, scope)
        if isinstance(node, (exp.Literal, exp.Null, exp.Boolean, exp.Star)):
            return EMPTY
        # Anything else combines its inputs into one value. An input that is a
        # whole container (to_json_string(metrics.boolean)) is consumed without
        # any one field being chosen, so what comes out is not specific to
        # them; see whole().
        acc = Leaf()
        for child in node.iter_expressions():
            got = self.shape(child, scope)
            if isinstance(got, dict) and not isinstance(got, Leaf):
                merge_into(acc, whole(got, field_name(child)))
            else:
                merge_into(acc, got)
        return mark(acc, transformed=True)

    def event_literals(self, node, scope):
        """A comparison of an event identity column to string literals, as a
        Leaf holding ("lit", ...) keys, or None if it is not one.

        Handles col = 'x', col IN ('x', 'y'), col LIKE 'x%' and
        CASE col WHEN 'x' THEN ... END. Which part of an event a literal names
        follows from the column: event_category, event_name, or a column built
        from both (events_stream's event, 'category.name').
        """
        if isinstance(node, exp.Case):
            col, lits = node.this, [i.this for i in node.args.get("ifs") or []]
            mode = "eq"
        elif isinstance(node, exp.In):
            col, lits, mode = node.this, list(node.expressions), "eq"
        elif isinstance(node, (exp.Like, exp.ILike)):
            col, lits, mode = node.this, [node.expression], "like"
        else:
            a, b = node.this, node.expression
            col, lits = (a, [b]) if is_string(b) else (b, [a])
            mode = "eq"
        lits = [x.this for x in lits if is_string(x)]
        if not lits:
            return None
        colshape = flatten(self.shape(col, scope))
        idents = collections.defaultdict(dict)
        for k, v in colshape.items():
            if isinstance(k, tuple) and k[0] == "ident":
                idents[k[1]][k[2]] = v
        if not idents:
            return None
        out = Leaf(colshape)
        for app, kinds in idents.items():
            kind = "full" if len(kinds) >= 2 else next(iter(kinds))
            h, via, t, n = next(iter(kinds.values()))
            for lit in lits:
                out[("lit", app, kind, lit.lower(), mode)] = (h, via, True, True)
        if isinstance(node, exp.Case):
            for i in node.args.get("ifs") or []:
                merge_into(out, flatten(self.shape(i.args.get("true"), scope)))
            merge_into(out, flatten(self.shape(node.args.get("default"), scope)))
        merge_into(self.touched, out)
        return out

    def udf_key(self, parts):
        """The registered name for a function called as parts, or None.
        `moz-fx-data-shared-prod`.udf.f, udf.f and mozfun.map.f all resolve."""
        parts = [p.lower() for p in parts if p]
        for k in (".".join(parts), ".".join(parts[1:]), ".".join(parts[-2:]) if len(parts) > 2 else None):
            if k and k in self.udfs:
                return k
        return None

    def value_struct(self, node, scope):
        """What a function that returns its argument gives for a struct
        argument: the struct, with its other arguments (ORDER BY keys, the
        offset) still read. None when it is not one, or the argument is not a
        struct."""
        if not is_value_func(node):
            return None
        first = value_arg(node)
        got = self.shape(first, scope) if first is not None else EMPTY
        if not isinstance(got, dict) or isinstance(got, Leaf) or not got:
            return None
        for child in node.iter_expressions():
            if child is not first and not first_of(child, first):
                self.shape(child, scope)
        return {k: mark(v, transformed=True) if isinstance(v, Leaf) else v
                for k, v in got.items()}

    def call_function(self, node, scope, key):
        """A call to a SQL function, expanded, unless it is one that returns
        its argument and is given a struct. Expanding mozfun.stats.mode_last
        or mozfun.json.from_nested_map collapsed the struct (a GROUP BY on the
        value, JSON rebuilt from dynamic keys), which lost every labeled
        counter in events_stream's metrics."""
        got = self.value_struct(node, scope)
        if got is not None:
            return got
        return self.call_udf(node, scope, key)

    def call_udf(self, node, scope, key=None):
        """A SQL function's body, evaluated with its parameters bound to the
        arguments' shapes: temp functions defined in the script, and the
        persistent ones in bigquery-etl's udf/ and mozfun/. events_stream's
        metrics_to_json(TO_JSON(metrics)) is one; udf.deanonymize_event, which
        names the legacy event fields, is another. As black boxes they collapsed
        the structs passed in."""
        name = key or node.name.lower()
        params, body = self.udfs[name]
        key = ("udf", name)
        if key in self.active:
            return mark(flatten({str(i): self.shape(a, scope)
                                 for i, a in enumerate(node.expressions)}), transformed=True)
        args = [self.shape(a, scope) for a in node.expressions]
        self.active.add(key)
        self.bindings.append(dict(zip(params, args)))
        self.call_seq += 1
        self.binding_ids.append(self.call_seq)
        try:
            got = self.shape(body, scope)
        finally:
            self.bindings.pop()
            self.binding_ids.pop()
            self.active.discard(key)
        if isinstance(got, Leaf):
            return mark(got, transformed=True)
        return got

    def json_edit(self, node, scope):
        """JSON_SET(json, path, value, ...) and JSON_REMOVE(json, path, ...) on
        a JSON value built from a struct, applied to that struct's shape."""
        args = list(node.expressions) if isinstance(node, exp.Anonymous) else \
            [node.this] + list(node.expressions)
        if not args:
            return None
        base = self.shape(args[0], scope)
        if not isinstance(base, dict) or isinstance(base, Leaf):
            return None
        out = copy_shape(base)
        rest = args[1:]
        is_set = type(node).__name__ == "JSONSet" or \
            (isinstance(node, exp.Anonymous) and node.name.lower() == "json_set")
        if is_set:
            for path, value in zip(rest[0::2], rest[1::2]):
                keys = json_path_keys(path)
                if keys:
                    put(out, keys, self.shape(value, scope))
        else:
            for path in rest:
                keys = json_path_keys(path)
                if keys:
                    drop(out, keys)
        return out

    def struct_value(self, node, scope):
        """A value that is a whole row or struct, as a Struct where possible.

        The incremental *_last_seen queries pick a whole row with
        IF(_current.client_id IS NOT NULL, _current, _previous).* or pass both
        rows to a UDF and star the result. Either way the columns come through,
        so struct-valued arguments are merged rather than collapsed.
        """
        got = self.shape(node, scope)
        if isinstance(got, dict) and not isinstance(got, Leaf):
            return got
        call = node.expression if isinstance(node, exp.Dot) and \
            isinstance(node.expression, exp.Func) else node
        if isinstance(call, exp.Func):
            merged = None
            for arg in call.iter_expressions():
                s = self.shape(arg, scope)
                if isinstance(s, dict) and not isinstance(s, Leaf) and s:
                    merged = merge_shapes(merged, s)
            if merged is not None:
                return {k: mark(v, transformed=True) if isinstance(v, Leaf) else v
                        for k, v in merged.items()}
        return got

    def branches(self, node):
        """The value branches of IF, CASE and COALESCE, else None."""
        if isinstance(node, exp.If):
            return [node.args.get("true"), node.args.get("false")]
        if isinstance(node, exp.Case):
            return [i.args.get("true") for i in node.args.get("ifs") or []] + \
                [node.args.get("default")]
        if isinstance(node, exp.Coalesce):
            return [node.this] + list(node.expressions)
        return None

    def scope_for(self, node, scope):
        """The child scope sqlglot built for a subquery expression."""
        target = node.this if isinstance(node, exp.Subquery) else node
        for child in scope.subquery_scopes + scope.derived_table_scopes:
            if child.expression is target:
                return child
        # sqlglot sometimes files a correlated subquery one level away
        for child in scope.traverse():
            if child.expression is target:
                return child
        return None

    # scopes

    # Memoizing with cycle guards. A result computed while something it reads
    # is still being computed is incomplete, and caching it for good poisoned
    # every later read (the events_stream and main_events_v1 UNNESTs both came
    # out empty that way). Not caching it at all made some queries recompute
    # without end. So such a result is cached provisionally, tagged with the
    # half-built keys it hit, and dropped when those finish.

    def _guarded(self, key, compute):
        if key in self.memo:
            return self.memo[key]
        if key in self.active:
            for frame in self.frames:
                frame.add(key)
            return {}
        self.active.add(key)
        self.frames.append(set())
        try:
            out = compute()
        finally:
            self.active.discard(key)
            hit = self.frames.pop() - {key}
        self.memo[key] = out
        if hit:
            for k in hit:
                self.waiting.setdefault(k, []).append(key)
        # anything that was waiting on this key is stale now that it is done
        for k in self.waiting.pop(key, []):
            self.memo.pop(k, None)
        return out

    def outputs(self, scope):
        return self._guarded(id(scope), lambda: self._outputs(scope))

    def _outputs(self, scope):
        if scope.union_scopes:
            # BigQuery lines branches up by position, but a star over a table
            # only yields the columns known to carry a metric, so positions do
            # not survive it. A column with the same name in both branches is
            # matched by name; the rest by position.
            branches = [self.outputs(b) for b in scope.union_scopes]
            names = list(branches[0])
            merged = {n: branches[0][n] for n in names}
            for br in branches[1:]:
                for i, (name, val) in enumerate(br.items()):
                    if name in merged:
                        merged[name] = merge_shapes(merged[name], val)
                    elif i < len(names) and names[i] not in br:
                        merged[names[i]] = merge_shapes(merged[names[i]], val)
                    else:
                        # The first branch may not list it at all: a star over
                        # a table nothing builds yields no columns, as in
                        # telemetry.events_v1, whose first branch reads the old
                        # parquet-era events table.
                        merged[name] = val
            return merged

        select = scope.expression
        out = {}
        if not isinstance(select, exp.Select):
            return out
        for i, proj in enumerate(select.expressions):
            star = star_of(proj)
            if star is not None:
                self.expand_star(proj, star, scope, out)
                continue
            name = (proj.alias_or_name or "").lower()
            if name == "*":
                continue
            if not name:
                # An unaliased expression still produces a column (BigQuery
                # calls it f0_), and a scalar subquery's only column is usually
                # one: (SELECT SUM(x.value) FROM UNNEST(...) x).
                name = f"f{i}_"
            val = self.projection(proj, scope)
            out[name] = merge_shapes(out.get(name), val) if name in out else val

        # columns used anywhere else in the select (WHERE, JOIN, GROUP BY, and
        # inside subqueries) still count as touched
        for clause in ("where", "joins", "group", "having", "qualify"):
            arg = select.args.get(clause)
            nodes = arg if isinstance(arg, list) else [arg]
            if clause == "group" and arg is not None:
                # GROUP BY install_source means the SELECT's install_source
                # when there is one, even if a FROM table has a column of that
                # name; the alias's expression was read above
                nodes = [n for n in arg.expressions
                         if not (isinstance(n, exp.Column) and not n.table
                                 and n.name.lower() in out)]
                for k, v in arg.args.items():  # ROLLUP, CUBE, GROUPING SETS
                    if k != "expressions" and isinstance(v, (exp.Expression, list)):
                        nodes += v if isinstance(v, list) else [v]
            for node in nodes:
                if node is not None:
                    self.shape(node, scope)
        # JOIN ... USING (client_id) reads the column on both sides, and its
        # names are bare identifiers rather than columns, so they are looked up
        # in every source here; an unqualified client_id in the SELECT list
        # resolves to only one of them.
        for j in select.args.get("joins") or []:
            for ident in j.args.get("using") or []:
                name = (ident.name or "").lower()
                for _, src in self.selected(scope):
                    got = access(self.source_shape(src, scope), [name])
                    if got:
                        merge_into(self.touched, referenced(got, name))
        return out

    def projection(self, proj, scope):
        """The shape of one SELECT item, keeping a struct a function rebuilds.

        The stable views write `udf.normalize_main_payload(payload) AS payload`
        and `mozfun.norm.glean_client_info_attribution(client_info, ...) AS
        client_info`. As ordinary functions those collapse every field into one
        value, which credited every reader of telemetry.main with all 3,240
        probes and every reader of a Glean ping view with every client_info
        field. A function whose first argument is a struct, written back under
        that struct's own name, is taken to return the same struct.
        """
        value = self.shape(proj, scope)
        if not isinstance(value, Leaf) or not isinstance(proj, exp.Alias):
            return value
        # Follow the first argument down through nested calls: the baseline
        # views write glean_client_info_attribution(
        #   glean_baseline_client_info(client_info, metrics), ...) AS client_info,
        # and stopping at the outer call collapsed client_info into every
        # metric in the ping, which then reached every downstream client_id.
        first = proj.this
        depth = 0
        while depth < 8:
            call = first
            if isinstance(call, exp.Dot) and isinstance(call.expression, exp.Func):
                call = call.expression
            if not isinstance(call, exp.Func):
                break
            args = list(call.expressions) if call.expressions else \
                [a for a in call.iter_expressions()]
            if not args:
                return value
            first = args[0]
            depth += 1
        if depth == 0:
            return value
        parts = path_parts(first)
        if not parts or parts[-1] != proj.alias.lower():
            return value
        inner = self.lookup(parts, scope)
        if isinstance(inner, dict) and not isinstance(inner, Leaf) and inner:
            return {k: mark(v, transformed=True) if isinstance(v, Leaf) else v
                    for k, v in inner.items()}
        return value

    def expand_star(self, proj, star, scope, out):
        if isinstance(proj, exp.Dot):
            fields = self.struct_value(proj.this, scope)
            if not isinstance(fields, dict) or isinstance(fields, Leaf):
                return
        elif isinstance(proj, exp.Column) and proj.table:
            parts = path_parts(proj, drop_star=True)
            lowered = {k.lower(): v for k, v in scope.sources.items()}
            if parts and parts[0] in lowered and len(parts) == 1:
                src = lowered[parts[0]]
                if isinstance(src, exp.Table):
                    self.star_tables.add(norm_table(src, self.project))
                fields = self.source_shape(src, scope)
            else:
                fields = self.lookup(parts, scope) if parts else {}
        else:
            fields = {}
            for alias, src in self.selected(scope):
                if isinstance(src, exp.Table):
                    self.star_tables.add(norm_table(src, self.project))
                s = self.source_shape(src, scope)
                if isinstance(s, Leaf):
                    fields[(alias or "").lower()] = s
                elif isinstance(s, dict):
                    for k, v in s.items():
                        fields.setdefault(k, v)
        if isinstance(fields, Leaf):
            return
        excluded = {e.name.lower() for e in (star.args.get("except_") or star.args.get("except") or [])}
        replaced = {}
        for r in star.args.get("replace") or []:
            if isinstance(r, exp.Alias):
                replaced[r.alias.lower()] = self.projection(r, scope)
        for k, v in fields.items():
            if k in excluded:
                continue
            # Forwarded unchanged. That this query did not name these is
            # recorded in touched below; the values themselves are as specific
            # as they were upstream, and downstream readers depend on that.
            out[k] = replaced[k] if k in replaced else v
        merge_into(self.touched, mark(flatten({k: v for k, v in fields.items()
                                                if k not in excluded}), named=False))


def is_string(node):
    return isinstance(node, exp.Literal) and node.is_string


def event_index(inventory):
    """app -> {"full": {"cat.name": mid}, "cat": {cat: [mid]}, "name": {name: [mid]}}
    for the Glean events in the inventory."""
    idx = collections.defaultdict(lambda: {"full": {}, "cat": collections.defaultdict(list),
                                           "name": collections.defaultdict(list)})
    for mid, r in enumerate(inventory):
        if r.get("source") == "legacy" and r.get("event_category"):
            # legacy: (category, methods, objects) per probe
            e = idx[r["app"]]
            e.setdefault("legacy", []).append((
                r["event_category"].lower(),
                {m.lower() for m in r.get("event_methods") or []},
                {o.lower() for o in r.get("event_objects") or []}, mid))
        elif r.get("type") == "event" and r.get("event_category") and r.get("event_name"):
            c, n = r["event_category"].lower(), r["event_name"].lower()
            e = idx[r["app"]]
            e["full"][f"{c}.{n}"] = mid
            e["cat"][c].append(mid)
            e["name"][n].append(mid)
    return idx


def like_re(pattern):
    return re.compile("^" + re.escape(pattern).replace("%", ".*").replace("_", ".") + "$")


def resolve_events(leaf, events):
    """The Leaf with its ("lit", ...) keys turned into the event metrics they
    pick out, and the literal keys dropped. Identity keys are kept so the column
    still carries them downstream.

    A full name, or a category and a name that exist together, names the event.
    A name alone names it when only one category has it. A category alone, or a
    LIKE pattern, reaches every event it matches without naming any of them.
    """
    if not any(isinstance(k, tuple) and k[0] == "lit" for k in leaf):
        return leaf
    out = Leaf({k: v for k, v in leaf.items() if not (isinstance(k, tuple) and k[0] == "lit")})
    by_app = collections.defaultdict(lambda: collections.defaultdict(list))
    for k, v in leaf.items():
        if isinstance(k, tuple) and k[0] == "lit":
            _, app, kind, lit, mode = k
            by_app[app][(kind, mode)].append((lit, v))
    for app, groups in by_app.items():
        e = events.get(app)
        if not e:
            continue
        found = {}

        def hit(mid, v, named):
            held = found.get(mid)
            if held is None or (named and not held[3]):
                found[mid] = (v[0], v[1], True, named)

        if e.get("legacy"):
            resolve_legacy(e["legacy"], groups, hit)
            merge_into(out, Leaf(found))
            continue
        cats = groups.get(("category", "eq"), [])
        names = groups.get(("name", "eq"), [])
        for lit, v in groups.get(("full", "eq"), []):
            if lit in e["full"]:
                hit(e["full"][lit], v, True)
        if cats and names:
            for c, v in cats:
                for n, _ in names:
                    if f"{c}.{n}" in e["full"]:
                        hit(e["full"][f"{c}.{n}"], v, True)
        if names and not cats:
            for n, v in names:
                mids = e["name"].get(n, [])
                for mid in mids:
                    hit(mid, v, len(mids) == 1)
        if cats and not names:
            for c, v in cats:
                for mid in e["cat"].get(c, []):
                    hit(mid, v, False)
        for (kind, mode), items in groups.items():
            if mode != "like":
                continue
            space = e["full"] if kind == "full" else e["cat"] if kind == "category" else e["name"]
            for pat, v in items:
                rx = like_re(pat)
                for key, mids in space.items():
                    if rx.match(key):
                        for mid in (mids if isinstance(mids, list) else [mids]):
                            hit(mid, v, False)
        merge_into(out, Leaf(found))
    return out


def resolve_legacy(probes, groups, hit):
    """Legacy events: probes whose category, methods and objects fit the
    literals. A category with a method or object names every probe that fits
    (address / detected picks out both address_form and address_form_ext). A
    method or object alone names its probe only if just one has it. A category
    alone, or a LIKE, reaches its probes without naming them."""
    lits = {k: {lit for lit, _ in groups.get((k, "eq"), [])} for k in ("category", "method", "object")}
    any_v = next((v for items in groups.values() for _, v in items), None)
    for lit, v in groups.get(("full", "eq"), []):
        for cat, methods, objects, mid in probes:
            if any(lit in (f"{cat}.{m}", f"{cat}.{m}.{o}") for m in methods for o in (objects or {""})):
                hit(mid, v, True)
    C, M, O = lits["category"], lits["method"], lits["object"]
    if C or M or O:
        fits = [mid for cat, methods, objects, mid in probes
                if (not C or cat in C) and (not M or methods & M) and (not O or objects & O)]
        named = (bool(C) and bool(M or O)) or (not C and len(fits) == 1)
        for mid in fits:
            hit(mid, any_v, named)
    for (kind, mode), items in groups.items():
        if mode != "like":
            continue
        for pat, v in items:
            rx = like_re(pat)
            for cat, methods, objects, mid in probes:
                values = {"category": {cat}, "method": methods, "object": objects}.get(kind, {cat})
                if any(rx.match(x) for x in values):
                    hit(mid, v, False)


def dotted_names(node):
    """Identifiers of a Dot chain like `moz-fx-data-shared-prod`.udf, or None."""
    if isinstance(node, exp.Identifier):
        return [node.name]
    if isinstance(node, exp.Column):
        return [p.name for p in node.parts]
    if isinstance(node, exp.Dot):
        left = dotted_names(node.this)
        right = dotted_names(node.expression)
        if left is not None and right is not None:
            return left + right
    return None


_PERSISTENT = None


def persistent_udfs():
    """name -> (params, body) for every SQL UDF bigquery-etl deploys.

    Each is registered as dataset.name and project.dataset.name, which with
    udf_key covers mozfun.map.get_key, udf.deanonymize_event and the backticked
    `moz-fx-data-shared-prod.udf.f`. JavaScript UDFs have a string body and
    are skipped; they stay black boxes.
    """
    global _PERSISTENT
    if _PERSISTENT is not None:
        return _PERSISTENT
    out = {}
    for f in sorted(SQL_ROOT.glob("*/*/*/udf.sql")):
        project = f.relative_to(SQL_ROOT).parts[0]
        try:
            statements = sqlglot.parse(f.read_text(), read="bigquery")
        except Exception:
            continue
        for st in statements:
            if not (isinstance(st, exp.Create) and (st.args.get("kind") or "").upper() == "FUNCTION"
                    and isinstance(st.this, exp.UserDefinedFunction)
                    and st.expression is not None and not isinstance(st.expression, exp.Literal)):
                continue
            t = st.this.this
            name, db = (t.name, t.db) if isinstance(t, exp.Table) else (st.this.name, "")
            params = [p.name.lower() for p in st.this.expressions]
            for key in (f"{db}.{name}", f"{project}.{db}.{name}"):
                out[key.lower()] = (params, st.expression)
    _PERSISTENT = out
    return out


def json_path_keys(node):
    """Keys of a JSON path ('$.uuid.x', '$["uuid"].x', a parsed JSONPath), or
    None when the path is not a literal. Array subscripts are skipped: an
    element of a metric is still that metric."""
    if isinstance(node, exp.JSONPath):
        return [k.this.lower() for k in node.expressions
                if isinstance(k, exp.JSONPathKey) and isinstance(k.this, str)]
    if isinstance(node, exp.Literal) and node.is_string:
        text = node.this
        if not text.startswith("$"):
            return None
        return [(a or b).lower() for a, b in
                re.findall(r"\[\s*['\"]([^'\"]+)['\"]\s*\]|\.([A-Za-z0-9_]+)", text)]
    return None


def copy_shape(shape):
    if isinstance(shape, Leaf) or not isinstance(shape, dict):
        return shape
    return {k: copy_shape(v) for k, v in shape.items()}


def put(tree, keys, value):
    node = tree
    for k in keys[:-1]:
        nxt = node.get(k)
        if not isinstance(nxt, dict) or isinstance(nxt, Leaf):
            nxt = node[k] = {}
        node = nxt
    node[keys[-1]] = value


def drop(tree, keys):
    node = tree
    for k in keys[:-1]:
        node = node.get(k)
        if not isinstance(node, dict) or isinstance(node, Leaf):
            return
    node.pop(keys[-1], None)


def is_value_func(call):
    if type(call).__name__.lower() in VALUE_FUNCS:
        return True
    # ARRAY(SELECT AS STRUCT ...) and a one-element [x] hold their element
    if isinstance(call, exp.Array) and len(call.expressions) == 1:
        return True
    return isinstance(call, exp.Anonymous) and bool(VALUE_UDF_RE.search(call.name or ""))


def value_arg(call):
    """The argument a value function returns, past ORDER BY, IGNORE NULLS, LIMIT."""
    # a UDF (exp.Anonymous) keeps its name as a string in `this`
    first = call.this if isinstance(call.this, exp.Expression) else \
        (call.expressions or [None])[0]
    while isinstance(first, (exp.IgnoreNulls, exp.RespectNulls, exp.Order, exp.Limit,
                             exp.HavingMax)):
        first = first.this
    if isinstance(first, exp.Distinct):
        first = (first.expressions or [None])[0]
    return first


def first_of(child, first):
    """Whether child wraps first (the modifiers value_arg looked through)."""
    node = first
    while node is not None and node is not child:
        node = node.parent
    return node is child


def merge_shapes(a, b):
    if a is None:
        return b
    if isinstance(a, dict) and not isinstance(a, Leaf) and \
            isinstance(b, dict) and not isinstance(b, Leaf):
        out = dict(a)
        for k, v in b.items():
            out[k] = merge_shapes(out.get(k), v) if k in out else v
        return out
    acc = Leaf()
    merge_into(acc, flatten(a))
    merge_into(acc, flatten(b))
    return acc


def star_of(proj):
    if isinstance(proj, exp.Star):
        return proj
    if isinstance(proj, exp.Column) and isinstance(proj.this, exp.Star):
        return proj.this
    # IF(_current.client_id IS NOT NULL, _current, _previous).*
    if isinstance(proj, exp.Dot) and isinstance(proj.expression, exp.Star):
        return proj.expression
    return None


def path_parts(node, drop_star=False):
    """Identifiers of a Column or a Dot chain of identifiers, else None."""
    # BigQuery column names are case-insensitive (SUM(DAU) reads dau), so every
    # name is compared lowercased: paths here, output and field names, aliases.
    if isinstance(node, exp.Column):
        parts = [p.name.lower() for p in node.parts]
        if drop_star:
            parts = [p for p in parts if p != "*"]
        return [p for p in parts if p] or None
    if isinstance(node, exp.Dot):
        left = path_parts(node.this, drop_star)
        right = node.expression
        if left and isinstance(right, exp.Identifier):
            return left + [right.name.lower()]
    return None


def field_name(node):
    """The last name in a column path (metrics.datetime gives datetime), else
    None."""
    parts = path_parts(node)
    return parts[-1] if parts else None


def norm_table(t, project):
    """project.dataset.table for a table reference, or the bare name for a CTE."""
    if isinstance(t, exp.Table):
        cat, db, name = t.catalog, t.db, t.name
    else:
        cat, db, name = t
    if not db:
        return name
    cat = cat or project
    return f"{PROJECT_ALIASES.get(cat, cat)}.{db}.{name}"


# ---------------------------------------------------------------- the corpus


class Artifact:
    def __init__(self, path):
        self.path = path
        rel = path.relative_to(SQL_ROOT)
        self.project, self.dataset, self.table = rel.parts[:3]
        self.kind = {"query.sql": "bqetl_table", "view.sql": "bqetl_view",
                     "query.py": "bqetl_table"}[path.name]
        self.id = f"{PROJECT_ALIASES.get(self.project, self.project)}.{self.dataset}.{self.table}"
        self.url = GITHUB.format(path=rel.as_posix())
        self.sql = None
        self.tree = None
        self.prelude = []
        self.note = None
        self.generated_by = None
        self.owners, self.description = [], None
        meta = path.parent / "metadata.yaml"
        if meta.exists():
            try:
                m = yaml.safe_load(meta.read_text()) or {}
                self.owners = [o for o in (m.get("owners") or []) if isinstance(o, str)]
                self.description = m.get("friendly_name") or None
            except yaml.YAMLError:
                pass

    def load(self):
        text = self.path.read_text()
        g = GENERATED_RE.search(text[:400])
        self.generated_by = g.group(1) if g else None
        if self.path.name == "query.py":
            # Many query.py files only run a SQL file kept beside them
            # (events_stream_v1 runs query_supplemental.sql). Read that if so.
            m = re.search(r'Path\(__file__\)\.parent\s*/\s*"([\w.]+\.sql)"', text)
            sibling = self.path.parent / m.group(1) if m else None
            if sibling is None or not sibling.exists():
                self.sql, self.note = text, "python, read by regex"
                return
            text = sibling.read_text()
            self.note = f"python, SQL from {sibling.name}"
        if "{{" in text or "{%" in text:
            try:
                text = render(text)
            except Exception as e:
                self.note = f"render failed: {type(e).__name__}"
        self.sql = text
        self.prelude, self.tree, note = parse_script(text)
        self.note = self.note or note

    def referenced_tables(self):
        if self.tree is not None:
            # the temp tables a script builds first read tables too, and those
            # are dependencies as much as anything the final SELECT reads
            trees = [t for _, t in self.prelude] + [self.tree]
            ctes = {c.alias for tr in trees for c in tr.find_all(exp.CTE)}
            out = set()
            for tr in trees:
                for t in tr.find_all(exp.Table):
                    n = norm_table(t, self.project)
                    if "." in n and n not in ctes:
                        out.add(n)
            return out
        return text_table_refs(self.sql or "", self.project)


def text_table_refs(text, project):
    """Table ids named in text that could not be parsed, by TABLE_REF_RE."""
    out = set()
    for m in TABLE_REF_RE.finditer(COMMENT_RE.sub(" ", text)):
        parts = (m.group(1) or m.group(2)).lower().split(".")
        if len(parts) == 3:
            out.add(norm_table(tuple(parts), project))
        elif len(parts) == 2 and not parts[0].isdigit():
            out.add(norm_table((None, parts[0], parts[1]), project))
    return out


# bigquery-etl's templates are almost all `{% if is_init() %}`, which picks the
# incremental query over the one that builds the table from scratch; anything
# else undefined renders empty.
_JINJA = jinja2.Environment(undefined=jinja2.ChainableUndefined)
_JINJA.globals["is_init"] = lambda: False


def render(text):
    return _JINJA.from_string(text).render()


def alter_sql_for_sqlglot(sql):
    """bigquery_etl.util.common's fix for UDFs named true, false or null, which
    sqlglot misparses (https://github.com/tobymao/sqlglot/issues/3332)."""
    return re.sub(r"\.(true|false|null)\(", r".`\1`(", sql, flags=re.IGNORECASE)


def parse_script(text):
    """(prelude, tree, note): temp tables built along the way, then the result.

    Most files open with CREATE TEMP FUNCTION statements and only then the
    query, so the result is the last statement that selects something, or the
    SELECT inside a CREATE VIEW / CREATE TABLE AS. A script that builds temp
    tables first and selects from them at the end needs those too, or nothing
    it read reaches the result: prelude is [(name, select)] for every CREATE
    TABLE AS before the last statement, in order. The plain SELECTs before the
    last are kept on prelude.selects; a Redash query runs all of them.
    """
    if re.search(r"^\s*DECLARE\b", text, flags=re.MULTILINE | re.I):
        # Scripts declare variables and then query. Without the DECLARE and
        # SET statements the rest usually parses, and a variable left in it is
        # an unresolved name, which carries nothing.
        text = re.sub(r"^\s*(DECLARE|SET)\b[^;]*;", "", text, flags=re.MULTILINE | re.I)
        # IF( at the start of a SELECT line is the function, not IF ... THEN
        if re.search(r"^\s*(IF\b(?!\s*\()|BEGIN\b|LOOP\b|WHILE\b|FOR\b|EXECUTE\b)", text,
                     flags=re.MULTILINE | re.I):
            return [], None, "script, read by regex"
    try:
        statements = sqlglot.parse(alter_sql_for_sqlglot(text), read="bigquery")
    except Exception as e:
        return [], None, f"parse failed, read by regex: {type(e).__name__}"
    found = []
    udfs = {}
    for st in statements:
        # SQL temp functions are kept so calls to them can be expanded; a
        # JavaScript one has a string body and stays a black box
        if isinstance(st, exp.Create) and (st.args.get("kind") or "").upper() == "FUNCTION" \
                and isinstance(st.this, exp.UserDefinedFunction) \
                and not isinstance(st.expression, exp.Literal) and st.expression is not None:
            params = [p.name.lower() for p in st.this.expressions]
            # the name is a Table node inside the UserDefinedFunction
            fname = st.this.this.name if isinstance(st.this.this, exp.Expression) else st.this.name
            udfs[fname.lower()] = (params, st.expression)
            continue
        # CREATE TEMP TABLE t AS (SELECT ...) wraps the body in a Subquery
        if isinstance(st, exp.Create) and isinstance(st.expression, exp.Subquery):
            st.set("expression", st.expression.this)
        if isinstance(st, exp.Create) and isinstance(st.expression, (exp.Select, exp.Union)):
            target = st.this.this if isinstance(st.this, exp.Schema) else st.this
            name = target.name if isinstance(target, exp.Table) else None
            found.append((name, st.expression))
        elif isinstance(st, (exp.Select, exp.Union, exp.Subquery)):
            found.append((None, st))
    if not found:
        return [], None, "no query statement, read by regex"
    prelude = Prelude((n, t) for n, t in found[:-1] if n)
    prelude.udfs = udfs
    prelude.selects = [t for n, t in found[:-1] if not n]
    return prelude, found[-1][1], None


class Prelude(list):
    """Temp tables in build order, plus the script's SQL temp functions and
    the plain SELECTs before the last statement."""
    udfs = {}
    selects = []


def resolve_script(prelude, tree, shapes, project, reads=None, all_selects=False):
    """(outputs, touched, star_tables), resolving temp tables in order first.

    reads, if given, collects every table id the resolver looked up. With
    all_selects the SELECTs before the last statement are read too and their
    outputs merged in, as for a Redash query, which runs every statement. A
    bigquery-etl table gets only the last one's output, so it leaves them out.
    """
    tables = collections.ChainMap({}, shapes)
    touched = Leaf()
    stars = set()
    for name, sel in prelude:
        root = build_scope(sel)
        if root is None:
            continue
        r = Resolver(tables, project, reads, {**persistent_udfs(), **getattr(prelude, "udfs", {})})
        tables.maps[0][name] = r.outputs(root)
        touch_all(r, root)
        merge_into(touched, r.touched)
        stars |= r.star_tables
    extra = []
    for sel in getattr(prelude, "selects", []) if all_selects else []:
        root = build_scope(sel)
        if root is None:
            continue
        r = Resolver(tables, project, reads, {**persistent_udfs(), **getattr(prelude, "udfs", {})})
        extra.append(r.outputs(root))
        touch_all(r, root)
        merge_into(touched, r.touched)
        stars |= r.star_tables
    root = build_scope(tree)
    if root is None:
        return {}, touched, stars
    r = Resolver(tables, project, reads, {**persistent_udfs(), **getattr(prelude, "udfs", {})})
    out = r.outputs(root)
    touch_all(r, root)
    merge_into(touched, r.touched)
    for e in extra:
        out = merge_shapes(e, out)
    return out, touched, stars | r.star_tables


def shapes_from_tables():
    """(inventory, shapes) rebuilt from the tables lineage.py wrote.

    For a consumer outside bigquery-etl (a Redash query, a Looker view) a
    derived table's shape is its carried columns, each a Leaf of the metrics it
    holds, with via pointing at that column.
    """
    # These tables are rewritten in place, so the reads are cached by when
    # they were last written. Metric ids are row positions, and looker.py
    # caches results by id, so the order has to be the same on every read.
    inventory = query(f"SELECT app, metric, source, type, columns, event_category, event_name, "
                      f"event_methods, event_objects "
                      f"FROM `{DEST}.{PREFIX}metrics` ORDER BY app, metric",
                      tables=["metrics"], name="inventory")
    shapes = stable_shapes(inventory)
    mid = {(r["app"], r["metric"]): i for i, r in enumerate(inventory)}
    for app in {r["app"] for r in inventory}:
        for kind, name in IDENTITY_METRIC.items():
            mid[(app, name)] = ("ident", app, kind)
    rows = query(f"SELECT `table`, path, app, metric, hops, named "
                 f"FROM `{DEST}.{PREFIX}carries`", tables=["carries"], name="carries")
    for r in rows:
        m = mid.get((r["app"], r["metric"]))
        if m is None:
            continue
        insert(shapes.setdefault(r["table"], {}), r["path"].split("."),
               Leaf({m: (r["hops"], f"{r['table']}:{r['path']}", False, bool(r["named"]))}))
    return inventory, shapes


def corpus():
    files = []
    for name in ("query.sql", "view.sql", "query.py"):
        files += SQL_ROOT.glob(f"*/*/*/{name}")
    out = []
    for f in sorted(files):
        s = str(f)
        if any(x in s for x in EXCLUDE_PATHS):
            continue
        if LEGACY_GLAM_RE.search(str(f.parent.relative_to(SQL_ROOT))):
            continue
        out.append(Artifact(f))
    return out


def topo_order(arts):
    """Artifacts ordered so producers come before consumers, by Tarjan's SCCs."""
    by_id = {a.id: a for a in arts}
    # sorted, so cycles are entered in the same order on every run
    deps = {a.id: [d for d in sorted(a.refs) if d in by_id and d != a.id] for a in arts}
    index, low, stack, on, order = {}, {}, [], set(), []
    counter = [0]

    def visit(v):
        index[v] = low[v] = counter[0]
        counter[0] += 1
        stack.append(v)
        on.add(v)
        for w in deps[v]:
            if w not in index:
                visit(w)
                low[v] = min(low[v], low[w])
            elif w in on:
                low[v] = min(low[v], index[w])
        if low[v] == index[v]:
            comp = []
            while True:
                w = stack.pop()
                on.discard(w)
                comp.append(w)
                if w == v:
                    break
            order.append(comp)

    for v in deps:
        if v not in index:
            visit(v)
    return [[by_id[i] for i in comp] for comp in order]


# ------------------------------------------------------------------- driving


DICTIONARY_PING = "https://dictionary.telemetry.mozilla.org/apps/{app}/pings/{ping}"


def ping_pages():
    """(dataset, view) -> Glean Dictionary ping page, for the generated ping views.

    The stable_views generator writes one view per ping into each channel's
    dataset (org_mozilla_firefox.metrics) and, for apps with several channels,
    a union view into the app's own dataset (fenix.metrics). Its SQL is
    boilerplate; the Dictionary page says what the ping is and what is in it.
    """
    from .inventory import glean_apps

    libraries = {lib["dependency_name"]: lib for lib in
                 get_json(f"{PROBEINFO}/v2/glean/library-variants")}
    pages = {}
    for app_name, app in glean_apps(False).items():
        sources = [v["v1_name"] for v in app["variants"]]
        sources += [libraries[d]["v1_name"] for d in app["dependencies"] if d in libraries]
        pings = set()
        for v1 in sources:
            try:
                pings |= set(get_json(f"{PROBEINFO}/glean/{v1}/pings"))
            except Exception:
                continue
        datasets = {app_name} | {v["bq_dataset_family"] for v in app["variants"]}
        for ping in pings:
            for ds in datasets:
                pages[(ds, snakecase(ping))] = DICTIONARY_PING.format(app=app_name, ping=ping)
    return pages


EVENT_FIELDS_SQL = r"""
SELECT table_schema AS dataset_id, table_name AS table_id, field_path
FROM `moz-fx-data-shared-prod.region-us.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
WHERE ENDS_WITH(table_schema, '_stable')
  AND (
    field_path IN ('events.category', 'events.name')
    -- legacy: main ping payload.processes.<p>.events, event ping payload.events.<p>
    OR (table_schema = 'telemetry_stable'
        AND REGEXP_CONTAINS(field_path, r'^payload\.(processes\.[a-z_]+\.events|events\.[a-z_]+)\.f[123]_$'))
  )
"""


def stable_shapes(inventory, force=False):
    """Table shapes for the stable tables, from the inventory's resolved columns,
    plus the event identity keys at events.category and events.name."""
    shapes = collections.defaultdict(dict)
    for mid, r in enumerate(inventory):
        for c in r["columns"] or []:
            table = f"{DEFAULT_PROJECT}.{c['table']}"
            insert(shapes[table], c["path"].split("."),
                   Leaf({mid: (0, f"{c['table']}:{c['path']}", False, True)}))
    from .inventory import glean_apps
    app_of = {f"{v['bq_dataset_family']}_stable": name
              for name, app in glean_apps(False).items() for v in app["variants"]}
    app_of["telemetry_stable"] = "telemetry"
    # lineage.py refreshes this; the scripts reading its tables reuse that copy
    for r in query(EVENT_FIELDS_SQL, force=force, name="event fields"):
        app = app_of.get(r["dataset_id"])
        if app is None:
            continue
        table = f"{DEFAULT_PROJECT}.{r['dataset_id']}.{r['table_id']}"
        kind = r["field_path"].split(".")[-1]
        kind = LEGACY_EVENT_FIELD.get(kind, kind)
        insert(shapes[table], r["field_path"].split("."),
               Leaf({("ident", app, kind): (0, f"{r['dataset_id']}.{r['table_id']}:{r['field_path']}",
                                            False, True)}))
    # *_live tables have the same schema as their stable twin and are read by
    # the near-real-time queries
    for table in list(shapes):
        project, dataset, name = table.split(".")
        if dataset.endswith("_stable"):
            shapes[f"{project}.{dataset[:-len('_stable')]}_live.{name}"] = shapes[table]
    return shapes


COMMENT_RE = re.compile(r"/\*.*?\*/|--[^\n]*|#[^\n]*", re.S)
GLEAN_TYPES = {"boolean", "counter", "custom_distribution", "datetime", "dual_labeled_counter",
               "labeled_boolean", "labeled_counter", "labeled_custom_distribution",
               "labeled_memory_distribution", "labeled_quantity", "labeled_string",
               "labeled_timing_distribution", "memory_distribution", "object", "quantity",
               "rate", "string", "string_list", "text", "timespan", "timing_distribution",
               "url", "uuid", "jwe", "labeled_rate", "metrics"}
WORD_RE = re.compile(r"[a-z_][a-z0-9_]*")


def regex_refs(art, inventory_index, shapes):
    """Metrics an unparseable file names, by column name. Used where the SQL
    cannot be parsed (scripts with control flow, syntax sqlglot rejects).

    A metric counts if the text, comments removed, contains as a word the name
    of a column holding it in a table the text names, and that column holds
    fewer than NARROW_MIN metrics: a rollup column's name says nothing about any
    one metric in it. A column named for a Glean type (events_stream's JSON
    metrics.labeled_counter) is a container, and a nested field with a plain
    name (client_info.distribution.name) needs its parent's name in the text
    too, or every e.name = '...' would match it; UNNEST(addons) AS addon ...
    addon.id still does. A word that appears only after AS is the query's own
    name (metrics.uuid.legacy_client_id AS client_id), not a column it reads.
    """
    text = COMMENT_RE.sub(" ", (art.sql or "").lower())
    aliased = collections.Counter(re.findall(r"\bas\s+`?([a-z_][a-z0-9_]*)", text))
    words = {w for w, n in collections.Counter(WORD_RE.findall(text)).items() if n > aliased[w]}
    quoted = set(re.findall(r"['\"]([^'\"\n]+)['\"]", text))
    found = Leaf()
    for tbl in sorted(art.refs):
        shape = shapes.get(tbl)
        if not shape:
            continue
        for path, leaf in output_paths(shape):
            if path[-1] not in words or len(leaf) >= NARROW_MIN or path[-1] in GLEAN_TYPES:
                continue
            if len(path) > 1 and "_" not in path[-1] and path[-2] not in words:
                continue
            for mid, (h, v, t, n) in leaf.items():
                if isinstance(mid, tuple):
                    # an event identity column: its events are matched below
                    if mid[0] == "ident" and inventory_index is not None:
                        e = inventory_index.get(mid[1])
                        # legacy: a quoted category with a quoted method or
                        # object, fitting every kind quoted for that category
                        # as resolve_legacy does: with 'install' quoted as a
                        # method, addonsManager.disable does not fit
                        legacy = e.get("legacy", []) if e else []
                        m_q = {c: set().union(*[ms for cc, ms, _, _ in legacy if cc == c]) & quoted
                               for c in {p[0] for p in legacy} & quoted}
                        o_q = {c: set().union(*[os_ for cc, _, os_, _ in legacy if cc == c]) & quoted
                               for c in m_q}
                        for cat, methods, objects, emid in legacy:
                            if cat in m_q and (m_q[cat] or o_q[cat]) \
                                    and (not m_q[cat] or methods & m_q[cat]) \
                                    and (not o_q[cat] or objects & o_q[cat]):
                                merge_into(found, Leaf({emid: (h, f"{tbl}:{'.'.join(path)}",
                                                               True, True)}))
                        for full, emid in (e["full"] if e else {}).items():
                            cat, _, name = full.rpartition(".")
                            if full in quoted or (cat in quoted and name in quoted):
                                merge_into(found, Leaf({emid: (h, f"{tbl}:{'.'.join(path)}",
                                                               True, True)}))
                    continue
                merge_into(found, Leaf({mid: (h, f"{tbl}:{'.'.join(path)}", True, True)}))
    return found


def resolve(art, shapes, reads=None):
    """(output Struct, touched Leaf) for one artifact."""
    out, touched, _ = resolve_script(art.prelude, art.tree, shapes, art.project, reads)
    return out, touched


def narrow(path, leaf, leaves):
    """Cut a packed column down to the metric its name says it holds.

    clients_daily_v6 and others pack dozens of probes into one array literal,
    aggregate the array with a UDF and read it back by position,
    map_sum_aggregates[OFFSET(95)].map AS contextual_services_..._sum. Positions
    do not survive the UDF, so every column out of the pack depends on every
    probe in it. bigquery-etl names those columns after the probe, so when one
    metric's column leaf appears in the column name and no other does, that is
    the one. Below NARROW_MIN metrics a column is more likely a real combination
    (COALESCE of a scalar and its older histogram) and is left alone.
    """
    if len(leaf) < NARROW_MIN:
        return leaf, False
    name = path[-1].lower()
    hits = [m for m in leaf if leaves.get(m) and leaves[m] in name]
    if not hits:
        return leaf, False
    longest = max(len(leaves[m]) for m in hits)
    best = [m for m in hits if len(leaves[m]) == longest]
    if len(best) != 1:
        return leaf, False
    return Leaf({best[0]: leaf[best[0]]}), True


def touch_all(r, root):
    """Evaluate every scope, including ones the final SELECT never reaches.

    A metric named in a CTE that the output reads only through a construct the
    resolver cannot follow should still count as named, as a filter at worst,
    rather than disappear. Scopes already evaluated are memoized, so this costs
    little.
    """
    for sc in root.traverse():
        if isinstance(sc.expression, (exp.Select, exp.Union)):
            try:
                r.outputs(sc)
            except Exception:
                pass


def resolve_events_in(shape, events):
    """resolve_events() applied to every Leaf in a shape."""
    if isinstance(shape, Leaf):
        return resolve_events(shape, events)
    return {k: resolve_events_in(v, events) for k, v in shape.items()}


def carried_shape(table_id, out):
    """The shape readers of a resolved table see: each output column holds
    what it was built from, one hop further on, read from that column. A
    reader starts out not having transformed anything, but a column that was
    specific to a metric stays so."""
    shape = {}
    for path, leaf in output_paths(out):
        insert(shape, list(path), Leaf({
            m: (h + 1, f"{table_id}:{'.'.join(path)}", False, n)
            for m, (h, v, t, n) in leaf.items()}))
    return shape


def output_paths(shape, prefix=()):
    if isinstance(shape, Leaf):
        if shape:
            yield prefix, shape
        return
    for k, v in shape.items():
        yield from output_paths(v, prefix + (k,))


def run(only=None, show=0):
    inventory = query(f"""
SELECT app, metric, source, type, columns, event_category, event_name,
  event_methods, event_objects
FROM `{DEST}.{PREFIX}metrics`
ORDER BY app, metric
""", tables=["metrics"], name="inventory")
    events = event_index(inventory)
    shapes = stable_shapes(inventory, force=True)
    # Environment fields end in words like count, name and version, which would
    # match far too many column names to narrow a pack by.
    leaves = {m: r["columns"][0]["path"].split(".")[-1]
              if r["columns"] and r["type"] != "environment" else None
              for m, r in enumerate(inventory)}
    log(f"  stable shapes for {len(shapes):,} tables")

    arts = corpus()
    log(f"  {len(arts):,} artifacts in generated-sql")
    pages = ping_pages()
    for i, a in enumerate(arts):
        a.load()
        a.refs = a.referenced_tables()
        if a.generated_by == "stable_views" and (a.dataset, a.table) in pages:
            a.url = pages[(a.dataset, a.table)]
            a.kind = "glean_ping_view"
        if i and i % 500 == 0:
            log(f"    loaded {i:,}")

    carries_rows, ref_rows, notes = [], [], collections.Counter()
    per_art = {}
    by_id = {a.id: a for a in arts}

    def resolve_one(a, reads):
        """(out, touched, note, shape, n_narrowed) for one artifact."""
        if not any(t in shapes for t in a.refs):
            return {}, Leaf(), "reads nothing carrying a metric", None, 0
        if a.tree is None:
            return {}, regex_refs(a, events, shapes), a.note, None, 0
        try:
            out, touched = resolve(a, shapes, reads)
            note = "ok"
        except Exception as e:
            return ({}, regex_refs(a, events, shapes),
                    f"resolve failed, read by regex: {type(e).__name__}", None, 0)
        out = resolve_events_in(out, events)
        touched = resolve_events(touched, events)
        cuts = 0
        for path, leaf in list(output_paths(out)):
            cut, did = narrow(path, leaf, leaves)
            if did:
                node = out
                for p in path[:-1]:
                    node = node[p]
                node[path[-1]] = cut
                cuts += 1
        return out, touched, note, carried_shape(a.id, out), cuts

    def signature(comp):
        """What a cycle's tables currently carry; hops and via left out, since
        only which metrics reach which columns decides convergence."""
        return {a.id: frozenset((".".join(path), m, v[3])
                                for path, leaf in output_paths(per_art[a.id][1])
                                for m, v in leaf.items())
                for a in comp}

    done, late = set(), collections.defaultdict(set)
    cycle_passes, capped = collections.Counter(), []
    narrowed_by = {}
    for comp in topo_order(arts):
        comp_ids = {a.id for a in comp}
        cyclic = len(comp) > 1 or any(a.id in a.refs for a in comp)
        prev, passes = None, 0
        while True:
            passes += 1
            for a in comp:
                reads = set()
                out, touched, note, shape, cuts = resolve_one(a, reads)
                if shape:
                    shapes[a.id] = shape
                per_art[a.id] = (a, out, touched, note)
                narrowed_by[a.id] = cuts
                # A bigquery-etl table read before it was resolved, outside
                # this cycle, is a dependency the ordering did not know about.
                for t in reads & by_id.keys():
                    if t not in done and t not in comp_ids:
                        late[a.id].add(t)
            if not cyclic:
                break
            sig = signature(comp)
            if sig == prev:
                break
            if passes >= MAX_CYCLE_PASSES:
                capped.append([a.id for a in comp])
                break
            prev = sig
        done |= comp_ids
        if cyclic:
            cycle_passes[passes] += 1
            if len(comp) > 1:
                log(f"  cycle of {len(comp)} resolved in {passes} passes: "
                    + ", ".join(sorted(a.id.split(".", 1)[1] for a in comp)))
    n_narrowed = sum(narrowed_by.values())
    global _LAST_SHAPES
    _LAST_SHAPES = shapes             # for debugging a run from a REPL

    log(f"  {sum(cycle_passes.values())} cycles (self-references included), by passes "
        f"needed: " + ", ".join(f"{k}: {v}" for k, v in sorted(cycle_passes.items())))
    for ids in capped:
        log(f"  WARNING: cycle hit the {MAX_CYCLE_PASSES} pass cap without settling: "
            + ", ".join(i.split(".", 1)[1] for i in ids))
    if late:
        log(f"  WARNING: {len(late)} artifacts read a bigquery-etl table resolved after them:")
        for aid, ts in sorted(late.items())[:10]:
            log(f"    {aid.split('.', 1)[1]} <- {', '.join(t.split('.', 1)[1] for t in sorted(ts))}")
    else:
        log("  no artifact read a bigquery-etl table resolved after it")
    external = collections.Counter(
        t for a in arts for t in a.refs
        if t not in by_id and "_stable." not in t and "_live." not in t)
    log(f"  {len(external):,} referenced tables are not built by bigquery-etl, so lineage "
        f"stops there; most referenced: "
        + ", ".join(f"{t.split('.', 1)[1]} ({n})" for t, n in external.most_common(5)))

    for a, out, touched, note in per_art.values():
        notes[note] += 1
        outputs = collections.defaultdict(list)
        for path, leaf in output_paths(out):
            for m, val in leaf.items():
                outputs[m].append(("/".join(path), val))
        # Two different facts share the name "named". On a carried column it
        # means the value is specifically about the metric (it was built by
        # naming it somewhere upstream), as opposed to a rollup of a whole
        # struct; downstream readers of the column inherit it. On a ref it
        # means this query's own SQL named the metric, which comes from
        # touched alone: a metric named in a CTE and forwarded by a trailing
        # `* EXCEPT` was still chosen by whoever wrote the query.
        for path, leaf in output_paths(out):
            for m, (h, via, t, n) in leaf.items():
                if isinstance(m, tuple):
                    # an event identity column, kept so readers of it resolve
                    app, metric = m[1], IDENTITY_METRIC[m[2]]
                else:
                    app, metric = inventory[m]["app"], inventory[m]["metric"]
                carries_rows.append({
                    "table": a.id, "path": ".".join(path), "app": app,
                    "metric": metric, "hops": h + 1,
                    "transformed": t, "named": n, "via": via,
                })
        for m, (h, via, t, n) in touched.items():
            if isinstance(m, tuple):
                continue
            r = inventory[m]
            outs = outputs.get(m, [])
            if not outs and not n:
                # read in bulk (SELECT * in a CTE) and then dropped: not a use
                continue
            ref_rows.append({
                "app": r["app"], "metric": r["metric"],
                "artifact_id": a.id, "artifact_kind": a.kind,
                "role": "output" if outs else "filter",
                "hops": h + 1,
                "named": n,
                "transformed": t or any(v[2] for _, v in outs),
                "via": via,
                "output_columns": sorted({p.replace("/", ".") for p, _ in outs})[:20],
                "n_output_columns": len({p for p, _ in outs}),
                "evidence": "parsed" if a.tree is not None else "regex",
            })

    art_rows = [{
        "artifact_id": a.id, "kind": a.kind, "name": a.id.split(".", 1)[1],
        "url": a.url, "owners": a.owners, "friendly_name": a.description,
        "generated_by": a.generated_by, "note": note,
        "n_metrics_named": sum(1 for m, v in touched.items() if v[3] and isinstance(m, int)),
        "n_metrics_total": sum(1 for m in touched if isinstance(m, int)),
    } for a, out, touched, note in per_art.values()]

    log("\nartifacts by outcome:")
    for k, n in notes.most_common():
        log(f"  {n:6,}  {k}")
    named = sum(1 for r in ref_rows if r["named"])
    log(f"\n{n_narrowed:,} packed output columns narrowed by name")
    log(f"{len(ref_rows):,} refs ({named:,} named, {len(ref_rows) - named:,} bulk), "
        f"{len(carries_rows):,} carried columns, "
        f"{len({(r['app'], r['metric']) for r in ref_rows}):,} distinct metrics referenced")

    if show:
        for a, out, touched, note in per_art.values():
            if only and only not in a.id:
                continue
            log(f"\n{a.id}  [{note}]")
            for path, leaf in list(output_paths(out))[:show]:
                names = [(f"{inventory[m]['app']}:{inventory[m]['metric']}" if isinstance(m, int)
                          else str(m))
                         + ("" if v[3] or touched.get(m, (0, 0, 0, False))[3] else " (bulk)")
                         for m, v in leaf.items()]
                log(f"    {'.'.join(path):<45} <- {', '.join(names[:3])}"
                    + (f" (+{len(names) - 3})" if len(names) > 3 else ""))
            filt = [f"{inventory[m]['app']}:{inventory[m]['metric']}"
                    for m, v in touched.items()
                    if isinstance(m, int) and v[3]
                    and not any(m in lf for _, lf in output_paths(out))]
            if filt:
                log(f"    filter/join only: {', '.join(filt[:8])}"
                    + (f" (+{len(filt) - 8})" if len(filt) > 8 else ""))
    return carries_rows, ref_rows, art_rows


SUFFIX = ("Prototype table maintained by the docker-etl metric-usage job, metric_usage/lineage.py "
          "from the generated-sql branch; replaced on each run.")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--only", help="substring of a target id, for --show")
    ap.add_argument("--show", type=int, default=0, help="print N output columns per target")
    args = ap.parse_args()
    if not SQL_ROOT.exists():
        raise SystemExit(f"{SQL_ROOT} is missing; run script/fetch_generated_sql.sh first")
    carries, refs, arts = run(args.only, args.show)
    if args.dry_run:
        return
    runs.add_runs(arts)
    from google.cloud.bigquery import SchemaField as F

    cs = [F("table", "STRING"), F("path", "STRING"), F("app", "STRING"),
          F("metric", "STRING"), F("hops", "INT64"), F("transformed", "BOOL"),
          F("named", "BOOL"), F("via", "STRING")]
    write_table("carries", carries, cs,
                "Which columns of which warehouse tables carry each metric, with "
                "hops from the stable table. " + SUFFIX, clustering=["app", "metric"])
    write_table("refs_bqetl", refs, ref_schema(),
                "bigquery-etl queries and views that read each metric, as output "
                "or filter, named or carried in bulk. " + SUFFIX,
                clustering=["app", "metric"])
    write_table("artifacts_bqetl", arts, artifact_schema(),
                "One row per bigquery-etl table or view. " + SUFFIX,
                clustering=["artifact_id"])
    union_views()


if __name__ == "__main__":
    # sqlglot trees for the larger queries (clients_daily_v6 and its long AND and
    # CONCAT chains) are deeper than the default recursion limit allows, and a
    # raised limit needs a thread with a bigger stack to go with it.
    import threading

    sys.setrecursionlimit(200000)
    threading.stack_size(1024 * 1024 * 1024)
    t = threading.Thread(target=main)
    t.start()
    t.join()
