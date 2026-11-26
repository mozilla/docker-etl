"""SQL functions are expanded with their parameters bound to the arguments'
shapes; JavaScript ones stay opaque. Persistent UDFs come from
tests/fixtures/sql."""

from harness import resolve, table

METRICS = table(
    "app_stable.metrics_v1",
    {
        "metrics.counter.a_count": "a.count",
        "metrics.counter.b_count": "b.count",
        "metrics.labeled_counter.x_errors.key": "x.errors",
        "metrics.labeled_counter.x_errors.value": "x.errors",
        "metrics.labeled_counter.y_errors.key": "y.errors",
        "metrics.labeled_counter.y_errors.value": "y.errors",
        "metrics.string.h_hist": "h.hist",
    },
)
TBL = "`moz-fx-data-shared-prod.app_stable.metrics_v1`"


def test_temp_function_body_names_what_it_reads():
    r = resolve(
        f"""
        CREATE TEMP FUNCTION pick(m ANY TYPE) AS (m.counter.a_count);
        SELECT pick(metrics) AS a FROM {TBL}""",
        METRICS,
    )
    assert set(r.carried("a")) == {"a.count"}
    assert r.uses["a.count"].named
    assert "b.count" not in r.uses


def test_temp_function_returning_a_struct_keeps_it():
    r = resolve(
        f"""
        CREATE TEMP FUNCTION wrap(m ANY TYPE) AS (
          STRUCT(m.counter.a_count AS a, m.counter.b_count AS b));
        SELECT wrap(metrics).* FROM {TBL}""",
        METRICS,
    )
    assert set(r.carried("a")) == {"a.count"}
    assert set(r.carried("b")) == {"b.count"}


def test_javascript_function_is_opaque():
    r = resolve(
        f'''
        CREATE TEMP FUNCTION js(m ANY TYPE) RETURNS INT64 LANGUAGE js AS """return 1;""";
        SELECT js(metrics.counter) AS v FROM {TBL}''',
        METRICS,
    )
    assert set(r.carried("v")) == {"a.count", "b.count"}
    assert not r.uses["a.count"].named


def test_persistent_udf_with_unnest_in_body():
    r = resolve(
        f"SELECT mozfun.map.get_key(metrics.labeled_counter.x_errors, 'k') AS x FROM {TBL}", METRICS
    )
    assert set(r.carried("x")) == {"x.errors"}
    assert r.uses["x.errors"].named


def test_two_calls_of_one_udf_do_not_share_results():
    # UDF bodies are shared AST; the UNNEST cache key once ignored the call
    r = resolve(
        f"""
        SELECT mozfun.map.get_key(metrics.labeled_counter.x_errors, 'k') AS x,
               mozfun.map.get_key(metrics.labeled_counter.y_errors, 'k') AS y
        FROM {TBL}""",
        METRICS,
    )
    assert set(r.carried("x")) == {"x.errors"}
    assert set(r.carried("y")) == {"y.errors"}


def test_persistent_udf_by_dataset_name():
    r = resolve(
        f"SELECT map.get_key(metrics.labeled_counter.x_errors, 'k') AS x FROM {TBL}", METRICS
    )
    assert set(r.carried("x")) == {"x.errors"}


def test_udf_inside_array_agg_ignore_nulls():
    # sqlglot parses mozfun.hist.extract(x) here as a Column named for the call
    r = resolve(
        f"""
        SELECT ARRAY_AGG(mozfun.hist.extract(metrics.string.h_hist) IGNORE NULLS) AS h
        FROM {TBL}""",
        METRICS,
    )
    assert "h.hist" in r.uses
    assert r.uses["h.hist"].named


def test_mode_last_of_a_struct_keeps_it():
    r = resolve(
        f"""
        SELECT mozfun.stats.mode_last(ARRAY_AGG(metrics.counter)) AS c FROM {TBL}""",
        METRICS,
    )
    assert set(r.carried("c.a_count")) == {"a.count"}
    assert set(r.carried("c.b_count")) == {"b.count"}


ADDONS = table(
    "addons_derived.addons_by_client_v1",
    {
        "client_id": "client_id",
        "addons.id": "addons.active_addons",
        "addons.version": "addons.active_addons",
    },
)
# bigquery-etl merges the theme into the same array, so every field holds both;
# with one metric, reading addon_id later would name it anyway
for field in ADDONS[1]["addons"].values():
    field["addons.theme"] = (1, "x", False, True)


def test_javascript_function_on_one_metric_in_parts_names_it():
    # a Redash query counted distinct addon ids through a JavaScript function
    # given the whole addons column
    r = resolve(
        '''
        CREATE TEMPORARY FUNCTION addon_ids(addons ARRAY<STRUCT<id STRING, version STRING>>)
        RETURNS ARRAY<STRING> LANGUAGE js AS """return addons.map(a => a.id)""";
        WITH d AS (SELECT * EXCEPT (addons)
                   FROM `moz-fx-data-shared-prod.addons_derived.addons_by_client_v1`
                   INNER JOIN UNNEST(addon_ids(addons)) AS addon_id)
        SELECT COUNT(DISTINCT addon_id) AS n FROM d''',
        ADDONS,
    )
    assert r.uses["addons.active_addons"].named
    assert r.uses["addons.theme"].named
