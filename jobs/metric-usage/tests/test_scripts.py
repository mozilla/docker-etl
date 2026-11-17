"""Multi-statement scripts: temp tables, DECLARE and SET, several SELECTs."""

from metric_usage import lineage as L

from harness import resolve, table

METRICS = table(
    "app_stable.metrics_v1",
    {
        "metrics.counter.a_count": "a.count",
        "metrics.counter.b_count": "b.count",
    },
)
TBL = "`moz-fx-data-shared-prod.app_stable.metrics_v1`"


def test_temp_tables_resolve_in_order():
    r = resolve(
        f"""
        CREATE TEMP TABLE t1 AS (SELECT metrics.counter.a_count AS a FROM {TBL});
        CREATE TEMP TABLE t2 AS SELECT a AS a2 FROM t1;
        SELECT a2 FROM t2""",
        METRICS,
    )
    assert set(r.carried("a2")) == {"a.count"}


def test_declare_and_set_are_stripped():
    r = resolve(
        f"""
        DECLARE d DATE DEFAULT CURRENT_DATE();
        SET d = DATE_SUB(d, INTERVAL 1 DAY);
        SELECT metrics.counter.a_count AS a FROM {TBL}""",
        METRICS,
    )
    assert set(r.carried("a")) == {"a.count"}


def test_if_function_at_line_start_is_not_scripting():
    prelude, tree, note = L.parse_script(f"""
        DECLARE d DATE;
        SELECT
        IF(metrics.counter.a_count > 0, 1, 0) AS a FROM {TBL}""")
    assert tree is not None, note


def test_scripting_falls_back_to_regex():
    prelude, tree, note = L.parse_script("DECLARE x INT64;\nIF x > 0 THEN SELECT 1; END IF;")
    assert tree is None
    assert "regex" in note


def test_earlier_selects_count_only_with_all_selects():
    sql = f"""
        SELECT metrics.counter.a_count AS a FROM {TBL};
        SELECT metrics.counter.b_count AS b FROM {TBL}"""
    assert set(resolve(sql, METRICS).columns) == {"b"}
    assert set(resolve(sql, METRICS, all_selects=True).columns) == {"a", "b"}
