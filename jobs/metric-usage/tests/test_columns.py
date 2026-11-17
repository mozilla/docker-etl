"""Column lineage: where a metric ends up and whether the query named it."""

from harness import resolve, table

METRICS = table("app_stable.metrics_v1", {
    "client_info.client_id": "client_id",
    "metrics.counter.a_count": "a.count",
    "metrics.counter.b_count": "b.count",
    "metrics.string.c_name": "c.name",
    "metrics.boolean.d_on": "d.on",
    "metrics.boolean.e_on": "e.on",
})
TBL = "`moz-fx-data-shared-prod.app_stable.metrics_v1`"


def test_named_column_reaches_output():
    r = resolve(f"SELECT metrics.counter.a_count AS a FROM {TBL}", METRICS)
    assert r.carried("a") == {"a.count": (True, False, f"{METRICS[0]}:metrics.counter.a_count")}
    assert r.uses["a.count"].role == "output"
    assert r.uses["a.count"].named
    assert "b.count" not in r.uses


def test_where_only_is_a_filter():
    r = resolve(f"SELECT client_info.client_id FROM {TBL} "
                f"WHERE metrics.boolean.d_on", METRICS)
    assert r.uses["d.on"] == ("filter", True, False, f"{METRICS[0]}:metrics.boolean.d_on")
    assert r.uses["client_id"].role == "output"


def test_aggregate_is_transformed_and_named():
    r = resolve(f"SELECT SUM(metrics.counter.a_count) AS total FROM {TBL}", METRICS)
    assert r.carried("total")["a.count"].transformed
    assert r.uses["a.count"] == ("output", True, True, f"{METRICS[0]}:metrics.counter.a_count")


def test_star_forwards_every_column_in_bulk():
    r = resolve(f"SELECT * FROM {TBL}", METRICS)
    assert set(r.columns) >= {"metrics.counter.a_count", "metrics.string.c_name"}
    assert r.uses["a.count"].role == "output"
    assert not r.uses["a.count"].named
    assert r.stars == {METRICS[0]}


def test_bulk_read_then_dropped_is_not_a_use():
    r = resolve(f"WITH t AS (SELECT * FROM {TBL}) SELECT client_info.client_id FROM t", METRICS)
    assert set(r.uses) == {"client_id"}


def test_whole_struct_keeps_paths_without_naming():
    r = resolve(f"SELECT metrics FROM {TBL}", METRICS)
    assert "metrics.counter.a_count" in r.columns
    assert not r.uses["a.count"].named


def test_struct_star_keeps_paths():
    r = resolve(f"SELECT metrics.counter.* FROM {TBL}", METRICS)
    assert set(r.columns) == {"a_count", "b_count"}


def test_star_except_drops_and_replace_rewrites():
    r = resolve(f"""
        WITH t AS (SELECT metrics.counter.a_count AS a, metrics.counter.b_count AS b,
                          metrics.string.c_name AS c FROM {TBL})
        SELECT * EXCEPT (b) REPLACE (UPPER(c) AS c) FROM t""", METRICS)
    assert set(r.columns) == {"a", "c"}
    assert r.carried("c")["c.name"].transformed


def test_struct_constructor_keeps_fields():
    r = resolve(f"SELECT STRUCT(metrics.counter.a_count AS a, metrics.string.c_name AS c) AS s "
                f"FROM {TBL}", METRICS)
    assert set(r.carried("s.a")) == {"a.count"}
    assert set(r.carried("s.c")) == {"c.name"}


def test_function_written_back_under_its_struct_keeps_the_struct():
    # glean_client_info_attribution(glean_baseline_client_info(client_info, ...)) AS
    # client_info once collapsed client_info into every metric in the ping
    r = resolve(f"SELECT some_udf(other_udf(metrics, 1), 2) AS metrics FROM {TBL}", METRICS)
    assert set(r.carried("metrics.counter.a_count")) == {"a.count"}


def test_implicit_alias_same_as_column():
    # FROM app.metrics aliases the table `metrics`, the same name as its column
    tid, shape = table("app.metrics", {"metrics.counter.a_count": "a.count"})
    r = resolve("SELECT metrics.counter.a_count AS a FROM `moz-fx-data-shared-prod.app.metrics`",
                (tid, shape))
    assert set(r.carried("a")) == {"a.count"}


def test_unnest_element_fields():
    tid, shape = table("app_stable.pings_v1", {"items.value": "item.value"})
    r = resolve("SELECT i.value AS v FROM `moz-fx-data-shared-prod.app_stable.pings_v1`, "
                "UNNEST(items) AS i", (tid, shape))
    assert set(r.carried("v")) == {"item.value"}
    assert r.uses["item.value"].named


def test_union_lines_up_by_name_then_position():
    # a star only yields carried columns, so positions are a fallback: z lines
    # up with a by position because nothing in its branch is named a
    r = resolve(f"""
        SELECT metrics.counter.a_count AS a, metrics.counter.b_count AS b FROM {TBL}
        UNION ALL
        SELECT metrics.string.c_name AS z, metrics.boolean.d_on AS b FROM {TBL}""", METRICS)
    assert set(r.carried("a")) == {"a.count", "c.name"}
    assert set(r.carried("b")) == {"b.count", "d.on"}


def test_union_keeps_columns_the_first_branch_lacks():
    # telemetry.events_v1's first branch stars over a table nothing builds
    r = resolve(f"""
        SELECT * FROM `moz-fx-data-shared-prod.elsewhere.t`
        UNION ALL
        SELECT metrics.counter.a_count AS a FROM {TBL}""", METRICS)
    assert set(r.carried("a")) == {"a.count"}



def test_except_outputs_only_the_left_branch():
    r = resolve(f"""
        SELECT client_info.client_id AS id, metrics.counter.a_count AS a FROM {TBL}
        EXCEPT DISTINCT
        SELECT client_info.client_id, metrics.counter.b_count FROM {TBL}""", METRICS)
    assert set(r.carried("id")) == {"client_id"}
    assert set(r.carried("a")) == {"a.count"}
    assert r.uses["b.count"].role == "filter"
    assert r.uses["b.count"].named


def test_view_over_intersect_in_a_cte():
    r = resolve(f"""
        CREATE OR REPLACE VIEW `moz-fx-data-shared-prod.app.both` AS
        WITH both_days AS (
          SELECT client_info.client_id AS id FROM {TBL} WHERE metrics.boolean.d_on
          INTERSECT DISTINCT
          SELECT client_info.client_id FROM {TBL} WHERE metrics.boolean.e_on
        )
        SELECT id FROM both_days""", METRICS)
    assert set(r.carried("id")) == {"client_id"}
    assert r.uses["d.on"].role == "filter"
    assert r.uses["e.on"].role == "filter"


def test_join_using_reads_both_sides():
    other = table("app_stable.other_v1", {"client_id": "other.client_id"})
    mine = table("app_stable.mine_v1", {"client_id": "mine.client_id"})
    r = resolve("SELECT 1 AS x FROM `moz-fx-data-shared-prod.app_stable.mine_v1` "
                "JOIN `moz-fx-data-shared-prod.app_stable.other_v1` USING (client_id)",
                other, mine)
    assert {"other.client_id", "mine.client_id"} <= set(r.uses)


def test_group_by_prefers_select_alias():
    r = resolve(f"SELECT metrics.string.c_name AS a_count, COUNT(*) AS n FROM {TBL} "
                f"GROUP BY a_count", METRICS)
    assert "a.count" not in r.uses


def test_unaliased_scalar_subquery_gets_positional_name():
    r = resolve(f"SELECT (SELECT MAX(metrics.counter.a_count) FROM {TBL}) FROM {TBL}", METRICS)
    assert set(r.carried("f0_")) == {"a.count"}


def test_if_over_structs_keeps_the_struct():
    # the incremental *_last_seen queries pick a whole row this way
    r = resolve(f"""
        WITH cur AS (SELECT metrics.counter AS c FROM {TBL}),
             prev AS (SELECT metrics.counter AS c FROM {TBL})
        SELECT IF(cur.c IS NOT NULL, cur.c, prev.c).* FROM cur JOIN prev ON TRUE""", METRICS)
    assert set(r.carried("a_count")) == {"a.count"}
    assert set(r.carried("b_count")) == {"b.count"}


def test_array_agg_offset_keeps_the_struct():
    r = resolve(f"""
        SELECT ARRAY_AGG(STRUCT(metrics.counter.a_count AS a, metrics.string.c_name AS c)
                         ORDER BY metrics.counter.b_count LIMIT 1)[OFFSET(0)] AS pick
        FROM {TBL}""", METRICS)
    assert set(r.carried("pick.a")) == {"a.count"}
    assert set(r.carried("pick.c")) == {"c.name"}


def test_whole_struct_consumed_names_nothing():
    # regexp_contains(to_json_string(metrics.boolean), ...) once named every boolean
    r = resolve(f"SELECT REGEXP_CONTAINS(TO_JSON_STRING(metrics.boolean), 'true') AS any_on "
                f"FROM {TBL}", METRICS)
    assert set(r.carried("any_on")) == {"d.on", "e.on"}
    assert not r.uses["d.on"].named
    assert not r.uses["e.on"].named


def test_reading_a_many_metric_column_keeps_its_flags():
    t = resolve(f"SELECT REGEXP_CONTAINS(TO_JSON_STRING(metrics.boolean), 'true') AS any_on "
                f"FROM {TBL}", METRICS).shape_for("app_derived.flags_v1")
    r = resolve("SELECT any_on FROM `moz-fx-data-shared-prod.app_derived.flags_v1`", t)
    assert not r.uses["d.on"].named


def test_reading_a_one_metric_column_names_it():
    t = resolve(f"SELECT * FROM {TBL}", METRICS).shape_for("app.metrics_view")
    r = resolve("SELECT metrics.counter.a_count AS a "
                "FROM `moz-fx-data-shared-prod.app.metrics_view`", t)
    assert r.uses["a.count"].named
    assert r.carried("a")["a.count"].via == "moz-fx-data-shared-prod.app.metrics_view:metrics.counter.a_count"


def test_metric_named_in_unreached_cte_still_counts():
    r = resolve(f"WITH unused AS (SELECT metrics.counter.a_count FROM {TBL}) "
                f"SELECT 1 AS x", METRICS)
    assert r.uses["a.count"] == ("filter", True, False, f"{METRICS[0]}:metrics.counter.a_count")


def test_unknown_table_carries_nothing():
    r = resolve("SELECT x FROM `moz-fx-data-shared-prod.elsewhere.t`", METRICS)
    assert r.columns == {}
    assert r.uses == {}


def test_names_compare_case_insensitively():
    # Table names stay case-sensitive, as dataset and table names are in
    # BigQuery. (A labelled SUM(DAU) miss in Redash was a from FROM typo
    # BigQuery rejects, not case.)
    tid, shape = table("app_derived.active_users_v1", {"dau": "usage.dau"})
    r = resolve("SELECT SUM(DAU) AS Total FROM `moz-fx-data-shared-prod.app_derived.active_users_v1`",
                (tid, shape))
    assert set(r.carried("total")) == {"usage.dau"}


PARTS = table("app_derived.hist_v1", {
    "hist.sum": "h.hist", "hist.values": "h.hist",
    "pair.id": "p.one", "pair.version": "p.one",
})
PARTS[1]["pair"]["id"][("p.two")] = (1, "x", False, True)
PARTS[1]["pair"]["version"][("p.two")] = (1, "x", False, True)
PARTS_TBL = "`moz-fx-data-shared-prod.app_derived.hist_v1`"


def test_one_metric_in_parts_used_whole_is_named():
    r = resolve(f"SELECT TO_JSON_STRING(hist) AS j, hist AS h FROM {PARTS_TBL}", PARTS)
    assert r.uses["h.hist"].named


def test_metrics_sharing_every_field_keep_their_flags():
    # bigquery-etl merges active_addons and theme into one addons array
    r = resolve(f"SELECT TO_JSON_STRING(pair) AS j FROM {PARTS_TBL}", PARTS)
    assert r.uses["p.one"].named and r.uses["p.two"].named


def test_one_field_struct_is_still_a_container():
    tid, shape = table("app_derived.one_v1", {"box.only": "o.only"})
    r = resolve("SELECT TO_JSON_STRING(box) AS j FROM `moz-fx-data-shared-prod.app_derived.one_v1`",
                (tid, shape))
    assert not r.uses["o.only"].named


def test_glean_type_container_with_one_metric_is_still_a_container():
    # firefox_crashes_v1 forwards metrics.datetime, whose only metric is
    # crash.time in two columns (the value and its raw string)
    tid, shape = table("app.crash", {"metrics.datetime.crash_time": "crash.time",
                                     "metrics.datetime.raw_crash_time": "crash.time"})
    r = resolve("SELECT STRUCT(metrics.datetime AS datetime) AS metrics, "
                "TO_JSON_STRING(metrics.datetime) AS j FROM `moz-fx-data-shared-prod.app.crash`",
                (tid, shape))
    assert not r.uses["crash.time"].named


def test_order_by_only_is_a_filter():
    r = resolve(f"SELECT client_info.client_id FROM {TBL} ORDER BY metrics.counter.a_count",
                METRICS)
    assert r.uses["a.count"].role == "filter"


def test_named_window_is_read():
    r = resolve(f"SELECT client_info.client_id, ROW_NUMBER() OVER w AS rn FROM {TBL} "
                "WINDOW w AS (PARTITION BY metrics.counter.b_count "
                "ORDER BY metrics.counter.a_count)", METRICS)
    assert r.uses["a.count"].role == "filter"
    assert r.uses["b.count"].role == "filter"


def test_order_by_an_alias_reads_the_select():
    # a_count here is the SELECT's, which holds b.count, not the table's a_count
    r = resolve(f"SELECT metrics.counter.b_count AS a_count FROM {TBL} ORDER BY a_count",
                METRICS)
    assert "a.count" not in r.uses
