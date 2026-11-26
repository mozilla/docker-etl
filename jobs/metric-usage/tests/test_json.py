"""JSON values keep the shape of the struct they were made from, and reading a
path out of one is field access."""

import pytest

from harness import resolve, table

PINGS = table(
    "app_stable.events_v1",
    {
        "metrics.string.c_name": "c.name",
        "metrics.uuid.legacy_client_id": "legacy.client_id",
        "metrics.labeled_counter.a_errors": "a.errors",
        "metrics.labeled_counter.b_errors": "b.errors",
        "metrics.url2.u_page": "u.page",
    },
)
TBL = "`moz-fx-data-shared-prod.app_stable.events_v1`"


def as_json_table(select):
    """A derived table whose metrics column is JSON, read by the next query."""
    return resolve(f"SELECT {select} AS metrics FROM {TBL}", PINGS).shape_for(
        "app_derived.stream_v1"
    )


STREAM = "`moz-fx-data-shared-prod.app_derived.stream_v1`"


def test_to_json_keeps_paths():
    r = resolve(f"SELECT TO_JSON(metrics) AS m FROM {TBL}", PINGS)
    assert set(r.carried("m.string.c_name")) == {"c.name"}
    assert set(r.carried("m.labeled_counter.a_errors")) == {"a.errors"}


@pytest.mark.parametrize(
    "expr",
    [
        "JSON_VALUE(metrics, '$.uuid.legacy_client_id')",
        "JSON_VALUE(metrics, '$[\"uuid\"].legacy_client_id')",
        "JSON_EXTRACT_SCALAR(metrics, '$.uuid.legacy_client_id')",
        "JSON_QUERY(metrics, '$.uuid.legacy_client_id')",
        "LAX_STRING(metrics.uuid.legacy_client_id)",
        "STRING(metrics['uuid']['legacy_client_id'])",
    ],
)
def test_reading_a_path_names_it(expr):
    t = as_json_table("TO_JSON(metrics)")
    r = resolve(f"SELECT {expr} AS id FROM {STREAM}", t)
    assert set(r.carried("id")) == {"legacy.client_id"}
    assert r.uses["legacy.client_id"].named
    assert set(r.uses) == {"legacy.client_id"}


def test_json_path_in_where_is_a_filter():
    t = as_json_table("TO_JSON(metrics)")
    r = resolve(
        f"SELECT 1 AS x FROM {STREAM} " f"WHERE JSON_VALUE(metrics, '$.string.c_name') = 'x'", t
    )
    assert r.uses["c.name"] == (
        "filter",
        True,
        False,
        "moz-fx-data-shared-prod.app_derived.stream_v1:metrics.string.c_name",
    )


def test_to_json_string_of_whole_column_names_nothing():
    t = as_json_table("TO_JSON(metrics)")
    r = resolve(f"SELECT TO_JSON_STRING(metrics) AS blob FROM {STREAM}", t)
    assert not any(u.named for u in r.uses.values())


def test_non_literal_path_names_nothing():
    t = as_json_table("TO_JSON(metrics)")
    r = resolve(f"SELECT JSON_VALUE(metrics, CONCAT('$.string.', 'c_name')) AS v FROM {STREAM}", t)
    assert not any(u.named for u in r.uses.values())


def test_json_strip_nulls_keeps_paths():
    r = resolve(
        f"SELECT JSON_STRIP_NULLS(TO_JSON(metrics), remove_empty => TRUE) AS m FROM {TBL}", PINGS
    )
    assert set(r.carried("m.string.c_name")) == {"c.name"}


def test_json_set_moves_and_json_remove_drops():
    # events_stream moves metrics.url2 to $.url and removes $.url2
    r = resolve(
        f"""
        SELECT JSON_REMOVE(JSON_SET(TO_JSON(metrics), '$.url', metrics.url2), '$.url2') AS m
        FROM {TBL}""",
        PINGS,
    )
    assert set(r.carried("m.url.u_page")) == {"u.page"}
    assert not any(p.startswith("m.url2") for p in r.columns)


def test_unqualified_from_nested_map_keeps_paths():
    r = resolve(f"SELECT from_nested_map(metrics.labeled_counter) AS lc FROM {TBL}", PINGS)
    assert set(r.carried("lc.a_errors")) == {"a.errors"}
    assert set(r.carried("lc.b_errors")) == {"b.errors"}


def test_qualified_from_nested_map_keeps_paths():
    r = resolve(
        f"SELECT mozfun.json.from_nested_map(metrics.labeled_counter) AS lc FROM {TBL}", PINGS
    )
    assert set(r.carried("lc.a_errors")) == {"a.errors"}
    assert set(r.carried("lc.b_errors")) == {"b.errors"}


def test_labeled_counter_read_from_json_names_only_that_metric():
    t = as_json_table(
        "JSON_SET(TO_JSON(metrics), '$.labeled_counter', "
        "mozfun.json.from_nested_map(metrics.labeled_counter))"
    )
    r = resolve(f"SELECT JSON_QUERY(metrics, '$.labeled_counter.a_errors') AS e FROM {STREAM}", t)
    assert set(r.carried("e")) == {"a.errors"}
    assert r.uses["a.errors"].named
