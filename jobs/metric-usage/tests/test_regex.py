"""The regex fallback, for SQL that does not parse: a metric counts when the
text names a table holding it and, as a word, that table's column for it."""

from metric_usage import lineage as L
from harness import events, regex, table

PINGS = table("app.metrics", {
    "client_info.client_id": "client_id",
    "client_info.distribution.name": "distribution.name",
    "metrics.counter.a_count": "a.count",
    "metrics.uuid.legacy_client_id": "legacy.client_id",
})
# a broken comma before FROM keeps these from parsing
FROM = "FROM `moz-fx-data-shared-prod.app.metrics`"


def test_column_word_in_a_named_table():
    assert regex(f"SELECT metrics.counter.a_count, {FROM}", PINGS) == {"a.count": True}


def test_table_not_named_matches_nothing():
    assert regex("SELECT a_count, FROM `moz-fx-data-shared-prod.other.t`", PINGS) == {}


def test_comments_are_ignored():
    assert regex(f"SELECT 1, -- a_count\n/* legacy_client_id */ {FROM}", PINGS) == {}


def test_case_insensitive():
    assert regex(f"SELECT METRICS.COUNTER.A_COUNT, {FROM}", PINGS) == {"a.count": True}


def test_plain_nested_name_needs_its_parent():
    # every e.name = '...' would match client_info.distribution.name otherwise
    assert regex(f"SELECT e.name, {FROM}", PINGS) == {}
    assert regex(f"SELECT client_info.distribution.name, {FROM}", PINGS) == \
        {"distribution.name": True}


def test_glean_type_container_is_not_a_metric():
    # events_stream's JSON metrics.labeled_counter holds every labeled counter
    tid, shape = table("app.events_stream", {"metrics.labeled_counter": "x.errors"})
    assert regex("SELECT metrics.labeled_counter, FROM `moz-fx-data-shared-prod.app.events_stream`",
                 (tid, shape)) == {}


def test_column_with_many_metrics_is_skipped():
    tid, shape = table("app_derived.rollup_v1")
    shape["total"] = L.Leaf({f"m{i}": (1, f"{tid}:total", True, False) for i in range(5)})
    assert regex("SELECT total, FROM `moz-fx-data-shared-prod.app_derived.rollup_v1`",
                 (tid, shape)) == {}


def test_quoted_event_names():
    t = table("app.events", events_app="app")
    idx = events("app", ("nav", "click"), ("nav", "open"))
    sql = "SELECT category, name, FROM `moz-fx-data-shared-prod.app.events`, UNNEST(events) WHERE "
    assert regex(sql + "event = 'nav.click'", t, events=idx) == {"nav.click": True}
    assert regex(sql + "category = 'nav' AND name = 'open'", t, events=idx) == {"nav.open": True}
    assert regex(sql + "category = 'nav'", t, events=idx) == {}


def test_alias_is_not_a_column():
    # an unparseable query read metrics.uuid.*_client_id AS client_id and was
    # credited with client_info.client_id (found in the AI review). That query
    # uses the alias again later, so it is still credited.
    found = regex(f"SELECT metrics.uuid.legacy_client_id AS client_id, {FROM}", PINGS)
    assert found == {"legacy.client_id": True}


def test_column_read_and_aliased_to_itself_counts():
    assert regex(f"SELECT client_info.client_id AS client_id, {FROM}", PINGS) == {"client_id": True}
