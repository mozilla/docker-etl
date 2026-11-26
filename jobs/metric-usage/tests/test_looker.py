"""Looker: filter values on event identity columns name events. looker.py keeps
its shapes and event index in module globals, set here as _init would."""

import pytest

from metric_usage import lineage as L
from metric_usage import looker
from harness import events, table

STREAM = table("app.events_stream")
STREAM[1]["event_category"] = L.Leaf(
    {("ident", "app", "category"): (1, "s:event_category", False, True)}
)
STREAM[1]["event_name"] = L.Leaf({("ident", "app", "name"): (1, "s:event_name", False, True)})
IDX, NAMES = events(
    "app", ("smart_window", "uri_load"), ("smart_window", "open_window"), ("other", "uri_load")
)
SQL = """
SELECT events_stream.event_category AS events_stream_event_category,
       events_stream.event_name AS events_stream_event_name,
       COUNT(*) AS events_stream_count
FROM `moz-fx-data-shared-prod.app.events_stream` AS events_stream
GROUP BY 1, 2"""


@pytest.fixture
def group(monkeypatch):
    monkeypatch.setattr(looker, "_SHAPES", dict([STREAM]))
    monkeypatch.setattr(looker, "_EVENTS", IDX)
    return looker.resolve_group(SQL)


def refs(group, filters):
    return {NAMES[m]: r for m, r in looker.query_refs({"filters": filters}, group).items()}


@pytest.mark.parametrize(
    "value, expected",
    [
        ("smart_window", [("smart_window", "eq")]),
        # the UI writes most values in double quotes, which once kept them
        ('"smart_window"', [("smart_window", "eq")]),
        ('"a,b",c', [("a,b", "eq"), ("c", "eq")]),
        ("foo^,bar", [("foo,bar", "eq")]),
        ("uri%", [("uri%", "like")]),
        ("-excluded, NULL, EMPTY, kept", [("kept", "eq")]),
        ("MiXeD", [("mixed", "eq")]),
    ],
)
def test_filter_values(value, expected):
    assert looker.filter_values(value) == expected


def test_group_records_identity_columns(group):
    named, idents, note = group
    assert note == "ok"
    assert set(idents) == {"events_stream_event_category", "events_stream_event_name"}


def test_filter_values_name_events(group):
    r = refs(
        group,
        {
            "events_stream.event_category": '"smart_window"',
            "events_stream.event_name": '"uri_load","open_window"',
        },
    )
    assert set(r) == {"smart_window.uri_load", "smart_window.open_window"}
    assert all(role == "filter" for role, *_ in r.values())


def test_name_alone_names_only_if_unique(group):
    assert set(refs(group, {"events_stream.event_name": "open_window"})) == {
        "smart_window.open_window"
    }
    assert refs(group, {"events_stream.event_name": "uri_load"}) == {}


def test_excluded_value_names_nothing(group):
    assert (
        refs(
            group,
            {
                "events_stream.event_category": "smart_window",
                "events_stream.event_name": "-uri_load",
            },
        )
        == {}
    )


def test_filter_on_other_field_is_ignored(group):
    assert refs(group, {"events_stream.count": "uri_load"}) == {}


def test_sql_past_the_limit_is_left_unparsed(monkeypatch):
    monkeypatch.setattr(looker, "MAX_SQL", 10)
    monkeypatch.setattr(looker, "resolve_group", lambda sql: pytest.fail("parsed"))
    key, res, secs = looker._work(("k", "SELECT " + "x, " * 10 + "1"))
    assert res == ({}, {}, "too long")
