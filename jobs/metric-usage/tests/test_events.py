"""Glean events are matched by comparing identity columns to string literals."""

from harness import events, resolve, table

EVENTS = table("app_stable.events_v1", {"client_info.client_id": "client_id"}, events_app="app")
IDX = events(
    "app", ("nav", "click"), ("nav", "open"), ("search", "open"), ("media.playback", "error")
)
FROM = "FROM `moz-fx-data-shared-prod.app_stable.events_v1`, UNNEST(events) AS e"


def run(sql):
    return resolve(sql, EVENTS, events=IDX)


def test_category_and_name_name_the_event():
    r = run(f"SELECT COUNT(*) AS n {FROM} WHERE e.category = 'nav' AND e.name = 'click'")
    assert r.uses["nav.click"] == ("filter", True, True, r.uses["nav.click"].via)
    assert "nav.open" not in r.uses


def test_in_list():
    r = run(f"SELECT 1 AS x {FROM} WHERE e.category = 'nav' AND e.name IN ('click', 'open')")
    assert {"nav.click", "nav.open"} <= set(r.uses)
    assert r.uses["nav.open"].named


def test_name_alone_names_only_if_unique():
    r = run(f"SELECT 1 AS x {FROM} WHERE e.name = 'click'")
    assert r.uses["nav.click"].named
    r = run(f"SELECT 1 AS x {FROM} WHERE e.name = 'open'")
    assert r.reached["nav.open"] is False
    assert r.reached["search.open"] is False
    # reached in a filter without being named is not a ref
    assert "nav.open" not in r.uses


def test_category_alone_reaches_without_naming():
    r = run(f"SELECT 1 AS x {FROM} WHERE e.category = 'nav'")
    assert r.reached == {"nav.click": False, "nav.open": False}


def test_like_reaches_without_naming():
    r = run(f"SELECT 1 AS x {FROM} WHERE e.name LIKE 'op%'")
    assert r.reached == {"nav.open": False, "search.open": False}


def test_dotted_category():
    r = run(f"SELECT 1 AS x {FROM} WHERE e.category = 'media.playback' AND e.name = 'error'")
    assert r.uses["media.playback.error"].named


def test_case_when():
    r = run(f"SELECT CASE e.name WHEN 'click' THEN 1 END AS n {FROM}")
    assert r.uses["nav.click"].named


def test_countif_carries_the_event_downstream():
    r = run(f"SELECT COUNTIF(e.category = 'nav' AND e.name = 'click') AS clicks {FROM}")
    assert "nav.click" in r.carried("clicks")
    assert r.uses["nav.click"].role == "output"


def test_identity_columns_carry_downstream():
    t = run(f"""
        SELECT e.category AS event_category, e.name AS event_name,
               ARRAY_TO_STRING([e.category, e.name], '.') AS event {FROM}""").shape_for(
        "app_derived.events_stream_v1"
    )
    stream = "FROM `moz-fx-data-shared-prod.app_derived.events_stream_v1`"
    r = resolve(f"SELECT 1 AS x {stream} WHERE event = 'nav.click'", t, events=IDX)
    assert r.uses["nav.click"].named
    r = resolve(
        f"SELECT 1 AS x {stream} WHERE event_category = 'search' AND event_name = 'open'",
        t,
        events=IDX,
    )
    assert r.uses["search.open"].named
    assert "nav.open" not in r.uses


def test_table_alias_same_as_events_column():
    # FROM app.events, UNNEST(events): events is the array, not the row
    tid, shape = table("app.events", events_app="app")
    r = resolve(
        "SELECT 1 AS x FROM `moz-fx-data-shared-prod.app.events`, UNNEST(events) AS event "
        "WHERE event.category = 'nav' AND event.name = 'click'",
        (tid, shape),
        events=IDX,
    )
    assert r.uses["nav.click"].named


def test_category_and_name_paired_across_scopes():
    # category filtered in a CTE, name picked in a later select from it, as in
    # a Redash query that was once a labelled parser miss for smart_window.uri_load
    t = run(
        f"SELECT e.category AS event_category, e.name AS event_name, "
        f"client_info.client_id AS client_id {FROM}"
    ).shape_for("app_derived.stream_v1")
    r = resolve(
        """
        WITH a AS (SELECT client_id, event_name
                   FROM `moz-fx-data-shared-prod.app_derived.stream_v1`
                   WHERE event_category = 'nav')
        SELECT COUNT(DISTINCT IF(b.event_name IN ('click', 'open'), b.client_id, NULL)) AS n
        FROM a AS b""",
        t,
        events=IDX,
    )
    assert r.uses["nav.click"].named
    assert r.uses["nav.open"].named
    assert "search.open" not in r.uses


def test_or_keeps_each_pair_to_its_side():
    r = run(
        f"SELECT COUNT(*) AS n {FROM} WHERE (e.category = 'nav' AND e.name = 'click') "
        "OR (e.category = 'search' AND e.name = 'open')"
    )
    assert r.uses["nav.click"].named
    assert r.uses["search.open"].named
    assert "nav.open" not in r.uses


def test_case_branches_pair_on_their_own():
    r = run(
        f"SELECT CASE WHEN e.category = 'nav' AND e.name = 'click' THEN 1 "
        f"WHEN e.category = 'search' AND e.name = 'open' THEN 2 END AS k {FROM}"
    )
    assert r.uses["nav.click"].named
    assert r.uses["search.open"].named
    assert "nav.open" not in r.uses


def test_select_items_pair_on_their_own():
    r = run(
        f"SELECT COUNTIF(e.category = 'nav' AND e.name = 'click') AS clicks, "
        f"COUNTIF(e.category = 'search' AND e.name = 'open') AS opens {FROM}"
    )
    assert r.uses["nav.click"].named
    assert r.uses["search.open"].named
    assert "nav.open" not in r.uses


def test_union_branches_pair_on_their_own():
    r = run(
        f"SELECT 1 AS x {FROM} WHERE e.category = 'nav' AND e.name = 'click' "
        f"UNION ALL SELECT 2 {FROM} WHERE e.category = 'search' AND e.name = 'open'"
    )
    assert r.uses["nav.click"].named
    assert r.uses["search.open"].named
    assert "nav.open" not in r.uses


def test_name_nothing_pairs_with_is_alone():
    # open is in two categories, so on its own it names neither
    r = run(
        f"SELECT COUNT(*) AS n {FROM} WHERE (e.category = 'nav' AND e.name = 'click') "
        "OR e.name = 'open'"
    )
    assert r.uses["nav.click"].named
    assert r.reached["nav.open"] is False
    assert r.reached["search.open"] is False
