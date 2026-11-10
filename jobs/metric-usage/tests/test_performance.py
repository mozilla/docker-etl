"""Queries that once stalled the resolver past Looker's 60 second limit. Each
takes milliseconds now, so the bound is loose."""

import time

from harness import resolve, table

LIMIT = 5

COUNTERS = table("app_stable.metrics_v1", {f"metrics.counter.c{i}": f"c{i}" for i in range(60)})


def timed(sql, *tables):
    start = time.monotonic()
    r = resolve(sql, *tables)
    return r, time.monotonic() - start


def test_nested_case_is_linear():
    # each level of a CASE whose branches are not all structs evaluated every
    # child again (49 levels in report_bitrise_builds); 22 levels without the
    # fix is four million evaluations
    expr = "0"
    for i in range(22):
        expr = f"CASE WHEN metrics.counter.c{i} > 0 THEN {expr} ELSE {i} END"
    r, took = timed(f"SELECT {expr} AS x FROM `moz-fx-data-shared-prod.app_stable.metrics_v1`",
                    COUNTERS)
    assert took < LIMIT
    assert len(r.carried("x")) == 22


def test_alias_fall_through_skips_unnests():
    # t.x where x carries nothing falls through to reading t as a column, and
    # that once evaluated every UNNEST in the FROM, each falling through again
    # (the client_counts exposure chunks have 60 LEFT JOIN UNNESTs). It takes
    # arrays that carry nothing: without the fix 8 joins took 6 seconds and 10
    # over 20.
    counts = table("app.client_counts", {"id": "client_id"})
    joins = "\n".join(f"LEFT JOIN UNNEST(client_counts.x{i}) AS u{i}" for i in range(12))
    r, took = timed(f"""
        SELECT client_counts.nothing AS n, client_counts.id AS id
        FROM `moz-fx-data-shared-prod.app.client_counts` AS client_counts
        {joins}""", counts)
    assert took < LIMIT
    assert set(r.carried("id")) == {"client_id"}
