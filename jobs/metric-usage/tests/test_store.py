"""What a resolved query kept between runs depends on, and when it is redone."""

import datetime

import pytest

from harness import table
from metric_usage import common, lineage as L, redash as R

INVENTORY = [{"app": "app", "metric": m} for m in ("a.count", "b.count", "c.count")]
A, B, C = range(3)
TBL = "moz-fx-data-shared-prod.app_stable.metrics_v1"
SQL = f"SELECT metrics.counter.a_count AS my_total FROM `{TBL}`"


def shapes(**columns):
    return dict([table("app_stable.metrics_v1", columns)])


@pytest.fixture
def loaded(monkeypatch):
    """Point redash's globals at a set of shapes, as load_shapes() does."""

    def load(s):
        monkeypatch.setattr(R, "_SHAPES", s)
        monkeypatch.setattr(R, "_DIGESTS", L.shape_digests(INVENTORY, s))

    return load


def test_metric_added_elsewhere_keeps_the_digest(loaded):
    loaded(shapes(**{"metrics.counter.a_count": A}))
    before = R.dependency_digest(TBL, ["a_count"])
    loaded(shapes(**{"metrics.counter.a_count": A, "metrics.counter.b_count": B}))
    assert R.dependency_digest(TBL, ["a_count"]) == before


def test_column_holding_another_metric_changes_the_digest(loaded):
    loaded(shapes(**{"metrics.counter.a_count": A}))
    before = R.dependency_digest(TBL, ["a_count"])
    loaded(shapes(**{"metrics.counter.a_count": C}))
    assert R.dependency_digest(TBL, ["a_count"]) != before


def test_digest_uses_names_not_inventory_positions():
    s = shapes(**{"metrics.counter.a_count": 0})
    moved = [{"app": "app", "metric": "new.metric"}] + INVENTORY[:1]
    s_moved = shapes(**{"metrics.counter.a_count": 1})
    assert L.shape_digests(INVENTORY, s)[TBL] == L.shape_digests(moved, s_moved)[TBL]


def test_dependencies_keep_schema_names_only(loaded):
    loaded(shapes(**{"metrics.counter.a_count": A}))
    deps = R.dependencies(SQL, "mozdata")
    assert deps[TBL] == ["a_count"]
    assert not any("my_total" in names for names in deps.values())


def test_dependencies_leave_out_names_lineage_does_not_know(loaded):
    # resolved_adhoc keeps these in BigQuery, so a literal or an alias from
    # somebody's query must not become a dep
    loaded(shapes(**{"metrics.counter.a_count": A}))
    sql = f"""SELECT t.a_count FROM `{TBL}` AS t
        JOIN scratch.alice_tmp USING (client_id)
        WHERE t.email LIKE '%@example.com' AND t.host = 'alice.smith'"""
    assert set(R.dependencies(sql, "mozdata")) == {TBL}


def test_table_that_loses_its_shape_is_redone(loaded):
    loaded(shapes(**{"metrics.counter.a_count": A}))
    before = R.dependency_digest(TBL, ["a_count"])
    loaded({})
    assert R.dependency_digest(TBL, ["a_count"]) != before


@pytest.fixture
def store(monkeypatch, loaded):
    loaded(shapes(**{"metrics.counter.a_count": A}))
    rows = []
    fake = type(
        "Client", (), {"query": lambda self, sql: type("Job", (), {"result": lambda self: rows})()}
    )()
    monkeypatch.setattr(common, "client", lambda: fake)
    s = common.ResolvedStore("resolved_test", "v1", R.dependency_digest, INVENTORY)
    deps = R.dependencies(SQL, "mozdata")
    s.put("k", ({A: ("output", 0, f"{TBL}:metrics.counter.a_count", False)}, [], "ok"), deps)
    s.held, s.kept = dict(s.kept), {}
    return s


def test_store_hands_back_what_was_put(store):
    named, stars, note = store.get("k")
    assert named == {A: ("output", 0, f"{TBL}:metrics.counter.a_count", False)}
    assert note == "ok"
    assert "k" in store.kept


def test_store_redoes_a_changed_column(store, loaded):
    loaded(shapes(**{"metrics.counter.a_count": C}))
    assert store.get("k") is None


def test_store_redoes_an_old_entry(store):
    store.today += datetime.timedelta(days=common.ResolvedStore.MAX_AGE)
    assert store.get("k") is None
    store.today -= datetime.timedelta(
        days=common.ResolvedStore.MAX_AGE - common.ResolvedStore.MIN_AGE + 1
    )
    assert store.get("k") is not None


def test_store_redoes_a_metric_no_longer_in_the_inventory(store):
    store.ids = {}
    assert store.get("k") is None


def test_store_keeps_an_old_entry_that_may_not_expire(store):
    store.today += datetime.timedelta(days=common.ResolvedStore.MAX_AGE)
    assert store.get("k", may_expire=False) is not None


def test_store_redoes_an_entry_from_another_version(store):
    store.version = "v2"
    assert store.get("k") is None


def test_store_keeps_a_frozen_entry_from_another_version(store, monkeypatch):
    # its text is gone, so dropping it on a resolver change would lose it for good
    store.version = "v2"
    assert store.get("k", frozen=True) is not None
    written = {}
    monkeypatch.setattr(
        common, "write_table", lambda name, rows, *a, **k: written.update(rows=rows)
    )
    store.save()
    assert [(r["key"], r["version"]) for r in written["rows"]] == [("k", "v1")]


def test_store_drops_held_deps_it_may_not_keep(monkeypatch, loaded):
    # rows written before deps were filtered still hold dotted names from the
    # text; a frozen entry is never put again, so loading has to scrub them
    loaded(shapes(**{"metrics.counter.a_count": A}))
    held = {
        "key": "k",
        "version": "v0",
        "resolved_on": "2026-01-01",
        "deps": [
            {"table": TBL, "names": ["a_count"], "digest": R.dependency_digest(TBL, ["a_count"])},
            {"table": "mozdata.alice.smith", "names": [], "digest": "x"},
        ],
        "refs": [
            {
                "app": "app",
                "metric": "a.count",
                "role": "output",
                "hops": 0,
                "via": None,
                "transformed": False,
            }
        ],
        "stars": [],
        "note": "ok",
    }
    fake = type(
        "Client",
        (),
        {"query": lambda self, sql: type("Job", (), {"result": lambda self: [held]})()},
    )()
    monkeypatch.setattr(common, "client", lambda: fake)
    s = common.ResolvedStore(
        "resolved_test", "v1", R.dependency_digest, INVENTORY, keep_table=R._DIGESTS.__contains__
    )
    assert s.get("k", frozen=True) is not None
    s.put("k2", ({}, [], "ok"), {TBL: ["a_count"], "mozdata.alice.smith": []})
    written = {}
    monkeypatch.setattr(
        common, "write_table", lambda name, rows, *a, **k: written.update(rows=rows)
    )
    s.save()
    assert [[d["table"] for d in r["deps"]] for r in written["rows"]] == [[TBL], [TBL]]


def test_memory_cache_writes_nothing_to_disk(monkeypatch, tmp_path):
    monkeypatch.setattr(common, "DISK_CACHE", False)
    monkeypatch.setattr(common, "_MEMORY", {})
    path = tmp_path / "sub" / "bq_x.json"
    assert common.read_cache(path) is None
    common.write_cache(path, b"[1]")
    assert common.read_cache(path) == b"[1]"
    assert not path.parent.exists()


def test_disk_cache_round_trips(monkeypatch, tmp_path):
    monkeypatch.setattr(common, "DISK_CACHE", True)
    path = tmp_path / "sub" / "bq_x.json"
    common.write_cache(path, b"[1]")
    assert path.read_bytes() == b"[1]"
    assert common.read_cache(path) == b"[1]"
