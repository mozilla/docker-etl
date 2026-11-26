"""adhoc.py's job history: which days are captured, and what happens to a query
whose text JOBS_BY_PROJECT no longer has."""

import datetime
import re

from metric_usage import adhoc as AH

END = datetime.date(2026, 11, 16)
FIRST, _ = AH.available(END)


def day(s):
    return datetime.date.fromisoformat(s)


class FakeClient:
    """History holds every day but `missing`; JOBS_BY_PROJECT has `jobs` by day."""

    def __init__(self, missing, jobs):
        self.missing, self.jobs, self.job_lists = set(missing), jobs, []

    def query(self, sql, **kw):
        if "JOBS_BY_PROJECT" in sql:
            start, stop = map(day, re.findall(r"TIMESTAMP\('([\d-]+)'\)", sql))
            self.job_lists.append((start, stop - datetime.timedelta(days=1)))
            rows = [r for d, r in self.jobs.items() if start <= d < stop]
        else:
            n = (END - FIRST).days + 1
            rows = [
                {"project": "p", "day": FIRST + datetime.timedelta(days=i)}
                for i in range(n)
                if FIRST + datetime.timedelta(days=i) not in self.missing
            ]
        return type("Job", (), {"result": lambda self: rows})()


def test_capture_fetches_missing_days_in_runs_and_marks_empty_ones(monkeypatch):
    job = {
        "key": "t1",
        "account": "a@example.com",
        "day": day("2026-11-15"),
        "wrote": None,
        "jobs": 3,
    }
    fake = FakeClient(
        missing=[day("2026-10-01"), day("2026-11-14"), day("2026-11-15"), day("2026-11-16")],
        jobs={day("2026-11-15"): job},
    )
    appended = []
    monkeypatch.setattr(AH, "client", lambda: fake)
    monkeypatch.setattr(AH, "window_end", lambda: END)
    monkeypatch.setattr(AH, "append_table", lambda name, rows, *a, **k: appended.append(rows))
    new = AH.capture(["p"], known=set(), save=True)
    assert fake.job_lists == [
        (day("2026-10-01"), day("2026-10-01")),
        (day("2026-11-14"), day("2026-11-16")),
    ]
    assert {(r["day"], r["key"]) for r in new} == {
        ("2026-10-01", None),
        ("2026-11-14", None),
        ("2026-11-15", "t1"),
        ("2026-11-16", None),
    }
    assert appended == [new]


def test_capture_writes_nothing_on_a_dry_run(monkeypatch):
    fake = FakeClient(missing=[day("2026-11-16")], jobs={})
    appended = []
    monkeypatch.setattr(AH, "client", lambda: fake)
    monkeypatch.setattr(AH, "window_end", lambda: END)
    monkeypatch.setattr(AH, "append_table", lambda *a, **k: appended.append(a))
    assert [r["day"] for r in AH.capture(["p"], known=set(), save=False)] == ["2026-11-16"]
    assert appended == []


class FakeStore:
    def __init__(self, held):
        self.held, self.calls = held, {}

    def get(self, key, may_expire=True, frozen=False):
        self.calls[key] = frozen
        return self.held.get(key)


def test_query_past_jobs_by_project_is_frozen_and_never_fetched(monkeypatch):
    old = (FIRST - datetime.timedelta(days=30)).isoformat()
    result = ({}, [], "ok")
    rows = [
        {"project": "p", "key": "old", "day": old},
        {"project": "p", "key": "gone", "day": old},
        {"project": "p", "key": "live", "day": END.isoformat()},
    ]
    store = FakeStore({"p:old": result, "p:live": result})
    monkeypatch.setattr(AH, "text_batches", lambda *a: (_ for _ in ()).throw(AssertionError))
    out = AH.resolve_all(rows, END, store)
    assert store.calls == {"p:old": True, "p:gone": True, "p:live": False}
    assert set(out) == {("p", "old"), ("p", "live")}
