"""What a step does when a source fails: stop rather than write a partial
table, except where the failure is expected."""

import datetime
import io
import urllib.error

import pytest
from google.api_core.exceptions import Forbidden

from metric_usage import adhoc as AH, common, lineage as L, looker


def http_error(code):
    return urllib.error.HTTPError("https://example.com", code, "x", {}, io.BytesIO())


@pytest.mark.parametrize("failed, stops", [(1, False), (10, False), (11, True)])
def test_looker_stops_past_its_transient_share(failed, stops):
    if stops:
        with pytest.raises(SystemExit):
            looker.check_transient(failed, 1000, "groups")
    else:
        looker.check_transient(failed, 1000, "groups")


def test_looker_check_passes_with_nothing_to_check():
    looker.check_transient(0, 0, "groups")


def test_not_found_is_none(monkeypatch):
    monkeypatch.setattr(common, "get_json", lambda *a: (_ for _ in ()).throw(http_error(404)))
    assert common.get_json_if_found("https://example.com") is None


def test_other_http_errors_raise(monkeypatch):
    monkeypatch.setattr(common, "get_json", lambda *a: (_ for _ in ()).throw(http_error(503)))
    with pytest.raises(urllib.error.HTTPError):
        common.get_json_if_found("https://example.com")


def test_partial_note():
    assert L.partial_note([]) == "ok"
    assert (
        L.partial_note(["KeyError", "KeyError", "TypeError"])
        == "partial: 3 scopes unread (KeyError, TypeError)"
    )


def test_text_of_a_project_without_access_is_none(monkeypatch):
    def denied(*a):
        raise Forbidden("Access Denied")

    monkeypatch.setattr(AH, "fetch_texts", denied)
    start = datetime.date(2026, 11, 1)
    got = list(AH.text_batches({("p", "k"): "2026-11-03"}, start, datetime.date(2026, 11, 16)))
    assert got == [("p", start, {"k"}, None)]


def test_utc_today_is_utc(monkeypatch):
    late = datetime.datetime(2026, 11, 17, 1, 0, tzinfo=datetime.timezone.utc)

    class Clock(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return late.astimezone(tz) if tz else late.astimezone().replace(tzinfo=None)

    monkeypatch.setattr(common.datetime, "datetime", Clock)
    assert common.utc_today() == datetime.date(2026, 11, 17)
    assert common.window_end() == datetime.date(2026, 11, 16)


def test_jetstream_and_opmon_configs_share_a_slug():
    from metric_usage import metric_hub as MH

    hub = type(
        "Hub",
        (),
        {
            "files": {
                "jetstream/my-rollout.toml": 1,
                "opmon/my-rollout.toml": 2,
                "jetstream/outcomes/x/y.toml": 3,
            },
            "get": lambda self, rel: self.files[rel],
        },
    )()
    assert MH.tool_configs(hub) == {("jetstream", "my_rollout"): 1, ("opmon", "my_rollout"): 2}


def _square_or_die(x):
    import os

    if x == 7:
        os._exit(1)  # as a segfault or an OOM kill would end the worker
    return x * x


def test_a_worker_that_dies_fails_only_its_item(monkeypatch):
    monkeypatch.setenv("METRIC_USAGE_WORKERS", "3")
    got = list(
        common.pool_map(
            _square_or_die, list(range(20)), chunksize=4, on_crash=lambda x: ("crashed", x)
        )
    )
    assert sorted(got, key=str) == sorted(
        [x * x for x in range(20) if x != 7] + [("crashed", 7)], key=str
    )


def test_a_worker_that_dies_raises_without_on_crash(monkeypatch):
    from concurrent.futures.process import BrokenProcessPool

    monkeypatch.setenv("METRIC_USAGE_WORKERS", "2")
    with pytest.raises(BrokenProcessPool):
        list(common.pool_map(_square_or_die, list(range(10))))


def test_worker_count_follows_the_environment(monkeypatch):
    monkeypatch.setenv("METRIC_USAGE_WORKERS", "5")
    assert common.worker_count() == 5
    monkeypatch.delenv("METRIC_USAGE_WORKERS")
    assert common.worker_count() >= 1
