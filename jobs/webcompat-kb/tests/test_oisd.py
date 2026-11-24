import argparse
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from google.api_core.exceptions import NotFound

from webcompat_kb.base import Context
from webcompat_kb.config import Config
from webcompat_kb.etl import oisd

SAMPLE_LIST = """# Version: 202611201839
# Title: oisd nsfw
# Syntax: Domains (wildcards) without *.
# Entries: 5
# Last modified: 2026-11-20T18:39:21+0000

# All domains and their subdomains should be blocked

0-0.asia
Example.COM
example.com
*.wild.example.net
trailing.dot.org.
xn--2q1bl84a.live
-bad-.com
nodot
under_score.example.com
"""

SAMPLE_DOMAINS = [
    "0-0.asia",
    "example.com",
    "trailing.dot.org",
    "under_score.example.com",
    "wild.example.net",
    "xn--2q1bl84a.live",
]


def make_context(bq_client, project, min_domains=1, force=False):
    args = argparse.Namespace(
        oisd_nsfw_url=oisd.OISD_NSFW_URL,
        oisd_nsfw_min_domains=min_domains,
        oisd_nsfw_force=force,
    )
    return Context(
        args=args,
        bq_client=bq_client,
        config=Config(stage=False, write=True),
        jobs=[],
        project=project,
    )


def set_last_import(bq_client, run_at):
    """Make the next query (the last-import lookup) return run_at, or no rows."""
    rows = [] if run_at is None else [SimpleNamespace(run_at=run_at)]
    bq_client.client.return_values["query"].append(SimpleNamespace(result=lambda: rows))


def set_import_runs_missing(bq_client):
    def result():
        raise NotFound("nsfw_import_runs")

    bq_client.client.return_values["query"].append(SimpleNamespace(result=result))


def load_calls(bq_client):
    return [
        call.arguments
        for call in bq_client.client.called
        if call.function == "load_table_from_json"
    ]


def fail_fetch(url):
    raise AssertionError("the list should not be downloaded")


def test_parse_domain_list():
    domains, declared = oisd.parse_domain_list(SAMPLE_LIST.splitlines())
    assert declared == 5
    assert domains == SAMPLE_DOMAINS


def test_parse_domain_list_no_header():
    domains, declared = oisd.parse_domain_list(["a.com", "", "b.com", "a.com"])
    assert declared is None
    assert domains == ["a.com", "b.com"]


@pytest.mark.parametrize(
    "count,declared,min_domains",
    [
        (0, None, 1),
        (5, None, 10),
        (80, 100, 1),
    ],
)
def test_check_domain_count_rejects(count, declared, min_domains):
    domains = [f"d{i}.com" for i in range(count)]
    with pytest.raises(ValueError):
        oisd.check_domain_count(domains, declared, min_domains)


def test_check_domain_count_accepts():
    domains = [f"d{i}.com" for i in range(95)]
    oisd.check_domain_count(domains, 100, 10)


NOW = datetime(2026, 11, 24, 15, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    "last_import,expected",
    [
        (None, False),
        (datetime(2026, 11, 1, 0, 0, tzinfo=UTC), True),
        (datetime(2026, 10, 31, 23, 59, tzinfo=UTC), False),
        (datetime(2025, 11, 24, 15, 0, tzinfo=UTC), False),
        # Naive values (e.g. from a DATETIME column) are treated as UTC
        (datetime(2026, 11, 3, 9, 0), True),  # noqa: DTZ001
    ],
)
def test_imported_this_month(last_import, expected):
    assert oisd.imported_this_month(last_import, NOW) is expected


@pytest.mark.parametrize("missing", [False, True])
def test_job_imports_when_never_run(monkeypatch, bq_client, project, missing):
    monkeypatch.setattr(oisd, "fetch_domain_list", lambda url: SAMPLE_LIST)
    if missing:
        set_import_runs_missing(bq_client)
    else:
        set_last_import(bq_client, None)

    oisd.OisdNsfwJob().main(make_context(bq_client, project))

    calls = load_calls(bq_client)
    assert len(calls) == 2

    nsfw, run = calls
    assert nsfw["table"].endswith(".oisd.nsfw")
    assert nsfw["job_config"].write_disposition == "WRITE_TRUNCATE"
    assert [field.name for field in nsfw["job_config"].schema] == ["domain"]
    assert nsfw["rows"] == [{"domain": domain} for domain in SAMPLE_DOMAINS]

    assert run["table"].endswith(".oisd.nsfw_import_runs")
    assert run["job_config"].write_disposition == "WRITE_APPEND"
    assert len(run["rows"]) == 1
    assert run["rows"][0]["domain_count"] == len(SAMPLE_DOMAINS)
    assert datetime.fromisoformat(run["rows"][0]["run_at"]).tzinfo is not None


def test_job_imports_when_last_run_was_last_month(monkeypatch, bq_client, project):
    monkeypatch.setattr(oisd, "fetch_domain_list", lambda url: SAMPLE_LIST)
    set_last_import(bq_client, datetime.now(tz=UTC) - timedelta(days=40))

    oisd.OisdNsfwJob().main(make_context(bq_client, project))

    assert len(load_calls(bq_client)) == 2


def test_job_skips_when_imported_this_month(monkeypatch, bq_client, project):
    monkeypatch.setattr(oisd, "fetch_domain_list", fail_fetch)
    set_last_import(bq_client, datetime.now(tz=UTC))

    oisd.OisdNsfwJob().main(make_context(bq_client, project))

    assert load_calls(bq_client) == []


def test_job_force_imports_even_if_imported_this_month(monkeypatch, bq_client, project):
    monkeypatch.setattr(oisd, "fetch_domain_list", lambda url: SAMPLE_LIST)
    set_last_import(bq_client, datetime.now(tz=UTC))

    oisd.OisdNsfwJob().main(make_context(bq_client, project, force=True))

    assert len(load_calls(bq_client)) == 2
    # --oisd-nsfw-force doesn't look up the last import at all
    assert not any(call.function == "query" for call in bq_client.client.called)


@pytest.mark.parametrize(
    "content", ["", SAMPLE_LIST.replace("# Entries: 5", "# Entries: 500000")]
)
def test_job_refuses_short_list(monkeypatch, bq_client, project, content):
    monkeypatch.setattr(oisd, "fetch_domain_list", lambda url: content)
    set_last_import(bq_client, None)

    with pytest.raises(ValueError):
        oisd.OisdNsfwJob().main(make_context(bq_client, project))

    # Neither the domain table nor the run log is written, so the next run retries
    assert load_calls(bq_client) == []
