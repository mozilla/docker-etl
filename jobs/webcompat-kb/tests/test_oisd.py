import argparse
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

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


@pytest.fixture(autouse=True)
def min_domains(monkeypatch):
    # The sample list is tiny compared to the real one
    monkeypatch.setattr(oisd, "MIN_DOMAINS", 1)


def make_context(bq_client, project, force=False):
    return Context(
        args=argparse.Namespace(oisd_nsfw_force=force),
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
        raise NotFound("import_runs")

    bq_client.client.return_values["query"].append(SimpleNamespace(result=result))


def allow_run_insert(bq_client):
    """Set up the client responses used when the import run is recorded."""
    bq_client.client.return_values["get_table"].append(
        bigquery.Table("project.oisd.import_runs")
    )
    bq_client.client.return_values["insert_rows"].append([])


def calls(bq_client, function):
    return [
        call.arguments for call in bq_client.client.called if call.function == function
    ]


def fail_fetch():
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
def test_check_domain_count_rejects(monkeypatch, count, declared, min_domains):
    monkeypatch.setattr(oisd, "MIN_DOMAINS", min_domains)
    domains = [f"d{i}.com" for i in range(count)]
    with pytest.raises(ValueError):
        oisd.check_domain_count(domains, declared)


def test_check_domain_count_accepts(monkeypatch):
    monkeypatch.setattr(oisd, "MIN_DOMAINS", 10)
    domains = [f"d{i}.com" for i in range(95)]
    oisd.check_domain_count(domains, 100)


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
    monkeypatch.setattr(oisd, "fetch_domain_list", lambda: SAMPLE_LIST)
    if missing:
        set_import_runs_missing(bq_client)
    else:
        set_last_import(bq_client, None)
    allow_run_insert(bq_client)

    oisd.OisdNsfwJob().main(make_context(bq_client, project))

    loads = calls(bq_client, "load_table_from_json")
    assert len(loads) == 1
    assert loads[0]["table"].endswith(".oisd.nsfw")
    assert loads[0]["job_config"].write_disposition == "WRITE_TRUNCATE"
    assert [field.name for field in loads[0]["job_config"].schema] == ["domain"]
    assert loads[0]["rows"] == [{"domain": domain} for domain in SAMPLE_DOMAINS]

    inserts = calls(bq_client, "insert_rows")
    assert len(inserts) == 1
    assert str(inserts[0]["table"].reference).endswith(".oisd.import_runs")
    assert len(inserts[0]["rows"]) == 1
    run = inserts[0]["rows"][0]
    assert run["domain_count"] == len(SAMPLE_DOMAINS)
    assert run["run_at"].tzinfo is not None

    # The run is only recorded after the table is replaced
    functions = [call.function for call in bq_client.client.called]
    assert functions.index("load_table_from_json") < functions.index("insert_rows")


def test_job_imports_when_last_run_was_last_month(monkeypatch, bq_client, project):
    monkeypatch.setattr(oisd, "fetch_domain_list", lambda: SAMPLE_LIST)
    set_last_import(bq_client, datetime.now(tz=UTC) - timedelta(days=40))
    allow_run_insert(bq_client)

    oisd.OisdNsfwJob().main(make_context(bq_client, project))

    assert len(calls(bq_client, "load_table_from_json")) == 1
    assert len(calls(bq_client, "insert_rows")) == 1


def test_job_skips_when_imported_this_month(monkeypatch, bq_client, project):
    monkeypatch.setattr(oisd, "fetch_domain_list", fail_fetch)
    set_last_import(bq_client, datetime.now(tz=UTC))

    oisd.OisdNsfwJob().main(make_context(bq_client, project))

    assert calls(bq_client, "load_table_from_json") == []
    assert calls(bq_client, "insert_rows") == []


def test_job_force_imports_even_if_imported_this_month(monkeypatch, bq_client, project):
    monkeypatch.setattr(oisd, "fetch_domain_list", lambda: SAMPLE_LIST)
    allow_run_insert(bq_client)

    oisd.OisdNsfwJob().main(make_context(bq_client, project, force=True))

    assert len(calls(bq_client, "load_table_from_json")) == 1
    assert len(calls(bq_client, "insert_rows")) == 1
    # --oisd-nsfw-force doesn't look up the last import at all
    assert calls(bq_client, "query") == []


@pytest.mark.parametrize(
    "content", ["", SAMPLE_LIST.replace("# Entries: 5", "# Entries: 500000")]
)
def test_job_refuses_short_list(monkeypatch, bq_client, project, content):
    monkeypatch.setattr(oisd, "fetch_domain_list", lambda: content)
    set_last_import(bq_client, None)

    with pytest.raises(ValueError):
        oisd.OisdNsfwJob().main(make_context(bq_client, project))

    # Neither the domain table nor the run log is written, so the next run retries
    assert calls(bq_client, "load_table_from_json") == []
    assert calls(bq_client, "insert_rows") == []
