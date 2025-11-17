import argparse
import logging
from datetime import datetime
from typing import Optional, Sequence
from urllib.parse import urlparse

from google.cloud import bigquery
import pydantic

from .base import Context, EtlJob, dataset_arg
from .bqhelpers import BigQuery
from .httphelpers import get_json


class StandardsPosition(pydantic.BaseModel):
    issue: Optional[int] = None
    position: Optional[str]
    venues: list[str]
    concerns: list[str]
    topics: list[str]
    title: str
    url: Optional[str]
    explainer: Optional[str]
    mdn: Optional[str]
    caniuse: Optional[str]
    bug: Optional[str]
    webkit: Optional[str]
    description: Optional[str] = None
    id: Optional[str] = None
    rationale: Optional[str] = None


class WebkitStandardsPosition(pydantic.BaseModel):
    id: str
    position: Optional[str]
    venues: list[str]
    concerns: list[str]
    topics: list[str]
    title: Optional[str]
    url: Optional[str]
    explainer: Optional[str]
    tag: Optional[str]
    mozilla: Optional[str]
    bugzilla: Optional[str]
    radar: Optional[str]


def get_last_import(
    client: BigQuery,
) -> tuple[bigquery.Table, Optional[str], Optional[str]]:
    runs_table = client.ensure_table(
        "import_runs",
        [
            bigquery.SchemaField("mozilla_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("run_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("webkit_id", "STRING"),
        ],
    )
    query = "SELECT mozilla_id, webkit_id FROM import_runs ORDER BY run_at DESC LIMIT 1"
    result = list(client.query(query))
    if len(result):
        row = result[0]
        return runs_table, row.mozilla_id, row.webkit_id
    return runs_table, None, None


def get_file_metadata(repo: str, path: str, ref: str = "main") -> tuple[str, str]:
    metadata = get_json(
        f"https://api.github.com/repos/{repo}/contents/{path}?ref={ref}",
        {"Accept": "application/vnd.github+json"},
    )
    assert isinstance(metadata, dict)
    sha = metadata["sha"]
    download_url = metadata["download_url"]
    assert isinstance(sha, str)
    assert isinstance(download_url, str)
    return sha, download_url


def get_gecko_sp_data(download_url: str) -> Sequence[StandardsPosition]:
    data = get_json(download_url)
    assert isinstance(data, dict)
    rv = []
    for issue_number, issue_data in data.items():
        sp = StandardsPosition.model_validate(issue_data)
        if sp.issue is None:
            sp.issue = int(issue_number)
        rv.append(sp)
    return rv


def get_webkit_sp_data(download_url: str) -> Sequence[WebkitStandardsPosition]:
    data = get_json(download_url)
    assert isinstance(data, list)
    rv = []
    for issue_data in data:
        rv.append(WebkitStandardsPosition.model_validate(issue_data))
    return rv


def get_issue_number(gh_url: str) -> int:
    parsed = urlparse(gh_url)
    if parsed.netloc != "github.com":
        raise ValueError(f"Expected github.com url, got {gh_url}")

    return int(parsed.path.rsplit("/", 1)[-1])


def update_gecko_sp_data(client: BigQuery, data: Sequence[StandardsPosition]) -> None:
    schema = [
        bigquery.SchemaField("issue", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("position", "STRING"),
        bigquery.SchemaField("venues", "STRING", mode="REPEATED"),
        bigquery.SchemaField("topics", "STRING", mode="REPEATED"),
        bigquery.SchemaField("concerns", "STRING", mode="REPEATED"),
        bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("url", "STRING"),
        bigquery.SchemaField("explainer", "STRING"),
        bigquery.SchemaField("mdn", "STRING"),
        bigquery.SchemaField("caniuse", "STRING"),
        bigquery.SchemaField("bug", "STRING"),
        bigquery.SchemaField("webkit", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("rationale", "STRING"),
    ]

    table = client.ensure_table("mozilla_standards_positions", schema)
    client.write_table(table, schema, [vars(item) for item in data], True)


def update_webkit_sp_data(
    client: BigQuery, data: Sequence[WebkitStandardsPosition]
) -> None:
    schema = [
        bigquery.SchemaField("issue", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("position", "STRING"),
        bigquery.SchemaField("venues", "STRING", mode="REPEATED"),
        bigquery.SchemaField("topics", "STRING", mode="REPEATED"),
        bigquery.SchemaField("concerns", "STRING", mode="REPEATED"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("url", "STRING"),
        bigquery.SchemaField("explainer", "STRING"),
        bigquery.SchemaField("tag", "STRING"),
        bigquery.SchemaField("mozilla", "STRING"),
        bigquery.SchemaField("bugzilla", "STRING"),
        bigquery.SchemaField("radar", "STRING"),
    ]

    table = client.ensure_table("webkit_standards_positions", schema)
    rows: list[dict[str, str | int | list[str]]] = []
    for item in data:
        row = vars(item)
        row["issue"] = get_issue_number(row["id"])
        rows.append(row)
    client.write_table(table, schema, rows, True)


def record_import(
    client: BigQuery, table: bigquery.Table, gecko_sha: str, webkit_sha: str
) -> None:
    client.insert_rows(
        table,
        [
            {
                "mozilla_id": gecko_sha,
                "webkit_id": webkit_sha,
                "run_at": datetime.now().isoformat(),
            }
        ],
    )


def update_gecko_standards_positions(
    client: BigQuery, last_import_sha: Optional[str]
) -> str:
    current_sha, download_url = get_file_metadata(
        "mozilla/standards-positions", "merged-data.json", ref="gh-pages"
    )

    if current_sha == last_import_sha:
        logging.info("No updates to Gecko merged-data.json")
        return last_import_sha

    updated_gecko_data = get_gecko_sp_data(download_url)
    update_gecko_sp_data(client, updated_gecko_data)
    return current_sha


def update_webkit_standards_positions(
    client: BigQuery, last_import_sha: Optional[str]
) -> str:
    current_sha, download_url = get_file_metadata(
        "WebKit/standards-positions", "summary.json", ref="main"
    )

    if current_sha == last_import_sha:
        logging.info("No updates to WebKit summary.json")
        return last_import_sha

    updated_webkit_data = get_webkit_sp_data(download_url)
    update_webkit_sp_data(client, updated_webkit_data)
    return current_sha


def update_standards_positions(client: BigQuery) -> None:
    runs_table, gecko_last_import_sha, webkit_last_import_sha = get_last_import(client)
    gecko_new_sha = update_gecko_standards_positions(client, gecko_last_import_sha)
    webkit_new_sha = update_webkit_standards_positions(client, webkit_last_import_sha)

    if (
        gecko_new_sha != gecko_last_import_sha
        or webkit_new_sha != webkit_last_import_sha
    ):
        record_import(client, runs_table, gecko_new_sha, webkit_new_sha)


class StandardsPositionsJob(EtlJob):
    name = "standards_positions"

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(
            title="Standards Positions", description="Standards Positions arguments"
        )
        group.add_argument(
            "--bq-standards-positions-dataset",
            type=dataset_arg,
            help="BigQuery Web Features dataset id",
        )

    def required_args(self) -> set[str | tuple[str, str]]:
        return {"bq_standards_positions_dataset"}

    def default_dataset(self, context: Context) -> str:
        return context.args.bq_standards_positions_dataset

    def main(self, context: Context) -> None:
        update_standards_positions(context.bq_client)
