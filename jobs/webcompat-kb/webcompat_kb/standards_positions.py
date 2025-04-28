import argparse
import logging
from datetime import datetime
from typing import Optional, Mapping, Sequence

from google.cloud import bigquery
import httpx
import pydantic

from .base import EtlJob
from .bqhelpers import BigQuery, Json


def get_json(url: str, headers: Optional[Mapping[str, str]] = None) -> Json:
    resp = httpx.get(url, headers=headers, follow_redirects=True)
    resp.raise_for_status()
    return resp.json()


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


def get_last_import(client: BigQuery) -> Optional[str]:
    client.ensure_table(
        "import_runs",
        [
            bigquery.SchemaField("mozilla_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("run_at", "TIMESTAMP", mode="REQUIRED"),
        ],
    )
    query = "SELECT mozilla_id FROM import_runs ORDER BY run_at DESC LIMIT 1"
    result = list(client.query(query))
    if len(result):
        return result[0]["mozilla_id"]
    return None


def get_sp_metadata() -> tuple[str, str]:
    metadata = get_json(
        "https://api.github.com/repos/mozilla/standards-positions/contents/merged-data.json?ref=gh-pages",
        {"Accept": "application/vnd.github+json"},
    )
    assert isinstance(metadata, dict)
    sha = metadata["sha"]
    download_url = metadata["download_url"]
    assert isinstance(sha, str)
    assert isinstance(download_url, str)
    return sha, download_url


def get_sp_data(download_url: str) -> Sequence[StandardsPosition]:
    data = get_json(download_url)
    assert isinstance(data, dict)
    rv = []
    for issue_number, issue_data in data.items():
        sp = StandardsPosition.model_validate(issue_data)
        if sp.issue is None:
            sp.issue = int(issue_number)
        rv.append(sp)
    return rv


def update_sp_data(client: BigQuery, data: Sequence[StandardsPosition]) -> None:
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


def record_import(client: BigQuery, current_sha: str) -> None:
    client.insert_rows(
        "import_runs",
        [{"mozilla_id": current_sha, "run_at": datetime.now().isoformat()}],
    )


def update_standards_positions(client: BigQuery) -> None:
    last_import_sha = get_last_import(client)
    current_sha, download_url = get_sp_metadata()

    if current_sha == last_import_sha:
        logging.info("No updates to merged-data.json")
        return

    data = get_sp_data(download_url)
    update_sp_data(client, data)
    record_import(client, current_sha)


class StandardsPositionsJob(EtlJob):
    name = "standards_positions"

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(
            title="Standards Positions", description="Standards Positions arguments"
        )
        group.add_argument(
            "--bq-standards-positions-dataset", help="BigQuery Web Features dataset id"
        )

    def default_dataset(self, args: argparse.Namespace) -> str:
        return args.bq_standards_positions_dataset

    def main(self, client: BigQuery, args: argparse.Namespace) -> None:
        if args.bq_standards_positions_dataset is None:
            raise ValueError(
                f"Must pass in --bq-standards-positions-dataset to run {self.name}"
            )
        update_standards_positions(client)
