import argparse
import re
import os
from datetime import datetime
from typing import Iterable, Mapping, MutableMapping, Optional, Sequence
from urllib.parse import urlencode

from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from pydantic import BaseModel

from .base import EtlJob, dataset_arg
from .bqhelpers import BigQuery
from .httphelpers import Json, get_json, get_paginated_json


class GitHubUser(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None
    login: str
    id: int


class GitHubLabel(BaseModel):
    id: Optional[int] = None
    url: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    color: Optional[str] = None
    default: Optional[bool] = None


class GitHubIssue(BaseModel):
    assignee: Optional[GitHubUser] = None
    body: str
    closed_at: Optional[datetime] = None
    comments: int
    comments_url: str
    draft: Optional[bool] = None
    events_url: str
    html_url: str
    id: int
    labels: list[str | GitHubLabel]
    labels_url: str
    number: int
    repository_url: str
    state: str
    title: str
    url: str
    user: Optional[GitHubUser] = None
    created_at: datetime
    updated_at: datetime


class GitHubComment(BaseModel):
    id: int
    body: str
    user: GitHubUser
    created_at: datetime
    updated_at: datetime


class InteropRow(BaseModel):
    year: int
    issue: int
    title: str
    proposal_type: str
    bugs: list[int]
    features: list[str]
    updated_at: datetime

    def to_json(self) -> Mapping[str, Json]:
        rv = self.dict()
        if rv["updated_at"] is not None:
            rv["updated_at"] = rv["updated_at"].replace(tzinfo=None).isoformat()
        return rv


class GitHub:
    def __init__(self, token: Optional[str]):
        self.token = token

    def headers(self) -> Mapping[str, str]:
        headers = {"X-GitHub-Api-Version": "2022-11-28"}
        if self.token is not None:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def issues(
        self, repo: str, labels: Iterable[str], last_updated: Optional[datetime]
    ) -> Sequence[GitHubIssue]:
        query = {}
        if labels is not None:
            query["labels"] = ",".join(labels)
        if last_updated is not None:
            query["since"] = last_updated.isoformat()

        url = f"https://api.github.com/repos/{repo}/issues?{urlencode(query)}"
        return [
            GitHubIssue.model_validate(item)
            for item in get_paginated_json(url, self.headers())
        ]

    def issue_comments(
        self, issue: GitHubIssue, all_pages: bool = False
    ) -> Sequence[GitHubComment]:
        if not all_pages:
            comments = get_json(issue.comments_url, self.headers())
            assert isinstance(comments, list)
        else:
            comments = get_paginated_json(issue.comments_url, self.headers())
        return [GitHubComment.model_validate(item) for item in comments]


def get_last_import(
    client: BigQuery,
) -> tuple[bigquery.Table, Optional[datetime]]:
    runs_table = client.ensure_table(
        "import_runs",
        [
            bigquery.SchemaField("run_at", "DATETIME", mode="REQUIRED"),
        ],
    )
    query = "SELECT run_at FROM import_runs ORDER BY run_at DESC LIMIT 1"
    try:
        result = list(client.query(query))
    except NotFound:
        # If we're running with --no-write and the table doesn't yet exist
        return runs_table, None
    if len(result):
        row = result[0]
        return runs_table, row.run_at
    return runs_table, None


def get_interop_issues(
    client: BigQuery, recreate: bool = False
) -> tuple[bigquery.Table, MutableMapping[int, InteropRow]]:
    schema = [
        bigquery.SchemaField("year", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("issue", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("proposal_type", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("bugs", "INTEGER", mode="REPEATED"),
        bigquery.SchemaField("features", "STRING", mode="REPEATED"),
        bigquery.SchemaField("updated_at", "DATETIME", mode="REQUIRED"),
    ]
    table = client.ensure_table("interop_proposals", schema)
    if recreate:
        return table, {}
    query = f"""SELECT * FROM {table.table_id}"""
    return table, {
        row.issue: InteropRow(
            year=row.year,
            issue=row.issue,
            title=row.title,
            proposal_type=row.proposal_type,
            bugs=row.bugs,
            features=row.features,
            updated_at=row.updated_at,
        )
        for row in client.query(query)
    }


bugzilla_re = re.compile(
    r"https://bugzilla\.mozilla\.org/show_bug\.cgi\?id=(?P<bug_id>\d+)"
)
web_features_re = re.compile(
    r"https://web-platform-dx\.github\.io/web-features-explorer/features/(?P<feature_name>[^/\?#]+)"
)


def get_bugs(body: str) -> set[int]:
    results = set()
    for m in bugzilla_re.finditer(body):
        try:
            results.add(int(m["bug_id"]))
        except ValueError:
            pass
    return results


def get_features(body: str) -> set[str]:
    return {m["feature_name"] for m in web_features_re.finditer(body)}


def extract_issue_data(
    gh_client: GitHub, issue: GitHubIssue, proposal_type: str
) -> InteropRow:
    rv = InteropRow(
        year=issue.created_at.year + 1,
        issue=issue.number,
        title=issue.title,
        proposal_type=proposal_type,
        bugs=[],
        features=[],
        updated_at=issue.updated_at,
    )
    bugs = get_bugs(issue.body)
    web_features = get_features(issue.body)
    comments = gh_client.issue_comments(issue, all_pages=False)
    for comment in comments:
        if comment.user.login == "github-actions[bot]":
            bugs |= get_bugs(comment.body)
            web_features |= get_features(comment.body)

    rv.bugs.extend(bugs)
    rv.features.extend(web_features)
    return rv


def update_interop_data(
    client: BigQuery, gh_client: GitHub, repo: str, recreate: bool
) -> None:
    runs_table, last_import = get_last_import(client)
    if last_import is None:
        recreate = True
    issues_table, interop_proposals = get_interop_issues(client, recreate)

    for proposal_type, label in [
        ("focus-area", "focus-area-proposal"),
        ("investigation", "investigation-proposal"),
    ]:
        last_updated = None
        if not recreate:
            update_times = [
                item.updated_at
                for item in interop_proposals.values()
                if item.proposal_type == proposal_type and item.updated_at is not None
            ]
            if update_times:
                last_updated = max(update_times)

        updated_issues = {
            item.number: item
            for item in gh_client.issues(
                repo=repo,
                labels=[label],
                last_updated=last_updated,
            )
        }

        for number, issue in updated_issues.items():
            interop_proposals[number] = extract_issue_data(
                gh_client, issue, proposal_type
            )

    client.write_table(
        issues_table,
        issues_table.schema,
        [item.to_json() for item in interop_proposals.values()],
        True,
    )
    client.insert_rows(runs_table, [{"run_at": datetime.now().isoformat()}])


def repo_arg(value: str) -> str:
    if "/" not in value:
        raise ValueError(f"{value} is not a valid repository (format is org/repo)")
    return value


class InteropJob(EtlJob):
    name = "interop"

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(
            title="Standards Positions", description="Standards Positions arguments"
        )
        group.add_argument(
            "--bq-interop-dataset",
            type=dataset_arg,
            help="BigQuery Interop dataset id",
        )
        group.add_argument(
            "--interop-repo",
            type=repo_arg,
            default="web-platform-tests/interop",
            help="Interop repository in the format org/repo",
        )
        group.add_argument(
            "--interop-recreate",
            action="store_true",
            help="Recreate Interop data",
        )
        group.add_argument(
            "--interop-github-token",
            default=os.environ.get("GH_TOKEN"),
            help="GitHub token",
        )

    def required_args(self) -> set[str | tuple[str, str]]:
        return {"bq_interop_dataset"}

    def default_dataset(self, args: argparse.Namespace) -> str:
        return args.bq_interop_dataset

    def main(self, client: BigQuery, args: argparse.Namespace) -> None:
        gh_client = GitHub(args.interop_github_token)
        update_interop_data(client, gh_client, args.interop_repo, args.interop_recreate)
