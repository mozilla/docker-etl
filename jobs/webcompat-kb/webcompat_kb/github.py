from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Iterator, Literal, Mapping, Optional, Sequence
from urllib.parse import urlencode

from pydantic import BaseModel
from .httphelpers import get_json, get_paginated_json, iter_paginated_json


class GitHubUser(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None
    login: str
    id: int


class GitHubLabel(BaseModel):
    id: int
    url: str
    name: str
    description: Optional[str]
    color: str
    default: bool


class GitHubMilestone(BaseModel):
    id: Optional[int]
    url: str
    html_url: str
    labels_url: str
    number: int
    state: str
    title: str
    description: Optional[str]
    creator: GitHubUser
    open_issues: int
    closed_issues: int
    created_at: datetime
    updated_at: datetime
    closed_at: Optional[datetime]
    due_on: Optional[datetime]


class GitHubIssue(BaseModel):
    assignee: Optional[GitHubUser] = None
    body: Optional[str] = None
    closed_at: Optional[datetime] = None
    comments: int
    comments_url: str
    draft: Optional[bool] = None
    events_url: str
    html_url: str
    id: int
    labels: list[str | GitHubLabel]
    labels_url: str
    milestone: Optional[GitHubMilestone]
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


class GitHubContentTreeLinks(BaseModel):
    self: str
    git: str
    html: str


class GitHubContentTree(BaseModel):
    name: str
    path: str
    sha: str
    size: int
    url: str
    html_url: str
    git_url: str
    download_url: Optional[str]
    type: str
    _links: GitHubContentTreeLinks


@dataclass
class GitHubIssuesPage:
    issues: Optional[Sequence[GitHubIssue]]
    next_url: Optional[str]
    resume_at: Optional[datetime]


class GitHub:
    def __init__(self, token: Optional[str]):
        self.token = token

    def headers(self) -> Mapping[str, str]:
        headers = {"X-GitHub-Api-Version": "2022-11-28"}
        if self.token is not None:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def issues(
        self,
        repo: str,
        labels: Iterable[str],
        last_updated: Optional[datetime],
        state: Optional[str] = "all",
    ) -> Sequence[GitHubIssue]:
        rows: list[GitHubIssue] = []
        for issues_page in self.iter_issues(repo, labels, last_updated, state=state):
            if issues_page.issues:
                rows.extend(issues_page.issues)
        return rows

    def latest_issue_update(self, repo: str) -> Optional[datetime]:
        """updated_at of the most recently updated issue in repo, if any."""
        query = {"state": "all", "per_page": 1, "sort": "updated", "direction": "desc"}
        data = get_json(
            f"https://api.github.com/repos/{repo}/issues?{urlencode(query)}",
            self.headers(),
        )
        assert isinstance(data, list)
        if not data:
            return None
        return GitHubIssue.model_validate(data[0]).updated_at

    def iter_issues(
        self,
        repo: str,
        labels: Iterable[str],
        last_updated: Optional[datetime] = None,
        state: Optional[str] = "all",
        sort: Optional[str] = None,
        direction: Optional[str] = None,
        milestone: Optional[int | Literal["none"]] = None,
        start_url: Optional[str] = None,
    ) -> Iterator[GitHubIssuesPage]:

        if start_url is None:
            query = {"state": state, "per_page": 100}
            if labels is not None:
                query["labels"] = ",".join(labels)
            if last_updated is not None:
                query["since"] = last_updated.isoformat()
            if sort is not None:
                query["sort"] = sort
            if direction is not None:
                query["direction"] = direction
            if milestone is not None:
                query["milestone"] = milestone

            url = f"https://api.github.com/repos/{repo}/issues?{urlencode(query)}"
        else:
            url = start_url
        for resp in iter_paginated_json(url, self.headers()):
            issues = (
                [GitHubIssue.model_validate(item) for item in resp.data]
                if resp.data
                else None
            )
            yield GitHubIssuesPage(issues, resp.next_url, resp.resume_at)

    def issue_comments(
        self, issue: GitHubIssue, all_pages: bool = False
    ) -> Sequence[GitHubComment]:
        if not all_pages:
            comments = get_json(issue.comments_url, self.headers())
            assert isinstance(comments, list)
        else:
            comments = get_paginated_json(issue.comments_url, self.headers())
        return [GitHubComment.model_validate(item) for item in comments]

    def repository_contents(self, repo: str, path: str) -> Sequence[GitHubContentTree]:
        if path[0] != "/":
            path = f"/{path}"
        url = f"https://api.github.com/repos/{repo}/contents{path}"
        return [
            GitHubContentTree.model_validate(item)
            for item in get_paginated_json(url, self.headers())
        ]
