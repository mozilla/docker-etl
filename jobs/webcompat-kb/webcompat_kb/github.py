from datetime import datetime
from typing import Iterable, Mapping, Optional, Sequence
from urllib.parse import urlencode

from pydantic import BaseModel
from .httphelpers import get_json, get_paginated_json


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
        query = {"state": state}
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

    def repository_contents(self, repo: str, path: str) -> Sequence[GitHubContentTree]:
        if path[0] != "/":
            path = f"/{path}"
        url = f"https://api.github.com/repos/{repo}/contents{path}"
        return [
            GitHubContentTree.model_validate(item)
            for item in get_paginated_json(url, self.headers())
        ]
