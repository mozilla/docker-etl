import logging
from enum import Enum
import argparse
import re
from typing import (
    Annotated,
    Iterable,
    Iterator,
    Optional,
    TypeVar,
    Generic,
)
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import html5lib
import zstandard
from google.cloud import bigquery
from pydantic import AfterValidator, BaseModel, PlainSerializer

from ..base import Context, EtlJob
from ..bqhelpers import BigQuery, TableSchema
from ..projectdata import Project
from .. import github
from ..serialization import to_naive_datetime, utc_from_naive_datetime


@dataclass
class ConfigurationValue:
    name: str
    value: str


@dataclass
class BodyData:
    ua_header: Optional[str] = None
    source: Optional[str] = None
    bug_url: Optional[str] = None
    browser: Optional[str] = None
    os: Optional[str] = None
    problem_type: Optional[str] = None
    description: Optional[str] = None
    steps_to_reproduce: Optional[str] = None
    screenshot: Optional[str] = None
    configuration: list[ConfigurationValue] = field(default_factory=list)


class MilestoneRecord(BaseModel):
    number: int
    title: str


class WebBugsRow(BaseModel):
    number: int
    state: str
    title: str
    body: str
    labels: list[str]
    milestone: Optional[MilestoneRecord]
    created_at: Annotated[
        datetime,
        AfterValidator(utc_from_naive_datetime),
        PlainSerializer(to_naive_datetime),
    ]
    updated_at: Annotated[
        datetime,
        AfterValidator(utc_from_naive_datetime),
        PlainSerializer(to_naive_datetime),
    ]
    moderated: bool
    ua_header: Optional[str]
    source: Optional[str]
    bug_url: Optional[str]
    browser: Optional[str]
    os: Optional[str]
    problem_type: Optional[str]
    description: Optional[str]
    steps_to_reproduce: Optional[str]
    screenshot: Optional[str]
    configuration: Optional[list[ConfigurationValue]]


OutputT = TypeVar("OutputT", bound=BaseModel)


@dataclass
class IssueBackfill:
    start_url: Optional[str]
    milestone: Optional[int]


@dataclass
class IssueUpdate:
    since: datetime


IssueParams = IssueBackfill | IssueUpdate


class SourceRepo(ABC, Generic[OutputT]):
    repo: str
    private_milestone: Optional[int]

    def __init__(self, gh_client: github.GitHub, dest_table: TableSchema):
        self.gh_client = gh_client
        self.dest_table = dest_table

    def skip_from_file(self, issue: github.GitHubIssue) -> bool:
        """Whether to ignore an issue in a data dump and refetch it from the API."""
        return False

    def iter_issues(self, params: IssueParams) -> Iterator[github.GitHubIssuesPage]:
        if isinstance(params, IssueUpdate):
            return self.gh_client.iter_issues(
                self.repo,
                labels=[],
                last_updated=params.since,
                sort="updated",
                direction="asc",
            )
        else:
            return self.gh_client.iter_issues(
                self.repo,
                labels=[],
                sort="created",
                direction="asc",
                start_url=params.start_url,
                milestone=params.milestone,
            )

    @abstractmethod
    def process_issue(self, issue: github.GitHubIssue) -> OutputT: ...


class WebcompatParserState(Enum):
    prelude = 1
    key_values = 2
    steps_to_reproduce = 3
    details = 4
    after_details = 5


class WebBugsRepo(SourceRepo[WebBugsRow]):
    repo = "webcompat/web-bugs"
    private_milestone = 8

    def skip_from_file(self, issue: github.GitHubIssue) -> bool:
        # Milestone 8 is invalid issues, private issues have both this milestone
        # and a fixed title. Unmoderated issues have a known label and a fixed title.
        return bool(
            (
                issue.title == "Issue closed."
                and issue.milestone
                and issue.milestone.number == self.private_milestone
            )
            or (
                issue.title == "In the moderation queue."
                and any(
                    (isinstance(item, str) and item == "action-needsmoderation")
                    or (
                        isinstance(item, github.GitHubLabel)
                        and item.name == "action-needsmoderation"
                    )
                    for item in issue.labels
                )
            )
        )

    def process_issue(self, issue: github.GitHubIssue) -> WebBugsRow:
        labels: list[str] = []
        for item in issue.labels:
            if isinstance(item, str):
                labels.append(item)
            elif item.name is not None:
                labels.append(item.name)

        moderated = "action-needsmoderation" not in labels
        if moderated and issue.body:
            body_data = self.parse_issue_body(issue.number, issue.body)
        else:
            body_data = BodyData()
        return WebBugsRow(
            number=issue.number,
            state=issue.state,
            title=issue.title,
            body=issue.body or "",
            labels=labels,
            created_at=issue.created_at,
            updated_at=issue.updated_at,
            milestone=MilestoneRecord(
                number=issue.milestone.number, title=issue.milestone.title
            )
            if issue.milestone
            else None,
            moderated=moderated,
            ua_header=body_data.ua_header,
            source=body_data.source,
            bug_url=body_data.bug_url,
            browser=body_data.browser,
            os=body_data.os,
            problem_type=body_data.problem_type,
            description=body_data.description,
            steps_to_reproduce=body_data.steps_to_reproduce,
            screenshot=body_data.screenshot,
            configuration=body_data.configuration,
        )

    def parse_issue_body(self, number: int, body: str) -> BodyData:
        if "<!-- @reported_with" in body:
            return self._parse_issue_body_webcompat(number, body)
        if "What seems to be the trouble?" in body:
            return self._parse_issue_body_webbugs(number, body)

        # Otherwise try both parsers and see which returns the most data.
        webcompat_parse = self._parse_issue_body_webcompat(number, body)
        webbugs_parse = self._parse_issue_body_webbugs(number, body)

        count_webcompat = sum(
            1 if value is not None else 0 for value in asdict(webcompat_parse).values()
        )
        count_webbugs = sum(
            1 if value is not None else 0 for value in asdict(webbugs_parse).values()
        )

        return webcompat_parse if count_webcompat > count_webbugs else webbugs_parse

    def _parse_issue_body_webcompat(self, number: int, body: str) -> BodyData:
        data = BodyData()

        state = WebcompatParserState.prelude
        prelude_re = re.compile(".*<!-- @([^: ]+): (.+) -->")

        prefix_extracts = {
            "bug_url": "**URL**:",
            "browser": "**Browser / Version**:",
            "os": "**Operating System**:",
            "problem_type": "**Problem type**:",
            "description": "**Description**:",
        }

        steps_to_reproduce = []
        details: list[str] = []

        def parse_details() -> None:
            try:
                document = html5lib.parseFragment(
                    "\n".join(details),
                    treebuilder="etree",
                    namespaceHTMLElements=False,
                )
            except Exception as e:
                logging.error(
                    f"Failed to <details> data from {self.repo} issue {number}: {e}"
                )
            else:
                tree = document.find("details")
                summary = tree.find("summary")
                summary_text = (
                    summary.text.lower()
                    if summary is not None and summary.text is not None
                    else ""
                )
                if "screenshot" in summary_text:
                    img = tree.find("img")
                    if img is not None:
                        data.screenshot = img.attrib.get("src")
                elif "configuration" in summary_text:
                    config_list = tree.find("ul") if tree is not None else None
                    if config_list is not None:
                        for elem in config_list.findall("li"):
                            if not elem.text:
                                continue
                            name_value = elem.text.split(":", 1)
                            if len(name_value) == 2:
                                name, value = name_value
                                data.configuration.append(
                                    ConfigurationValue(
                                        name=name.strip(), value=value.strip()
                                    )
                                )
                else:
                    logging.warning(
                        f"Found <details> element, but no corresponding <body> type parsing {self.repo} issue {number}"
                    )

        for line in body.splitlines():
            line = line.strip()
            if state == WebcompatParserState.prelude:
                if not line:
                    continue
                m = prelude_re.match(line)
                if m is None:
                    # Reprocess in next state
                    state = WebcompatParserState.key_values
                else:
                    name, value = m.groups()
                    if name == "ua_header":
                        data.ua_header = value
                    elif name == "reported_with":
                        data.source = value
            if state == WebcompatParserState.key_values:
                for attr, prefix in prefix_extracts.items():
                    if getattr(data, attr) is None:
                        if line.startswith(prefix):
                            setattr(data, attr, line[len(prefix) :].strip())
                if line.startswith("**Steps to Reproduce**"):
                    state = WebcompatParserState.steps_to_reproduce
                    continue
            if state == WebcompatParserState.steps_to_reproduce:
                if line.startswith("<details>"):
                    state = WebcompatParserState.details
                else:
                    steps_to_reproduce.append(line)
            if state == WebcompatParserState.after_details:
                if line.startswith("<details>"):
                    state = WebcompatParserState.details
            if state == WebcompatParserState.details:
                details.append(line)
                if "</details>" in line:
                    state = WebcompatParserState.after_details
                    parse_details()
                    details = []

        data.steps_to_reproduce = "\n".join(steps_to_reproduce).strip()

        return data

    def _parse_issue_body_webbugs(self, number: int, body: str) -> BodyData:
        data = BodyData(source="web-bugs")
        basic_sections = {
            "url": "URL",
            "browser": "Browser/Version",
            "os": "Operating System",
            "steps_to_reproduce": "Steps to Reproduce",
            "screenshot": "Screenshot",
        }
        sections_regexp = {
            key: re.compile(rf"\s*\**{re.escape(value)}\s*\**:?")
            for key, value in basic_sections.items()
        }
        sections_regexp["problem_type"] = re.compile(
            r"\s*\**What seems to be the trouble\?\s*(?:\(Required\))?\s*\**:?"
        )
        sections_regexp["expected"] = re.compile(
            r"\s*[\*_]*Expected Behavior:?\s*[\*_]*:?"
        )
        sections_regexp["actual"] = re.compile(r"\s*[\*_]*Actual Behavior:?\s*[\*_]*:?")

        checklist_regexp = re.compile(r"\s*-\s*\[(.)\]\s*(.*)")
        url_re = re.compile(r"https?://[^ \)]*")

        seen_sections: set[str] = set()
        in_section = None
        buffer = []

        def flush_parsed_lines() -> None:
            if in_section is None:
                return

            seen_sections.add(in_section)

            if in_section == "url":
                for line in buffer:
                    urls = url_re.findall(line)
                    if urls:
                        data.bug_url = urls[0]
                        break

            elif in_section in {"browser", "os"}:
                for line in buffer:
                    if line.strip():
                        setattr(data, in_section, line.strip())
                        break

            elif in_section == "problem_type":
                for line in buffer:
                    m = checklist_regexp.match(line)
                    if m:
                        check_mark, value = m.groups()
                        if check_mark != " ":
                            data.problem_type = value.strip()

            elif in_section == "steps_to_reproduce":
                data.steps_to_reproduce = "\n".join(buffer).strip()

            elif in_section in {"expected", "actual"}:
                if data.description is None:
                    data.description = ""
                else:
                    data.description += "\n"
                data.description += (
                    f"{in_section.capitalize()} Behavior:\n"
                    + "\n".join(buffer).lstrip()
                )

            elif in_section == "screenshot":
                for line in buffer:
                    urls = url_re.findall(line)
                    if urls:
                        data.screenshot = urls[0]
                        break

            buffer.clear()

        for line in body.splitlines():
            line = line.strip()

            # Check if we're entering a new section
            for new_section, regexp in sections_regexp.items():
                m = regexp.match(line)
                if m:
                    if new_section not in seen_sections:
                        flush_parsed_lines()
                        in_section = new_section
                        line = line[m.end() :]
                        break
            buffer.append(line)

        flush_parsed_lines()

        return data


class SourceState(BaseModel):
    source_time: Optional[
        Annotated[
            datetime,
            AfterValidator(utc_from_naive_datetime),
            PlainSerializer(to_naive_datetime),
        ]
    ] = None
    next_url: Optional[str] = None


class BigQueryService:
    def __init__(self, project: Project, bq_client: BigQuery):
        self.project = project
        self.bq_client = bq_client
        self.import_runs_table = project["web_bugs"]["import_runs"].table()

    def get_source_states(self) -> Mapping[str, SourceState]:
        """Return the most recently recorded state for each source."""
        rows = self.bq_client.query(f"""
SELECT source, source_time, next_url
FROM (
  SELECT
    run_infos.source AS source,
    run_infos.source_time AS source_time,
    run_infos.next_url AS next_url,
    ROW_NUMBER() OVER (
      PARTITION BY run_infos.source ORDER BY run_at DESC
    ) AS rn
  FROM `{self.import_runs_table}`
  JOIN UNNEST(run_infos) AS run_infos
)
WHERE rn = 1""")
        return {
            row.source: SourceState(
                source_time=row.source_time,
                next_url=row.next_url,
            )
            for row in rows
        }

    def record_update(self, source_states: Mapping[str, SourceState]) -> None:
        self.bq_client.insert_query(
            self.import_runs_table,
            columns=[item.name for item in self.import_runs_table.fields],
            query="SELECT CURRENT_DATETIME() as run_at, @run_infos AS run_infos",
            parameters=[
                bigquery.ArrayQueryParameter(
                    "run_infos",
                    "RECORD",
                    [
                        bigquery.StructQueryParameter(
                            None,
                            bigquery.ScalarQueryParameter("source", "STRING", source),
                            bigquery.ScalarQueryParameter(
                                "source_time", "DATETIME", state.source_time
                            ),
                            bigquery.ScalarQueryParameter(
                                "next_url", "STRING", state.next_url
                            ),
                        )
                        for source, state in source_states.items()
                    ],
                ),
            ],
        )

    def insert_issues(
        self, table: TableSchema, issues: Mapping[int, BaseModel]
    ) -> None:
        if not issues:
            return
        self.bq_client.delete_query(
            table,
            condition="number IN UNNEST(@bug_numbers)",
            parameters=[
                bigquery.ArrayQueryParameter(
                    "bug_numbers", "INTEGER", list(issues.keys())
                )
            ],
        )
        # write_table here avoids streaming vs insert_rows
        self.bq_client.write_table(
            table,
            table.schema,
            [row.model_dump(mode="json") for row in issues.values()],
            overwrite=False,
        )


def load_from_file(
    bq_service: BigQueryService,
    repo: SourceRepo,
    path: Path,
) -> datetime:
    """Import issues from a data dump, returning how far up to date the dump is."""
    logging.info(f"Backfilling from file {path}")
    if path.suffix in {".zst", ".zstd"}:
        f = zstandard.open(path, "r")
    else:
        f = open(path)

    rows: dict[int, BaseModel] = {}
    source_time = None
    with f:
        for line in f:
            issue = github.GitHubIssue.model_validate_json(line)
            # Skipped issues still count towards the source time; they're
            # refetched from the API rather than being missing from the dump.
            if source_time is None or issue.updated_at > source_time:
                source_time = issue.updated_at
            if not repo.skip_from_file(issue):
                rows[issue.number] = repo.process_issue(issue)

            if len(rows) >= 10000:
                bq_service.insert_issues(repo.dest_table, rows)
                rows = {}

    bq_service.insert_issues(repo.dest_table, rows)

    if source_time is None:
        raise ValueError(f"No issues found in {path}")

    return source_time


class IncompleteImport(Exception):
    """An import made some progress, but didn't run to completion."""

    def __init__(self, state: SourceState):
        super().__init__()
        self.state = state


def import_issues(
    bq_service: BigQueryService,
    repo: SourceRepo,
    params: IssueParams,
    source_time: datetime,
) -> datetime:
    """Import the issues matching params, returning the latest time at which
    we have all known issues.

    For a backfill the returned time will be equal to the input source_time.
    This is conservative to ensure that we don't miss any issues.
    """
    is_backfill = isinstance(params, IssueBackfill)
    resume_point = None
    try:
        rows = {}
        for issues_page in repo.iter_issues(params):
            if issues_page.issues:
                if not is_backfill:
                    source_time = max(
                        source_time,
                        max(issue.updated_at for issue in issues_page.issues),
                    )
                rows.update(
                    {
                        issue.number: repo.process_issue(issue)
                        for issue in issues_page.issues
                    }
                )

            if rows and (
                len(rows) >= 1000
                or issues_page.next_url is None
                or issues_page.resume_at
            ):
                bq_service.insert_issues(repo.dest_table, rows)

                if issues_page.next_url is not None:
                    resume_point = SourceState(
                        source_time=source_time,
                        next_url=issues_page.next_url if is_backfill else None,
                    )
                rows = {}
    except (Exception, KeyboardInterrupt) as e:
        if resume_point is None:
            raise
        raise IncompleteImport(resume_point) from e

    return source_time


def import_repo(
    bq_service: BigQueryService,
    repo: SourceRepo,
    state: Optional[SourceState],
    backfill: bool,
    backfill_file: Optional[Path],
) -> SourceState:
    source_time = (
        state.source_time.replace(tzinfo=UTC)
        if state is not None and state.source_time is not None
        else None
    )
    next_url = state.next_url if state is not None else None

    # If we're doing a backfill start with that
    if backfill or source_time is None or next_url:
        milestone = None
        if next_url is None and backfill_file is not None:
            source_time = load_from_file(bq_service, repo, backfill_file)
            # The dump doesn't contain the content of private issues, so fetch
            # those from the API.
            milestone = repo.private_milestone
        elif source_time is None or next_url is None:
            latest = repo.gh_client.latest_issue_update(repo.repo)
            if latest is None:
                # This implies the repo is empty
                return SourceState()
            source_time = latest

        assert source_time is not None
        import_issues(bq_service, repo, IssueBackfill(next_url, milestone), source_time)

    # Now update to the latest state of the upstream repo either based on the
    # source_time of the last import, or of the backfill
    try:
        source_time = import_issues(
            bq_service, repo, IssueUpdate(source_time), source_time
        )
    except IncompleteImport:
        raise
    except (Exception, KeyboardInterrupt) as e:
        raise IncompleteImport(SourceState(source_time=source_time)) from e

    return SourceState(source_time=source_time)


def run(
    project: Project,
    bq_client: BigQuery,
    repos: Iterable[SourceRepo],
    backfill: bool = False,
    backfill_files: Optional[Mapping[str, Path]] = None,
    backfill_restart: bool = False,
) -> bool:
    if backfill_files is None:
        backfill_files = {}

    bq_service = BigQueryService(project, bq_client)

    source_states = bq_service.get_source_states()
    new_states: dict[str, SourceState] = {}

    for repo in repos:
        state = (
            source_states.get(repo.repo)
            if not (backfill and backfill_restart)
            else None
        )
        try:
            new_states[repo.repo] = import_repo(
                bq_service, repo, state, backfill, backfill_files.get(repo.repo)
            )
        except IncompleteImport as e:
            if isinstance(e.__cause__, Exception):
                logging.error(f"Failed with exception: {e.__cause__}")
            new_states[repo.repo] = e.state
            bq_service.record_update(new_states)
            return False

    bq_service.record_update(new_states)
    return True


def parse_backfill_paths(args: Iterable[str]) -> Mapping[str, Path]:
    rv = {}
    valid_repos = {
        item.repo
        for item in globals().values()
        if isinstance(item, type)
        and issubclass(item, SourceRepo)
        and item is not SourceRepo
    }
    for item in args:
        if ":" not in item:
            raise ValueError(
                f"Invalid --web-bugs-backfill-file {item}; must use the format repo:path"
            )
        repo, path = item.split(":", 1)
        if repo not in valid_repos:
            raise ValueError(
                f"Invalid --web-bugs-backfill-file {item}; {repo} is not a known repo"
            )
        path_obj = Path(path)
        if not path_obj.exists():
            raise ValueError(
                f"Invalid --web-bugs-backfill-file {item}; path does not exist"
            )
        rv[repo] = path_obj
    return rv


class WebBugsJob(EtlJob):
    name = "web-bugs"

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(
            title="web-bugs", description="web-bugs arguments"
        )
        group.add_argument(
            "--web-bugs-backfill",
            action="store_true",
            help="Backfill all web-bugs data.",
        )
        group.add_argument(
            "--web-bugs-backfill-file",
            action="append",
            help="repo:filename for JSONL file to use as the starting points for backfill",
        )
        group.add_argument(
            "--web-bugs-backfill-restart",
            action="store_true",
            help="Ignore any in-progress backfill.",
        )

    def default_dataset(self, context: Context) -> str:
        return "web_bugs"

    def main(self, context: Context) -> bool:
        gh_client = github.GitHub(context.args.github_token)

        return run(
            context.project,
            context.bq_client,
            [WebBugsRepo(gh_client, context.project["web_bugs"]["web_bugs"].table())],
            backfill=context.args.web_bugs_backfill,
            backfill_files=parse_backfill_paths(context.args.web_bugs_backfill_file)
            if context.args.web_bugs_backfill_file
            else None,
            backfill_restart=context.args.web_bugs_backfill_restart,
        )
