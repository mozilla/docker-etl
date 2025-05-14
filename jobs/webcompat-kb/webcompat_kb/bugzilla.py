import argparse
import enum
import json
import logging
import os
import re
import time
from typing import (
    Any,
    Iterable,
    Iterator,
    MutableMapping,
    Optional,
    Self,
)
from collections import defaultdict
from collections.abc import Sequence, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta

import bugdantic
from google.cloud import bigquery

from .base import EtlJob
from .bqhelpers import BigQuery


class BugLoadError(Exception):
    pass


@dataclass(frozen=True)
class Bug:
    id: int
    summary: str
    status: str
    resolution: str
    product: str
    component: str
    creator: str
    see_also: list[str]
    depends_on: list[int]
    blocks: list[int]
    priority: Optional[int]
    severity: Optional[int]
    creation_time: datetime
    assigned_to: Optional[str]
    keywords: list[str]
    url: str
    user_story: str
    last_resolved: Optional[datetime]
    last_change_time: datetime
    size_estimate: Optional[str]
    whiteboard: str
    webcompat_priority: Optional[str]
    webcompat_score: Optional[int]

    @property
    def parsed_user_story(self) -> Mapping[str, Any]:
        return parse_user_story(self.user_story)

    @property
    def resolved(self) -> Optional[datetime]:
        if self.status in {"RESOLVED", "VERIFIED"} and self.last_resolved:
            return self.last_resolved
        return None

    @classmethod
    def from_bugzilla(cls, bug: bugdantic.bugzilla.Bug) -> Self:
        assert bug.id is not None
        assert bug.summary is not None
        assert bug.status is not None
        assert bug.resolution is not None
        assert bug.product is not None
        assert bug.component is not None
        assert bug.creator is not None
        assert bug.see_also is not None
        assert bug.depends_on is not None
        assert bug.blocks is not None
        assert bug.priority is not None
        assert bug.severity is not None
        assert bug.creation_time is not None
        assert bug.assigned_to is not None
        assert bug.keywords is not None
        assert bug.url is not None
        assert bug.last_change_time is not None
        assert bug.whiteboard is not None
        assert bug.cf_user_story is not None

        return cls(
            id=bug.id,
            summary=bug.summary,
            status=bug.status,
            resolution=bug.resolution,
            product=bug.product,
            component=bug.component,
            see_also=bug.see_also,
            depends_on=bug.depends_on,
            blocks=bug.blocks,
            priority=extract_int_from_field(
                bug.priority,
                value_map={
                    "--": None,
                },
            ),
            severity=extract_int_from_field(
                bug.severity,
                value_map={
                    "n/a": None,
                    "--": None,
                    "blocker": 1,
                    "critical": 1,
                    "major": 2,
                    "normal": 3,
                    "minor": 4,
                    "trivial": 4,
                    "enhancement": 4,
                },
            ),
            creation_time=bug.creation_time,
            assigned_to=bug.assigned_to
            if bug.assigned_to != "nobody@mozilla.org"
            else None,
            keywords=bug.keywords,
            url=bug.url,
            user_story=bug.cf_user_story,
            last_resolved=bug.cf_last_resolved,
            last_change_time=bug.last_change_time,
            whiteboard=bug.whiteboard,
            creator=bug.creator,
            size_estimate=(
                bug.cf_size_estimate if bug.cf_size_estimate != "---" else None
            ),
            webcompat_priority=(
                bug.cf_webcompat_priority
                if bug.cf_webcompat_priority != "---"
                else None
            ),
            webcompat_score=extract_int_from_field(
                bug.cf_webcompat_score,
                value_map={
                    "---": None,
                    "?": None,
                },
            ),
        )

    def to_json(self) -> Mapping[str, Any]:
        fields = {**vars(self)}
        for key in fields:
            if isinstance(fields[key], datetime):
                fields[key] = fields[key].isoformat()
        return fields

    @classmethod
    def from_json(cls, bug_data: Mapping[str, Any]) -> Self:
        return cls(
            id=bug_data["id"],
            summary=bug_data["summary"],
            status=bug_data["status"],
            resolution=bug_data["resolution"],
            product=bug_data["product"],
            component=bug_data["component"],
            see_also=bug_data["see_also"],
            depends_on=bug_data["depends_on"],
            blocks=bug_data["blocks"],
            priority=bug_data["priority"],
            severity=bug_data["severity"],
            creation_time=datetime.fromisoformat(bug_data["creation_time"]),
            assigned_to=bug_data["assigned_to"],
            keywords=bug_data["keywords"],
            url=bug_data["url"],
            user_story=bug_data["user_story"],
            last_resolved=datetime.fromisoformat(bug_data["last_resolved"])
            if bug_data["last_resolved"] is not None
            else None,
            last_change_time=datetime.fromisoformat(bug_data["last_change_time"]),
            whiteboard=bug_data["whiteboard"],
            creator=bug_data["creator"],
            size_estimate=bug_data["size_estimate"],
            webcompat_priority=bug_data["webcompat_priority"],
            webcompat_score=bug_data["webcompat_score"],
        )


@dataclass(frozen=True)
class BugHistoryChange:
    field_name: str
    added: str
    removed: str


@dataclass(frozen=True)
class BugHistoryEntry:
    number: int
    who: str
    change_time: datetime
    changes: list[BugHistoryChange]


@dataclass(frozen=True)
class HistoryChange:
    number: int
    who: str
    change_time: datetime
    field_name: str
    added: str
    removed: str


class PropertyChange(enum.StrEnum):
    added = "added"
    removed = "removed"


@dataclass(frozen=True)
class PropertyHistoryItem:
    change_time: datetime
    change: PropertyChange


class PropertyHistory:
    """Representation of the history of a specific boolean property
    (i.e. one that can be present or not)"""

    def __init__(self) -> None:
        self.data: list[PropertyHistoryItem] = []

    def __len__(self) -> int:
        return len(self.data)

    def add(self, change_time: datetime, change: PropertyChange) -> None:
        self.data.append(PropertyHistoryItem(change_time=change_time, change=change))

    def missing_initial_add(self) -> bool:
        """Check if the property was initially added"""
        self.data.sort(key=lambda x: x.change_time)
        return len(self.data) == 0 or self.data[0].change == PropertyChange.removed


BugId = int
BugsById = Mapping[BugId, Bug]
MutBugsById = MutableMapping[BugId, Bug]

HistoryByBug = Mapping[BugId, Sequence[BugHistoryEntry]]

BUG_QUERIES: Mapping[str, dict[str, str | list[str]]] = {
    "webcompat_product": {
        "component": [
            "Knowledge Base",
            "Privacy: Site Reports",
            "Site Reports",
            "Interventions",
        ],
        "product": "Web Compatibility",
        "j_top": "OR",
        "f1": "bug_status",
        "o1": "changedafter",
        "v1": "2020-01-01",
        "f2": "resolution",
        "o2": "isempty",
    },
    "webcompat_other": {
        "f1": "product",
        "o1": "notequals",
        "v1": "Web Compatibility",
        "f2": "OP",
        "j2": "OR",
        "f3": "keywords",
        "o3": "casesubstring",
        "v3": "webcompat:",
        "f4": "keywords",
        "o4": "casesubstring",
        "v4": "parity-",
        "f5": "cf_user_story",
        "o5": "casesubstring",
        "v5": "web-feature:",
        "f6": "CP",
        "f7": "OP",
        "j7": "OR",
        "f8": "bug_status",
        "o8": "changedafter",
        "v8": "2020-01-01",
        "f9": "resolution",
        "o9": "isempty",
        "f10": "CP",
    },
}


@dataclass
class BugLinkConfig:
    table_name: str
    from_field_name: str
    to_field_name: str


@dataclass
class ExternalLinkConfig:
    table_name: str
    field_name: str
    match_substrs: list[str]

    def get_links(
        self, all_bugs: BugsById, kb_bugs: set[BugId]
    ) -> Mapping[BugId, set[str]]:
        rv: defaultdict[int, set[str]] = defaultdict(set)
        for kb_bug_id in kb_bugs:
            bug_ids = [kb_bug_id] + all_bugs[kb_bug_id].depends_on
            for bug_id in bug_ids:
                if bug_id not in all_bugs:
                    continue
                bug = all_bugs[bug_id]
                for entry in bug.see_also:
                    if any(substr in entry for substr in self.match_substrs):
                        rv[kb_bug_id].add(entry)
        return rv


EXTERNAL_LINK_CONFIGS = {
    config.table_name: config
    for config in [
        ExternalLinkConfig(
            "interventions",
            "code_url",
            ["github.com/mozilla-extensions/webcompat-addon"],
        ),
        ExternalLinkConfig(
            "other_browser_issues",
            "issue_url",
            ["bugs.chromium.org", "bugs.webkit.org", "crbug.com"],
        ),
        ExternalLinkConfig(
            "standards_issues",
            "issue_url",
            ["github.com/w3c", "github.com/whatwg", "github.com/wicg"],
        ),
        ExternalLinkConfig(
            "standards_positions", "discussion_url", ["standards-positions"]
        ),
    ]
}


def extract_int_from_field(
    field_value: Optional[str], value_map: Optional[Mapping[str, Optional[int]]] = None
) -> Optional[int]:
    if field_value:
        if value_map and field_value.lower() in value_map:
            return value_map[field_value.lower()]

        match = re.search(r"\d+", field_value)
        if match:
            return int(match.group())
        logging.warning(
            f"Unexpected field value '{field_value}', could not convert to integer"
        )
    return None


def parse_user_story(input_string: str) -> Mapping[str, str | list[str]]:
    if not input_string:
        return {}

    lines = input_string.splitlines()

    result_dict: dict[str, str | list[str]] = {}

    for line in lines:
        if line:
            key_value = line.split(":", 1)
            if len(key_value) == 2:
                key, value = key_value
                if key in result_dict:
                    current_value = result_dict[key]
                    if isinstance(current_value, list):
                        current_value.append(value)
                    else:
                        result_dict[key] = [current_value, value]
                else:
                    result_dict[key] = value
    if not result_dict:
        return {}

    return result_dict


class BugCache(Mapping):
    def __init__(self, bz_client: bugdantic.Bugzilla):
        self.bz_client = bz_client
        self.bugs: MutBugsById = {}

    def __getitem__(self, key: BugId) -> Bug:
        return self.bugs[key]

    def __len__(self) -> int:
        return len(self.bugs)

    def __iter__(self) -> Iterator[BugId]:
        yield from self.bugs

    def bz_fetch_bugs(
        self,
        params: Optional[dict[str, str | list[str]]] = None,
        bug_ids: Optional[Sequence[BugId]] = None,
    ) -> None:
        if (params is None and bug_ids is None) or (
            params is not None and bug_ids is not None
        ):
            raise ValueError("Must pass params or ids but not both")

        fields = [
            "id",
            "summary",
            "status",
            "resolution",
            "product",
            "component",
            "creator",
            "see_also",
            "depends_on",
            "blocks",
            "priority",
            "severity",
            "creation_time",
            "assigned_to",
            "keywords",
            "url",
            "cf_user_story",
            "cf_last_resolved",
            "last_change_time",
            "whiteboard",
            "cf_size_estimate",
            "cf_webcompat_priority",
            "cf_webcompat_score",
        ]

        try:
            if params is not None:
                bugs = self.bz_client.search(
                    query=params, include_fields=fields, page_size=200
                )
            else:
                assert bug_ids is not None
                bugs = self.bz_client.bugs(
                    bug_ids, include_fields=fields, page_size=200
                )
            for bug in bugs:
                assert bug.id is not None
                self.bugs[bug.id] = Bug.from_bugzilla(bug)
        except Exception as e:
            logging.error(f"Error: {e}")
            raise

    def missing_relations(self, bugs: BugsById, relation: str) -> set[BugId]:
        related_ids = set()
        for bug in bugs.values():
            related_ids |= {
                bug_id for bug_id in getattr(bug, relation) if bug_id not in self
            }
        return related_ids

    def into_mapping(self) -> BugsById:
        """Convert the data into a plain dict.

        Also reset this object, so we aren't sharing the state between multiple places
        """
        bugs = self.bugs
        self.bugs = {}
        return bugs


def is_site_report(bug: Bug) -> bool:
    return (bug.product == "Web Compatibility" and bug.component == "Site Reports") or (
        bug.product != "Web Compatibility" and "webcompat:site-report" in bug.keywords
    )


def is_etp_report(bug: Bug) -> bool:
    return (
        bug.product == "Web Compatibility" and bug.component == "Privacy: Site Reports"
    )


def is_kb_entry(bug: Bug) -> bool:
    """Get things that are directly in the knowledge base.

    This doesn't include core bugs that should be considered part of the knowledge base
    because they directly block a platform bug."""
    return bug.product == "Web Compatibility" and bug.component == "Knowledge Base"


def is_webcompat_platform_bug(bug: Bug) -> bool:
    """Check if a bug is a platform bug .

    These are only actually in the kb if they also block a site report"""
    return (
        bug.product != "Web Compatibility" and "webcompat:platform-bug" in bug.keywords
    )


def get_kb_bug_core_bugs(
    all_bugs: BugsById, kb_bugs: set[BugId], platform_bugs: set[BugId]
) -> Mapping[BugId, set[BugId]]:
    rv = defaultdict(set)
    for kb_id in kb_bugs:
        if kb_id not in platform_bugs:
            for bug_id in all_bugs[kb_id].depends_on:
                if bug_id in platform_bugs:
                    rv[kb_id].add(bug_id)
    return rv


def get_kb_bug_site_report(
    all_bugs: BugsById, kb_bugs: set[BugId], site_report_bugs: set[BugId]
) -> Mapping[BugId, set[BugId]]:
    rv = defaultdict(set)
    for kb_id in kb_bugs:
        if kb_id in site_report_bugs:
            rv[kb_id].add(kb_id)
        for bug_id in all_bugs[kb_id].blocks:
            if bug_id in site_report_bugs:
                rv[kb_id].add(bug_id)
    return rv


def get_etp_breakage_reports(
    all_bugs: BugsById, etp_reports: set[BugId]
) -> Mapping[BugId, set[BugId]]:
    rv = {}
    for bug_id in etp_reports:
        report_bug = all_bugs[bug_id]
        meta_bugs = {
            meta_id
            for meta_id in report_bug.depends_on + report_bug.blocks
            if meta_id in all_bugs and "meta" in all_bugs[meta_id].keywords
        }
        if meta_bugs:
            rv[bug_id] = meta_bugs
    return rv


def fetch_all_bugs(
    bz_client: bugdantic.Bugzilla,
) -> BugsById:
    """Get all the bugs that should be imported into BigQuery.

    :returns: A tuple of (all bugs, site report bugs, knowledge base bugs,
                          core bugs, ETP report bugs, ETP dependencies)."""

    bug_cache = BugCache(bz_client)

    for category, filter_config in BUG_QUERIES.items():
        logging.info(f"Fetching {category} bugs")
        bug_cache.bz_fetch_bugs(params=filter_config)

    tried_to_fetch: set[BugId] = set()
    missing_relations = None
    # Add a limit on how many fetches we will try
    recurse_limit = 10
    for _ in range(recurse_limit):
        # Get all blocking bugs for site reports or kb entries or etp site reports
        # This can take more than one iteration if dependencies themselves turn out
        # to be site reports that were excluded by a the date cutoff
        missing_relations = bug_cache.missing_relations(
            {
                bug_id: bug
                for bug_id, bug in bug_cache.items()
                if is_site_report(bug) or is_kb_entry(bug) or is_etp_report(bug)
            },
            "depends_on",
        )
        missing_relations |= bug_cache.missing_relations(
            {bug_id: bug for bug_id, bug in bug_cache.items() if is_etp_report(bug)},
            "blocks",
        )
        # If we already tried to fetch a bug don't try to fetch it again
        missing_relations -= tried_to_fetch
        if not missing_relations:
            break

        tried_to_fetch |= missing_relations
        logging.info("Fetching related bugs")
        bug_cache.bz_fetch_bugs(bug_ids=list(missing_relations))
        for bug_id in missing_relations:
            if bug_id not in bug_cache:
                logging.warning(f"Failed to fetch bug {bug_id}")
    else:
        logging.warning(
            f"Failed to fetch all dependencies after {recurse_limit} attempts"
        )

    return bug_cache.into_mapping()


class BugHistoryUpdater:
    def __init__(
        self,
        bq_client: BigQuery,
        bz_client: bugdantic.Bugzilla,
    ):
        self.bq_client = bq_client
        self.bz_client = bz_client

    def run(self, all_bugs: BugsById, recreate: bool) -> HistoryByBug:
        if not recreate:
            existing_records = self.bigquery_fetch_history(all_bugs.keys())
            new_bugs, existing_bugs = self.group_bugs(all_bugs)
            existing_bugs_history = self.missing_records(
                existing_records, self.existing_bugs_history(existing_bugs)
            )
        else:
            existing_records = {}
            new_bugs = all_bugs
            existing_bugs_history = {}

        new_bugs_history = self.missing_records(
            existing_records, self.new_bugs_history(new_bugs)
        )

        if not (new_bugs_history or existing_bugs_history):
            logging.info("No relevant history updates")
            return {}

        return self.merge_history(existing_bugs_history, new_bugs_history)

    def group_bugs(self, all_bugs: BugsById) -> tuple[BugsById, BugsById]:
        all_ids = set(all_bugs.keys())
        existing_ids = self.bigquery_fetch_imported_ids()
        new_ids = all_ids - existing_ids

        new_bugs = {
            bug_id: bug for bug_id, bug in all_bugs.items() if bug_id in new_ids
        }
        existing_bugs = {
            bug_id: bug for bug_id, bug in all_bugs.items() if bug_id not in new_ids
        }
        return new_bugs, existing_bugs

    def merge_history(self, *sources: HistoryByBug) -> HistoryByBug:
        history: defaultdict[BugId, list[BugHistoryEntry]] = defaultdict(list)
        for source in sources:
            for bug_id, changes in source.items():
                history[bug_id].extend(changes)
        return history

    def new_bugs_history(self, new_bugs: BugsById) -> HistoryByBug:
        history = self.bugzilla_fetch_history(new_bugs.keys())
        synthetic_history = self.create_initial_history_entry(new_bugs, history)
        return self.merge_history(history, synthetic_history)

    def existing_bugs_history(self, existing_bugs: BugsById) -> HistoryByBug:
        last_import_time = self.bigquery_last_import()

        if last_import_time is None:
            logging.info("No previous history update found")
            return {}

        updated_bugs = {
            bug_id
            for bug_id, bug in existing_bugs.items()
            if bug.last_change_time > last_import_time
        }

        if not updated_bugs:
            logging.info(f"No updated bugs since {last_import_time.isoformat()}")
            return {}

        logging.info(
            f"Fetching history for bugs {','.join(str(item) for item in updated_bugs)} updated since {last_import_time.isoformat()}"
        )

        bugs_full_history = self.bugzilla_fetch_history(updated_bugs)
        # Filter down to only recent updates, since we always get the full history
        bugs_history = {}
        for bug_id, bug_full_history in bugs_full_history.items():
            bug_history = [
                item for item in bug_full_history if item.change_time > last_import_time
            ]
            if bug_history:
                bugs_history[bug_id] = bug_history

        return bugs_history

    def missing_records(
        self, existing_records: HistoryByBug, updates: HistoryByBug
    ) -> HistoryByBug:
        if not existing_records:
            return updates

        existing_history = set(self.flatten_history(existing_records))
        new_history = set(self.flatten_history(updates))

        diff = new_history - existing_history

        return self.unflatten_history(item for item in new_history if item in diff)

    def bigquery_fetch_imported_ids(self) -> set[int]:
        query = """SELECT number FROM bugzilla_bugs"""
        res = self.bq_client.query(query)
        rows = list(res)

        imported_ids = {bug["number"] for bug in rows}

        return imported_ids

    def bigquery_last_import(self) -> Optional[datetime]:
        query = """
                SELECT MAX(run_at) AS last_run_at
                FROM import_runs
                WHERE is_history_fetch_completed = TRUE
            """
        res = self.bq_client.query(query)
        row = list(res)[0]
        return row["last_run_at"]

    def bugzilla_fetch_history(self, ids: Iterable[int]) -> HistoryByBug:
        history: dict[int, list[bugdantic.bugzilla.History]] = {}
        chunk_size = 100
        ids_list = list(ids)
        max_retries = 3

        for retry in range(max_retries):
            retry += 1
            retry_ids: list[int] = []

            chunks: list[list[int]] = []
            for i in range((len(ids_list) // chunk_size) + 1):
                offset = i * chunk_size
                bug_ids = ids_list[offset : offset + chunk_size]
                if bug_ids:
                    chunks.append(bug_ids)

            for i, bug_ids in enumerate(chunks):
                logging.info(
                    f"Fetching history from bugzilla for {','.join(str(item) for item in bug_ids)} ({i + 1}/{len(chunks)})"
                )
                try:
                    results = self.bz_client.search(
                        query={"id": bug_ids}, include_fields=["id", "history"]
                    )
                except Exception as e:
                    logging.warning(f"Search request failed:\n{e}")
                    results = []

                for bug in results:
                    assert bug.id is not None
                    assert bug.history is not None
                    history[bug.id] = bug.history

                # Add anything missing from the response to the retry list
                retry_ids.extend(item for item in bug_ids if item not in history)

                time.sleep(1)

            ids_list = retry_ids
            if retry_ids:
                time.sleep(10)
                logging.info(f"Retrying {len(retry_ids)} bugs with missing history")
            else:
                break

        if len(ids_list) != 0:
            raise BugLoadError(
                f"Failed to fetch bug history for {','.join(str(item) for item in ids_list)}"
            )

        return self.bugzilla_to_history_entry(history)

    def bugzilla_to_history_entry(
        self, updated_history: Mapping[int, list[bugdantic.bugzilla.History]]
    ) -> HistoryByBug:
        rv: dict[int, list[BugHistoryEntry]] = defaultdict(list)

        for bug_id, history in updated_history.items():
            # Need to ensure we have an entry for every bug even if there isn't any history
            rv[bug_id] = []
            for record in history:
                relevant_changes = [
                    BugHistoryChange(
                        field_name=change.field_name,
                        added=change.added,
                        removed=change.removed,
                    )
                    for change in record.changes
                    if change.field_name
                    in ["keywords", "status", "url", "cf_user_story"]
                ]

                if relevant_changes:
                    filtered_record = BugHistoryEntry(
                        number=bug_id,
                        who=record.who,
                        change_time=record.when,
                        changes=relevant_changes,
                    )
                    rv[bug_id].append(filtered_record)

        return rv

    def bigquery_fetch_history(self, bug_ids: Iterable[int]) -> HistoryByBug:
        rv: defaultdict[int, list[BugHistoryEntry]] = defaultdict(list)
        formatted_numbers = ", ".join(str(bug_id) for bug_id in bug_ids)
        query = f"""
                    SELECT *
                    FROM bugs_history
                    WHERE number IN ({formatted_numbers})
                """
        result = self.bq_client.query(query)
        for row in result:
            rv[row["number"]].append(
                BugHistoryEntry(
                    row["number"],
                    row["who"],
                    row["change_time"],
                    changes=[
                        BugHistoryChange(
                            change["field_name"], change["added"], change["removed"]
                        )
                        for change in row["changes"]
                    ],
                )
            )
        return rv

    def keyword_history(
        self, history: HistoryByBug
    ) -> Mapping[BugId, Mapping[str, PropertyHistory]]:
        """Get the time each keyword has been added and removed from each bug"""
        keyword_history: defaultdict[int, dict[str, PropertyHistory]] = defaultdict(
            dict
        )

        for bug_id, records in history.items():
            for record in records:
                for change in record.changes:
                    if change.field_name == "keywords":
                        for src, change_type in [
                            (change.added, PropertyChange.added),
                            (change.removed, PropertyChange.removed),
                        ]:
                            if src:
                                for keyword in src.split(", "):
                                    if keyword not in keyword_history[bug_id]:
                                        keyword_history[bug_id][keyword] = (
                                            PropertyHistory()
                                        )
                                    keyword_history[bug_id][keyword].add(
                                        change_time=record.change_time,
                                        change=change_type,
                                    )

        return keyword_history

    def get_missing_keywords(
        self,
        bug_id: int,
        current_keywords: list[str],
        keyword_history: Mapping[BugId, Mapping[str, PropertyHistory]],
    ) -> set[str]:
        missing_keywords = set()

        # Check if keyword exists, but is not in "added" history
        for keyword in current_keywords:
            if bug_id not in keyword_history or keyword not in keyword_history[bug_id]:
                missing_keywords.add(keyword)

        # Check for keywords that have "removed" record as the earliest
        # event in the sorted timeline
        if bug_id in keyword_history:
            for keyword, history in keyword_history[bug_id].items():
                if history.missing_initial_add():
                    missing_keywords.add(keyword)

        return missing_keywords

    def create_initial_history_entry(
        self, all_bugs: BugsById, history: HistoryByBug
    ) -> HistoryByBug:
        """Backfill history entries for bug creation.

        If a bug has keywords set, but there isn't a history entry corresponding
        to the keyword being added, we assume they were set on bug creation, and
        create a history entry to represent that."""
        result: dict[int, list[BugHistoryEntry]] = {}

        keyword_history = self.keyword_history(history)
        for bug_id, bug in all_bugs.items():
            missing_keywords = self.get_missing_keywords(
                bug_id, bug.keywords, keyword_history
            )

            if missing_keywords:
                logging.debug(
                    f"Adding initial history for bug {bug_id} with keywords {missing_keywords}"
                )
                record = BugHistoryEntry(
                    number=bug.id,
                    who=bug.creator,
                    change_time=bug.creation_time,
                    changes=[
                        BugHistoryChange(
                            added=", ".join(sorted(missing_keywords)),
                            field_name="keywords",
                            removed="",
                        )
                    ],
                )
                result[bug.id] = [record]
        return result

    def flatten_history(self, history: HistoryByBug) -> Iterable[HistoryChange]:
        for records in history.values():
            for record in records:
                for change in record.changes:
                    yield HistoryChange(
                        record.number,
                        record.who,
                        record.change_time,
                        change.field_name,
                        change.added,
                        change.removed,
                    )

    def unflatten_history(self, diff: Iterable[HistoryChange]) -> HistoryByBug:
        changes: dict[tuple[int, str, datetime], BugHistoryEntry] = {}
        for item in diff:
            key = (item.number, item.who, item.change_time)

            if key not in changes:
                changes[key] = BugHistoryEntry(
                    number=item.number,
                    who=item.who,
                    change_time=item.change_time,
                    changes=[],
                )
            changes[key].changes.append(
                BugHistoryChange(
                    field_name=item.field_name,
                    added=item.added,
                    removed=item.removed,
                )
            )

        rv: dict[int, list[BugHistoryEntry]] = {}
        for change in changes.values():
            if change.number not in rv:
                rv[change.number] = []
            rv[change.number].append(change)
        return rv


class BigQueryImporter:
    """Class to handle all writes to BigQuery"""

    def __init__(self, bq_client: BigQuery):
        self.client = bq_client

    def write_table(
        self,
        table_name: str,
        schema: list[bigquery.SchemaField],
        rows: Sequence[Mapping[str, Any]],
        overwrite: bool,
    ) -> None:
        table = self.client.ensure_table(table_name, schema, False)
        self.client.write_table(table, schema, rows, overwrite)

    def convert_bug(self, bug: Bug) -> Mapping[str, Any]:
        return {
            "number": bug.id,
            "title": bug.summary,
            "status": bug.status,
            "resolution": bug.resolution,
            "product": bug.product,
            "component": bug.component,
            "creator": bug.creator,
            "severity": bug.severity,
            "priority": bug.priority,
            "creation_time": bug.creation_time.isoformat(),
            "assigned_to": bug.assigned_to,
            "keywords": bug.keywords,
            "url": bug.url,
            "user_story": bug.parsed_user_story,
            "user_story_raw": bug.user_story,
            "resolved_time": bug.resolved.isoformat()
            if bug.resolved is not None
            else None,
            "whiteboard": bug.whiteboard,
            "size_estimate": bug.size_estimate,
            "webcompat_priority": bug.webcompat_priority,
            "webcompat_score": bug.webcompat_score,
            "depends_on": bug.depends_on,
            "blocks": bug.blocks,
        }

    def convert_history_entry(self, entry: BugHistoryEntry) -> Mapping[str, Any]:
        return {
            "number": entry.number,
            "who": entry.who,
            "change_time": entry.change_time.isoformat(),
            "changes": [
                {
                    "field_name": change.field_name,
                    "added": change.added,
                    "removed": change.removed,
                }
                for change in entry.changes
            ],
        }

    def insert_bugs(self, all_bugs: BugsById) -> None:
        table = "bugzilla_bugs"
        schema = [
            bigquery.SchemaField("number", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("resolution", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("product", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("component", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("creator", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("severity", "INTEGER"),
            bigquery.SchemaField("priority", "INTEGER"),
            bigquery.SchemaField("creation_time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("assigned_to", "STRING"),
            bigquery.SchemaField("keywords", "STRING", mode="REPEATED"),
            bigquery.SchemaField("url", "STRING"),
            bigquery.SchemaField("user_story", "JSON"),
            bigquery.SchemaField("user_story_raw", "STRING"),
            bigquery.SchemaField("resolved_time", "TIMESTAMP"),
            bigquery.SchemaField("size_estimate", "STRING"),
            bigquery.SchemaField("whiteboard", "STRING"),
            bigquery.SchemaField("webcompat_priority", "STRING"),
            bigquery.SchemaField("webcompat_score", "INTEGER"),
            bigquery.SchemaField("depends_on", "INTEGER", mode="REPEATED"),
            bigquery.SchemaField("blocks", "INTEGER", mode="REPEATED"),
        ]
        rows = [self.convert_bug(bug) for bug in all_bugs.values()]
        self.write_table(table, schema, rows, overwrite=True)

    def insert_history_changes(
        self, history_entries: HistoryByBug, recreate: bool
    ) -> None:
        schema = [
            bigquery.SchemaField("number", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("who", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("change_time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField(
                "changes",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("field_name", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("added", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("removed", "STRING", mode="REQUIRED"),
                ],
            ),
        ]

        rows = [
            self.convert_history_entry(entry)
            for entries in history_entries.values()
            for entry in entries
        ]
        self.write_table("bugs_history", schema, rows, overwrite=recreate)

    def insert_bug_list(
        self, table_name: str, field_name: str, bugs: Iterable[BugId]
    ) -> None:
        schema = [bigquery.SchemaField(field_name, "INTEGER", mode="REQUIRED")]
        rows = [{field_name: bug_id} for bug_id in bugs]
        self.write_table(table_name, schema, rows, overwrite=True)

    def insert_bug_links(
        self, link_config: BugLinkConfig, links_by_bug: Mapping[BugId, Iterable[BugId]]
    ) -> None:
        schema = [
            bigquery.SchemaField(
                link_config.from_field_name, "INTEGER", mode="REQUIRED"
            ),
            bigquery.SchemaField(link_config.to_field_name, "INTEGER", mode="REQUIRED"),
        ]
        rows = [
            {
                link_config.from_field_name: from_bug_id,
                link_config.to_field_name: to_bug_id,
            }
            for from_bug_id, to_bug_ids in links_by_bug.items()
            for to_bug_id in to_bug_ids
        ]
        self.write_table(link_config.table_name, schema, rows, overwrite=True)

    def insert_external_links(
        self,
        link_config: ExternalLinkConfig,
        links_by_bug: Mapping[BugId, Iterable[str]],
    ) -> None:
        schema = [
            bigquery.SchemaField("knowledge_base_bug", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField(link_config.field_name, "STRING", mode="REQUIRED"),
        ]
        rows = [
            {"knowledge_base_bug": bug_id, link_config.field_name: link_text}
            for bug_id, links in links_by_bug.items()
            for link_text in links
        ]
        self.write_table(link_config.table_name, schema, rows, overwrite=True)

    def record_import_run(
        self,
        start_time: float,
        count: int,
        history_count: Optional[int],
        last_change_time: datetime,
    ) -> None:
        elapsed_time = time.monotonic() - start_time
        elapsed_time_delta = timedelta(seconds=elapsed_time)
        run_at = last_change_time - elapsed_time_delta

        schema = [
            bigquery.SchemaField("run_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("bugs_imported", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("bugs_history_updated", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField(
                "is_history_fetch_completed", "BOOLEAN", mode="REQUIRED"
            ),
        ]

        rows_to_insert = [
            {
                "run_at": run_at,
                "bugs_imported": count,
                "bugs_history_updated": history_count
                if history_count is not None
                else 0,
                "is_history_fetch_completed": history_count is not None,
            },
        ]
        import_runs_table = self.client.ensure_table("import_runs", schema=schema)
        self.client.insert_rows(import_runs_table, rows_to_insert)


def get_kb_entries(all_bugs: BugsById, site_report_blockers: set[BugId]) -> set[BugId]:
    direct_kb_entries = {bug_id for bug_id, bug in all_bugs.items() if is_kb_entry(bug)}
    kb_blockers = {
        dependency
        for bug_id in direct_kb_entries
        for dependency in all_bugs[bug_id].depends_on
        if dependency in all_bugs
    }
    # We include any bug that's blocking a site report but isn't in Web Compatibility
    platform_site_report_blockers = {
        bug_id
        for bug_id in site_report_blockers
        if bug_id not in kb_blockers and all_bugs[bug_id].product != "Web Compatibility"
    }
    # We also include all other bugs that are platform bugs but don't depend on a kb entry
    # TODO: This is probably too many bugs; platform bugs that don't block any site reports
    # should likely be excluded
    platform_kb_entries = {
        bug_id
        for bug_id in all_bugs
        if bug_id not in kb_blockers and is_webcompat_platform_bug(all_bugs[bug_id])
    }
    return direct_kb_entries | platform_site_report_blockers | platform_kb_entries


def group_bugs(
    all_bugs: BugsById,
) -> tuple[set[BugId], set[BugId], set[BugId], set[BugId]]:
    """Extract groups of bugs according to their types"""
    site_reports = {bug_id for bug_id, bug in all_bugs.items() if is_site_report(bug)}
    etp_reports = {bug_id for bug_id, bug in all_bugs.items() if is_etp_report(bug)}
    site_report_blockers = {
        dependency
        for bug_id in site_reports
        for dependency in all_bugs[bug_id].depends_on
        # This might not be true if the dependency is a bug we can't access
        if dependency in all_bugs
    }
    kb_bugs = get_kb_entries(all_bugs, site_report_blockers)
    platform_bugs = {
        bug_id for bug_id in all_bugs if all_bugs[bug_id].product != "Web Compatibility"
    }
    return site_reports, etp_reports, kb_bugs, platform_bugs


def write_bugs(
    path: str,
    all_bugs: BugsById,
    site_reports: set[BugId],
    etp_reports: set[BugId],
    kb_bugs: set[BugId],
    platform_bugs: set[BugId],
    bug_links: Iterable[tuple[BugLinkConfig, Mapping[BugId, set[BugId]]]],
    external_links: Iterable[tuple[ExternalLinkConfig, Mapping[BugId, set[str]]]],
) -> None:
    data: dict[str, Any] = {}
    data["all_bugs"] = {bug_id: bug.to_json() for bug_id, bug in all_bugs.items()}
    data["site_report"] = list(site_reports)
    data["etp_reports"] = list(etp_reports)
    data["kb_bugs"] = list(kb_bugs)
    data["platform_bugs"] = list(platform_bugs)
    for bug_link_config, link_data in bug_links:
        data[bug_link_config.table_name] = {
            bug_id: list(values) for bug_id, values in link_data.items()
        }
    for external_link_config, external_link_data in external_links:
        data[external_link_config.table_name] = {
            bug_id: list(values) for bug_id, values in external_link_data.items()
        }

    with open(path, "w") as f:
        json.dump(data, f)


def load_bugs(
    bz_client: bugdantic.Bugzilla, load_bug_data_path: Optional[str]
) -> BugsById:
    if load_bug_data_path is not None:
        try:
            logging.info(f"Reading bug data from {load_bug_data_path}")
            with open(load_bug_data_path) as f:
                data = json.load(f)
            return {
                int(bug_id): Bug.from_json(bug_data)
                for bug_id, bug_data in data["all_bugs"].items()
            }
        except Exception as e:
            raise BugLoadError(f"Reading bugs from {load_bug_data_path} failed") from e
    else:
        try:
            return fetch_all_bugs(bz_client)
        except Exception as e:
            raise BugLoadError(
                "Fetching bugs from Bugzilla was not completed due to an error, aborting."
            ) from e


def run(
    bq_client: BigQuery,
    bq_dataset_id: str,
    bz_client: bugdantic.Bugzilla,
    write: bool,
    include_history: bool,
    recreate_history: bool,
    write_bug_data_path: Optional[str],
    load_bug_data_path: Optional[str],
) -> None:
    start_time = time.monotonic()

    all_bugs = load_bugs(bz_client, load_bug_data_path)

    history_changes = None
    if include_history:
        history_updater = BugHistoryUpdater(bq_client, bz_client)
        try:
            history_changes = history_updater.run(all_bugs, recreate_history)
        except Exception as e:
            logging.error(f"Exception updating history: {e}")
            raise
    else:
        logging.info("Not updating bug history")

    site_reports, etp_reports, kb_bugs, platform_bugs = group_bugs(all_bugs)

    # Links between different kinds of bugs
    bug_links = [
        (
            BugLinkConfig("breakage_reports", "knowledge_base_bug", "breakage_bug"),
            get_kb_bug_site_report(all_bugs, kb_bugs, site_reports),
        ),
        (
            BugLinkConfig("core_bugs", "knowledge_base_bug", "core_bug"),
            get_kb_bug_core_bugs(all_bugs, kb_bugs, platform_bugs),
        ),
        (
            BugLinkConfig("etp_breakage_reports", "breakage_bug", "etp_meta_bug"),
            get_etp_breakage_reports(all_bugs, etp_reports),
        ),
    ]

    # Links between bugs and external data sources
    external_links = [
        (config, config.get_links(all_bugs, kb_bugs))
        for config in EXTERNAL_LINK_CONFIGS.values()
    ]

    if write_bug_data_path is not None:
        write_bugs(
            write_bug_data_path,
            all_bugs,
            site_reports,
            etp_reports,
            kb_bugs,
            platform_bugs,
            bug_links,
            external_links,
        )

    last_change_time_max = max(bug.last_change_time for bug in all_bugs.values())

    # Finally do the actual import
    importer = BigQueryImporter(bq_client)
    importer.insert_bugs(all_bugs)
    if history_changes is not None:
        importer.insert_history_changes(history_changes, recreate=recreate_history)

    importer.insert_bug_list("kb_bugs", "number", kb_bugs)

    for bug_link_config, data in bug_links:
        importer.insert_bug_links(bug_link_config, data)

    for external_link_config, links_by_bug in external_links:
        importer.insert_external_links(external_link_config, links_by_bug)

    importer.record_import_run(
        start_time,
        len(all_bugs),
        len(history_changes) if history_changes is not None else None,
        last_change_time_max,
    )


class BugzillaJob(EtlJob):
    name = "bugzilla"

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(
            title="Bugzilla", description="Bugzilla import arguments"
        )
        group.add_argument(
            "--bugzilla-api-key",
            help="Bugzilla API key",
            default=os.environ.get("BUGZILLA_API_KEY"),
        )
        group.add_argument(
            "--bugzilla-no-history",
            dest="bugzilla_include_history",
            action="store_false",
            default=True,
            help="Don't read or update bug history",
        )
        group.add_argument(
            "--bugzilla-recreate-history",
            action="store_true",
            help="Re-read bug history from scratch",
        )
        group.add_argument(
            "--bugzilla-write-bug-data",
            action="store",
            help="Path to write bug data as a JSON file",
        )
        group.add_argument(
            "--bugzilla-load-bug-data",
            action="store",
            help="Path to JSON file to load bug data from",
        )

    def default_dataset(self, args: argparse.Namespace) -> str:
        return args.bq_kb_dataset

    def main(self, bq_client: BigQuery, args: argparse.Namespace) -> None:
        bz_config = bugdantic.BugzillaConfig(
            "https://bugzilla.mozilla.org",
            args.bugzilla_api_key,
            allow_writes=args.write,
        )
        bz_client = bugdantic.Bugzilla(bz_config)

        run(
            bq_client,
            args.bq_kb_dataset,
            bz_client,
            args.write,
            args.bugzilla_include_history,
            args.bugzilla_recreate_history,
            args.bugzilla_write_bug_data,
            args.bugzilla_load_bug_data,
        )
