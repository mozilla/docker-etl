import argparse
import logging
import os
import re
import time
from typing import (
    Any,
    Iterable,
    Iterator,
    Mapping,
    MutableMapping,
    NamedTuple,
    Optional,
    Union,
    cast,
)
from datetime import datetime, timedelta

import bugdantic
from google.cloud import bigquery

from .base import EtlJob

Bug = Mapping[str, Any]
BugsById = Mapping[int, Bug]
MutBugsById = MutableMapping[int, Bug]
BugHistoryResponse = Mapping[str, Any]
BugHistoryEntry = Mapping[str, Any]
Relations = Mapping[str, list[Mapping[str, Any]]]
RelationConfig = Mapping[str, Mapping[str, Any]]


class HistoryRow(NamedTuple):
    number: int
    who: str
    change_time: datetime
    field_name: str
    added: list[str]
    removed: list[str]


class BugFetchError(Exception):
    pass


BUGZILLA_URL = "https://bugzilla.mozilla.org/"

OTHER_BROWSER = ["bugs.chromium.org", "bugs.webkit.org", "crbug.com"]
STANDARDS_ISSUES = ["github.com/w3c", "github.com/whatwg", "github.com/wicg"]
STANDARDS_POSITIONS = ["standards-positions"]
INTERVENTIONS = ["github.com/mozilla-extensions/webcompat-addon"]
FIELD_MAP = {
    "blocker": 1,
    "critical": 1,
    "major": 2,
    "normal": 3,
    "minor": 4,
    "trivial": 4,
    "enhancement": 4,
    "n/a": None,
    "--": None,
}

FILTER_CONFIG = {
    "site_reports_wc": {
        "product": "Web Compatibility",
        "component": "Site Reports",
        "f1": "OP",
        "f2": "bug_status",
        "o2": "changedafter",
        "v2": "2020-01-01",
        "j1": "OR",
        "f3": "resolution",
        "o3": "isempty",
        "f4": "CP",
    },
    "site_reports_other": {
        "f1": "product",
        "o1": "notequals",
        "v1": "Web Compatibility",
        "f2": "keywords",
        "o2": "substring",
        "v2": "webcompat:site-report",
    },
    "knowledge_base": {
        "product": "Web Compatibility",
        "component": "Knowledge Base",
    },
    "platform_bugs": {
        "f1": "product",
        "o1": "notequals",
        "v1": "Web Compatibility",
        "f2": "keywords",
        "o2": "substring",
        "v2": "webcompat:platform-bug",
    },
    "interventions": {
        "product": "Web Compatibility",
        "component": "Interventions",
    },
    "other": {
        "f1": "product",
        "v1": "Web Compatibility",
        "o1": "notequals",
        "keywords": "webcompat:",
        "keywords_type": "regexp",
    },
    "parity": {
        "f1": "OP",
        "f2": "bug_status",
        "o2": "changedafter",
        "v2": "2020-01-01",
        "j1": "OR",
        "f3": "resolution",
        "o3": "isempty",
        "f4": "CP",
        "f5": "keywords",
        "o5": "regexp",
        "v5": "parity-",
    },
    "site_reports_etp": {
        "product": "Web Compatibility",
        "component": "Privacy: Site Reports",
        "f1": "OP",
        "f2": "bug_status",
        "o2": "changedafter",
        "v2": "2020-01-01",
        "j1": "OR",
        "f3": "resolution",
        "o3": "isempty",
        "f4": "CP",
    },
}

RELATION_CONFIG = {
    "core_bugs": {
        "fields": [
            {"name": "knowledge_base_bug", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "core_bug", "type": "INTEGER", "mode": "REQUIRED"},
        ],
        "source": "depends_on",
        "store_id": "core",
    },
    "breakage_reports": {
        "fields": [
            {"name": "knowledge_base_bug", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "breakage_bug", "type": "INTEGER", "mode": "REQUIRED"},
        ],
        "source": "blocks",
        "store_id": "breakage",
    },
    "interventions": {
        "fields": [
            {"name": "knowledge_base_bug", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "code_url", "type": "STRING", "mode": "REQUIRED"},
        ],
        "source": "see_also",
        "condition": INTERVENTIONS,
    },
    "other_browser_issues": {
        "fields": [
            {"name": "knowledge_base_bug", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "issue_url", "type": "STRING", "mode": "REQUIRED"},
        ],
        "source": "see_also",
        "condition": OTHER_BROWSER,
    },
    "standards_issues": {
        "fields": [
            {"name": "knowledge_base_bug", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "issue_url", "type": "STRING", "mode": "REQUIRED"},
        ],
        "source": "see_also",
        "condition": STANDARDS_ISSUES,
    },
    "standards_positions": {
        "fields": [
            {"name": "knowledge_base_bug", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "discussion_url", "type": "STRING", "mode": "REQUIRED"},
        ],
        "source": "see_also",
        "condition": STANDARDS_POSITIONS,
    },
}

ETP_RELATION_CONFIG = {
    "etp_breakage_reports": {
        "fields": [
            {"name": "breakage_bug", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "etp_meta_bug", "type": "INTEGER", "mode": "REQUIRED"},
        ],
        "source": "depends_on",
        "store_id": "breakage",
    },
}

LINK_FIELDS = ["other_browser_issues", "standards_issues", "standards_positions"]
PLATFORM_RELATION_CONFIG = {key: RELATION_CONFIG[key] for key in LINK_FIELDS}


def extract_int_from_field(field: Optional[str]) -> Optional[int]:
    if field:
        if field.lower() in FIELD_MAP:
            return FIELD_MAP[field.lower()]

        match = re.search(r"\d+", field)
        if match:
            return int(match.group())

    return None


def parse_string_to_json(input_string: str) -> Union[str, Mapping[str, Any]]:
    if not input_string:
        return ""

    lines = input_string.splitlines()

    result_dict: dict[str, Any] = {}

    for line in lines:
        if line:
            key_value = line.split(":", 1)
            if len(key_value) == 2:
                key, value = key_value
                if key in result_dict:
                    if isinstance(result_dict[key], list):
                        result_dict[key].append(value)
                    else:
                        result_dict[key] = [result_dict[key], value]
                else:
                    result_dict[key] = value
    if not result_dict:
        return ""

    return result_dict


class BugzillaToBigQuery:
    def __init__(
        self,
        client: bigquery.Client,
        bq_dataset_id: str,
        bugzilla_api_key: Optional[str],
        write: bool,
        include_history: bool,
    ):
        bz_config = bugdantic.BugzillaConfig(
            BUGZILLA_URL, bugzilla_api_key, allow_writes=write
        )
        self.bz_client = bugdantic.Bugzilla(bz_config)
        self.client = client
        self.bq_dataset_id = bq_dataset_id
        self.history_fetch_completed = include_history
        self.write = write
        self.include_history = include_history

    def fetch_bugs(self, params: dict[str, str]) -> tuple[bool, MutBugsById]:
        fields = [
            "id",
            "summary",
            "status",
            "resolution",
            "product",
            "component",
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
            "creator",
            "cf_webcompat_priority",
            "cf_webcompat_score",
        ]

        try:
            bugs = self.bz_client.search(query=params, include_fields=fields)
            data: MutBugsById = {}
            for bug in bugs:
                assert bug.id is not None
                data[bug.id] = bug.to_dict()
            fetch_completed = True
        except Exception as e:
            logging.error(f"Error: {e}")
            fetch_completed = False
            data = {}

        return fetch_completed, data

    def kb_bugs_from_platform_bugs(
        self,
        platform_bugs: BugsById,
        kb_ids: set[int],
        site_report_ids: set[int],
    ) -> BugsById:
        """Get a list of platform bugs that should also be considered knowledge base bugs

        These are platform bugs that aren't blocking an existing kb entry
        """

        filtered = {}

        for bug_id, source_bug in platform_bugs.items():
            # Check if the platform bug already has a kb entry and skip if so
            if any(blocked_id in kb_ids for blocked_id in source_bug["blocks"]):
                continue

            bug = {**source_bug}

            # Only store a breakage bug as it's the relation we care about
            bug["blocks"] = [
                blocked_id
                for blocked_id in bug["blocks"]
                if blocked_id in site_report_ids
            ]

            # Don't store bugs that platform bug depends on
            bug["depends_on"] = []

            filtered[bug_id] = bug

        return filtered

    def unify_etp_dependencies(
        self,
        etp_reports: BugsById,
        etp_dependencies: BugsById,
    ) -> BugsById:
        """Unify blocked and depends_on for each ETP bug as their dependencies are inconsistent,
        keep only ETP meta bugs and store them only in depends_on field."""

        filtered = {}

        for bug_id, source_bug in etp_reports.items():
            bug = {**source_bug}

            blocks = [
                blocked_id
                for blocked_id in bug["blocks"]
                if blocked_id in etp_dependencies
                and "meta" in etp_dependencies[blocked_id]["keywords"]
            ]

            depends = [
                depends_id
                for depends_id in bug["depends_on"]
                if depends_id in etp_dependencies
                and "meta" in etp_dependencies[depends_id]["keywords"]
            ]

            bug["depends_on"] = blocks + depends
            bug["blocks"] = []

            filtered[bug_id] = bug

        return filtered

    def chunked_list(self, data: list[int], size: int) -> Iterator[list[int]]:
        for i in range(0, len(data), size):
            yield data[i : i + size]

    def fetch_by_id(self, bug_ids: set[int]) -> tuple[bool, MutBugsById]:
        chunk_size = 400
        all_bugs: dict[int, Bug] = {}
        all_completed = True

        for chunk in self.chunked_list(list(bug_ids), chunk_size):
            logging.info(f"Fetching {len(chunk)} bugs")

            completed, bugs = self.fetch_bugs({"id": ",".join(map(str, chunk))})
            if completed:
                all_bugs.update(bugs)
            else:
                all_completed = False
                break

        return all_completed, all_bugs

    def fetch_related_bugs(
        self, bugs: BugsById, relations: list[str], all_bugs: MutBugsById
    ) -> tuple[bool, MutBugsById]:
        related_ids: set[int] = set()
        related_bugs: dict[int, Bug] = {}
        for bug in bugs.values():
            for relation_property in relations:
                related_ids |= set(
                    bug_id
                    for bug_id in bug[relation_property]
                    if bug_id not in all_bugs
                )
                related_bugs.update(
                    {
                        bug_id: all_bugs[bug_id]
                        for bug_id in bug[relation_property]
                        if bug_id in all_bugs
                    }
                )

        completed, fetched_bugs = self.fetch_by_id(related_ids)
        all_bugs.update(fetched_bugs)
        related_bugs.update(fetched_bugs)
        return completed, related_bugs

    def filter_core_bugs(self, bugs: BugsById) -> BugsById:
        return {
            bug_id: bug
            for bug_id, bug in bugs.items()
            if bug["product"] != "Web Compatibility"
        }

    def fetch_all_bugs(
        self,
    ) -> Optional[
        tuple[
            MutBugsById, MutBugsById, MutBugsById, MutBugsById, MutBugsById, MutBugsById
        ]
    ]:
        """Get all the bugs that should be imported into BigQuery.

        :returns: A tuple of (all bugs, site report bugs, knowledge base bugs,
                              core bugs, ETP report bugs, ETP dependencies)."""
        fetched_bugs = {}
        all_bugs: dict[int, Bug] = {}

        for category, filter_config in FILTER_CONFIG.items():
            logging.info(f"Fetching {category.replace('_', ' ').title()} bugs")
            completed, fetched_bugs[category] = self.fetch_bugs(filter_config)
            all_bugs.update(fetched_bugs[category])
            if not completed:
                return None

        site_reports = fetched_bugs["site_reports_wc"]
        site_reports.update(fetched_bugs["site_reports_other"])

        logging.info("Fetching site-report blocking bugs")
        completed_site_report_deps, site_reports_deps = self.fetch_related_bugs(
            site_reports, ["depends_on"], all_bugs
        )

        if not completed_site_report_deps:
            logging.error("Failed to fetch site report blocking bugs")
            return None

        kb_bugs = fetched_bugs["knowledge_base"]

        logging.info("Fetching blocking bugs for KB bugs")
        completed_platform_bugs, kb_deps = self.fetch_related_bugs(
            kb_bugs, ["depends_on"], all_bugs
        )

        if not completed:
            logging.error("Failed to fetch blocking bugs")
            return None

        platform_bugs = fetched_bugs["platform_bugs"]
        platform_bugs.update(self.filter_core_bugs(site_reports_deps))
        platform_bugs.update(self.filter_core_bugs(kb_deps))

        etp_reports = fetched_bugs["site_reports_etp"]

        etp_completed, etp_dependencies = self.fetch_related_bugs(
            etp_reports, ["depends_on", "blocks"], all_bugs
        )

        if not etp_completed:
            logging.error("Failed to fetch etp blocking bugs")
            return None

        return (
            all_bugs,
            site_reports,
            kb_bugs,
            platform_bugs,
            etp_reports,
            etp_dependencies,
        )

    def process_relations(
        self, bugs: BugsById, relation_config: RelationConfig
    ) -> tuple[Mapping[int, Mapping[str, list[int | str]]], Mapping[str, set[int]]]:
        """Build relationship tables based on information in the bugs.

        :returns: A mapping {bug_id: {relationship name: [related items]}} and
                  a mapping {store id: {bug ids}}
        """
        # The types here are wrong; the return values are lists of ints or lists of strings but not both.
        # However enforcing that property is hard without building specific types for the two cases
        relations: dict[int, dict[str, list[int | str]]] = {}
        related_bug_ids: dict[str, set[int]] = {}

        for config in relation_config.values():
            if "store_id" in config:
                related_bug_ids[config["store_id"]] = set()

        for bug_id, bug in bugs.items():
            relations[bug_id] = {rel: [] for rel in relation_config.keys()}

            for rel, config in relation_config.items():
                related_items = bug[config["source"]]

                for item in related_items:
                    if "condition" in config and not any(
                        c in item for c in config["condition"]
                    ):
                        continue

                    relations[bug_id][rel].append(item)

                    if config.get("store_id"):
                        assert isinstance(item, int)
                        related_bug_ids[config["store_id"]].add(item)

        return relations, related_bug_ids

    def add_kb_entry_breakage(
        self,
        kb_data: Mapping[int, Mapping[str, list[int | str]]],
        kb_dep_ids: Mapping[str, set[int]],
        site_reports: BugsById,
    ) -> None:
        """Add breakage relations for bugs that are both kb entries and also site reports

        If a core bug has the webcompat:platform-bug keyword it's a kb entry.
        If it also has the webcompat:site-report keyword it's a site report.
        In this case we want the bug to reference itself in the breakage_reports table."""
        for bug_id in set(kb_data.keys()) & set(site_reports.keys()):
            if bug_id not in kb_dep_ids["breakage"]:
                kb_data[bug_id]["breakage_reports"].append(bug_id)
                kb_dep_ids["breakage"].add(bug_id)

    def fetch_missing_deps(
        self, all_bugs: BugsById, kb_dep_ids: Mapping[str, set[int]]
    ) -> Optional[tuple[BugsById, BugsById]]:
        dep_ids = {item for sublist in kb_dep_ids.values() for item in sublist}

        # Check for missing bugs
        missing_ids = dep_ids - set(all_bugs.keys())

        if missing_ids:
            logging.info(
                "Fetching missing core bugs and breakage reports from Bugzilla"
            )
            completed, missing_bugs = self.fetch_bugs(
                {"id": ",".join(map(str, missing_ids))}
            )
            if not completed:
                return None

            # Separate core bugs for updating relations.
            core_dependenies = set(kb_dep_ids.get("core", set()))
            core_missing = {
                bug_id: bug
                for bug_id, bug in missing_bugs.items()
                if bug_id in core_dependenies
            }
        else:
            missing_bugs, core_missing = {}, {}

        return missing_bugs, core_missing

    def add_links(
        self,
        kb_processed: Mapping[int, Mapping[str, list[int | str]]],
        dep_processed: Mapping[int, Mapping[str, list[int | str]]],
    ) -> Mapping[int, Mapping[str, list[int | str]]]:
        """Create links between kb entries and external data such
        as standards issues."""
        result = {**kb_processed}

        for kb_bug_id in result:
            for core_bug_id in result[kb_bug_id]["core_bugs"]:
                assert isinstance(core_bug_id, int)
                for sub_key in LINK_FIELDS:
                    if sub_key in result[kb_bug_id] and sub_key in dep_processed.get(
                        core_bug_id, {}
                    ):
                        for link_item in dep_processed[core_bug_id][sub_key]:
                            if link_item not in result[kb_bug_id][sub_key]:
                                result[kb_bug_id][sub_key].append(link_item)

        return result

    def build_relations(
        self, bugs: BugsById, relation_config: RelationConfig
    ) -> Relations:
        relations: dict[str, list[Mapping[str, Any]]] = {
            key: [] for key in relation_config.keys()
        }

        for bug_id, bug in bugs.items():
            for field_key, items in bug.items():
                fields = relation_config[field_key]["fields"]

                for row in items:
                    relation_row = {fields[0]["name"]: bug_id, fields[1]["name"]: row}
                    relations[field_key].append(relation_row)

        return relations

    def convert_bug_data(self, bug: Bug) -> dict[str, Any]:
        resolved = None
        if bug["status"] in ["RESOLVED", "VERIFIED"] and bug["cf_last_resolved"]:
            resolved = bug["cf_last_resolved"]

        user_story = parse_string_to_json(bug["cf_user_story"])

        assigned_to = (
            bug["assigned_to"] if bug["assigned_to"] != "nobody@mozilla.org" else None
        )
        webcompat_priority = (
            bug.get("cf_webcompat_priority")
            if bug.get("cf_webcompat_priority") != "---"
            else None
        )

        return {
            "number": bug["id"],
            "title": bug["summary"],
            "status": bug["status"],
            "resolution": bug["resolution"],
            "product": bug["product"],
            "component": bug["component"],
            "severity": extract_int_from_field(bug["severity"]),
            "priority": extract_int_from_field(bug["priority"]),
            "creation_time": bug["creation_time"].isoformat(),
            "assigned_to": assigned_to,
            "keywords": bug["keywords"],
            "url": bug["url"],
            "user_story": user_story,
            "resolved_time": resolved.isoformat() if resolved is not None else None,
            "whiteboard": bug["whiteboard"],
            "webcompat_priority": webcompat_priority,
            "webcompat_score": extract_int_from_field(bug.get("cf_webcompat_score")),
        }

    def update_bugs(self, bugs: BugsById) -> None:
        res = [self.convert_bug_data(bug) for bug in bugs.values()]

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=[
                bigquery.SchemaField("number", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("resolution", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("product", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("component", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("severity", "INTEGER"),
                bigquery.SchemaField("priority", "INTEGER"),
                bigquery.SchemaField("creation_time", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("assigned_to", "STRING"),
                bigquery.SchemaField("keywords", "STRING", mode="REPEATED"),
                bigquery.SchemaField("url", "STRING"),
                bigquery.SchemaField("user_story", "JSON"),
                bigquery.SchemaField("resolved_time", "TIMESTAMP"),
                bigquery.SchemaField("whiteboard", "STRING"),
                bigquery.SchemaField("webcompat_priority", "STRING"),
                bigquery.SchemaField("webcompat_score", "INTEGER"),
            ],
            write_disposition="WRITE_TRUNCATE",
        )

        bugs_table = f"{self.bq_dataset_id}.bugzilla_bugs"

        job = self.client.load_table_from_json(
            res,
            bugs_table,
            job_config=job_config,
        )

        logging.info("Writing to `bugzilla_bugs` table")

        try:
            job.result()
        except Exception as e:
            print(f"ERROR: {e}")
            if job.errors:
                for error in job.errors:
                    logging.error(error)

        table = self.client.get_table(bugs_table)
        logging.info(f"Loaded {table.num_rows} rows into {table}")

    def update_kb_ids(self, ids: Iterable[int]) -> None:
        res = [{"number": kb_id} for kb_id in ids]

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=[
                bigquery.SchemaField("number", "INTEGER", mode="REQUIRED"),
            ],
            write_disposition="WRITE_TRUNCATE",
        )

        kb_bugs_table = f"{self.bq_dataset_id}.kb_bugs"

        job = self.client.load_table_from_json(
            res,
            kb_bugs_table,
            job_config=job_config,
        )

        logging.info("Writing to `kb_bugs` table")

        try:
            job.result()
        except Exception as e:
            print(f"ERROR: {e}")
            if job.errors:
                for error in job.errors:
                    logging.error(error)

        table = self.client.get_table(kb_bugs_table)
        logging.info(f"Loaded {table.num_rows} rows into {table}")

    def update_relations(
        self, relations: Relations, relation_config: RelationConfig
    ) -> None:
        for key, value in relations.items():
            if value:
                job_config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    schema=[
                        bigquery.SchemaField(
                            item["name"], item["type"], mode=item["mode"]
                        )
                        for item in relation_config[key]["fields"]
                    ],
                    write_disposition="WRITE_TRUNCATE",
                )

                relation_table = f"{self.bq_dataset_id}.{key}"
                job = self.client.load_table_from_json(
                    cast(Iterable[dict[str, Any]], value),
                    relation_table,
                    job_config=job_config,
                )

                logging.info(f"Writing to `{relation_table}` table")

                try:
                    job.result()
                except Exception as e:
                    print(f"ERROR: {e}")
                    if job.errors:
                        for error in job.errors:
                            logging.error(error)

                table = self.client.get_table(relation_table)
                logging.info(f"Loaded {table.num_rows} rows into {table}")

    def get_last_import_datetime(self) -> Optional[datetime]:
        query = f"""
                SELECT MAX(run_at) AS last_run_at
                FROM `{self.bq_dataset_id}.import_runs`
                WHERE is_history_fetch_completed = TRUE
            """
        res = self.client.query(query).result()
        row = list(res)[0]
        return row["last_run_at"]

    def fetch_history(
        self, bug_id: int, last_import_time: Optional[datetime] = None
    ) -> Optional[BugHistoryResponse]:
        if not bug_id:
            raise ValueError("No bug id provided")

        try:
            history = self.bz_client.bug_history(bug_id, new_since=last_import_time)
            return history.to_dict()
        except Exception as e:
            logging.error(f"Error: {e}")
            self.history_fetch_completed = False
            return None

    def fetch_bugs_history(
        self, ids: Iterable[int], last_import_time: Optional[datetime] = None
    ) -> list[BugHistoryResponse]:
        history = []

        for bug_id in ids:
            try:
                logging.info(f"Fetching history from bugzilla for {bug_id}")
                bug_history = self.fetch_history(bug_id, last_import_time)
                if bug_history is not None:
                    history.append(bug_history)
                time.sleep(2)

            except Exception as e:
                logging.error(f"Failed to fetch history for bug {bug_id}: {e}")

        return history

    def serialize_history_entry(self, entry: BugHistoryEntry) -> dict[str, Any]:
        return {
            "number": entry["number"],
            "who": entry["who"],
            "change_time": entry["change_time"].isoformat(),
            "changes": entry["changes"],
        }

    def update_history(self, records: list[BugHistoryEntry]) -> None:
        if not records:
            return

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=[
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
            ],
            write_disposition="WRITE_APPEND",
        )

        history_table = f"{self.bq_dataset_id}.bugs_history"

        job = self.client.load_table_from_json(
            (self.serialize_history_entry(item) for item in records),
            history_table,
            job_config=job_config,
        )

        logging.info("Writing to `bugs_history` table")

        try:
            job.result()
        except Exception as e:
            print(f"ERROR: {e}")
            if job.errors:
                for error in job.errors:
                    logging.error(error)

        table = self.client.get_table(history_table)
        logging.info(f"Loaded {len(records)} rows into {table}")

    def get_existing_history_records_by_ids(
        self, bug_ids: Iterable[int]
    ) -> Iterator[bigquery.Row]:
        formatted_numbers = ", ".join(str(bug_id) for bug_id in bug_ids)

        query = f"""
                    SELECT *
                    FROM `{self.bq_dataset_id}.bugs_history`
                    WHERE number IN ({formatted_numbers})
                """
        result = self.client.query(query).result()
        return result

    def extract_flattened_history(
        self, records: Iterable[bigquery.Row | Mapping[str, Any]]
    ) -> set[HistoryRow]:
        history_set = set()
        for record in records:
            changes = record["changes"]
            change_time = record["change_time"]

            for change in changes:
                history_row = HistoryRow(
                    record["number"],
                    record["who"],
                    change_time,
                    change["field_name"],
                    change["added"],
                    change["removed"],
                )
                history_set.add(history_row)

        return history_set

    def unflatten_history(self, diff: set[HistoryRow]) -> list[BugHistoryEntry]:
        changes: dict[tuple[int, str, datetime], dict[str, Any]] = {}
        for item in diff:
            key = (item.number, item.who, item.change_time)

            if key not in changes:
                changes[key] = {
                    "number": item.number,
                    "who": item.who,
                    "change_time": item.change_time,
                    "changes": [],
                }

            changes[key]["changes"].append(
                {
                    "field_name": item.field_name,
                    "added": item.added,
                    "removed": item.removed,
                }
            )

        return list(changes.values())

    def filter_only_unsaved_changes(
        self, history_updates: list[BugHistoryEntry], bug_ids: set[int]
    ) -> list[BugHistoryEntry]:
        existing_records = self.get_existing_history_records_by_ids(bug_ids)

        if not existing_records:
            return history_updates

        existing_history = self.extract_flattened_history(existing_records)
        new_history = self.extract_flattened_history(history_updates)

        diff = new_history - existing_history

        return self.unflatten_history(diff)

    def extract_history_fields(
        self, updated_history: list[BugHistoryResponse]
    ) -> tuple[list[BugHistoryEntry], set[int]]:
        result = []
        bug_ids = set()

        for bug_history in updated_history:
            filtered_changes = []

            for record in bug_history["history"]:
                relevant_changes = [
                    change
                    for change in record.get("changes", [])
                    if change.get("field_name") in ["keywords", "status"]
                ]

                if relevant_changes:
                    filtered_record: Mapping[str, Any] = {
                        "number": bug_history["id"],
                        "who": record["who"],
                        "change_time": record["when"],
                        "changes": relevant_changes,
                    }
                    filtered_changes.append(filtered_record)
                    bug_ids.add(bug_history["id"])

            if filtered_changes:
                result.extend(filtered_changes)

        return result, bug_ids

    def filter_relevant_history(
        self, updated_history: list[BugHistoryResponse]
    ) -> list[BugHistoryEntry]:
        only_unsaved_changes = []
        result, bug_ids = self.extract_history_fields(updated_history)

        if result:
            only_unsaved_changes = self.filter_only_unsaved_changes(result, bug_ids)

        return only_unsaved_changes

    def get_bugs_updated_since_last_import(
        self, all_bugs: BugsById, last_import_time: datetime
    ) -> set[int]:
        return {
            bug["id"]
            for bug in all_bugs.values()
            if bug["last_change_time"] > last_import_time
        }

    def get_imported_ids(self) -> set[int]:
        query = f"""
                SELECT number
                FROM `{self.bq_dataset_id}.bugzilla_bugs`
            """
        res = self.client.query(query).result()
        rows = list(res)

        imported_ids = {bug["number"] for bug in rows}

        return imported_ids

    def create_keyword_map(
        self, history: list[BugHistoryEntry]
    ) -> Mapping[int, Mapping[str, Mapping[str, list[datetime]]]]:
        keyword_history: dict[int, dict[str, dict[str, list[datetime]]]] = {}

        for record in history:
            bug_id = record["number"]
            timestamp = record["change_time"]

            for change in record["changes"]:
                if "keywords" in change["field_name"]:
                    if bug_id not in keyword_history:
                        keyword_history[bug_id] = {"added": {}, "removed": {}}

                    keyword_records = keyword_history[bug_id]

                    for action in ["added", "removed"]:
                        keywords = change[action]
                        if keywords:
                            for keyword in keywords.split(", "):
                                if keyword not in keyword_records[action]:
                                    keyword_records[action][keyword] = []

                                keyword_records[action][keyword].append(timestamp)

        return keyword_history

    def is_removed_earliest(
        self, added_times: list[datetime], removed_times: list[datetime]
    ) -> bool:
        events = [(at, "added") for at in added_times] + [
            (rt, "removed") for rt in removed_times
        ]
        events.sort()

        if not events:
            return False

        return events[0][1] == "removed"

    def get_missing_keywords(
        self,
        bug_id: int,
        current_keywords: list[str],
        keyword_history: Mapping[int, Mapping[str, Mapping[str, list[datetime]]]],
    ) -> list[str]:
        missing_keywords = []

        # Check if keyword exists, but is not in "added" history
        for keyword in current_keywords:
            if bug_id not in keyword_history or keyword not in keyword_history[
                bug_id
            ].get("added", {}):
                if keyword not in missing_keywords:
                    missing_keywords.append(keyword)

        # Check for keywords that have "removed" record as the earliest
        # event in the sorted timeline
        if bug_id in keyword_history:
            for keyword, removed_times in (
                keyword_history[bug_id].get("removed", {}).items()
            ):
                added_times = keyword_history[bug_id].get("added", {}).get(keyword, [])

                removed_earliest = self.is_removed_earliest(added_times, removed_times)

                if removed_earliest and keyword not in missing_keywords:
                    missing_keywords.append(keyword)

        return missing_keywords

    def build_missing_history(
        self, bugs_without_history: Iterable[tuple[Bug, list[str]]]
    ) -> list[BugHistoryEntry]:
        result: list[BugHistoryEntry] = []
        for bug, missing_keywords in bugs_without_history:
            record = {
                "number": bug["id"],
                "who": bug["creator"],
                "change_time": bug["creation_time"],
                "changes": [
                    {
                        "added": ", ".join(missing_keywords),
                        "field_name": "keywords",
                        "removed": "",
                    }
                ],
            }
            result.append(record)
        return result

    def create_synthetic_history(
        self, bugs: BugsById, history: list[BugHistoryEntry]
    ) -> list[BugHistoryEntry]:
        keyword_history = self.create_keyword_map(history)

        bugs_without_history = []

        for bug_id, bug in bugs.items():
            current_keywords = bug["keywords"]

            missing_keywords = self.get_missing_keywords(
                bug_id, current_keywords, keyword_history
            )

            if missing_keywords:
                bugs_without_history.append((bug, missing_keywords))

        return self.build_missing_history(bugs_without_history)

    def fetch_history_for_new_bugs(
        self, all_bugs: BugsById
    ) -> tuple[list[BugHistoryEntry], set[int]]:
        only_unsaved_changes = []

        existing_ids = self.get_imported_ids()
        all_ids = set(all_bugs.keys())
        new_ids = all_ids - existing_ids

        logging.info(f"Fetching new bugs history: {list(new_ids)}")

        new_bugs = {
            bug_id: bug for bug_id, bug in all_bugs.items() if bug_id in new_ids
        }

        history, _ = self.extract_history_fields(
            self.fetch_bugs_history(new_bugs.keys())
        )

        synthetic_history = self.create_synthetic_history(new_bugs, history)

        new_bugs_history = history + synthetic_history

        if new_bugs_history:
            only_unsaved_changes = self.filter_only_unsaved_changes(
                new_bugs_history, new_ids
            )

        return only_unsaved_changes, new_ids

    def fetch_history_updates(
        self, all_existing_bugs: BugsById
    ) -> list[BugHistoryResponse]:
        last_import_time = self.get_last_import_datetime()

        if last_import_time:
            updated_bug_ids = self.get_bugs_updated_since_last_import(
                all_existing_bugs, last_import_time
            )

            logging.info(
                f"Fetching bugs updated after last import: {updated_bug_ids} at {last_import_time.strftime('%Y-%m-%dT%H:%M:%SZ')}"  # noqa
            )

            if updated_bug_ids:
                bugs_history = self.fetch_bugs_history(
                    updated_bug_ids, last_import_time
                )

                return bugs_history

        return []

    def fetch_bug_history(self, all_bugs: BugsById) -> list[BugHistoryEntry]:
        filtered_new_history, new_ids = self.fetch_history_for_new_bugs(all_bugs)

        existing_bugs = {
            bug_id: bug for bug_id, bug in all_bugs.items() if bug_id not in new_ids
        }

        existing_bugs_history = self.fetch_history_updates(existing_bugs)

        if (
            filtered_new_history or existing_bugs_history
        ) and self.history_fetch_completed:
            filtered_existing = self.filter_relevant_history(existing_bugs_history)
            filtered_records = filtered_existing + filtered_new_history
            return filtered_records

        return []

    def record_import_run(
        self,
        start_time: float,
        history_fetch_completed: bool,
        count: int,
        history_count: int,
        last_change_time: datetime,
    ) -> None:
        elapsed_time = time.monotonic() - start_time
        elapsed_time_delta = timedelta(seconds=elapsed_time)
        run_at = last_change_time - elapsed_time_delta
        formatted_time = run_at.strftime("%Y-%m-%dT%H:%M:%SZ")

        rows_to_insert = [
            {
                "run_at": formatted_time,
                "bugs_imported": count,
                "bugs_history_updated": history_count,
                "is_history_fetch_completed": history_fetch_completed,
            },
        ]
        bugbug_runs_table = f"{self.bq_dataset_id}.import_runs"
        errors = self.client.insert_rows_json(bugbug_runs_table, rows_to_insert)
        if errors:
            logging.error(errors)
        else:
            logging.info("Last import run recorded")

    def run(self) -> None:
        start_time = time.monotonic()

        fetch_all_result = self.fetch_all_bugs()

        if fetch_all_result is None:
            raise BugFetchError(
                "Fetching bugs from Bugzilla was not completed due to an error, aborting."
            )

        (
            all_bugs,
            site_reports,
            kb_bugs,
            platform_bugs,
            etp_reports,
            etp_dependencies,
        ) = fetch_all_result

        # Add platform bugs that should be imported as knowledge base bugs (with some
        # modifications to their dependencies)
        kb_bugs.update(
            self.kb_bugs_from_platform_bugs(
                platform_bugs, set(kb_bugs.keys()), set(site_reports.keys())
            )
        )

        # Process KB bugs fields and get their dependant core/breakage bugs ids.
        kb_data, kb_dep_ids = self.process_relations(kb_bugs, RELATION_CONFIG)
        self.add_kb_entry_breakage(kb_data, kb_dep_ids, site_reports)

        fetch_missing_result = self.fetch_missing_deps(all_bugs, kb_dep_ids)
        if fetch_missing_result is None:
            raise BugFetchError(
                "Fetching missing dependencies from Bugzilla was not completed due to an error, aborting."
            )

        missing_bugs, core_missing = fetch_missing_result

        platform_bugs.update(core_missing)
        all_bugs.update(missing_bugs)

        # Process core bugs and update KB data with missing links from core bugs.
        if platform_bugs:
            core_data, _ = self.process_relations(
                platform_bugs, PLATFORM_RELATION_CONFIG
            )
            kb_data = self.add_links(kb_data, core_data)

        # Build relations for BQ tables.
        rels = self.build_relations(kb_data, RELATION_CONFIG)

        kb_ids = list(kb_data.keys())

        if self.include_history:
            history_changes = self.fetch_bug_history(all_bugs)
        else:
            logging.info("Not updating bug history")
            history_changes = []

        etp_rels: Mapping[str, list[Mapping[str, Any]]] = {}
        if etp_reports:
            etp_reports_unified = self.unify_etp_dependencies(
                etp_reports, etp_dependencies
            )
            etp_data, _ = self.process_relations(
                etp_reports_unified, ETP_RELATION_CONFIG
            )

            etp_rels = self.build_relations(etp_data, ETP_RELATION_CONFIG)

        if self.write:
            self.update_history(history_changes)
            self.update_bugs(all_bugs)
            self.update_kb_ids(kb_ids)
            self.update_relations(rels, RELATION_CONFIG)
            self.update_relations(etp_rels, ETP_RELATION_CONFIG)

            last_change_time_max = max(
                all_bugs.values(), key=lambda x: x["last_change_time"]
            )["last_change_time"]

            self.record_import_run(
                start_time,
                self.history_fetch_completed,
                len(all_bugs),
                len(history_changes),
                last_change_time_max,
            )
        else:
            logging.info("Skipping writes")


class BugzillaJob(EtlJob):
    name = "bugzilla"

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(
            title="Bugzilla", description="Bugzilla import arguments"
        )
        parser.add_argument(
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

    def main(self, client: bigquery.Client, args: argparse.Namespace) -> None:
        bz_bq = BugzillaToBigQuery(
            client,
            args.bq_kb_dataset,
            args.bugzilla_api_key,
            args.write,
            args.bugzilla_include_history,
        )

        bz_bq.run()
