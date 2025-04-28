import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Mapping

from google.cloud import bigquery

from .bqhelpers import BigQuery, Json, get_client


def get_parser_create_test_dataset() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--log-level",
        choices=["debug", "info", "warn", "error"],
        default="info",
        help="Log level",
    )

    parser.add_argument(
        "--bq-project", dest="bq_project_id", help="BigQuery project id"
    )
    parser.add_argument("--bq-kb-dataset", help="BigQuery knowledge base dataset id")
    parser.add_argument("--write", action="store_true", help="Write changes")
    return parser


def create_test_dataset() -> None:
    logging.basicConfig()

    parser = get_parser_create_test_dataset()
    args = parser.parse_args()
    logging.getLogger().setLevel(logging.getLevelNamesMapping()[args.log_level.upper()])

    test_dataset_name = f"{args.bq_kb_dataset}_test"
    tables = [
        "breakage_reports",
        "bugs_history",
        "bugzilla_bugs",
        "core_bugs",
        "etp_breakage_reports",
        "import_runs",
        "interventions",
        "kb_bugs",
        "other_browser_issues",
        "standards_issues",
        "standards_positions",
    ]

    logging.info(f"Will create dataset {args.bq_project_id}.{test_dataset_name}")
    for table in tables:
        logging.info(
            f"Will create table {args.bq_project_id}.{test_dataset_name}.{table} from {args.bq_project_id}.{args.bq_kb_dataset}.{table}"
        )

    res = ""
    while res not in {"y", "n"}:
        res = input("Continue y/N? ").strip().lower()
        res = "n" if res == "" else res

    if res != "y":
        sys.exit(1)

    client = BigQuery(get_client(args.bq_project_id), test_dataset_name, args.write)

    if not args.write:
        logging.info("Not writing; pass --write to commit changes")
    else:
        client.client.create_dataset(test_dataset_name, exists_ok=True)

    for table_name in tables:
        target = f"{test_dataset_name}.{table_name}"
        if args.write:
            client.delete_table(target, not_found_ok=True)
        else:
            logging.info(f"Would delete table {target}")

        src = f"{args.bq_kb_dataset}.{table_name}"
        query = f"""
CREATE TABLE {target}
CLONE {src}
"""
        if args.write:
            logging.info(f"Creating table {target} from {src}")
            client.query(query)
        else:
            logging.info(f"Would run query:{query}")


@dataclass(frozen=True)
class HistoryKey:
    number: int
    who: str
    change_time: datetime


def get_parser_backfill_history() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--log-level",
        choices=["debug", "info", "warn", "error"],
        default="info",
        help="Log level",
    )

    parser.add_argument(
        "--bq-project", dest="bq_project_id", help="BigQuery project id"
    )
    parser.add_argument(
        "--bq-kb-src-dataset", help="BigQuery knowledge base source dataset id"
    )
    parser.add_argument(
        "--bq-kb-dest-dataset", help="BigQuery knowledge base source dataset id"
    )
    parser.add_argument(
        "--write", action="store_true", default=False, help="Write changes"
    )
    return parser


def normalize_change(change: dict[str, str]) -> dict[str, str]:
    if change["field_name"] == "keywords":
        for key in ["added", "removed"]:
            items = change[key].split(", ")
            items.sort()
            change[key] = ", ".join(items)

    return change


def backfill_history() -> None:
    logging.basicConfig()

    parser = get_parser_backfill_history()
    args = parser.parse_args()
    logging.getLogger().setLevel(logging.getLevelNamesMapping()[args.log_level.upper()])

    src_dataset = args.bq_kb_src_dataset
    dest_dataset = args.bq_kb_dest_dataset

    client = BigQuery(get_client(args.bq_project_id), dest_dataset, args.write)

    existing_records_dest: dict[HistoryKey, list[dict[str, str]]] = {}
    existing_records_src: dict[HistoryKey, list[dict[str, str]]] = {}
    for dataset, records in [
        (dest_dataset, existing_records_dest),
        (src_dataset, existing_records_src),
    ]:
        for row in client.query("""SELECT * FROM bugs_history""", dataset_id=dataset):
            key = HistoryKey(row.number, row.who, row.change_time)
            if key in records:
                logging.warning(
                    f"Got duplicate src data for {key}: {row.changes}, {records[key]}"
                )
                for change in row.changes:
                    if change not in records[key]:
                        records[key].append(change)
            else:
                records[key] = row.changes

    logging.info(
        f"Started with {len(existing_records_src)} records in {src_dataset} and {len(existing_records_dest)} in {dest_dataset}"
    )

    new_records: list[tuple[datetime, Mapping[str, Json]]] = []

    new_count = 0
    updated_count = 0
    unchanged_count = 0
    for key, changes in existing_records_src.items():
        if key in existing_records_dest:
            existing = [
                normalize_change(change) for change in existing_records_dest[key]
            ]
            new = [normalize_change(change) for change in changes]
            if new == existing or (
                all(item in existing for item in new)
                and all(item in new for item in existing)
            ):
                unchanged_count += 1
            else:
                missing = [item for item in existing if item not in new]
                if missing:
                    logging.warning(
                        f"Updating record {key}, merging {new} with {existing}"
                    )
                    changes.extend(missing)
                updated_count += 1
        else:
            new_count += 1
        new_records.append(
            (
                key.change_time,
                {
                    "number": key.number,
                    "who": key.who,
                    "change_time": key.change_time.isoformat(),
                    "changes": changes,
                },
            )
        )

    for key, changes in existing_records_dest.items():
        if key not in existing_records_src:
            unchanged_count += 1
            new_records.append(
                (
                    key.change_time,
                    {
                        "number": key.number,
                        "who": key.who,
                        "change_time": key.change_time.isoformat(),
                        "changes": changes,
                    },
                )
            )

    logging.info(
        f"Writing {len(new_records)} records to {dest_dataset}, {unchanged_count} unchanged, {updated_count} updated, {new_count} new"
    )

    new_records.sort()
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
    client.write_table(
        "bugs_history", schema, [item[1] for item in new_records], overwrite=True
    )
