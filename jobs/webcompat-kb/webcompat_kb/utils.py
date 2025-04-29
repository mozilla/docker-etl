import argparse
import logging
import sys

from .bqhelpers import BigQuery, get_client


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
