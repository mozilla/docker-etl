import argparse
import logging
import os

from . import bugzilla


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--log_level",
        choices=["debug", "info", "warn", "error"],
        default="info",
        help="Log level",
    )
    parser.add_argument("--bq_project_id", required=True, help="BigQuery project id")
    parser.add_argument("--bq_dataset_id", required=True, help="BigQuery dataset id")
    parser.add_argument(
        "--bugzilla_api_key",
        help="Bugzilla API key",
        default=os.environ.get("BUGZILLA_API_KEY"),
    )
    parser.add_argument(
        "--no-history",
        dest="include_history",
        action="store_false",
        default=True,
        help="Don't read or update bug history",
    )
    parser.add_argument(
        "--no-write",
        dest="write",
        action="store_false",
        default=True,
        help="Don't write updates to BigQuery",
    )
    return parser


def main() -> None:
    args = get_parser().parse_args()
    logging.basicConfig()
    logging.getLogger().setLevel(logging.getLevelNamesMapping()[args.log_level.upper()])

    bugzilla.main(args)


if __name__ == "__main__":
    main()
