"""Import the OISD NSFW domain list into BigQuery.

The table is consumed by WebCompatManager (``import_domain_list --name nsfw``),
which matches bucket hostnames *exactly* against the ``domain`` column and
removes the ``nsfw`` label from buckets whose domain is no longer listed. So:

* values must be bare, lowercase hostnames (no ``*.`` prefix, no comments);
* subdomains are kept even when a parent domain is listed, since dropping them
  would stop those buckets from being labelled;
* we refuse to write a suspiciously small list, since truncating the table
  would expose every NSFW bucket to QA on the next sync.

The ETL runs every 15 minutes, but the list only needs refreshing monthly. Each
successful import is recorded in ``nsfw_import_runs``, and the job skips if
there is already a record for the current calendar month (UTC). The record is
only written after the table has been replaced, so a failed import is retried
on the next run.
"""

import argparse
import logging
import re
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Optional

import httpx
from google.api_core.exceptions import NotFound

from ..base import Context, EtlJob
from ..bqhelpers import BigQuery, Json, TableSchema

OISD_NSFW_URL = "https://nsfw.oisd.nl/domainswild2"

# The list has ~500k entries; anything far below that means a broken download.
DEFAULT_MIN_DOMAINS = 100_000
# Minimum fraction of the header's declared entry count we must parse.
MIN_DECLARED_FRACTION = 0.9

ENTRIES_RE = re.compile(r"^#\s*Entries:\s*(\d+)\s*$", re.IGNORECASE)
LABEL = r"(?!-)[a-z0-9_-]{1,63}(?<!-)"
HOSTNAME_RE = re.compile(rf"^(?=.{{1,253}}$){LABEL}(?:\.{LABEL})+$")


def parse_domain_list(lines: Iterable[str]) -> tuple[list[str], Optional[int]]:
    """Parse an OISD domain list.

    Returns the sorted, de-duplicated domains and the entry count declared in
    the list header, if any."""
    domains: set[str] = set()
    declared: Optional[int] = None
    invalid = 0

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#"):
            if match := ENTRIES_RE.match(line):
                declared = int(match.group(1))
            continue

        domain = line.lower().removeprefix("*.").rstrip(".")
        if HOSTNAME_RE.match(domain):
            domains.add(domain)
        else:
            invalid += 1
            logging.debug(f"Skipping invalid domain entry {line!r}")

    if invalid:
        logging.warning(f"Skipped {invalid} invalid domain entries")

    return sorted(domains), declared


def check_domain_count(
    domains: list[str], declared: Optional[int], min_domains: int
) -> None:
    if len(domains) < min_domains:
        raise ValueError(
            f"Only parsed {len(domains)} domains, expected at least {min_domains}; "
            "refusing to overwrite the table"
        )
    if declared is not None and len(domains) < declared * MIN_DECLARED_FRACTION:
        raise ValueError(
            f"Parsed {len(domains)} domains but the list declares {declared} entries; "
            "refusing to overwrite the table"
        )


def fetch_domain_list(url: str) -> str:
    resp = httpx.get(url, follow_redirects=True, timeout=120)
    resp.raise_for_status()
    return resp.text


def get_last_import(
    client: BigQuery, import_runs_table: TableSchema
) -> Optional[datetime]:
    query = f"SELECT run_at FROM {import_runs_table} ORDER BY run_at DESC LIMIT 1"
    try:
        result = list(client.query(query))
    except NotFound:
        # The table doesn't exist until the first successful import
        return None
    if result:
        return result[0].run_at
    return None


def imported_this_month(last_import: Optional[datetime], now: datetime) -> bool:
    if last_import is None:
        return False
    if last_import.tzinfo is None:
        last_import = last_import.replace(tzinfo=UTC)
    last_import = last_import.astimezone(UTC)
    return (last_import.year, last_import.month) == (now.year, now.month)


class OisdNsfwJob(EtlJob):
    name = "oisd-nsfw"

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(
            title="OISD NSFW", description="OISD NSFW domain list arguments"
        )
        group.add_argument(
            "--oisd-nsfw-url",
            default=OISD_NSFW_URL,
            help="URL of the OISD NSFW domain list",
        )
        group.add_argument(
            "--oisd-nsfw-min-domains",
            type=int,
            default=DEFAULT_MIN_DOMAINS,
            help="Fail instead of writing if fewer domains than this are parsed",
        )
        group.add_argument(
            "--oisd-nsfw-force",
            action="store_true",
            help="Import even if the list was already imported this month",
        )

    def default_dataset(self, context: Context) -> str:
        return "oisd"

    def main(self, context: Context) -> None:
        client = context.bq_client
        nsfw_table = context.project["oisd"]["nsfw"].table()
        import_runs_table = context.project["oisd"]["nsfw_import_runs"].table()
        now = datetime.now(tz=UTC)

        if not context.args.oisd_nsfw_force:
            last_import = get_last_import(client, import_runs_table)
            if imported_this_month(last_import, now):
                logging.info(
                    f"NSFW domain list already imported this month (last import "
                    f"{last_import}); skipping"
                )
                return

        url = context.args.oisd_nsfw_url
        logging.info(f"Downloading NSFW domain list from {url}")
        domains, declared = parse_domain_list(fetch_domain_list(url).splitlines())
        logging.info(f"Parsed {len(domains)} domains (list declares {declared})")

        check_domain_count(domains, declared, context.args.oisd_nsfw_min_domains)

        rows: list[dict[str, Json]] = [{"domain": domain} for domain in domains]
        client.write_table(nsfw_table, nsfw_table.schema, rows, overwrite=True)

        # Record the run only after the table was replaced, so failures are retried.
        # A load job (rather than a streaming insert) creates the table on first use.
        run: dict[str, Json] = {"run_at": now.isoformat(), "domain_count": len(domains)}
        client.write_table(
            import_runs_table, import_runs_table.schema, [run], overwrite=False
        )
