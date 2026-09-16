"""Collect feedback on Bugzilla comments posted by autowebcompat.

Reactions aren't part of a bug's history and don't update its last_change_time,
so this job is polling the comments we know about and record their current state.
"""

import argparse
import logging
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from itertools import batched
from typing import Any, Optional

import bugdantic
from pydantic import BaseModel, ConfigDict, Field, field_serializer

from ..base import Context, EtlJob
from ..bqhelpers import BigQuery, TableSchema
from ..projectdata import Project


def get_last_import(
    bq_client: BigQuery, import_runs_table: TableSchema
) -> Optional[datetime]:
    query = f"SELECT run_at FROM {import_runs_table} ORDER BY run_at DESC LIMIT 1"
    result = list(bq_client.query(query))
    if result:
        return result[0].run_at
    return None


def get_comment_ids(
    bq_client: BigQuery, comment_table: TableSchema, unreadable_table: TableSchema
) -> list[int]:
    """Get the ids of the comments autowebcompat has posted that we can still read"""
    return [
        row.comment_id
        for row in bq_client.query(f"""
    SELECT DISTINCT comment_id FROM `{comment_table}`
    EXCEPT DISTINCT
    SELECT comment_id FROM `{unreadable_table}`
    """)
    ]


class CommentWithReactions(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: int = Field(serialization_alias="comment_id")
    reactions: dict[str, int]

    @field_serializer("reactions")
    def serialize_reactions(self, reactions: dict[str, int]) -> list[Mapping[str, Any]]:
        return [{"name": name, "count": count} for name, count in reactions.items()]

    def to_bq_row(self) -> Mapping[str, Any]:
        return self.model_dump(by_alias=True)


class FetchResult(BaseModel):
    comments: list[CommentWithReactions] = []
    unreadable_ids: dict[int, int] = {}
    failed_ids: list[int] = []


def fetch_comments(
    bz_client: bugdantic.Bugzilla, comment_ids: Sequence[int]
) -> FetchResult:
    """Get comment data for comment_ids, identifying any we can't read.

    Bugzilla fails the whole request if any comment in it was deleted, is
    private, or belongs to a bug we can't access, and the error only reports
    the first id it objected to. So ask in chunks, and only fall back to asking
    one at a time for a chunk that failed."""
    chunk_size = 100

    result = FetchResult()
    for chunk in batched(comment_ids, chunk_size):
        try:
            result.comments.extend(
                bz_client.comments_as(list(chunk), CommentWithReactions)
            )
        except bugdantic.BugzillaError as e:
            logging.info(
                f"Request for {len(chunk)} comments failed ({e}), "
                "retrying them one at a time"
            )
            chunk_result = fetch_comments_individually(bz_client, chunk)
            result.comments.extend(chunk_result.comments)
            result.unreadable_ids.update(chunk_result.unreadable_ids)
            result.failed_ids.extend(chunk_result.failed_ids)
    return result


def fetch_comments_individually(
    bz_client: bugdantic.Bugzilla, comment_ids: Sequence[int]
) -> FetchResult:
    """Get each comment in its own request, recording the ones we can't read."""
    unreadable_codes = {
        102,  # access denied to the bug
        110,  # the comment is private
        111,  # not a valid comment id, i.e. it was deleted
    }

    result = FetchResult()
    for comment_id in comment_ids:
        try:
            result.comments.extend(
                bz_client.comments_as([comment_id], CommentWithReactions)
            )
        except bugdantic.BugzillaResponseError as e:
            if e.code in unreadable_codes:
                logging.warning(f"Comment {comment_id} is unreadable: {e}")
                result.unreadable_ids[comment_id] = e.code
            else:
                logging.warning(f"Skipping comment {comment_id} this run: {e}")
                result.failed_ids.append(comment_id)
        time.sleep(1)
    return result


def mark_unreadable(
    bq_client: BigQuery,
    unreadable_table: TableSchema,
    unreadable_ids: Mapping[int, int],
) -> None:
    """Record that we should stop requesting these comments"""
    now = datetime.now(tz=UTC)
    bq_client.insert_rows(
        unreadable_table,
        [
            {"comment_id": comment_id, "error_code": code, "first_seen_at": now}
            for comment_id, code in unreadable_ids.items()
        ],
    )


def update_comment_reactions(
    project: Project, bq_client: BigQuery, bz_client: bugdantic.Bugzilla
) -> bool:
    """Update the stored reactions, returning whether the run was complete"""
    comment_table = project["autowebcompat"]["bugzilla_comments"].table()
    reactions_table = project["autowebcompat"]["bugzilla_comment_reactions"].table()
    unreadable_table = project["autowebcompat"]["bugzilla_comments_unreadable"].table()
    import_runs_table = project["autowebcompat"]["import_runs_feedback"].table()

    last_import = get_last_import(bq_client, import_runs_table)
    if last_import is not None and last_import.date() == datetime.now(tz=UTC).date():
        logging.info("Already updated comment reactions today")
        return True

    comment_ids = get_comment_ids(bq_client, comment_table, unreadable_table)
    if not comment_ids:
        logging.info("No autowebcompat comments to check")
        return True

    logging.info(f"Fetching reactions for {len(comment_ids)} comments")
    result = fetch_comments(bz_client, comment_ids)

    mark_unreadable(bq_client, unreadable_table, result.unreadable_ids)

    if result.failed_ids:
        logging.error(
            f"Didn't get full data for {len(result.failed_ids)} comments, "
            "not updating reactions"
        )
        return False

    rows = [comment.to_bq_row() for comment in result.comments]

    bq_client.write_table(reactions_table, reactions_table.schema, rows, overwrite=True)
    bq_client.insert_rows(import_runs_table, [{"run_at": datetime.now(tz=UTC)}])
    return True


class AutowebcompatFeedbackJob(EtlJob):
    name = "autowebcompat-feedback"

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        pass

    def required_args(self) -> set[str | tuple[str, str]]:
        return {"bugzilla_api_key"}

    def default_dataset(self, context: Context) -> str:
        return "autowebcompat"

    def main(self, context: Context) -> bool:
        bz_config = bugdantic.BugzillaConfig(
            "https://bugzilla.mozilla.org",
            context.args.bugzilla_api_key,
            allow_writes=context.config.write,
        )
        bz_client = bugdantic.Bugzilla(bz_config)
        return update_comment_reactions(context.project, context.bq_client, bz_client)
