"""Collect feedback on Bugzilla comments posted by autowebcompat.

Reactions aren't part of a bug's history and don't update its last_change_time,
so this job is polling the comments we know about and record their current state.
"""

import argparse
import logging
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Optional

import bugdantic
from pydantic import BaseModel, ConfigDict

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


def get_comment_ids(bq_client: BigQuery, comment_table: TableSchema) -> list[int]:
    """Get the ids of the comments autowebcompat has posted that we can still read"""
    return [
        row.comment_id
        for row in bq_client.query(
            f"SELECT DISTINCT comment_id FROM {comment_table} WHERE is_readable"
        )
    ]


class CommentWithReactions(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: int
    reactions: dict[str, int]


def error_body(error: Exception) -> Mapping[str, Any]:
    """Get the error body Bugzilla returned for a failed request, if there is one."""
    response = getattr(error, "response", None)
    if response is None:
        return {}
    try:
        body = response.json()
    except ValueError:
        return {}
    return body if isinstance(body, Mapping) else {}


def error_message(error: Exception) -> str:
    """Get the message Bugzilla returned for a failed request"""
    return str(error_body(error).get("message", error))


def error_code(error: Exception) -> Optional[int]:
    """Get the code Bugzilla returned for a failed request"""
    code = error_body(error).get("code")
    return code if isinstance(code, int) else None


def is_permanent_error(error: Exception) -> bool:
    """Whether an error means the comment is unreadable"""
    return error_code(error) in {
        102,  # access denied to the bug
        110,  # the comment is private
        111,  # not a valid comment id, i.e. it was deleted
    }


class FetchResult(BaseModel):
    """The comments we read, and which ids we couldn't read"""

    comments: list[CommentWithReactions] = []
    # Comments Bugzilla will never give us again, so stop asking for them
    unreadable_ids: list[int] = []
    # Comments we just didn't get this time; their reactions are unknown rather
    # so makes results of this run incomplete
    failed_ids: list[int] = []


def fetch_comments(
    bz_client: bugdantic.Bugzilla, comment_ids: Sequence[int]
) -> FetchResult:
    """Get comment data for comment_ids, identifying any we can't read.

    Bugzilla fails the whole request if any comment in it was deleted, is
    private, or belongs to a bug we can't access, and the error only reports
    the first id it objected to. So ask for them all at once, and only if that
    fails ask one at a time to find out which ones to give up on."""
    try:
        return FetchResult(
            comments=bz_client.comments_as(list(comment_ids), CommentWithReactions)
        )
    except Exception as e:
        logging.info(
            f"Request for {len(comment_ids)} comments failed ({error_message(e)}), "
            "retrying them one at a time"
        )

    return fetch_comments_individually(bz_client, comment_ids)


def fetch_comments_individually(
    bz_client: bugdantic.Bugzilla, comment_ids: Sequence[int]
) -> FetchResult:
    """Get each comment in its own request, recording the ones we can't read."""
    result = FetchResult()
    for comment_id in comment_ids:
        try:
            result.comments.extend(
                bz_client.comments_as([comment_id], CommentWithReactions)
            )
        except Exception as e:
            if is_permanent_error(e):
                logging.warning(
                    f"Comment {comment_id} is unreadable: {error_message(e)}"
                )
                result.unreadable_ids.append(comment_id)
            else:
                logging.warning(
                    f"Skipping comment {comment_id} this run: {error_message(e)}"
                )
                result.failed_ids.append(comment_id)
        time.sleep(1)
    return result


def to_rows(
    comments: Sequence[CommentWithReactions],
) -> list[Mapping[str, Any]]:
    return [
        {
            "comment_id": comment.id,
            "reactions": [
                {"name": name, "count": count}
                for name, count in sorted(comment.reactions.items())
            ],
        }
        for comment in sorted(comments, key=lambda item: item.id)
    ]


def mark_unreadable(
    bq_client: BigQuery, comment_table: TableSchema, comment_ids: Sequence[int]
) -> None:
    """Record that we should stop requesting these comments"""
    if not comment_ids:
        return

    bq_client.update_query(
        comment_table,
        ["is_readable"],
        from_query=(
            "SELECT comment_id, FALSE AS is_readable "
            f"FROM UNNEST({sorted(comment_ids)}) AS comment_id"
        ),
        condition="target.comment_id = source.comment_id",
    )


def update_comment_reactions(
    project: Project, bq_client: BigQuery, bz_client: bugdantic.Bugzilla
) -> bool:
    """Update the stored reactions, returning whether the run was complete"""
    comment_table = project["autowebcompat"]["bugzilla_comments"].table()
    reactions_table = project["autowebcompat_feedback"][
        "bugzilla_comment_reactions"
    ].table()
    import_runs_table = project["autowebcompat_feedback"]["import_runs"].table()

    last_import = get_last_import(bq_client, import_runs_table)
    if last_import is not None and last_import.date() == datetime.now(tz=UTC).date():
        logging.info("Already updated comment reactions today")
        return True

    comment_ids = get_comment_ids(bq_client, comment_table)
    if not comment_ids:
        logging.info("No autowebcompat comments recorded")
        return True

    logging.info(f"Fetching reactions for {len(comment_ids)} comments")
    result = fetch_comments(bz_client, comment_ids)

    mark_unreadable(bq_client, comment_table, result.unreadable_ids)

    if result.failed_ids:
        logging.error(
            f"Didn't get full data for {len(result.failed_ids)} comments, "
            "not updating reactions"
        )
        return False

    rows = to_rows(result.comments)

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
        return "autowebcompat_feedback"

    def main(self, context: Context) -> bool:
        bz_config = bugdantic.BugzillaConfig(
            "https://bugzilla.mozilla.org",
            context.args.bugzilla_api_key,
            allow_writes=context.config.write,
        )
        bz_client = bugdantic.Bugzilla(bz_config)
        return update_comment_reactions(context.project, context.bq_client, bz_client)
