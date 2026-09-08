"""Collect feedback on Bugzilla comments posted by autowebcompat.

Reactions aren't part of a bug's history and don't update its last_change_time,
so there's no way to be notified when one is added; so this job is polling
the comments we know about and record their current state.
"""

import argparse
import logging
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
    """Get the ids of all the comments autowebcompat has posted"""
    return [
        row.comment_id
        for row in bq_client.query(f"SELECT DISTINCT comment_id FROM {comment_table}")
    ]


class CommentWithReactions(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: int
    reactions: dict[str, int]


def error_message(error: Exception) -> str:
    """Get the message Bugzilla returned for a failed request, if there is one"""
    response = getattr(error, "response", None)
    if response is not None:
        try:
            return str(response.json().get("message", error))
        except ValueError:
            pass
    return str(error)


def fetch_comments(
    bz_client: bugdantic.Bugzilla, comment_ids: Sequence[int]
) -> list[CommentWithReactions]:
    """Get comment data for comment_ids, skipping any we can't read.

    Bugzilla fails the whole request if any comment in it was deleted, is
    private, or belongs to a bug we can't access, and for the last case the
    error only identifies the bug. So on failure split the batch to find the
    comments that are still readable."""
    if not comment_ids:
        return []

    try:
        return bz_client.comments_as(list(comment_ids), CommentWithReactions)
    except Exception as e:
        if len(comment_ids) == 1:
            logging.warning(f"Skipping comment {comment_ids[0]}: {error_message(e)}")
            return []

    middle = len(comment_ids) // 2
    return fetch_comments(bz_client, comment_ids[:middle]) + fetch_comments(
        bz_client, comment_ids[middle:]
    )


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


def update_comment_reactions(
    project: Project, bq_client: BigQuery, bz_client: bugdantic.Bugzilla
) -> None:
    comment_table = project["autowebcompat"]["autowebcompat_bugzilla_comment"].table()
    reactions_table = project["autowebcompat_feedback"][
        "bugzilla_comment_reactions"
    ].table()
    import_runs_table = project["autowebcompat_feedback"]["import_runs"].table()

    last_import = get_last_import(bq_client, import_runs_table)
    if last_import is not None and last_import.date() == datetime.now(tz=UTC).date():
        logging.info("Already updated comment reactions today")
        return

    comment_ids = get_comment_ids(bq_client, comment_table)
    if not comment_ids:
        logging.info("No autowebcompat comments recorded")
        return

    logging.info(f"Fetching reactions for {len(comment_ids)} comments")
    comments = fetch_comments(bz_client, comment_ids)

    skipped = len(comment_ids) - len(comments)
    if skipped:
        logging.warning(f"Didn't get data for {skipped} comments")

    rows = to_rows(comments)
    with_reactions = sum(1 for row in rows if row["reactions"])
    logging.info(f"{with_reactions}/{len(rows)} comments have reactions")

    bq_client.write_table(reactions_table, reactions_table.schema, rows, overwrite=True)
    bq_client.insert_rows(import_runs_table, [{"run_at": datetime.now(tz=UTC)}])


class AutowebcompatFeedbackJob(EtlJob):
    name = "autowebcompat-feedback"

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        pass

    def required_args(self) -> set[str | tuple[str, str]]:
        return {"bugzilla_api_key"}

    def default_dataset(self, context: Context) -> str:
        return "autowebcompat_feedback"

    def main(self, context: Context) -> None:
        bz_config = bugdantic.BugzillaConfig(
            "https://bugzilla.mozilla.org",
            context.args.bugzilla_api_key,
            allow_writes=context.config.write,
        )
        bz_client = bugdantic.Bugzilla(bz_config)
        update_comment_reactions(context.project, context.bq_client, bz_client)
