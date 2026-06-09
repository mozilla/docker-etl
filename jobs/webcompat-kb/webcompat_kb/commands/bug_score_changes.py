import argparse
import logging
from typing import Mapping, Optional

from google.cloud import bigquery

from .. import projectdata
from ..base import Command
from ..bqhelpers import BigQuery, get_client
from ..config import Config
from ..metric_changes import (
    BugChange,
    BugFieldChange,
    bugs_historic_states,
    compute_historic_scores,
    compute_score_changes,
    get_current_scores,
    get_bugs,
)
from ..projectdata import Project


def get_bug_changes(
    project: Project,
    client: BigQuery,
    bug: int,
) -> Mapping[int, list[BugChange]]:
    rv: dict[int, list[BugChange]] = {}

    query = f"""
SELECT number, who, change_time, changes
FROM `{project["webcompat_knowledge_base"]["bugs_history"]}`
WHERE number = @bug
ORDER BY change_time ASC
"""

    query_parameters = [bigquery.ScalarQueryParameter("bug", "INTEGER", bug)]

    bug_changes = client.query(query, parameters=query_parameters)
    for row in bug_changes:
        bug_id = row.number
        if bug_id not in rv:
            rv[bug_id] = []
        changes = [
            BugFieldChange(change["field_name"], change["added"], change["removed"])
            for change in row.changes
        ]
        rv[bug_id].append(BugChange(row.who, row.change_time, changes))

    logging.info(f"Got {bug_changes.num_results} changes for {len(rv)} bugs")
    return rv


class BugScoreChanges(Command):
    def argument_parser(self) -> argparse.ArgumentParser:
        parser = super().argument_parser()
        parser.add_argument("bug", action="store", type=int)
        return parser

    def main(self, args: argparse.Namespace) -> Optional[int]:
        config = Config(write=args.write, stage=args.stage)

        client = get_client(args.bq_project_id)
        project = projectdata.load(
            client, args.bq_project_id, args.data_path, set(), config
        )

        kb_dataset = project["webcompat_knowledge_base"].id

        bq_client = BigQuery(
            client,
            kb_dataset,
            args.write,
            set(),
        )

        bug_id = args.bug

        changes_by_bug = get_bug_changes(project, bq_client, bug_id)
        if not changes_by_bug:
            logging.info("No bug changes found")
            return None
        current_bug_data = get_bugs(
            project, bq_client, None, iter(changes_by_bug.keys())
        )
        historic_states = bugs_historic_states(current_bug_data, changes_by_bug)
        current_scores = get_current_scores(project, bq_client)
        historic_scores = compute_historic_scores(
            project, bq_client, historic_states, current_scores
        )
        assert len(historic_scores) == len(historic_states)
        print("= Historic states =")
        for state, score in zip(historic_states[bug_id], historic_scores[bug_id]):
            print(f"== Change {state.change_idx} ==")
            print(f"  * Status: {state.status}")
            print(f"  * Component: {state.product} :: {state.component}")
            print(f"  * URL: {state.url}")
            print(f"  * Keywords: {', '.join(state.keywords)}")
            print(f"  * User story: ```{state.user_story.strip()}```")
            print(f"  * Score: {score}")
            print("")
        score_changes = compute_score_changes(
            changes_by_bug,
            current_bug_data,
            {},
            historic_states,
            historic_scores,
            None,
        )
        print("= Score changes =")
        for score_change in score_changes[args.bug]:
            print(
                f"* {score_change.change_time.isoformat()}: From {score_change.old_score} to {score_change.new_score} (delta {score_change.score_delta}) due to {','.join(score_change.reasons)} by {score_change.who}"
            )

        return None


main = BugScoreChanges()
