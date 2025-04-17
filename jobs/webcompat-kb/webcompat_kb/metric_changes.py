import argparse
import logging
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator, Mapping, Optional, Sequence

from google.cloud import bigquery

from .base import EtlJob
from .bugzilla import parse_string_to_json
from .bqhelpers import ensure_table

FIXED_STATES = {"RESOLVED", "VERIFIED"}


@dataclass
class BugFieldChange:
    field_name: str
    added: str
    removed: str


@dataclass
class BugChange:
    who: str
    change_time: datetime
    changes: list[BugFieldChange]


@dataclass
class BugData:
    number: int
    status: str
    resolution: str
    product: str
    component: str
    creator: str
    creation_time: datetime
    resolved_time: datetime
    keywords: list[str]
    url: str
    user_story: str


@dataclass
class BugState:
    status: str
    product: str
    component: str
    keywords: list[str]
    url: str
    user_story: str
    change_idx: Optional[int]


@dataclass
class ScoreChange:
    who: str
    change_time: datetime
    score_delta: float
    reasons: list[str]


def get_last_recorded_date(client: bigquery.Client, bq_dataset_id: str) -> datetime:
    query = f"""
            SELECT change_time
            FROM `{bq_dataset_id}.webcompat_topline_metric_changes`
            ORDER BY change_time DESC
            LIMIT 1"""
    result = list(client.query(query).result())

    if result:
        return result[0]["change_time"]
    return datetime(2024, 1, 1, tzinfo=timezone.utc)


def get_bug_changes(
    client: bigquery.Client, bq_dataset_id: str, last_change_time: datetime
) -> Mapping[int, list[BugChange]]:
    rv: dict[int, list[BugChange]] = {}

    query = f"""
SELECT number, who, change_time, changes
FROM {bq_dataset_id}.bugs_history
WHERE change_time > @last_change_time
ORDER BY change_time ASC
"""

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "last_change_time", "TIMESTAMP", last_change_time
            )
        ]
    )

    bug_changes = client.query(query, job_config=job_config).result()
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


def get_bugs(
    client: bigquery.Client,
    bq_dataset_id: str,
    last_change_time: datetime,
    bugs: Iterator[int],
) -> Mapping[int, BugData]:
    rv: dict[int, BugData] = {}

    query = f"""
SELECT number, status, resolution, product, component, creator, creation_time, resolved_time, keywords, url, user_story_raw
FROM {bq_dataset_id}.bugzilla_bugs
WHERE number IN UNNEST(@bugs) OR creation_time > @last_change_time
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "last_change_time", "TIMESTAMP", last_change_time
            ),
            bigquery.ArrayQueryParameter("bugs", "INT64", list(bugs)),
        ]
    )
    job = client.query(query, job_config=job_config).result()

    for row in job:
        rv[row.number] = BugData(
            row.number,
            row.status,
            row.resolution,
            row.product,
            row.component,
            row.creator,
            row.creation_time,
            row.resolved_time,
            row.keywords,
            row.url,
            row.user_story_raw,
        )
    logging.info(f"Processing a total of {len(rv)} bugs, including newly opened bugs")
    return rv


def is_webcompat_bug(product: str, component: str, keywords: Sequence[str]) -> bool:
    return (product == "Web Compatibility" and component == "Site Reports") or (
        product != "Web Compatibility" and "webcompat:site-report" in keywords
    )


header_pattern = re.compile(r"^@@ -(\d+),?(\d+)? \+(\d+),?(\d+)? @@$")


def reverse_apply_diff(input_str: str, diff: str) -> str:
    input_lines = input_str.splitlines(True)
    input_idx = 0
    diff_lines = diff.splitlines(True)
    diff_idx = 0
    output_lines = []
    while diff_idx < len(diff_lines):
        m = header_pattern.match(diff_lines[diff_idx])
        if m is None:
            raise ValueError(f"Bad user story diff (missing header line):\n{diff}")
        start_line_number = int(m.group(3))
        end_line_number = int(m.group(4)) if m.group(4) else None
        start_idx = max(start_line_number - 1, 0)
        if start_idx < input_idx or (
            end_line_number is not None and end_line_number > len(input_lines)
        ):
            raise ValueError(f"Bad user story diff (index out of bounds):\n{diff}")
        output_lines.extend(input_lines[input_idx:start_idx])
        diff_idx += 1
        input_idx = start_idx
        while diff_idx < len(diff_lines) and diff_lines[diff_idx][0] != "@":
            change_char = diff_lines[diff_idx][0]
            data = diff_lines[diff_idx][1:]
            if change_char == "+":
                input_idx += 1
            elif change_char == " ":
                if input_lines[input_idx].strip() != data.strip():
                    raise ValueError(
                        f"Bad user story diff (patch doesn't match):\n{diff}\nInput line {input_idx} expected {data} got {input_lines[input_idx]}"
                    )
                output_lines.append(data)
                input_idx += 1
            else:
                output_lines.append(data)
            diff_idx += 1

    output_lines.extend(input_lines[input_idx:])
    return "".join(output_lines)


def bugs_historic_states(
    bug_data: Mapping[int, BugData],
    changes_by_bug: Mapping[int, list[BugChange]],
) -> Mapping[int, list[BugState]]:
    rv: dict[int, list[BugState]] = {}
    for bug_id, bug in bug_data.items():
        states = [
            BugState(
                bug.status,
                bug.product,
                bug.component,
                bug.keywords,
                bug.url,
                bug.user_story,
                change_idx=None,
            )
        ]

        bug_changes = changes_by_bug.get(bug_id, [])
        prev_changes = None
        for count, change in enumerate(reversed(bug_changes)):
            index = len(bug_changes) - count - 1
            if prev_changes is not None and prev_changes == change.changes:
                # Sometimes we seem to get duplicate changes we should skip
                continue

            current = states[-1]
            current.change_idx = index
            prev = BugState(
                current.status,
                current.product,
                current.component,
                current.keywords[:],
                current.url,
                current.user_story,
                change_idx=None,
            )
            for field_change in change.changes:
                if field_change.field_name == "keywords":
                    for keyword in field_change.added.split(", "):
                        if keyword:
                            try:
                                prev.keywords.remove(keyword)
                            except ValueError:
                                # Occasionally keywords change case
                                for prev_keyword in prev.keywords:
                                    if prev_keyword.lower() == keyword.lower():
                                        prev.keywords.remove(prev_keyword)
                                        logging.warning(
                                            f"Didn't find keyword {keyword} using {prev_keyword}"
                                        )
                                        break
                                else:
                                    raise
                    for keyword in field_change.removed.split(", "):
                        if keyword:
                            prev.keywords.append(keyword)
                elif field_change.field_name in {
                    "status",
                    "product",
                    "component",
                    "url",
                }:
                    assert getattr(prev, field_change.field_name) == field_change.added
                    setattr(prev, field_change.field_name, field_change.removed)
                elif field_change.field_name == "cf_user_story":
                    prev.user_story = reverse_apply_diff(
                        prev.user_story, field_change.added
                    )
                else:
                    continue
            prev_changes = change.changes
            states.append(prev)

        rv[bug_id] = states

    return rv


def get_current_scores(
    client: bigquery.Client, bq_dataset_id: str
) -> Mapping[int, float]:
    rv: dict[int, float] = {}
    query = f"""SELECT number, IFNULL(triage_score, 0) as score from `{bq_dataset_id}.scored_site_reports`"""
    for row in client.query(query).result():
        rv[row.number] = row.score
    return rv


def compute_historic_scores(
    client: bigquery.Client,
    bq_dataset_id: str,
    historic_states: Mapping[int, list[BugState]],
    current_scores: Mapping[int, float],
) -> Mapping[int, list[float]]:
    rv: dict[int, list[float]] = {}

    tmp_name = f"{bq_dataset_id}.tmp_{uuid.uuid4()}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=[
            bigquery.SchemaField("number", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("index", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("keywords", "STRING", mode="REPEATED"),
            bigquery.SchemaField("url", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("user_story", "JSON", mode="REQUIRED"),
        ],
    )

    rows = []
    for bug_id, states in historic_states.items():
        rv[bug_id] = [0] * len(states)
        for i, state in enumerate(states):
            is_open = state.status not in FIXED_STATES
            is_webcompat = (
                state.product == "Web Compatibility"
                and state.component == "Site Reports"
            ) or (
                state.product != "Web Compatibility"
                and "webcompat:site-report" in state.keywords
            )
            if is_open and is_webcompat:
                rows.append(
                    {
                        "number": bug_id,
                        "index": i,
                        "keywords": state.keywords,
                        "url": state.url,
                        "user_story": parse_string_to_json(state.user_story),
                    }
                )

    score_query = f"""
DECLARE crux_yyyymm INT64 DEFAULT 202409;

SELECT number,
       index,
       url, keywords, user_story,
       `moz-fx-dev-dschubert-wckb.webcompat_knowledge_base.WEBCOMPAT_METRIC_SCORE_NO_SITE_RANK`(keywords, user_story) * `moz-fx-dev-dschubert-wckb.webcompat_knowledge_base.WEBCOMPAT_METRIC_SCORE_SITE_RANK_MODIFER`(url, crux_yyyymm) as score
FROM `{tmp_name}`
"""

    client.load_table_from_json(rows, tmp_name, job_config=job_config).result()
    bugs_with_webcompat_states = set()
    try:
        scores = client.query(score_query).result()
        for row in scores:
            bugs_with_webcompat_states.add(row.number)
            logging.debug(
                f"Bug {row.number}: {row.url} {row.keywords}, {repr(row.user_story)} SCORE: {row.score}"
            )
            rv[row.number][row.index] = row.score
    finally:
        client.delete_table(tmp_name)

    for bug_id, computed_scores in rv.items():
        current_score = float(current_scores.get(bug_id, 0))
        if computed_scores[0] != current_score and states[0].status not in FIXED_STATES:
            history_logging = "\n".join(
                f"    {score}: {state}"
                for score, state in zip(computed_scores, historic_states[bug_id])
            )
            logging.warning(f"""Bug {bug_id}, current score is {current_score} but computed {computed_scores[-1]}
  STATES:
{history_logging}
""")

    logging.info(
        f"Got {scores.num_results} historic scores for {len(bugs_with_webcompat_states)} bugs"
    )
    return rv


def get_change_reasons(changes: Sequence[BugFieldChange]) -> list[str]:
    reasons = set()
    for change in changes:
        if change.field_name == "url":
            reasons.add("url-updated")
        elif change.field_name == "cf_user_story":
            reasons.add("triage")
        elif change.field_name == "keywords":
            if "webcompat:sitepatch-applied" in change.added:
                reasons.add("intervention-added")
            elif "webcompat:sitepatch-applied" in change.removed:
                reasons.add("intervention-removed")
            if "webcompat:site-report" in change.added:
                reasons.add("site-report-added")
            elif "webcompat:site-report" in change.removed:
                reasons.add("site-report-removed")
        elif change.field_name == "status":
            if change.added in FIXED_STATES and change.removed not in FIXED_STATES:
                reasons.add("resolved")
            elif change.removed in FIXED_STATES and change.added not in FIXED_STATES:
                reasons.add("reopened")

    rv = list(reasons)
    rv.sort()
    return rv


def compute_score_changes(
    changes_by_bug: Mapping[int, list[BugChange]],
    bug_data: Mapping[int, BugData],
    historic_states: Mapping[int, list[BugState]],
    historic_scores: Mapping[int, list[float]],
    last_change_time: datetime,
) -> Mapping[int, list[ScoreChange]]:
    rv: dict[int, list[ScoreChange]] = {}

    for bug_id, states in historic_states.items():
        rv[bug_id] = []

        scores = historic_scores[bug_id]
        changes = changes_by_bug.get(bug_id)
        bug = bug_data[bug_id]
        newly_created = bug.creation_time > last_change_time

        assert len(states) == len(scores)

        prev_score = None if not newly_created else 0.0

        for state, score in zip(reversed(states), reversed(scores)):
            if prev_score is None:
                score_change = ScoreChange(
                    who=bug.creator,
                    change_time=bug.creation_time,
                    score_delta=score,
                    reasons=["created"],
                )
            elif state.change_idx is not None:
                assert changes is not None
                score_delta = score - prev_score
                change = changes[state.change_idx]
                reasons = get_change_reasons(change.changes)
                if not reasons and score_delta > 0:
                    logging.warning(
                        f"No change reason for {bug_id} with change {change.changes}"
                    )

                score_change = ScoreChange(
                    who=change.who,
                    change_time=change.change_time,
                    score_delta=score_delta,
                    reasons=reasons,
                )
            else:
                assert state == states[-1]
                score_change = None

            prev_score = score
            if score_change is not None and score_change.score_delta != 0:
                rv[bug_id].append(score_change)

        if len(rv[bug_id]) == 0 and any(
            is_webcompat_bug(state.product, state.component, state.keywords)
            for state in states
        ):
            logging.debug(f"""No score changes for WebCompat bug {bug_id}:
  STATES:  {states}
  CHANGES: {changes}
  SCORES:  {scores}
""")

    changed_bugs = {key: len(value) for key, value in rv.items() if len(value)}
    logging.info(
        f"Got {sum(changed_bugs.values())} score changes in {len(changed_bugs)} bugs"
    )

    return rv


def insert_score_changes(
    client: bigquery.Client,
    bq_dataset_id: str,
    write: bool,
    score_changes: Mapping[int, Sequence[ScoreChange]],
) -> None:
    changes_table = f"{bq_dataset_id}.webcompat_topline_metric_changes"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=[
            bigquery.SchemaField("number", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("who", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("change_time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("score_delta", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("reasons", "STRING", mode="REPEATED"),
        ],
        write_disposition="WRITE_APPEND",
    )
    rows = []
    for bug_id, changes in score_changes.items():
        for change in changes:
            rows.append(
                {
                    "number": bug_id,
                    "who": change.who,
                    "change_time": change.change_time.isoformat(),
                    "score_delta": change.score_delta,
                    "reasons": change.reasons,
                }
            )

    if write:
        client.load_table_from_json(
            rows,
            changes_table,
            job_config=job_config,
        ).result()
        logging.info(f"Wrote {len(rows)} records into {changes_table}")
    else:
        logging.info("Skipping writes, would have written:")
        for row in rows:
            logging.info(f"{row}")


def update_metric_changes(
    client: bigquery.Client, bq_dataset_id: str, write: bool, recreate: bool
) -> None:
    schema = [
        bigquery.SchemaField("number", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("who", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("change_time", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("score_delta", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("reasons", "STRING", mode="REPEATED"),
    ]
    ensure_table(
        client, bq_dataset_id, "webcompat_topline_metric_changes", schema, recreate
    )
    last_recorded_date = get_last_recorded_date(client, bq_dataset_id)
    logging.info(f"Last change time {last_recorded_date}")
    changes_by_bug = get_bug_changes(client, bq_dataset_id, last_recorded_date)
    current_bug_data = get_bugs(
        client, bq_dataset_id, last_recorded_date, iter(changes_by_bug.keys())
    )
    historic_states = bugs_historic_states(current_bug_data, changes_by_bug)
    current_scores = get_current_scores(client, bq_dataset_id)
    historic_scores = compute_historic_scores(
        client, bq_dataset_id, historic_states, current_scores
    )
    score_changes = compute_score_changes(
        changes_by_bug,
        current_bug_data,
        historic_states,
        historic_scores,
        last_recorded_date,
    )
    insert_score_changes(client, bq_dataset_id, write, score_changes)


class MetricChangesJob(EtlJob):
    name = "metric_changes"

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(
            title="Metric Changes", description="Metric changes arguments"
        )
        group.add_argument(
            "--recreate-metric-changes",
            action="store_true",
            help="Delete and recreate changes table from scratch",
        )

    def main(self, client: bigquery.Client, args: argparse.Namespace) -> None:
        update_metric_changes(
            client, args.bq_kb_dataset, args.write, args.recreate_metric_changes
        )
