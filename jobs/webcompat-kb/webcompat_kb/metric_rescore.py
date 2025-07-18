import argparse
import logging
from datetime import datetime
from typing import Iterable, Mapping, Sequence

from google.cloud import bigquery

from .base import EtlJob
from .bqhelpers import BigQuery
from .metric_changes import ScoreChange, insert_score_changes


def score_bug_changes(
    client: BigQuery, new_scored_site_reports: str, change_time: datetime
) -> Mapping[int, Sequence[ScoreChange]]:
    score_changes = {}
    score_query = f"""
with scores as (
  SELECT number,
         new_scored_site_reports.score AS new_score,
         old_scored_site_reports.score as old_score,
         old_scored_site_reports.is_sightline AS is_sightline_old,
         old_scored_site_reports.is_japan_1000 AS is_japan_1000_old,
         old_scored_site_reports.is_global_1000 AS is_global_1000_old,
         new_scored_site_reports.is_sightline AS is_sightline_new,
         new_scored_site_reports.is_japan_1000 AS is_japan_1000_new,
         new_scored_site_reports.is_global_1000 AS is_global_1000_new,
  FROM `{new_scored_site_reports}` as new_scored_site_reports
  FULL OUTER JOIN `scored_site_reports` AS old_scored_site_reports USING(number)
  WHERE new_scored_site_reports.resolution = ""
)
SELECT number, new_score, old_score, is_sightline_old, is_sightline_new, is_japan_1000_old, is_japan_1000_new, is_global_1000_old, is_global_1000_new, new_score - old_score as delta FROM scores
"""
    score_deltas = {"all": 0, "sightline": 0, "japan_1000": 0, "global_1000": 0}
    for bug in client.query(score_query):
        if bug.new_score is None or bug.old_score is None:
            raise ValueError(
                f"Missing data for bug {bug.number}; old score was {bug.old_score} new score is {bug.new_score}"
            )
        if (
            bug.delta != 0
            or (bug.is_sightline_new != bug.is_sightline_old)
            or (bug.is_global_1000_new != bug.is_global_1000_old)
        ):
            reasons = []
            if bug.delta:
                reasons.append("rescore")
            score_deltas["all"] += bug.delta
            if bug.is_sightline_old and bug.is_sightline_new:
                score_deltas["sightline"] += bug.delta
            elif bug.is_sightline_old:
                score_deltas["sightline"] -= bug.old_score
                reasons.append("sightline-removed")
            elif bug.is_sightline_new:
                score_deltas["sightline"] += bug.new_score
                reasons.append("sightline-added")
            if bug.is_japan_1000_old and bug.is_japan_1000_new:
                score_deltas["japan_1000"] += bug.delta
            elif bug.is_japan_1000_old:
                score_deltas["japan_1000"] -= bug.old_score
                reasons.append("japan-1000-removed")
            elif bug.is_japan_1000_new:
                score_deltas["japan_1000"] += bug.new_score
                reasons.append("japan-1000-added")
            if bug.is_global_1000_old and bug.is_global_1000_new:
                score_deltas["global_1000"] += bug.delta
            elif bug.is_global_1000_old:
                score_deltas["global_1000"] -= bug.old_score
                reasons.append("global-1000-removed")
            elif bug.is_global_1000_new:
                score_deltas["global_1000"] += bug.new_score
                reasons.append("global-1000-added")
            score_changes[bug.number] = [
                ScoreChange(
                    who="",
                    change_time=change_time,
                    old_score=bug.old_score,
                    new_score=bug.new_score,
                    score_delta=bug.delta,
                    reasons=reasons,
                )
            ]
    logging.info(
        f"{len(score_changes)} bugs are rescored. "
        f"Total score changes {' '.join(f'{key}: {value}' for key, value in score_deltas.items())}"
    )
    return score_changes


def insert_metric_changes(
    client: BigQuery, new_scored_site_reports: str, reason: str, change_time: datetime
) -> None:
    """Add a row to the webcompat_topline_metric_rescores table with the datetime,
    before and after scores, and a reason for the rescore."""
    bq_dataset_id = client.default_dataset_id

    change_states = ["before", "after"]
    score_fields = [
        "bug_count",
        "needs_diagnosis_score",
        "not_supported_score",
        "total_score",
    ]
    score_types = ["all", "sightline", "japan_1000", "global_1000"]

    score_change_schema = [
        bigquery.SchemaField("change_time", "DATETIME", mode="REQUIRED"),
        bigquery.SchemaField("reason", "STRING", mode="REQUIRED"),
    ]

    field_conditionals = {
        "needs_diagnosis_score": "metric_type_needs_diagnosis",
        "not_supported_score": "metric_type_firefox_not_supported",
    }

    query_parts = [
        "@change_time as change_time",
        "@reason as reason",
    ]

    for change_state in change_states:
        for score_field in score_fields:
            for score_type in score_types:
                score_change_schema.append(
                    bigquery.SchemaField(
                        f"{change_state}_{score_field}_{score_type}",
                        "NUMERIC",
                    )
                )
                condition = (
                    f"{change_state}.is_{score_type}"
                    if score_type != "all"
                    else f"{change_state}.number IS NOT NULL"
                )
                if score_field in field_conditionals:
                    condition += (
                        f" AND {change_state}.{field_conditionals[score_field]}"
                    )
                group_fn = (
                    f"COUNTIF({condition})"
                    if score_field == "bug_count"
                    else f"SUM(IF({condition}, {change_state}.score, 0))"
                )
                query_parts.append(
                    f"{group_fn} as {change_state}_{score_field}_{score_type}"
                )

    query = f"""
SELECT
{",\n".join(query_parts)}
FROM
`{bq_dataset_id}.scored_site_reports` AS before
JOIN `{bq_dataset_id}.{new_scored_site_reports}` AS after USING(number)
WHERE before.resolution = ""
"""

    table = client.ensure_table(
        "webcompat_topline_metric_rescores", schema=score_change_schema
    )

    parameters = [
        bigquery.ScalarQueryParameter("change_time", "DATETIME", change_time),
        bigquery.ScalarQueryParameter("reason", "STRING", reason),
    ]

    if client.write:
        insert_fields = ", ".join(f"`{item.name}`" for item in score_change_schema)
        insert_query = f"""
INSERT {bq_dataset_id}.{table.table_id}
({insert_fields})
({query})"""
        client.query(insert_query, parameters=parameters)
    else:
        result = list(client.query(query, parameters=parameters))[0]
        assert all(hasattr(result, item.name) for item in score_change_schema)
        logging.info(
            f"Score changes all: {result.after_total_score_all - result.before_total_score_all}, "
            f"sightline: {result.after_total_score_sightline - result.before_total_score_sightline}, "
            f"japan_1000: {result.after_total_score_japan_1000 - result.before_total_score_japan_1000}, "
            f"global_1000: {result.after_total_score_global_1000 - result.before_total_score_global_1000}"
        )


def get_view_definitions(
    client: BigQuery,
    new_scored_site_reports: str,
    routine_map: Mapping[bigquery.Routine, bigquery.Routine],
    archive_suffix: str,
) -> tuple[str, str]:
    """SQL for current scored_site_reports and new scored_site_reports

    These are edited (by string subsitution) so that the routine names are those
    after the transition i.e. so that new scored_site_reports is using canonical
    routine names and scored_site_reports is using archive routine names."""
    sql_definitions = []
    for table_name in ["scored_site_reports", new_scored_site_reports]:
        table = client.get_table(table_name)
        if table.view_query is None:
            raise ValueError(f"{table_name} is not a view")
        sql_definitions.append(table.view_query)
    for canonical, replacement in routine_map.items():
        canonical_ids = [
            str(canonical.reference),
            f"{canonical.dataset_id}.{canonical.routine_id}",
        ]
        replacement_ids = [
            str(replacement.reference),
            f"{replacement.dataset_id}.{replacement.routine_id}",
        ]
        if not any(
            f"`{canonical_id}`" in sql_definitions[0] for canonical_id in canonical_ids
        ):
            logging.debug(
                f"Looked for {', '.join(canonical_ids)} in:\n{sql_definitions[0]}"
            )
            # This isn't an error yet because we didn't initially use routines for scoring
            logging.warning(
                f"{' and '.join(canonical_ids)} don't appear in scored_site_reports"
            )
        if not any(
            f"`{replacement_id}`" in sql_definitions[1]
            for replacement_id in replacement_ids
        ):
            logging.debug(
                f"Looked for {' '.join(replacement_ids)} in:\n{sql_definitions[1]}"
            )
            raise ValueError(
                f"{' and '.join(replacement_ids)} don't appear in {new_scored_site_reports}"
            )
        for canonical_id in canonical_ids:
            logging.debug(
                f"Replacing `{canonical_id}` with `{canonical_id}{archive_suffix}` in scored_site_reports"
            )
            sql_definitions[0] = sql_definitions[0].replace(
                f"`{canonical_id}`", f"`{canonical_id}{archive_suffix}`"
            )
        for replacement_id in replacement_ids:
            logging.debug(
                f"Replacing `{replacement_id}` with `{canonical_ids[0]}` in new scored_site_reports"
            )
            sql_definitions[1] = sql_definitions[1].replace(
                f"`{replacement_id}`", f"`{canonical_ids[0]}`"
            )

    return sql_definitions[0], sql_definitions[1]


def serialize_datatype(datatype: bigquery.StandardSqlDataType) -> str:
    if datatype.type_kind is None:
        raise ValueError(f"Can't serialize {datatype}")
    rv = datatype.type_kind.value
    if datatype.array_element_type is not None:
        rv = f"{rv}<{serialize_datatype(datatype.array_element_type)}>"
    if datatype.struct_type is not None:
        raise ValueError("Serializing structs not implemented")
    if datatype.range_element_type is not None:
        raise ValueError("Serializing ranges not implemented")
    return rv


def serialize_arguments(args: Iterable[bigquery.RoutineArgument]) -> str:
    return ", ".join(f"{arg.name} {serialize_datatype(arg.data_type)}" for arg in args)


def update_views(
    client: BigQuery,
    new_scored_site_reports: str,
    routines_map: Mapping[bigquery.Routine, bigquery.Routine],
    view_definitions: tuple[str, str],
    archive_suffix: str,
) -> None:
    """Move the views and routines to apply the update.

    Move scored_site_reports to scored_site_reports_before_{timestamp} and the
    new_scored_site_reports to scored_site_reports.

    Also, for each routine being updated, move the current version to
    {name}_before_{timestamp} and the new version to {name}."""
    dataset_id = client.default_dataset_id
    to_delete = [f"{dataset_id}.{new_scored_site_reports}"]

    query = ""

    # This doesn't handle cases where one routine depends on another modified routine
    # We do the routines first since the views depend on them
    for old_routine, new_routine in routines_map.items():
        query += f"""
CREATE FUNCTION `{old_routine.reference}{archive_suffix}`({serialize_arguments(old_routine.arguments)})
RETURNS {serialize_datatype(old_routine.return_type)}
AS (
{old_routine.body}
);
"""

        query += f"""
CREATE OR REPLACE FUNCTION `{old_routine.reference}`({serialize_arguments(new_routine.arguments)})
RETURNS {serialize_datatype(new_routine.return_type)} AS
(
{new_routine.body}
);
"""
        to_delete.append(new_routine.reference)

    query += f"""
CREATE VIEW `{dataset_id}.scored_site_reports{archive_suffix}` AS (
{view_definitions[0]}
);
CREATE OR REPLACE VIEW `{dataset_id}.scored_site_reports` AS (
{view_definitions[1]}
);
"""

    if client.write:
        client.query(query)
        # Don't do this automatically for now
        logging.info(
            f"Now delete {dataset_id}.{new_scored_site_reports} in the BigQuery UI"
        )
    else:
        logging.info(f"Would have run update query:\n{query}")


def get_routines(
    client: BigQuery, routines: Sequence[str]
) -> Mapping[bigquery.Routine, bigquery.Routine]:
    rv = {}
    for item in routines:
        if item.count(":") != 1:
            raise ValueError(
                f"Routine mapping must be in the form canonical_name:updated_definition_name, got {item}"
            )
        canonical, replacement = item.split(":")
        routine_ids = []
        for routine_name in (canonical, replacement):
            segment_count = routine_name.count(".")
            if segment_count > 1:
                raise ValueError(
                    f"Expected routine name in the form [dataset_id].ROUTINE_ID, got {routine_name}"
                )
            elif segment_count == 0:
                routine_name = f"{client.default_dataset_id}.{routine_name}"
            routine_ids.append(f"{client.client.project}.{routine_name}")
        rv[client.get_routine(routine_ids[0])] = client.get_routine(routine_ids[1])
    return rv


def rescore(
    client: BigQuery,
    new_scored_site_reports: str,
    reason: str,
    update_routines: Sequence[str],
) -> None:
    change_time = client.current_datetime()
    archive_suffix = f"_before_{change_time.strftime('%Y%m%d%H%M')}"
    routines_map = get_routines(client, update_routines)
    view_definitions = get_view_definitions(
        client, new_scored_site_reports, routines_map, archive_suffix
    )
    score_changes = score_bug_changes(client, new_scored_site_reports, change_time)
    insert_metric_changes(client, new_scored_site_reports, reason, change_time)
    insert_score_changes(client, score_changes)
    update_views(
        client, new_scored_site_reports, routines_map, view_definitions, archive_suffix
    )


class MetricRescoreJob(EtlJob):
    name = "metric-rescore"
    default = False

    def default_dataset(self, args: argparse.Namespace) -> str:
        return args.bq_kb_dataset

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(
            title="Metric Rescore", description="Metric rescore arguments"
        )
        group.add_argument(
            "--metric-rescore-new-scored-site-reports",
            dest="new_scored_site_reports",
            action="store",
            help="Table containing updated scored_site_reports",
        )
        group.add_argument(
            "--metric-rescore-reason",
            action="store",
            help="Description of reason for updating the score",
        )
        group.add_argument(
            "--metric-rescore-update-routine",
            action="append",
            help="Routines to update in the form canonical_name:updated_definition_name e.g. test_dataset.WEBCOMPAT_METRIC_SCORE:test_dataset.WEBCOMPAT_METRIC_SCORE_NEW",
        )

    def main(self, client: BigQuery, args: argparse.Namespace) -> None:
        if args.new_scored_site_reports is None:
            raise ValueError(
                "Must provide view containing new scored_site_reports query with --metric-rescore-new-scored-site-reports"
            )
        if args.metric_rescore_reason is None:
            raise ValueError(
                "Must provide a reason for rescore with --metric-rescore-reason"
            )
        rescore(
            client,
            args.new_scored_site_reports,
            args.metric_rescore_reason,
            args.metric_rescore_update_routine,
        )
