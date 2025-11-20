import argparse
import logging
from datetime import datetime
from typing import Iterable, Mapping, Sequence

from google.cloud import bigquery

from .base import Context, EtlJob
from .bqhelpers import BigQuery
from .metrics import metrics
from .metric_changes import ScoreChange, insert_score_changes
from .projectdata import Project


class ConditionalMetric:
    def __init__(self, metric: metrics.Metric):
        self.metric = metric

    @property
    def name(self) -> str:
        return self.metric.name

    def condition(self, table: str) -> str:
        return self.metric.condition(table)

    @property
    def is_old_field(self) -> str:
        return f"is_{self.name}_old"

    @property
    def is_new_field(self) -> str:
        return f"is_{self.name}_new"


_conditional_metrics = None


def conditional_metrics(project: Project) -> Sequence[ConditionalMetric]:
    global _conditional_metrics
    if _conditional_metrics is None:
        _conditional_metrics = [
            ConditionalMetric(item)
            for item in project.data.metric_dfns
            if item.conditional
        ]
    return _conditional_metrics


def score_bug_changes(
    project: Project,
    client: BigQuery,
    new_scored_site_reports: str,
    change_time: datetime,
) -> Mapping[int, Sequence[ScoreChange]]:
    score_changes = {}

    score_deltas = {"all": 0}
    scores_query_fields = ["number"]
    query_fields = ["number"]

    score_types = ["old", "new"]

    for score_type in score_types:
        src_table = f"{score_type}_scored_site_reports"
        field_name = f"{score_type}_score"
        scores_query_fields.append(f"{src_table}.score AS {field_name}")
        query_fields.append(field_name)

    for metric in conditional_metrics(project):
        score_deltas[metric.name] = 0
        for score_type, field_name in [
            ("old", metric.is_old_field),
            ("new", metric.is_new_field),
        ]:
            src_table = f"{score_type}_scored_site_reports"
            scores_query_fields.append(f"{metric.condition(src_table)} AS {field_name}")
            query_fields.append(field_name)

    query_fields.append("new_score - old_score AS delta")

    score_query = f"""
with scores as (
  SELECT
    {",\n    ".join(scores_query_fields)}
  FROM `{new_scored_site_reports}` as new_scored_site_reports
  FULL OUTER JOIN `scored_site_reports` AS old_scored_site_reports USING(number)
  WHERE new_scored_site_reports.resolution = ""
)
SELECT
  {",\n  ".join(query_fields)}
FROM scores
"""
    for bug in client.query(score_query):
        if bug.new_score is None or bug.old_score is None:
            raise ValueError(
                f"Missing data for bug {bug.number}; old score was {bug.old_score} new score is {bug.new_score}"
            )
        has_change = bug.delta != 0 or any(
            bug[metric.is_old_field] != bug[metric.is_new_field]
            for metric in conditional_metrics(project)
        )
        if has_change:
            reasons = []
            if bug.delta:
                reasons.append("rescore")
            score_deltas["all"] += bug.delta
            for metric in conditional_metrics(project):
                if metric.name == "all":
                    continue
                in_old_metric = bug[metric.is_old_field]
                in_new_metric = bug[metric.is_new_field]
                reason_name = metric.name.replace("_", "-")

                if in_old_metric and in_new_metric:
                    score_deltas[metric.name] += bug.delta
                elif in_old_metric:
                    score_deltas[metric.name] -= bug.old_score
                    reasons.append(f"{reason_name}-removed")
                elif in_new_metric:
                    score_deltas[metric.name] += bug.new_score
                    reasons.append(f"{reason_name}-added")

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
    project: Project,
    client: BigQuery,
    new_scored_site_reports: str,
    reason: str,
    change_time: datetime,
) -> None:
    """Add a row to the webcompat_topline_metric_rescores table with the datetime,
    before and after scores, and a reason for the rescore."""
    metric_dfns, metric_types = project.data.metric_dfns, project.data.metric_types
    bq_dataset_id = client.default_dataset_id

    change_states = ["before", "after"]

    score_change_schema = [
        bigquery.SchemaField("change_time", "DATETIME", mode="REQUIRED"),
        bigquery.SchemaField("reason", "STRING", mode="REQUIRED"),
    ]

    query_parts = [
        "@change_time as change_time",
        "@reason as reason",
    ]

    for change_state in change_states:
        for metric_type in metric_types:
            if "daily" not in metric_type.contexts:
                continue
            for metric in metric_dfns:
                score_change_schema.append(
                    bigquery.SchemaField(
                        f"{change_state}_{metric_type.name}_{metric.name}",
                        "NUMERIC",
                    )
                )
                agg_function = metric_type.agg_function(change_state, metric)
                query_parts.append(
                    f"{agg_function} AS {change_state}_{metric_type.name}_{metric.name}"
                )

    query = f"""
SELECT
  {",\n  ".join(query_parts)}
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
        changes = []
        for metric in metric_dfns:
            score_change = (
                result[f"after_total_score_{metric.name}"]
                - result[f"before_total_score_{metric.name}"]
            )
            changes.append(f"{metric.name}: {score_change}")
        msg = f"Score_changes:\n  {'\n  '.join(changes)}"
        logging.info(msg)


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
    project: Project,
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
    score_changes = score_bug_changes(
        project, client, new_scored_site_reports, change_time
    )
    insert_metric_changes(project, client, new_scored_site_reports, reason, change_time)
    changes_table = project["webcompat_knowledge_base"][
        "webcompat_topline_metric_changes"
    ].table()
    insert_score_changes(client, score_changes, changes_table)
    update_views(
        client, new_scored_site_reports, routines_map, view_definitions, archive_suffix
    )


class MetricRescoreJob(EtlJob):
    name = "metric-rescore"
    default = False

    def default_dataset(self, context: Context) -> str:
        return context.args.bq_kb_dataset

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

    def required_args(self) -> set[str | tuple[str, str]]:
        return {
            "bq_kb_dataset",
            ("new_scored_site_reports", "--metric-rescore-new-scored-site-reports"),
            "metric_rescore_reason",
        }

    def main(self, context: Context) -> None:
        rescore(
            context.project,
            context.bq_client,
            context.args.new_scored_site_reports,
            context.args.metric_rescore_reason,
            context.args.metric_rescore_update_routine or [],
        )
