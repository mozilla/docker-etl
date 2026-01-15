import logging
from datetime import datetime
from typing import Mapping, Sequence

from google.cloud import bigquery

from . import metric_changes
from .bqhelpers import (
    BigQuery,
    DatasetId,
    SchemaId,
    SchemaType,
    TableSchema,
)
from .metrics import metrics, rescores
from .metrics.rescores import Rescore
from .metric_changes import ScoreChange
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
    rescore: Rescore,
    change_time: datetime,
) -> Mapping[int, Sequence[ScoreChange]]:
    kb_dataset = DatasetId(project.id, "webcompat_knowledge_base")

    score_changes = {}
    score_deltas = {"all": 0}

    score_query = f"SELECT * FROM {rescore.delta_schema_id(kb_dataset)}"
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
    rescores_table: TableSchema,
    old_scored_site_reports: SchemaId,
    new_scored_site_reports: SchemaId,
    rescore: Rescore,
    change_time: datetime,
) -> None:
    """Add a row to the webcompat_topline_metric_rescores table with the datetime,
    before and after scores, and a reason for the rescore."""
    metric_dfns, metric_types = project.data.metric_dfns, project.data.metric_types

    change_states = ["before", "after"]

    query_parts = [
        ("@change_time", "change_time"),
        ("@reason", "reason"),
    ]

    for change_state in change_states:
        for metric_type in metric_types:
            if "daily" not in metric_type.contexts:
                continue
            for metric in metric_dfns:
                agg_function = metric_type.agg_function(change_state, metric)
                query_parts.append(
                    (agg_function, f"{change_state}_{metric_type.name}_{metric.name}")
                )
    query_parts.append(("@name", "name"))

    columns = [name for _, name in query_parts]
    select_str = [f"{expr} AS {name}" for expr, name in query_parts]
    query = f"""
SELECT
  {",\n  ".join(select_str)}
FROM
`{old_scored_site_reports}` AS before
JOIN `{new_scored_site_reports}` AS after USING(number)
WHERE before.resolution = ""
"""

    assert set(columns) == set(item.name for item in rescores_table.fields)
    parameters = [
        bigquery.ScalarQueryParameter("change_time", "DATETIME", change_time),
        bigquery.ScalarQueryParameter("reason", "STRING", rescore.reason),
        bigquery.ScalarQueryParameter("name", "STRING", rescore.name),
    ]

    client.insert_query(rescores_table, columns, query, parameters=parameters)
    if not client.write:
        # Extra logging
        result = list(client.query(query, parameters=parameters))[0]
        changes = []
        for metric in metric_dfns:
            score_change = (
                result[f"after_total_score_{metric.name}"]
                - result[f"before_total_score_{metric.name}"]
            )
            changes.append(f"{metric.name}: {score_change}")
        msg = f"Score_changes:\n  {'\n  '.join(changes)}"
        logging.info(msg)


def record_rescore(
    project: Project, client: BigQuery, rescores_table: TableSchema, rescore: Rescore
) -> None:
    old_scored_site_reports = SchemaId(
        project.id, "webcompat_knowledge_base", "scored_site_reports"
    )
    new_scored_site_reports = rescore.staging_schema_id(
        SchemaType.view, old_scored_site_reports
    )

    changes_table = project["webcompat_knowledge_base"][
        "webcompat_topline_metric_changes"
    ].table()
    change_time = client.current_datetime()
    score_changes = score_bug_changes(project, client, rescore, change_time)
    metric_changes.insert_score_changes(client, score_changes, changes_table)
    insert_metric_changes(
        project,
        client,
        rescores_table,
        old_scored_site_reports,
        new_scored_site_reports,
        rescore,
        change_time,
    )


def get_undeployed_rescores(
    project: Project, client: BigQuery, rescores_table: TableSchema
) -> Mapping[str, Rescore]:
    rescore_dfns = rescores.load(
        project.data.path, project["webcompat_knowledge_base"].canonical_id
    )
    deployed_rescores = {
        row.name for row in client.query(f"SELECT DISTINCT name FROM {rescores_table}")
    }
    missing_rescore_names = {name for name in rescore_dfns} - deployed_rescores
    missing_rescores = {name: rescore_dfns[name] for name in missing_rescore_names}
    return {
        name: rescore for name, rescore in missing_rescores.items() if not rescore.stage
    }


def record_rescores(project: Project, client: BigQuery) -> None:
    rescores_table = project["webcompat_knowledge_base"][
        "webcompat_topline_metric_rescores"
    ].table()
    undeployed_rescores = get_undeployed_rescores(project, client, rescores_table)
    if not undeployed_rescores:
        logging.debug("No undeployed rescores found")
        return

    if len(undeployed_rescores) > 1:
        raise ValueError(
            f"Can only deploy one rescore at a time, found {', '.join(undeployed_rescores.keys())}"
        )

    rescore = next(iter(undeployed_rescores.values()))
    record_rescore(project, client, rescores_table, rescore)

    # Now clean up all the staging data
    client.delete_table(
        rescore.staging_schema_id(
            SchemaType.view,
            SchemaId(project.id, "webcompat_knowledge_base", "scored_site_reports"),
        )
    )
    client.delete_table(
        rescore.delta_schema_id(
            DatasetId(project.id, "webcompat_knowledge_base"),
        )
    )
    for routine_id in rescore.staging_routine_ids().values():
        client.delete_routine(routine_id)
