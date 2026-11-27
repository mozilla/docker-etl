from datetime import UTC, datetime
from decimal import Decimal
from typing import Optional
from unittest.mock import Mock

from google.cloud import bigquery

from webcompat_kb.bqhelpers import SchemaId
from webcompat_kb.etl.metric_changes import ScoreChange
from webcompat_kb.metric_rescore import conditional_metrics, score_bug_changes


def delta_row(
    project, number: int, old_score: int, new_score: int, last_score: Optional[int]
) -> bigquery.Row:
    fields = {
        "number": number,
        "old_score": Decimal(old_score),
        "new_score": Decimal(new_score),
        "delta": Decimal(new_score - old_score),
        "last_score": Decimal(last_score) if last_score is not None else None,
    }
    for metric in conditional_metrics(project):
        fields[metric.is_old_field] = True
        fields[metric.is_new_field] = True
    return bigquery.Row(
        list(fields.values()), {name: idx for idx, name in enumerate(fields)}
    )


def test_score_bug_changes_last_recorded_score(project, bq_client):
    change_time = datetime(2026, 5, 7, 19, 2, 43, tzinfo=UTC)
    rows = [
        # Recorded score already matches the rescored score, so nothing changes
        delta_row(project, 1, 1600, 160, last_score=160),
        # Recorded score differs from the pre-rescore score
        delta_row(project, 2, 456, 342, last_score=228),
        # Recorded score matches the pre-rescore score
        delta_row(project, 3, 50, 70, last_score=50),
        # No recorded score, so use the pre-rescore score
        delta_row(project, 4, 50, 60, last_score=None),
        # Nothing changed
        delta_row(project, 5, 100, 100, last_score=100),
    ]
    bq_client.client.return_values["query"].append(Mock(result=lambda: rows))
    rescore = Mock()
    rescore.delta_schema_id.return_value = SchemaId(
        "project", "webcompat_knowledge_base", "rescore_test_delta"
    )

    result = score_bug_changes(project, bq_client, rescore, change_time)

    assert result == {
        2: [
            ScoreChange(
                "", change_time, Decimal(228), Decimal(342), Decimal(114), ["rescore"]
            )
        ],
        3: [
            ScoreChange(
                "", change_time, Decimal(50), Decimal(70), Decimal(20), ["rescore"]
            )
        ],
        4: [
            ScoreChange(
                "", change_time, Decimal(50), Decimal(60), Decimal(10), ["rescore"]
            )
        ],
    }
