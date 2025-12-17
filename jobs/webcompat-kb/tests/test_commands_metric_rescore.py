import pytest

from webcompat_kb.bqhelpers import DatasetId, SchemaId
from webcompat_kb.commands import metric_rescore


@pytest.mark.parametrize(
    "source_dataset,dest_dataset,update_schema_ids,expected",
    [
        (
            DatasetId("project", "dataset"),
            DatasetId("project", "dataset"),
            {
                SchemaId("project", "dataset", "routine_id"): SchemaId(
                    "project", "dataset", "new_routine_id"
                )
            },
            """SELECT *, `{{ ref('new_routine_id') }}`()
FROM {{ ref('other_table') }}
JOIN {{ ref('other_dataset.other_table') }}
JOIN {{ ref("other_project.other_dataset.other_table") }}""",
        ),
        (
            DatasetId("project", "dataset"),
            DatasetId("project", "other_dataset"),
            {},
            """SELECT *, `{{ ref('dataset.routine_id') }}`()
FROM {{ ref('dataset.other_table') }}
JOIN {{ ref('other_table') }}
JOIN {{ ref("other_project.other_dataset.other_table") }}""",
        ),
        (
            DatasetId("project", "dataset"),
            DatasetId("other_project", "dataset"),
            {},
            """SELECT *, `{{ ref('project.dataset.routine_id') }}`()
FROM {{ ref('project.dataset.other_table') }}
JOIN {{ ref('project.other_dataset.other_table') }}
JOIN {{ ref("other_dataset.other_table") }}""",
        ),
        (
            DatasetId("project", "dataset"),
            DatasetId("project", "other_dataset"),
            {
                SchemaId("project", "dataset", "other_table"): SchemaId(
                    "project", "new_dataset", "other_table"
                )
            },
            """SELECT *, `{{ ref('dataset.routine_id') }}`()
FROM {{ ref('new_dataset.other_table') }}
JOIN {{ ref('other_table') }}
JOIN {{ ref("other_project.other_dataset.other_table") }}""",
        ),
    ],
)
def test_rewrite_refs(source_dataset, dest_dataset, update_schema_ids, expected):
    template = """SELECT *, `{{ ref('routine_id') }}`()
FROM {{ ref('other_table') }}
JOIN {{ ref('other_dataset.other_table') }}
JOIN {{ ref("other_project.other_dataset.other_table") }}"""
    rewriter = metric_rescore.rewrite_refs(
        source_dataset, dest_dataset, update_schema_ids
    )
    assert rewriter(template) == expected
