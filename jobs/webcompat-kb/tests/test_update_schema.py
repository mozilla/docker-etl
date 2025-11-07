import pytest

from webcompat_kb import update_schema
from webcompat_kb.update_schema import DatasetId, SchemaId


def test_stage_dataset():
    assert update_schema.stage_dataset(DatasetId("project", "dataset")) == DatasetId(
        "project", "dataset_test"
    )


def test_add_routine_options():
    sql = """CREATE OR REPLACE FUNCTION `test`(input STRING) RETURNS INT64 AS (
(
    SELECT SAFE_CAST(input AS INT64)
);"""
    expected = f"""{sql[:-1]}\nOPTIONS(description="Test routine");"""
    assert update_schema.add_routine_options(sql, "Test routine") == expected


@pytest.mark.parametrize(
    "input,expected",
    [
        ("table", ("default_project", "default_dataset", "table")),
        ("dataset.table", ("default_project", "dataset", "table")),
        ("project.dataset.table", ("project", "dataset", "table")),
    ],
)
def test_schema_id_from_str(input, expected):
    expected_id = SchemaId(*expected)
    actual_id = SchemaId.from_str(input, "default_project", "default_dataset")
    assert expected_id == actual_id
    expected_dataset_id = DatasetId(*expected[:2])
    assert expected_dataset_id == actual_id.dataset_id


def test_schema_id_mapper():
    mapper = update_schema.SchemaIdMapper(
        dataset_mapping={
            DatasetId("input_project", "input_dataset"): DatasetId(
                "output_project", "output_dataset"
            )
        },
        rewrite_tables={SchemaId("input_project", "input_dataset", "table_rewrite")},
    )

    # Things we do rewrite
    assert mapper(
        SchemaId("input_project", "input_dataset", "view"),
        type=update_schema.ReferenceType.view,
    ) == SchemaId("output_project", "output_dataset", "view")
    assert mapper(
        SchemaId("input_project", "input_dataset", "routine"),
        type=update_schema.ReferenceType.routine,
    ) == SchemaId("output_project", "output_dataset", "routine")
    assert mapper(
        SchemaId("input_project", "input_dataset", "table_rewrite"),
        type=update_schema.ReferenceType.table,
    ) == SchemaId("output_project", "output_dataset", "table_rewrite")

    # Things we don't rewrite
    assert mapper(
        SchemaId("input_project", "input_dataset", "view"),
        type=update_schema.ReferenceType.external,
    ) == SchemaId("input_project", "input_dataset", "view")

    assert mapper(
        SchemaId("other_project", "input_dataset", "view"),
        type=update_schema.ReferenceType.view,
    ) == SchemaId("other_project", "input_dataset", "view")
    assert mapper(
        SchemaId("input_project", "other_dataset", "view"),
        type=update_schema.ReferenceType.view,
    ) == SchemaId("input_project", "other_dataset", "view")

    assert mapper(
        SchemaId("other_project", "input_dataset", "routine"),
        type=update_schema.ReferenceType.routine,
    ) == SchemaId("other_project", "input_dataset", "routine")
    assert mapper(
        SchemaId("input_project", "other_dataset", "routine"),
        type=update_schema.ReferenceType.routine,
    ) == SchemaId("input_project", "other_dataset", "routine")

    assert mapper(
        SchemaId("input_project", "input_dataset", "table_no_rewrite"),
        type=update_schema.ReferenceType.table,
    ) == SchemaId("input_project", "input_dataset", "table_no_rewrite")


def test_reference_resolver():
    mapper = update_schema.SchemaIdMapper(
        dataset_mapping={
            DatasetId("input_project", "input_dataset"): DatasetId(
                "output_project", "output_dataset"
            )
        },
        rewrite_tables={SchemaId("input_project", "input_dataset", "table")},
    )

    resolver = update_schema.ReferenceResolver(
        schema_id=SchemaId("input_project", "input_dataset", "source"),
        schema_id_mapper=mapper,
        view_ids={
            SchemaId("input_project", "input_dataset", "view"),
            SchemaId("input_project", "other_dataset", "view"),
        },
        routine_ids={
            SchemaId("input_project", "input_dataset", "routine"),
            SchemaId("input_project", "other_dataset", "routine"),
        },
    )

    assert resolver("table") == SchemaId("output_project", "output_dataset", "table")
    assert resolver("input_dataset.table") == SchemaId(
        "output_project", "output_dataset", "table"
    )
    assert resolver("input_project.input_dataset.table") == SchemaId(
        "output_project", "output_dataset", "table"
    )

    assert resolver("other_dataset.table") == SchemaId(
        "input_project", "other_dataset", "table"
    )
    assert resolver("other_project.input_dataset.table") == SchemaId(
        "other_project", "input_dataset", "table"
    )
    # Table that isn't in the set to rewrite
    assert resolver("other_table") == SchemaId(
        "input_project", "input_dataset", "other_table"
    )

    assert resolver("view") == SchemaId("output_project", "output_dataset", "view")
    assert resolver("input_dataset.view") == SchemaId(
        "output_project", "output_dataset", "view"
    )
    assert resolver("other_dataset.view") == SchemaId(
        "input_project", "other_dataset", "view"
    )

    assert resolver("routine") == SchemaId(
        "output_project", "output_dataset", "routine"
    )
    assert resolver("input_dataset.routine") == SchemaId(
        "output_project", "output_dataset", "routine"
    )
    assert resolver("other_dataset.routine") == SchemaId(
        "input_project", "other_dataset", "routine"
    )

    assert resolver.references == update_schema.References(
        views={
            SchemaId("output_project", "output_dataset", "view"),
            SchemaId("input_project", "other_dataset", "view"),
        },
        routines={
            SchemaId("output_project", "output_dataset", "routine"),
            SchemaId("input_project", "other_dataset", "routine"),
        },
        tables={
            SchemaId("output_project", "output_dataset", "table"),
            SchemaId("input_project", "other_dataset", "table"),
            SchemaId("input_project", "input_dataset", "other_table"),
        },
        external={SchemaId("other_project", "input_dataset", "table")},
    )
