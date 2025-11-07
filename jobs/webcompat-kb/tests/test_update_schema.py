from webcompat_kb import update_schema
from webcompat_kb.projectdata import DatasetId, SchemaId, SchemaIdMapper, SchemaType


def test_add_routine_options():
    sql = """CREATE OR REPLACE FUNCTION `test`(input STRING) RETURNS INT64 AS (
(
    SELECT SAFE_CAST(input AS INT64)
);"""
    expected = f"""{sql[:-1]}\nOPTIONS(description="Test routine");"""
    assert update_schema.add_routine_options(sql, "Test routine") == expected


def test_reference_resolver():
    mapper = SchemaIdMapper(
        dataset_mapping={
            DatasetId("input_project", "input_dataset"): DatasetId(
                "output_project", "output_dataset"
            )
        },
        rewrite_tables={SchemaId("input_project", "input_dataset", "table")},
    )

    references = update_schema.References()
    resolver = update_schema.ReferenceResolver(
        schema_id=SchemaId("input_project", "input_dataset", "source"),
        schema_id_mapper=mapper,
        known_schema_ids={
            SchemaId("input_project", "input_dataset", "table"): SchemaType.table,
            SchemaId("input_project", "other_dataset", "table"): SchemaType.table,
            SchemaId("input_project", "input_dataset", "view"): SchemaType.view,
            SchemaId("input_project", "other_dataset", "view"): SchemaType.view,
            SchemaId("input_project", "input_dataset", "routine"): SchemaType.routine,
            SchemaId("input_project", "other_dataset", "routine"): SchemaType.routine,
        },
        references=references,
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

    assert references == update_schema.References(
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
        },
        external={
            SchemaId("other_project", "input_dataset", "table"),
            SchemaId("input_project", "input_dataset", "other_table"),
        },
    )
