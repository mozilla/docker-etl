from webcompat_kb import update_schema
from webcompat_kb.metrics.ranks import RankColumn
from webcompat_kb.projectdata import (
    DatasetId,
    DatasetTemplates,
    Project,
    RoutineTemplate,
    SchemaId,
    SchemaIdMapper,
    SchemaMetadata,
    SchemaType,
    TableMetadata,
    TableTemplate,
    ViewTemplate,
    create_datasets,
)


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
        map_tables={SchemaId("input_project", "input_dataset", "table")},
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


def test_render_schema(project_data):
    dataset_id = DatasetId("test", "dataset")
    project_data.rank_dfns = [RankColumn("rank1"), RankColumn("rank2")]

    table_template = TableTemplate(
        path="/tmp/dataset/tables/test_table",
        metadata=TableMetadata(name="test_table"),
        template="""[field]
type="STRING"
mode="REQUIRED"
{% for rank in ranks -%}
[{{rank.name}}]
type="INTEGER"
mode="NULLABLE"
{% endfor %}
""",
    )
    view_template = ViewTemplate(
        "/tmp/dataset/views/test_view",
        metadata=SchemaMetadata(name="test_view"),
        template="""SELECT * FROM {{ ref('test_table') }}""",
    )
    routine_template = RoutineTemplate(
        "/tmp/dataset/routines/test_routine",
        metadata=SchemaMetadata(name="test_routine"),
        template="""CREATE OR REPLACE FUNCTION `{{ ref(name) }}`() RETURNS INT64 AS ( 1 )""",
    )

    project_data.templates_by_dataset = {
        dataset_id: DatasetTemplates(
            id=dataset_id,
            description="",
            tables=[table_template],
            views=[view_template],
            routines=[routine_template],
        )
    }

    def schema_id_mapper(reference_type, schema_id):
        return schema_id

    def dataset_id_mapper(dataset_id):
        return dataset_id

    datasets = create_datasets(
        "test", project_data, dataset_id_mapper, schema_id_mapper
    )
    project = Project(
        "test", project_data, datasets, dataset_id_mapper, schema_id_mapper
    )
    assert update_schema.render_schemas(
        project, [SchemaId("test", "dataset", "test_table")]
    ) == {
        SchemaId("test", "dataset", "test_table"): (
            SchemaType.table,
            """[field]
type="STRING"
mode="REQUIRED"
[rank1]
type="INTEGER"
mode="NULLABLE"
[rank2]
type="INTEGER"
mode="NULLABLE"
""",
        )
    }

    assert update_schema.render_schemas(
        project,
        [
            SchemaId("test", "dataset", "test_view"),
            SchemaId("test", "dataset", "test_routine"),
        ],
    ) == {
        SchemaId("test", "dataset", "test_view"): (
            SchemaType.view,
            "SELECT * FROM test.dataset.test_table",
        ),
        SchemaId("test", "dataset", "test_routine"): (
            SchemaType.routine,
            "CREATE OR REPLACE FUNCTION `test.dataset.test_routine`() RETURNS INT64 AS ( 1 )",
        ),
    }
