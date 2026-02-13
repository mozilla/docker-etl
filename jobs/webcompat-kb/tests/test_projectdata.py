import os
import pytest

from webcompat_kb.bqhelpers import SchemaField, TableSchema
from webcompat_kb.metrics.ranks import RankColumn
from webcompat_kb.projectdata import (
    DatasetId,
    ReferenceType,
    RoutineTemplate,
    SchemaId,
    SchemaIdMapper,
    SchemaType,
    TableSchemaCreator,
    TableTemplate,
    TableMetadata,
    ViewTemplate,
    stage_dataset,
)


def test_stage_dataset():
    assert stage_dataset(DatasetId("project", "dataset")) == DatasetId(
        "project", "dataset_test"
    )


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
    mapper = SchemaIdMapper(
        dataset_mapping={
            DatasetId("input_project", "input_dataset"): DatasetId(
                "output_project", "output_dataset"
            )
        },
        map_tables={SchemaId("input_project", "input_dataset", "table_rewrite")},
    )

    # Things we do rewrite
    assert mapper(
        ReferenceType.view,
        SchemaId("input_project", "input_dataset", "view"),
    ) == SchemaId("output_project", "output_dataset", "view")
    assert mapper(
        ReferenceType.routine,
        SchemaId("input_project", "input_dataset", "routine"),
    ) == SchemaId("output_project", "output_dataset", "routine")
    assert mapper(
        ReferenceType.table,
        SchemaId("input_project", "input_dataset", "table_rewrite"),
    ) == SchemaId("output_project", "output_dataset", "table_rewrite")

    # Things we don't rewrite
    assert mapper(
        ReferenceType.external,
        SchemaId("input_project", "input_dataset", "view"),
    ) == SchemaId("input_project", "input_dataset", "view")

    assert mapper(
        ReferenceType.view,
        SchemaId("other_project", "input_dataset", "view"),
    ) == SchemaId("other_project", "input_dataset", "view")
    assert mapper(
        ReferenceType.view,
        SchemaId("input_project", "other_dataset", "view"),
    ) == SchemaId("input_project", "other_dataset", "view")

    assert mapper(
        ReferenceType.routine,
        SchemaId("other_project", "input_dataset", "routine"),
    ) == SchemaId("other_project", "input_dataset", "routine")
    assert mapper(
        ReferenceType.routine,
        SchemaId("input_project", "other_dataset", "routine"),
    ) == SchemaId("input_project", "other_dataset", "routine")

    assert mapper(
        ReferenceType.table,
        SchemaId("input_project", "input_dataset", "table_no_rewrite"),
    ) == SchemaId("input_project", "input_dataset", "table_no_rewrite")


def test_table_schema_creator(project_data):
    project_data.rank_dfns = [RankColumn("rank1"), RankColumn("rank2")]
    creator = TableSchemaCreator(
        project_data, lambda x, y: SchemaId(y.project, f"{y.dataset}_output", y.name)
    )
    # We don't depend on the schema template acually being in the project
    template = TableTemplate(
        os.path.join(project_data.path, "test", "tables", "test_table"),
        TableMetadata(
            name="test_table", description="Table description", etl=[], partition=None
        ),
        template="""
[id]
type="INTEGER"
mode="REQUIRED"
{% for rank in ranks -%}
[{{rank.name}}]
type="INTEGER"
mode="NULLABLE"
{% endfor %}
""",
    )
    expected = TableSchema(
        SchemaId("project", "dataset_output", "test_table"),
        canonical_id=SchemaId("project", "dataset", "test_table"),
        description="Table description",
        fields=[
            SchemaField("id", "INTEGER", "REQUIRED"),
            SchemaField("rank1", "INTEGER", "NULLABLE"),
            SchemaField("rank2", "INTEGER", "NULLABLE"),
        ],
        etl=set(),
        partition=None,
    )
    actual = creator.create_table_schema(DatasetId("project", "dataset"), template)
    assert actual == expected


def test_get_schema_template_view(project_data):
    template = project_data.templates_by_dataset.get_schema_template(
        SchemaType.view,
        SchemaId("test", "webcompat_knowledge_base", "scored_site_reports"),
    )
    assert isinstance(template, ViewTemplate)
    assert (
        template.path.resolve()
        == (
            project_data.path
            / "sql"
            / "webcompat_knowledge_base"
            / "views"
            / "scored_site_reports"
        ).resolve()
    )
    assert template.metadata.name == "scored_site_reports"


def test_get_schema_template_table(project_data):
    template = project_data.templates_by_dataset.get_schema_template(
        SchemaType.table,
        SchemaId("test", "webcompat_knowledge_base", "bugzilla_bugs"),
    )
    assert isinstance(template, TableTemplate)
    assert (
        template.path.resolve()
        == (
            project_data.path
            / "sql"
            / "webcompat_knowledge_base"
            / "tables"
            / "bugzilla_bugs"
        ).resolve()
    )
    assert template.metadata.name == "bugzilla_bugs"


def test_get_schema_template_routine(project_data):
    template = project_data.templates_by_dataset.get_schema_template(
        SchemaType.routine,
        SchemaId("test", "webcompat_knowledge_base", "WEBCOMPAT_HOST"),
    )
    assert isinstance(template, RoutineTemplate)
    assert (
        template.path.resolve()
        == (
            project_data.path
            / "sql"
            / "webcompat_knowledge_base"
            / "routines"
            / "WEBCOMPAT_HOST"
        ).resolve()
    )
    assert template.metadata.name == "WEBCOMPAT_HOST"


@pytest.mark.parametrize(
    "schema_type,schema_id",
    [
        (
            SchemaType.routine,
            SchemaId("test", "webcompat_knowledge_base", "scored_site_reports"),
        ),
        (
            SchemaType.view,
            SchemaId(
                "test", "webcompat_knowledge_base", "notexisting_scored_site_reports"
            ),
        ),
    ],
)
def test_get_schema_template_invalid(project_data, schema_type, schema_id):
    with pytest.raises(KeyError):
        project_data.templates_by_dataset.get_schema_template(schema_type, schema_id)
