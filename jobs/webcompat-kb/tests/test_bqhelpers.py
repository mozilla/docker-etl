import pytest

from webcompat_kb.bqhelpers import (
    Dataset,
    DatasetId,
    SchemaId,
    TableSchema,
    ViewSchema,
)
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField

from .conftest import Call


@pytest.mark.parametrize(
    "input,expected",
    [
        ("table", ("default_project", "default_dataset", "table")),
        ("dataset.table", ("default_project", "dataset", "table")),
        ("project.dataset.table", ("project", "dataset", "table")),
        ("project:dataset.table", ("project", "dataset", "table")),
    ],
)
def test_schema_id_from_str(input, expected):
    expected_id = SchemaId(*expected)
    actual_id = SchemaId.from_str(input, "default_project", "default_dataset")
    assert expected_id == actual_id
    expected_dataset_id = DatasetId(*expected[:2])
    assert expected_dataset_id == actual_id.dataset_id


@pytest.mark.parametrize(
    "dataset_id, expected",
    [
        (None, DatasetId("project", "default_dataset")),
        ("project.dataset", DatasetId("project", "dataset")),
        ("dataset", DatasetId("project", "dataset")),
        (
            Dataset(
                DatasetId("project", "dataset"),
                DatasetId("other_project", "other_dataset"),
                [],
            ),
            DatasetId("project", "dataset"),
        ),
        (DatasetId("project", "dataset"), DatasetId("project", "dataset")),
    ],
)
def test_get_dataset_id(bq_client, dataset_id, expected):
    actual = bq_client.get_dataset_id(dataset_id)
    assert actual == expected


@pytest.mark.parametrize(
    "dataset_id, table, expected",
    [
        (
            None,
            bigquery.Table("project.dataset.table"),
            SchemaId("project", "dataset", "table"),
        ),
        (None, "project.dataset.table", SchemaId("project", "dataset", "table")),
        (
            DatasetId("other_project", "other_dataset"),
            "dataset.table",
            SchemaId("other_project", "dataset", "table"),
        ),
        (
            DatasetId("other_project", "other_dataset"),
            "table",
            SchemaId("other_project", "other_dataset", "table"),
        ),
        (
            DatasetId("other_project", "other_dataset"),
            TableSchema(
                SchemaId("project", "dataset", "table"),
                SchemaId("other_project", "other_dataset", "table"),
                [],
                set(),
            ),
            SchemaId("project", "dataset", "table"),
        ),
        (
            DatasetId("other_project", "other_dataset"),
            ViewSchema(
                SchemaId("project", "dataset", "table"),
                SchemaId("other_project", "other_dataset", "table"),
            ),
            SchemaId("project", "dataset", "table"),
        ),
    ],
)
def test_get_table_id(bq_client, dataset_id, table, expected):
    actual = bq_client.get_table_id(dataset_id, table)
    assert actual == expected


@pytest.mark.parametrize(
    "table_id",
    [
        "project.dataset.table",
        TableSchema(
            SchemaId("project", "dataset", "table"),
            SchemaId("other_project", "other_dataset", "other_table"),
            [],
            set(),
        ),
        SchemaId("project", "dataset", "table"),
    ],
)
def test_ensure_table(bq_client, table_id):
    bq_client.ensure_table(table_id, [], "default_dataset")
    assert bq_client.client.called == [
        Call(
            function="create_table",
            arguments={
                "table": bigquery.Table("project.dataset.table"),
                "exists_ok": True,
            },
        )
    ]


@pytest.mark.parametrize(
    "table_id",
    [
        "project.dataset.table",
        TableSchema(
            SchemaId("project", "dataset", "table"),
            SchemaId("other_project", "other_dataset", "other_table"),
            [],
            set(),
        ),
        SchemaId("project", "dataset", "table"),
    ],
)
def test_get_table(bq_client, table_id):
    bq_client.get_table(table_id, None)
    assert bq_client.client.called == [
        Call(
            function="get_table",
            arguments={
                "table": "project.dataset.table",
            },
        )
    ]


@pytest.mark.parametrize(
    "table",
    [
        "project.dataset.table",
        TableSchema(
            SchemaId("project", "dataset", "table"),
            SchemaId("other_project", "other_dataset", "other_table"),
            [],
            set(),
        ),
        SchemaId("project", "dataset", "table"),
    ],
)
def test_write_table(bq_client, table):
    schema = [bigquery.SchemaField("id", "INTEGER", "REQUIRED")]
    rows = [{"id": 1}]
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
        write_disposition="WRITE_APPEND",
    )
    bq_client.write_table(table, schema, rows, False)
    assert bq_client.client.called == [
        Call(
            function="load_table_from_json",
            arguments={
                "rows": rows,
                "table": "project.dataset.table",
                "job_config": job_config,
            },
        )
    ]


@pytest.mark.parametrize(
    "table",
    [
        "project.dataset.table",
        bigquery.Table("project.dataset.table"),
        TableSchema(
            SchemaId("project", "dataset", "table"),
            SchemaId("other_project", "other_dataset", "other_table"),
            [],
            set(),
        ),
        SchemaId("project", "dataset", "table"),
    ],
)
def test_insert_rows(bq_client, table):
    rows = [{"id": 1}]
    bq_client.client.return_values["get_table"].append(
        bigquery.Table("project.dataset.table")
    )
    bq_client.insert_rows(table, rows)
    assert bq_client.client.called[0] == Call(
        function="get_table",
        arguments={
            "table": "project.dataset.table",
        },
    )
    insert_rows_call = bq_client.client.called[1]
    assert insert_rows_call.function == "insert_rows"
    assert set(insert_rows_call.arguments.keys()) == {"rows", "table"}
    assert insert_rows_call.arguments["rows"] == rows
    assert isinstance(insert_rows_call.arguments["table"], bigquery.Table)


@pytest.mark.parametrize(
    "table",
    [
        "project.dataset.table",
        bigquery.Table("project.dataset.table"),
        TableSchema(
            SchemaId("project", "dataset", "table"),
            SchemaId("other_project", "other_dataset", "other_table"),
            [],
            set(),
        ),
        SchemaId("project", "dataset", "table"),
    ],
)
def test_insert_query(bq_client, table):
    cols = ["id", "other"]
    query = "SELECT 1,2"
    bq_client.insert_query(table, cols, query)
    job_config = bigquery.QueryJobConfig(default_dataset="project.default_dataset")
    assert bq_client.client.called == [
        Call(
            function="query",
            arguments={
                "query": """INSERT `project.dataset.table` (id, other)
(SELECT 1,2)""",
                "job_config": job_config,
            },
        )
    ]


@pytest.mark.parametrize(
    "table",
    [
        "project.dataset.table",
        bigquery.Table("project.dataset.table"),
        TableSchema(
            SchemaId("project", "dataset", "table"),
            SchemaId("other_project", "other_dataset", "other_table"),
            [],
            set(),
        ),
        SchemaId("project", "dataset", "table"),
    ],
)
def test_delete_query(bq_client, table):
    condition = "FALSE"
    bq_client.delete_query(table, condition)
    job_config = bigquery.QueryJobConfig(default_dataset="project.default_dataset")
    assert bq_client.client.called == [
        Call(
            function="query",
            arguments={
                "query": """DELETE FROM `project.dataset.table` WHERE FALSE""",
                "job_config": job_config,
            },
        )
    ]


@pytest.mark.parametrize(
    "view_id",
    [
        "project.dataset.view",
        ViewSchema(
            SchemaId("project", "dataset", "view"),
            SchemaId("other_project", "other_dataset", "other_view"),
        ),
        SchemaId("project", "dataset", "view"),
    ],
)
def test_create_view(bq_client, view_id):
    query = "SELECT * FROM table"
    bq_client.create_view(view_id, query)
    view_table = bigquery.Table("project.dataset.view")
    view_table.view_query = query
    assert bq_client.client.called == [
        Call(
            function="delete_table",
            arguments={
                "table": "project.dataset.view",
                "not_found_ok": True,
            },
        ),
        Call(
            function="create_table",
            arguments={"table": view_table, "exists_ok": False},
        ),
    ]


@pytest.mark.parametrize(
    "table",
    [
        bigquery.Table("project.dataset.table"),
        "project.dataset.table",
        TableSchema(
            SchemaId("project", "dataset", "table"),
            SchemaId("other_project", "other_dataset", "other_table"),
            [],
            set(),
        ),
        SchemaId("project", "dataset", "table"),
    ],
)
def test_delete_table(bq_client, table):
    for not_found_ok in [True, False]:
        bq_client.delete_table(table, not_found_ok)
    assert bq_client.client.called == [
        Call(
            function="delete_table",
            arguments={
                "table": "project.dataset.table",
                "not_found_ok": True,
            },
        ),
        Call(
            function="delete_table",
            arguments={
                "table": "project.dataset.table",
                "not_found_ok": False,
            },
        ),
    ]


@pytest.mark.parametrize(
    "current_schema,new_schema",
    [
        ([SchemaField("foo", "INTEGER")], [SchemaField("bar", "INTEGER")]),
        ([SchemaField("foo", "INTEGER")], [SchemaField("foo", "STRING")]),
        (
            [SchemaField("foo", "INTEGER", mode="REQUIRED")],
            [SchemaField("foo", "STRING", mode="NULLABLE")],
        ),
        (
            [SchemaField("foo", "INTEGER"), SchemaField("bar", "INTEGER")],
            [SchemaField("foo", "INTEGER")],
        ),
        (
            [SchemaField("foo", "INTEGER")],
            [
                SchemaField("foo", "INTEGER"),
                SchemaField("bar", "INTEGER", mode="REQUIRED"),
            ],
        ),
    ],
)
def test_get_new_fields_invalid(bq_client, current_schema, new_schema):
    with pytest.raises(ValueError):
        bq_client._get_new_fields("test.table", current_schema, new_schema)


@pytest.mark.parametrize(
    "current_schema,new_schema,expected",
    [
        ([SchemaField("foo", "INTEGER")], [SchemaField("foo", "INTEGER")], []),
        (
            [SchemaField("foo", "INTEGER")],
            [
                SchemaField("foo", "INTEGER"),
                SchemaField("bar", "INTEGER", mode="NULLABLE"),
            ],
            [SchemaField("bar", "INTEGER", mode="NULLABLE")],
        ),
        (
            [SchemaField("foo", "INTEGER")],
            [
                SchemaField("bar", "INTEGER", mode="NULLABLE"),
                SchemaField("foo", "INTEGER"),
            ],
            [SchemaField("bar", "INTEGER", mode="NULLABLE")],
        ),
    ],
)
def test_get_new_fields_value(bq_client, current_schema, new_schema, expected):
    assert (
        bq_client._get_new_fields("test.table", current_schema, new_schema) == expected
    )


@pytest.mark.parametrize(
    "table",
    [
        bigquery.Table("project.dataset.table"),
        "project.dataset.table",
        TableSchema(
            SchemaId("project", "dataset", "table"),
            SchemaId("other_project", "other_dataset", "other_table"),
            [],
            set(),
        ),
        SchemaId("project", "dataset", "table"),
    ],
)
def test_add_table_fields(bq_client, table):
    old_schema = [bigquery.SchemaField("id", "INTEGER", "REQUIRED")]
    new_schema = [
        bigquery.SchemaField("new1", "STRING"),
        bigquery.SchemaField("id", "INTEGER", "REQUIRED"),
        bigquery.SchemaField("new2", "DATETIME", "REPEATED"),
    ]
    final_schema = [
        bigquery.SchemaField("id", "INTEGER", "REQUIRED"),
        bigquery.SchemaField("new1", "STRING"),
        bigquery.SchemaField("new2", "DATETIME", "REPEATED"),
    ]
    if isinstance(table, bigquery.Table):
        table.schema = old_schema
        target_table = table
    else:
        target_table = bigquery.Table("project.dataset.table", schema=old_schema)

    bq_client.client.return_values["get_table"].append(target_table)
    bq_client.client.return_values["update_table"].append(target_table)

    updated_table = bq_client.add_table_fields(table, new_schema)
    assert updated_table.schema == final_schema
    assert bq_client.client.called[-1] == Call(
        "update_table", arguments={"table": target_table, "fields": ["schema"]}
    )


def test_check_write_targets(bq_client):
    allowed = SchemaId("test", "dataset", "allowed")
    forbidden = SchemaId("test", "dataset", "forbidden")
    bq_client.write_targets = {allowed}

    for schema_id in [allowed, forbidden]:
        for fn, args in [
            (bq_client.check_write_target, (schema_id,)),
            (bq_client.ensure_table, (schema_id, [])),
            (bq_client.write_table, (schema_id, [], [], False)),
            (bq_client.insert_query, (schema_id, [], "SELECT 1")),
            (bq_client.delete_query, (schema_id, "FALSE")),
            (bq_client.create_view, (schema_id, "SELECT 1")),
            (bq_client.delete_table, (schema_id,)),
            (bq_client.delete_routine, (schema_id,)),
        ]:
            if schema_id == forbidden:
                with pytest.raises(ValueError):
                    fn(*args)
            else:
                fn(*args)

    # Methods that require some setup
    bq_client.client.return_values["get_table"].append(bigquery.Table(str(allowed)))
    bq_client.insert_rows(allowed, [])
    with pytest.raises(ValueError):
        bq_client.insert_rows(forbidden, [])

    bq_client.client.return_values["get_table"].append(bigquery.Table(str(allowed)))
    bq_client.add_table_fields(allowed, [])
    with pytest.raises(ValueError):
        bq_client.add_table_fields(forbidden, [])


@pytest.mark.parametrize(
    "schema_id,dataset_id,expected",
    [
        (
            SchemaId("project", "dataset", "table"),
            DatasetId("project", "dataset"),
            "table",
        ),
        (
            SchemaId("project", "dataset", "table"),
            DatasetId("project", "other_dataset"),
            "dataset.table",
        ),
        (
            SchemaId("project", "dataset", "table"),
            DatasetId("other_project", "dataset"),
            "project.dataset.table",
        ),
        (
            SchemaId("project", "dataset", "table"),
            DatasetId("other_project", "other_dataset"),
            "project.dataset.table",
        ),
    ],
)
def test_schema_id_relative_string(schema_id, dataset_id, expected):
    assert schema_id.relative_string(dataset_id) == expected
