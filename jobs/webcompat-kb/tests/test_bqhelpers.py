import inspect
from dataclasses import dataclass
from unittest.mock import Mock
from typing import Any

import pytest

from webcompat_kb.bqhelpers import (
    BigQuery,
    Dataset,
    DatasetId,
    SchemaId,
    TableSchema,
    ViewSchema,
)
from google.cloud import bigquery


@dataclass
class Call:
    function: str
    arguments: dict[str, Any]

    def __eq__(self, other):
        if type(other) is not type(self):
            return False

        if self.function != other.function:
            return False

        if set(self.arguments.keys()) != set(other.arguments.keys()):
            return False

        for arg_name, self_value in self.arguments.items():
            other_value = other.arguments[arg_name]
            if self_value != other_value:
                # In case the values aren't equal consider if they're the same
                # type with the same data
                if type(self_value) is not type(other_value):
                    return False

                if not hasattr(self_value, "__dict__") or not hasattr(
                    other_value, "__dict__"
                ):
                    return False

                if self_value.__dict__ != other_value.__dict__:
                    return False
        return True


class MockClient:
    def __init__(self, project):
        self.project = project
        self.called = []

    def _record(self):
        current_frame = inspect.currentframe()
        assert hasattr(current_frame, "f_back")
        caller = current_frame.f_back
        assert caller is not None
        arguments = {}
        args = inspect.getargvalues(caller)
        for arg in args.args:
            arguments[arg] = args.locals[arg]
        if args.varargs:
            arguments[args.varargs] = args.locals[args.varargs]
        if args.keywords:
            arguments.update(args.locals.get(args.keywords, {}))
        if "self" in arguments:
            del arguments["self"]
        call = Call(function=caller.f_code.co_name, arguments=arguments)
        self.called.append(call)

    def create_table(self, table, exists_ok=False):
        self._record()
        assert isinstance(table, bigquery.Table)

    def get_table(self, table):
        self._record()
        assert isinstance(table, (str, bigquery.Table))

    def load_table_from_json(self, rows, table, job_config):
        self._record()
        assert isinstance(table, (str, bigquery.Table))
        assert isinstance(job_config, bigquery.LoadJobConfig)
        return Mock()

    def insert_rows(self, table, rows):
        self._record()
        assert isinstance(table, (str, bigquery.Table))

    def get_routine(self, routine):
        self._record()
        assert isinstance(routine, (str, bigquery.Routine))

    def list_routines(self, dataset):
        self._record()
        assert isinstance(dataset, str)

    def list_tables(self, dataset):
        self._record()
        assert isinstance(dataset, str)

    def query(self, query, job_config):
        self._record()
        assert isinstance(query, str)
        assert isinstance(job_config, bigquery.QueryJobConfig)
        return Mock()

    def delete_table(self, table, not_found_ok):
        self._record()
        assert isinstance(table, (str, bigquery.Table))
        assert isinstance(not_found_ok, bool)

    def delete_routine(self, routine, not_found_ok):
        self._record()
        assert isinstance(routine, (str, bigquery.Routine))
        assert isinstance(not_found_ok, bool)


@pytest.fixture
def bq_client():
    return BigQuery(MockClient("project"), "default_dataset", True)


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
    bq_client.insert_rows(table, rows)
    assert bq_client.client.called == [
        Call(
            function="insert_rows",
            arguments={
                "rows": rows,
                "table": "project.dataset.table",
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
