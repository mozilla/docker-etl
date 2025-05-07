import pytest
from google.cloud.bigquery import SchemaField
from google.cloud.exceptions import NotFound
from google.cloud import storage

from fxci_etl.loaders import bigquery
from fxci_etl.schemas import generate_schema, get_record_cls


@pytest.fixture(autouse=True)
def storage_mock(mocker):
    storage_mock = mocker.MagicMock()
    mocker.patch.object(storage, "Client", return_value=storage_mock)


@pytest.fixture
def client_mock(mocker):
    client_mock = mocker.MagicMock()
    mocker.patch.object(bigquery, "Client", return_value=client_mock)
    return client_mock


@pytest.fixture
def run_ensure_table(make_config, mocker, client_mock):
    table = "tasks"
    config = make_config(
        **{
            "bigquery": {
                "project": "project",
                "dataset": "dataset",
                "tables": {table: "table_v1"},
            }
        }
    )

    def inner(schema=None, exists=True):
        if not schema:
            schema = generate_schema(get_record_cls(table))

        if exists:
            table_mock = mocker.MagicMock()
            table_mock.schema = schema
            client_mock.get_table.return_value = table_mock
        else:
            client_mock.get_table.side_effect = NotFound("message")

        return bigquery.BigQueryLoader(config, table)

    return inner


def test_ensure_table_schemas_match(run_ensure_table):
    loader = run_ensure_table()
    loader.client.get_table.assert_called_once_with(loader.table_name)
    assert not loader.client.create_table.called


def test_ensure_table_schemas_differ_value(run_ensure_table):
    schema = generate_schema(get_record_cls("tasks"))
    schema[0]._properties["mode"] = "NULLABLE"

    with pytest.raises(Exception):
        run_ensure_table(schema)


def test_ensure_table_schemas_differ_extra_column(run_ensure_table):
    schema = generate_schema(get_record_cls("tasks"))
    schema.append(SchemaField("foo", "STRING", "NULLABLE"))

    with pytest.raises(Exception):
        run_ensure_table(schema)


def test_ensure_table_missing(run_ensure_table):
    loader = run_ensure_table(exists=False)
    loader.client.get_table.assert_called_once_with(loader.table_name)
    loader.client.create_table.assert_called_once()
