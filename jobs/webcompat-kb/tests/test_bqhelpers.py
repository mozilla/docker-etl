import pytest

from google.cloud.bigquery import SchemaField


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
def test_new_fields_invalid(bq_client, current_schema, new_schema):
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
def test_new_fields_value(bq_client, current_schema, new_schema, expected):
    assert (
        bq_client._get_new_fields("test.table", current_schema, new_schema) == expected
    )
