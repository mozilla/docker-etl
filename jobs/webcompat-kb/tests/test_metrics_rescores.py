import pytest

from webcompat_kb.bqhelpers import SchemaId, SchemaType
from webcompat_kb.metrics import rescores


@pytest.mark.parametrize(
    "schema_type,expected",
    [
        (SchemaType.table, SchemaId("project", "dataset", "rescore_test_name")),
        (SchemaType.view, SchemaId("project", "dataset", "rescore_test_name")),
        (SchemaType.routine, SchemaId("project", "dataset", "RESCORE_TEST_name")),
    ],
)
def test_staging_schema_id(schema_type, expected):
    rescore = rescores.Rescore("test", "reason", [], False)
    assert (
        rescore.staging_schema_id(schema_type, SchemaId("project", "dataset", "name"))
        == expected
    )


@pytest.mark.parametrize(
    "schema_type,expected",
    [
        (
            SchemaType.table,
            SchemaId("project", "dataset_archive", "name_before_rescore_test"),
        ),
        (
            SchemaType.view,
            SchemaId("project", "dataset_archive", "name_before_rescore_test"),
        ),
        (
            SchemaType.routine,
            SchemaId("project", "dataset_archive", "name_BEFORE_RESCORE_TEST"),
        ),
    ],
)
def test_archive_schema_id(schema_type, expected):
    rescore = rescores.Rescore("test", "reason", [], False)
    assert (
        rescore.archive_schema_id(schema_type, SchemaId("project", "dataset", "name"))
        == expected
    )
