import hashlib
import pathlib
import os

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


def test_tree(tmp_path: pathlib.Path):
    file_1 = tmp_path / "file1.txt"
    file_2 = tmp_path / "file2.txt"
    subdir = tmp_path / "sub"
    subdir.mkdir()
    file_3 = subdir / "file3.txt"

    file_1_contents = b"file1"
    file_1.write_bytes(file_1_contents)
    file_2_contents = b"file2"
    file_2.write_bytes(file_2_contents)
    file_3_contents = b"file3"
    file_3.write_bytes(file_3_contents)

    tree = update_schema.build_tree(tmp_path)
    assert isinstance(tree.content, update_schema.Tree)

    assert len(tree.content.contents) == 3

    file_1_blob = [
        item.content
        for item in tree.content.contents
        if os.path.basename(item.path) == b"file1.txt"
    ][0]
    file_2_blob = [
        item.content
        for item in tree.content.contents
        if os.path.basename(item.path) == b"file2.txt"
    ][0]
    subdir_tree = [
        item.content
        for item in tree.content.contents
        if os.path.basename(item.path) == b"sub"
    ][0]

    assert isinstance(file_1_blob, update_schema.Blob)
    file_1_blob_data = b"blob 5\x00%b" % file_1_contents
    file_1_hash = hashlib.sha1(file_1_blob_data).digest()
    assert file_1_blob.serialize() == file_1_blob_data
    assert file_1_blob.hash() == file_1_hash

    file_2_blob_data = b"blob 5\x00%b" % file_2_contents
    file_2_hash = hashlib.sha1(file_2_blob_data).digest()
    assert isinstance(file_2_blob, update_schema.Blob)
    assert file_2_blob.serialize() == file_2_blob_data
    assert file_2_blob.hash() == file_2_hash

    assert isinstance(subdir_tree, update_schema.Tree)
    assert len(subdir_tree.contents) == 1
    file_3_blob = subdir_tree.contents[0].content

    assert isinstance(file_3_blob, update_schema.Blob)
    file_3_blob_data = b"blob 5\x00%b" % file_3_contents
    file_3_hash = hashlib.sha1(file_3_blob_data).digest()
    assert file_3_blob.serialize() == file_3_blob_data
    assert file_3_blob.hash() == file_3_hash

    subdir_tree_data = b"tree 37\x00100644 file3.txt\x00%b" % file_3_blob.hash()
    subdir_hash = hashlib.sha1(subdir_tree_data).digest()
    assert subdir_tree.serialize() == subdir_tree_data
    assert subdir_tree.hash() == subdir_hash

    root_tree_data = (
        b"tree 104\x00100644 file1.txt\x00%b100644 file2.txt\x00%b40000 sub\x00%b"
        % (file_1_hash, file_2_hash, subdir_hash)
    )
    root_tree_hash = hashlib.sha1(root_tree_data).digest()
    assert tree.content.serialize() == root_tree_data
    assert tree.content.hash() == root_tree_hash
