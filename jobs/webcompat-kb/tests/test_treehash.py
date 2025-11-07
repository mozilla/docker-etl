import hashlib
import pathlib
import os

from webcompat_kb import treehash


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

    tree = treehash.build_tree(tmp_path)
    assert isinstance(tree.content, treehash.Tree)

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

    assert isinstance(file_1_blob, treehash.Blob)
    file_1_blob_data = b"blob 5\x00%b" % file_1_contents
    file_1_hash = hashlib.sha1(file_1_blob_data).digest()
    assert file_1_blob.serialize() == file_1_blob_data
    assert file_1_blob.hash() == file_1_hash

    file_2_blob_data = b"blob 5\x00%b" % file_2_contents
    file_2_hash = hashlib.sha1(file_2_blob_data).digest()
    assert isinstance(file_2_blob, treehash.Blob)
    assert file_2_blob.serialize() == file_2_blob_data
    assert file_2_blob.hash() == file_2_hash

    assert isinstance(subdir_tree, treehash.Tree)
    assert len(subdir_tree.contents) == 1
    file_3_blob = subdir_tree.contents[0].content

    assert isinstance(file_3_blob, treehash.Blob)
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
