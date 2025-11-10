import hashlib
import os
import stat
from typing import Self


class Blob:
    """Git-like Blob object

    This represents the bytes content of a file, using the same representation as git."""

    def __init__(self, data: bytes):
        self.data = data

    def serialize(self) -> bytes:
        return b"blob %d\0%b" % (len(self.data), self.data)

    def hash(self) -> bytes:
        return hashlib.sha1(self.serialize()).digest()


class Tree:
    """Git-like Tree object

    This represents the content of a directory, using the same representation as git."""

    def __init__(self) -> None:
        self.contents: list[TreeEntry] = []

    def serialize(self) -> bytes:
        data = b""
        for item in sorted(self.contents, key=lambda x: x.path):
            data += b"%b %b\0%b" % (item.mode, os.path.basename(item.path), item.hash())
        return b"tree %d\0%b" % (len(data), data)

    def hash(self) -> bytes:
        return hashlib.sha1(self.serialize()).digest()


class TreeEntry:
    def __init__(self, path: bytes, mode: bytes, content: Blob | Tree):
        self.path = path
        self.mode = mode
        self.content = content

    def hash(self) -> bytes:
        return self.content.hash()

    @classmethod
    def from_path(cls, path: bytes | str | os.PathLike) -> Self:
        st = os.stat(path)

        if isinstance(path, os.PathLike):
            path = path.__fspath__()
        if isinstance(path, bytes):
            path_bytes = path
        else:
            path_bytes = str(path).encode("utf-8")

        # These modes match the subset supported by git
        if stat.S_ISDIR(st.st_mode):
            mode = b"40000"
            content: Tree | Blob = Tree()
        else:
            if stat.S_IXUSR & st.st_mode:
                mode = b"100755"
            elif stat.S_ISLNK(st.st_mode):
                mode = b"120000"
            else:
                mode = b"100644"
            with open(path, "rb") as f:
                content = Blob(f.read())
        return cls(path_bytes, mode, content)

    def append(self, other: Self) -> None:
        assert other != self
        if isinstance(self.content, Blob):
            raise ValueError("Cannot append to a Blob TreeEntry")
        self.content.contents.append(other)


def build_tree(root: str | os.PathLike) -> TreeEntry:
    root_path = str(root)
    root_tree = TreeEntry.from_path(root_path)
    tree_entries = {root_path: root_tree}
    for dir_path, dir_names, file_names in os.walk(root_path):
        parent_tree = tree_entries[dir_path]
        assert isinstance(parent_tree.content, Tree)
        for name in dir_names + file_names:
            path = os.path.join(dir_path, name)
            tree_entry = TreeEntry.from_path(path)
            parent_tree.append(tree_entry)
            assert path not in tree_entries
            tree_entries[path] = tree_entry
    return root_tree


def hash_tree(path: str | os.PathLike) -> bytes:
    root_tree = build_tree(path)
    return root_tree.hash()
