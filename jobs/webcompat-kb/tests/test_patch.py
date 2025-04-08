import difflib

import pytest

from webcompat_kb.metric_changes import reverse_apply_diff


@pytest.mark.parametrize(
    "doc1,doc2",
    [
        ("line1\n", "line2\n"),
        ("", "line2\n"),
        ("line1\n", ""),
        ("line1\nline2\n", "line1\nline3\nline2\n"),
        ("line1\nline2\nline3\n", "line1\nline2\nline4\nline3\nline5\n"),
    ],
)
def test_reverse_diff(doc1, doc2):
    diff = "".join(
        list(difflib.unified_diff(doc1.splitlines(True), doc2.splitlines(True)))[2:]
    )
    assert reverse_apply_diff(doc2, diff) == doc1
