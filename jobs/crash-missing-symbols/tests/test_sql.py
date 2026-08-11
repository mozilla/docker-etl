"""Checks on the query text itself.

These don't hit BigQuery. They pin the things about the query that are easy to
change by accident and expensive to notice: the window boundary and the two
dedupe modes.
"""

import pathlib
import re

SQL = (
    pathlib.Path(__file__).resolve().parent.parent
    / "crash_missing_symbols"
    / "sql"
    / "modules_with_missing_symbols.sql"
).read_text()


def strip_comments(sql):
    return "\n".join(
        line for line in sql.splitlines() if not line.strip().startswith("--")
    )


BODY = strip_comments(SQL)


class TestWindow:
    def test_window_is_half_open(self):
        """end_date is excluded, matching the local baseline run.

        With `<=` a run whose end_date has data covers an extra day. On
        2026-08-06 that was 321 rows against the correct 251.
        """
        assert "crash_date < @end_date" in BODY
        assert "crash_date <= @end_date" not in BODY

    def test_window_lower_bound_uses_window_days(self):
        assert (
            "crash_date >= DATE_SUB(@end_date, INTERVAL @window_days DAY)" in BODY
        )


class TestDedupeModes:
    def test_address_fields_are_gated_on_the_dedupe_key(self):
        """They must only participate when reproducing the Spark dedupe."""
        for field in ("base_addr", "end_addr", "code_id"):
            assert f"IF(@dedupe_key = 'module-struct', {field}, NULL)" in BODY

    def test_dedupe_selects_the_displayed_fields(self):
        distinct = re.search(r"SELECT DISTINCT(.*?)FROM modules", BODY, re.S).group(1)
        for field in ("uuid", "name", "version", "debug_id", "debug_file"):
            assert re.search(rf"\b{field}\b", distinct)

    def test_grouping_ignores_the_address_fields(self):
        """Addresses affect the dedupe, never the output grouping."""
        group_by = re.search(r"GROUP BY(.*?)HAVING", BODY, re.S).group(1)
        for field in ("base_addr", "end_addr", "code_id"):
            assert field not in group_by


class TestParameters:
    def test_all_parameters_are_bound_by_the_caller(self):
        """Every @param in the SQL must be one main.py actually supplies."""
        supplied = {"end_date", "window_days", "min_crash_count", "dedupe_key"}
        assert set(re.findall(r"@(\w+)", SQL)) == supplied
