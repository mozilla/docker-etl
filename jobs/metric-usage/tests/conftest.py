"""Runs the resolver tests offline. Persistent UDFs come from
tests/fixtures/sql, a few copied from bigquery-etl's generated-sql branch,
rather than .cache, so the tests need neither a fetch nor BigQuery."""

import pathlib

from metric_usage import lineage

ROOT = pathlib.Path(__file__).resolve().parent.parent

lineage.SQL_ROOT = ROOT / "tests" / "fixtures" / "sql"
lineage._PERSISTENT = None
