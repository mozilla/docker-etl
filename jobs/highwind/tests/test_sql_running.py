"""How a run names and disposes of its intermediate tables, and counts their units."""

import datetime
import types

from highwind import sql_running

AS_OF = datetime.date(2026, 8, 20)


def test_two_runs_of_one_date_write_to_tables_of_their_own():
    # Airflow retries as a matter of course, and a retry can start while the attempt it replaces is
    # still running. On a shared name each would truncate the other's cohort out from under the
    # source queries already reading it.
    first = sql_running.cohort_table_name(AS_OF)
    second = sql_running.cohort_table_name(AS_OF)

    assert first != second


def test_the_table_name_says_which_dataset_and_which_run_date_it_belongs_to():
    name = sql_running.cohort_table_name(AS_OF)

    assert name.startswith(f"{sql_running.SCRATCH_DATASET}.highwind_cohort_2026_08_20_")


def test_two_unit_segments_runs_of_one_date_write_to_tables_of_their_own():
    # Mirrors the cohort table's own uniqueness: a retry can start while the attempt it replaces is
    # still running, and a shared name would let one run's materialization truncate the other's.
    first = sql_running.unit_segments_table_name(AS_OF)
    second = sql_running.unit_segments_table_name(AS_OF)

    assert first != second


def test_the_unit_segments_table_name_says_which_dataset_and_which_run_date_it_belongs_to():
    name = sql_running.unit_segments_table_name(AS_OF)

    assert name.startswith(f"{sql_running.SCRATCH_DATASET}.highwind_unit_segments_2026_08_20_")


class UnitCountClient:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    def query(self, sql, job_config=None):
        self.queries.append(sql)
        return types.SimpleNamespace(result=lambda: self.rows)


def test_cohort_enrollment_bounds_is_keyed_by_slug():
    client = UnitCountClient(
        [
            {
                "slug": "a-slug",
                "earliest": datetime.date(2026, 5, 1),
                "latest": datetime.date(2026, 5, 20),
            },
            {
                "slug": "b-slug",
                "earliest": datetime.date(2026, 6, 1),
                "latest": datetime.date(2026, 6, 1),
            },
        ]
    )

    bounds = sql_running.cohort_enrollment_bounds(client, "SELECT 1")

    assert client.queries == ["SELECT 1"]
    assert bounds == {
        "a-slug": (datetime.date(2026, 5, 1), datetime.date(2026, 5, 20)),
        "b-slug": (datetime.date(2026, 6, 1), datetime.date(2026, 6, 1)),
    }


def test_branch_unit_counts_are_keyed_by_slug_then_branch():
    client = UnitCountClient(
        [
            {"slug": "a-slug", "branch": "control", "units": 40},
            {"slug": "a-slug", "branch": "treatment", "units": 41},
            {"slug": "b-slug", "branch": "control", "units": 7},
        ]
    )

    units = sql_running.count_branch_units(client, "SELECT 1")

    assert client.queries == ["SELECT 1"]
    assert units == {"a-slug": {"control": 40, "treatment": 41}, "b-slug": {"control": 7}}


def test_segment_branch_unit_counts_are_keyed_by_slug_then_segment_then_branch():
    client = UnitCountClient(
        [
            {"slug": "a-slug", "segment": "regular_user", "branch": "control", "units": 30},
            {"slug": "a-slug", "segment": "regular_user", "branch": "treatment", "units": 28},
            {"slug": "b-slug", "segment": "new_profile", "branch": "control", "units": 5},
        ]
    )

    units = sql_running.count_segment_branch_units(client, "SELECT 1")

    assert client.queries == ["SELECT 1"]
    assert units == {
        "a-slug": {"regular_user": {"control": 30, "treatment": 28}},
        "b-slug": {"new_profile": {"control": 5}},
    }


class RecordingClient:
    """Records the metadata update in place of a BigQuery client."""

    def __init__(self):
        self.table = None
        self.updated = []

    def get_table(self, table):
        self.table = SimpleTable(table)
        return self.table

    def update_table(self, table, fields):
        self.updated.append((table, fields))
        return table


class SimpleTable:
    def __init__(self, name):
        self.name = name
        self.expires = None


def test_the_cohort_table_is_given_an_expiry_so_a_runs_intermediate_does_not_accumulate():
    # One table per run means a table per run to clean up, and the run that wrote it may fail before
    # it could delete it, so the expiry is set as soon as the table exists.
    client = RecordingClient()

    sql_running.expire_scratch_table(client, "a.dataset.a_table")

    (table, fields) = client.updated[0]
    assert fields == ["expires"]
    assert table.expires > datetime.datetime.now(datetime.timezone.utc)


class MaterializeClient:
    """Stands in for the BigQuery client the materialize functions drive: records every query's
    job config, and the table whose expiry it set."""

    def __init__(self):
        self.queries = []
        self.tables = {}

    def query(self, sql, job_config=None):
        self.queries.append((sql, job_config))
        return types.SimpleNamespace(
            result=lambda: [], slot_millis=1000, total_bytes_processed=2_000_000_000
        )

    def get_table(self, table):
        self.tables[table] = SimpleTable(table)
        return self.tables[table]

    def update_table(self, table, fields):
        return table


def test_materialize_scratch_table_writes_under_the_given_disposition_and_expires_it():
    client = MaterializeClient()

    table, timing = sql_running.materialize_scratch_table(
        client, "SELECT 1", "a.dataset.a_table", "a_source", write_disposition="WRITE_APPEND"
    )

    assert table == "a.dataset.a_table"
    assert timing["source"] == "a_source"
    [(sql, job_config)] = client.queries
    assert sql == "SELECT 1"
    assert job_config.write_disposition == "WRITE_APPEND"
    assert str(job_config.destination) == table
    assert client.tables[table].expires > datetime.datetime.now(datetime.timezone.utc)


def test_materialize_scratch_table_validates_without_writing_or_expiring():
    client = MaterializeClient()

    table, timing = sql_running.materialize_scratch_table(
        client, "SELECT 1", "a.dataset.a_table", "a_source", validate_only=True
    )

    assert table is None
    assert timing["source"] == "a_source"
    [(sql, job_config)] = client.queries
    assert job_config.dry_run is True
    assert client.tables == {}


def test_materialize_cohort_writes_its_own_table_and_gives_it_an_expiry():
    client = MaterializeClient()

    table, timing = sql_running.materialize_cohort(client, "SELECT 1", AS_OF)

    assert table.startswith(f"{sql_running.SCRATCH_DATASET}.highwind_cohort_2026_08_20_")
    assert timing["source"] == "cohort"
    [(sql, job_config)] = client.queries
    assert sql == "SELECT 1"
    assert job_config.write_disposition == "WRITE_TRUNCATE"
    assert str(job_config.destination) == table
    assert client.tables[table].expires > datetime.datetime.now(datetime.timezone.utc)


def test_materialize_cohort_validates_without_writing_or_expiring_a_table():
    client = MaterializeClient()

    table, timing = sql_running.materialize_cohort(client, "SELECT 1", AS_OF, validate_only=True)

    assert table is None
    assert timing["source"] == "cohort"
    [(sql, job_config)] = client.queries
    assert job_config.dry_run is True
    assert client.tables == {}


def test_materialize_cohort_and_unit_segments_both_use_the_shared_helper(monkeypatch):
    # The two near-identical materializations share one helper rather than duplicating its
    # dry-run/write/expire logic.
    calls = []
    original = sql_running.materialize_scratch_table

    def spy(client, sql, table, source, validate_only=False, write_disposition="WRITE_TRUNCATE"):
        calls.append(source)
        return original(client, sql, table, source, validate_only, write_disposition)

    monkeypatch.setattr(sql_running, "materialize_scratch_table", spy)
    client = MaterializeClient()

    sql_running.materialize_cohort(client, "SELECT 1", AS_OF)
    sql_running.materialize_unit_segments(
        client, {("clients_last_seen", "client_id"): "SELECT 2"}, AS_OF
    )

    assert calls == ["cohort", "unit_segments/clients_last_seen/client_id"]


def test_materialize_unit_segments_makes_no_query_when_no_segment_resolved():
    # `queries` is empty whenever no experiment in the run resolved a segment.
    client = MaterializeClient()

    table, timings, failures = sql_running.materialize_unit_segments(client, {}, AS_OF)

    assert (table, timings, failures) == (None, [], {})
    assert client.queries == []


def test_materialize_unit_segments_validates_every_query_without_writing_a_table():
    client = MaterializeClient()
    queries = {
        ("clients_last_seen", "client_id"): "SELECT 1",
        ("newtab", "client_id"): "SELECT 2",
    }

    table, timings, failures = sql_running.materialize_unit_segments(
        client, queries, AS_OF, validate_only=True
    )

    assert table is None
    assert failures == {}
    assert [timing["source"] for timing in timings] == [
        "unit_segments/clients_last_seen/client_id",
        "unit_segments/newtab/client_id",
    ]
    assert all(job_config.dry_run is True for _, job_config in client.queries)
    assert client.tables == {}


def test_materialize_unit_segments_appends_every_source_into_one_table():
    client = MaterializeClient()
    queries = {
        ("clients_last_seen", "client_id"): "SELECT 1",
        ("newtab", "client_id"): "SELECT 2",
    }

    table, timings, failures = sql_running.materialize_unit_segments(client, queries, AS_OF)

    assert table.startswith(f"{sql_running.SCRATCH_DATASET}.highwind_unit_segments_2026_08_20_")
    assert failures == {}
    assert [timing["source"] for timing in timings] == [
        "unit_segments/clients_last_seen/client_id",
        "unit_segments/newtab/client_id",
    ]
    destinations = {str(job_config.destination) for _, job_config in client.queries}
    assert destinations == {table}
    dispositions = {job_config.write_disposition for _, job_config in client.queries}
    assert dispositions == {"WRITE_APPEND"}
    assert client.tables[table].expires > datetime.datetime.now(datetime.timezone.utc)


class OneFailingSourceClient(MaterializeClient):
    """Fails the segment source whose own SQL names it, and succeeds on every other one."""

    def __init__(self, failing_source):
        super().__init__()
        self.failing_source = failing_source

    def query(self, sql, job_config=None):
        if self.failing_source in sql:
            raise RuntimeError(f"the {self.failing_source} source failed")
        return super().query(sql, job_config)


def test_materialize_unit_segments_keeps_the_table_when_only_some_sources_fail():
    client = OneFailingSourceClient(failing_source="newtab")
    queries = {
        ("clients_last_seen", "client_id"): "SELECT 1 -- clients_last_seen",
        ("newtab", "client_id"): "SELECT 2 -- newtab",
    }

    table, timings, failures = sql_running.materialize_unit_segments(client, queries, AS_OF)

    assert table is not None
    assert set(failures) == {("newtab", "client_id")}
    assert "the newtab source failed" in str(failures[("newtab", "client_id")])
    assert [timing["source"] for timing in timings] == ["unit_segments/clients_last_seen/client_id"]


def test_materialize_unit_segments_writes_no_table_when_every_source_fails():
    client = OneFailingSourceClient(failing_source="SELECT")
    queries = {("clients_last_seen", "client_id"): "SELECT 1 -- clients_last_seen"}

    table, timings, failures = sql_running.materialize_unit_segments(client, queries, AS_OF)

    assert table is None
    assert set(failures) == {("clients_last_seen", "client_id")}
    assert timings == []


def test_sufficient_stats_carries_the_winsorize_columns_through():
    row = {
        "n": 10,
        "sum": 5.0,
        "sum_squares": 5.0,
        "pre_sum": 4.0,
        "pre_sum_squares": 4.0,
        "sum_x_pre": 3.0,
        "winsorize_mode": "quantile",
        "post_winsorize_threshold": 500.0,
        "pre_winsorize_threshold": 2000.0,
        "clipped_share": 0.01,
    }

    stats = sql_running.sufficient_stats(row)

    assert stats["winsorize_mode"] == "quantile"
    assert stats["post_winsorize_threshold"] == 500.0
    assert stats["pre_winsorize_threshold"] == 2000.0
    assert stats["clipped_share"] == 0.01


def test_sufficient_stats_leaves_an_off_cells_winsorize_columns_as_the_null_the_row_carried():
    row = {
        "n": 10,
        "sum": 5.0,
        "sum_squares": 5.0,
        "pre_sum": 4.0,
        "pre_sum_squares": 4.0,
        "sum_x_pre": 3.0,
        "winsorize_mode": "off",
        "post_winsorize_threshold": None,
        "pre_winsorize_threshold": None,
        "clipped_share": None,
    }

    stats = sql_running.sufficient_stats(row)

    assert stats["post_winsorize_threshold"] is None
    assert stats["pre_winsorize_threshold"] is None
    assert stats["clipped_share"] is None
