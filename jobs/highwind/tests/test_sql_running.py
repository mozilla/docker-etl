"""How a run names and disposes of the cohort table every source query reads."""

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

    assert name.startswith(f"{sql_running.COHORT_DATASET}.highwind_cohort_2026_08_20_")


class UnitCountClient:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    def query(self, sql, job_config=None):
        self.queries.append(sql)
        return types.SimpleNamespace(result=lambda: self.rows)


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

    sql_running.expire_cohort_table(client, "a.dataset.a_table")

    (table, fields) = client.updated[0]
    assert fields == ["expires"]
    assert table.expires > datetime.datetime.now(datetime.timezone.utc)


def test_sufficient_stats_carries_the_winsorize_columns_through():
    row = {
        "n": 10,
        "sum": 5.0,
        "sum_squares": 5.0,
        "pre_sum": 4.0,
        "pre_sum_squares": 4.0,
        "sum_x_pre": 3.0,
        "winsorize_mode": "fraction",
        "outcome_threshold": 500.0,
        "covariate_threshold": 2000.0,
        "clipped_share": 0.01,
    }

    stats = sql_running.sufficient_stats(row)

    assert stats["winsorize_mode"] == "fraction"
    assert stats["outcome_threshold"] == 500.0
    assert stats["covariate_threshold"] == 2000.0
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
        "outcome_threshold": None,
        "covariate_threshold": None,
        "clipped_share": None,
    }

    stats = sql_running.sufficient_stats(row)

    assert stats["outcome_threshold"] is None
    assert stats["covariate_threshold"] is None
    assert stats["clipped_share"] is None
