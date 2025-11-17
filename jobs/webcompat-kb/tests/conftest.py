import inspect
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Mapping
from unittest.mock import Mock


import pytest
from google.cloud import bigquery

from webcompat_kb.bqhelpers import BigQuery


@dataclass
class Call:
    function: str
    arguments: Mapping[str, Any]

    def __eq__(self, other):
        if type(other) is not type(self):
            return False

        if self.function != other.function:
            return False

        if set(self.arguments.keys()) != set(other.arguments.keys()):
            return False

        for arg_name, self_value in self.arguments.items():
            other_value = other.arguments[arg_name]
            if self_value != other_value:
                # In case the values aren't equal consider if they're the same
                # type with the same data
                if type(self_value) is not type(other_value):
                    return False

                if not hasattr(self_value, "__dict__") or not hasattr(
                    other_value, "__dict__"
                ):
                    return False

                if self_value.__dict__ != other_value.__dict__:
                    return False
        return True


class MockClient:
    def __init__(self, project):
        self.project = project
        self.called = []
        # Map between fn name and list of return values for that function
        self.return_values = defaultdict(deque)

    def _record(self):
        current_frame = inspect.currentframe()
        assert hasattr(current_frame, "f_back")
        caller = current_frame.f_back
        assert caller is not None
        arguments = {}
        args = inspect.getargvalues(caller)
        for arg in args.args:
            arguments[arg] = args.locals[arg]
        if args.varargs:
            arguments[args.varargs] = args.locals[args.varargs]
        if args.keywords:
            arguments.update(args.locals.get(args.keywords, {}))
        if "self" in arguments:
            del arguments["self"]
        call = Call(function=caller.f_code.co_name, arguments=arguments)
        self.called.append(call)
        rvs = self.return_values.get(call.function)
        if rvs:
            return rvs.popleft()
        return Mock()

    def create_table(self, table, exists_ok=False):
        rv = self._record()
        assert isinstance(table, bigquery.Table)
        return rv

    def get_table(self, table):
        rv = self._record()
        assert isinstance(table, (str, bigquery.Table))
        return rv

    def load_table_from_json(self, rows, table, job_config):
        rv = self._record()
        assert isinstance(table, (str, bigquery.Table))
        assert isinstance(job_config, bigquery.LoadJobConfig)
        return rv

    def insert_rows(self, table, rows):
        rv = self._record()
        assert isinstance(table, (str, bigquery.Table))
        return rv

    def get_routine(self, routine):
        rv = self._record()
        assert isinstance(routine, (str, bigquery.Routine))
        return rv

    def list_routines(self, dataset):
        rv = self._record()
        assert isinstance(dataset, str)
        return rv

    def list_tables(self, dataset):
        rv = self._record()
        assert isinstance(dataset, str)
        return rv

    def query(self, query, job_config):
        rv = self._record()
        assert isinstance(query, str)
        assert isinstance(job_config, bigquery.QueryJobConfig)
        return rv

    def delete_table(self, table, not_found_ok):
        self._record()
        assert isinstance(table, (str, bigquery.Table))
        assert isinstance(not_found_ok, bool)

    def delete_routine(self, routine, not_found_ok):
        self._record()
        assert isinstance(routine, (str, bigquery.Routine))
        assert isinstance(not_found_ok, bool)

    def update_table(self, table, fields):
        rv = self._record()
        assert isinstance(table, bigquery.Table)
        return rv


@pytest.fixture
def bq_client():
    return BigQuery(MockClient("project"), "default_dataset", True)
