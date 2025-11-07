import logging
import uuid
from dataclasses import dataclass
from datetime import datetime
from types import TracebackType
from typing import Any, Iterable, Iterator, Mapping, Optional, Self, Sequence, cast

import google.auth
from google.cloud import bigquery

from .httphelpers import Json


def get_client(bq_project_id: str) -> bigquery.Client:
    credentials, _ = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/bigquery",
        ]
    )

    return bigquery.Client(credentials=credentials, project=bq_project_id)


@dataclass
class RangePartition:
    field: str
    start: int
    end: int
    interval: int = 1


class BigQuery:
    def __init__(self, client: bigquery.Client, default_dataset_id: str, write: bool):
        self.client = client
        self.project_id = client.project
        self.default_dataset_id = default_dataset_id
        self.write = write

    def get_dataset(self, dataset_id: Optional[str]) -> str:
        if dataset_id is None:
            return self.default_dataset_id
        return dataset_id

    def get_table_id(
        self, dataset_id: Optional[str], table: bigquery.Table | str
    ) -> str:
        if isinstance(table, bigquery.Table):
            return table.full_table_id.replace(":", ".")

        if "." in table:
            return table

        dataset_id = self.get_dataset(dataset_id)
        return f"{self.client.project}.{dataset_id}.{table}"

    def ensure_table(
        self,
        table_id: str,
        schema: Iterable[bigquery.SchemaField],
        dataset_id: Optional[str] = None,
        partition: Optional[RangePartition] = None,
        update_fields: Optional[bool] = False,
    ) -> bigquery.Table:
        table_id = str(self.get_table_id(dataset_id, table_id))
        table = bigquery.Table(table_id, schema=schema)
        if partition:
            table.range_partitioning = bigquery.table.RangePartitioning(
                bigquery.table.PartitionRange(
                    partition.start, partition.end, partition.interval
                ),
                field=partition.field,
            )

        if self.write:
            table = self.client.create_table(table_id, exists_ok=True)
        else:
            table = self.get_table(table_id)
        if update_fields:
            table = self.add_table_fields(table_id, schema)
        return table

    def _get_new_fields(
        self,
        table_id: str,
        current_schema: Iterable[bigquery.SchemaField],
        new_schema: Iterable[bigquery.SchemaField],
    ) -> Sequence[bigquery.SchemaField]:
        new_fields = []
        new_by_name = {item.name: item for item in new_schema}
        current_by_name = {item.name: item for item in current_schema}

        if len(current_by_name) > len(new_by_name):
            raise ValueError(
                f"Requested schema for {table_id} is shorter than new schema, deleting columns isn't supported"
            )

        if any(item not in new_by_name for item in current_by_name):
            raise ValueError(
                f"Requested schema for {table_id} isn't an extension of existing schema: can't delete fields"
            )

        for new_field in new_by_name.values():
            if new_field.name not in current_by_name:
                new_fields.append(new_field)
            else:
                current_field = current_by_name[new_field.name]
                if (
                    new_field.name != current_field.name
                    or new_field.field_type != current_field.field_type
                    or new_field.max_length != current_field.max_length
                ):
                    raise ValueError(
                        f"Requested schema for {table_id} isn't an extension of existing schema"
                    )

        if not new_fields:
            logging.debug(f"Updating {table_id}: nothing to do")
            return []

        for new_field in new_fields:
            if new_field.mode == "REQUIRED":
                raise ValueError("Adding required fields isn't supported")

        for new_field in new_fields:
            logging.info(
                f"Updating {table_id}: Adding field {new_field.name} with type {new_field.field_type}"
            )
        return new_fields

    def add_table_fields(
        self,
        table: str | bigquery.Table,
        schema: Iterable[bigquery.SchemaField],
        dataset_id: Optional[str] = None,
    ) -> bigquery.Table:
        try:
            table = self.get_table(table)
        except Exception:
            if self.write:
                raise
            # If we're not writing the table might not exist
            # but we can pretend that it does
            if not isinstance(table, bigquery.Table):
                table = bigquery.Table(str(table))

        table_id = self.get_table_id(dataset_id, table)
        current_schema = table.schema

        new_fields = self._get_new_fields(table_id, current_schema, schema)
        if not new_fields:
            return table

        new_schema = table.schema + new_fields
        table.schema = new_schema
        if self.write:
            table = self.client.update_table(table, ["schema"])
        else:
            logging.info(
                f"Skipping writes, would have added {len(new_fields)} new fields"
            )

        if len(table.schema) != len(new_schema):
            raise Exception("Table update failed")
        return table

    def get_table(
        self, table_id: str | bigquery.Table, dataset_id: Optional[str] = None
    ) -> bigquery.Table:
        table = self.get_table_id(dataset_id, table_id)
        return self.client.get_table(table)

    def write_table(
        self,
        table: bigquery.Table | str,
        schema: list[bigquery.SchemaField],
        rows: Sequence[Mapping[str, Json]],
        overwrite: bool,
        dataset_id: Optional[str] = None,
    ) -> None:
        table = self.get_table_id(dataset_id, table)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=schema,
            write_disposition="WRITE_APPEND" if not overwrite else "WRITE_TRUNCATE",
        )

        if self.write:
            job = self.client.load_table_from_json(
                cast(Iterable[dict[str, Any]], rows),
                table,
                job_config=job_config,
            )
            job.result()
            logging.info(f"Wrote {len(rows)} records into {table}")
        else:
            logging.info(f"Skipping writes, would have written {len(rows)} to {table}")
            for row in rows:
                logging.debug(f"  {row}")

    def insert_rows(
        self,
        table: str | bigquery.Table,
        rows: Sequence[Mapping[str, Any]],
        dataset_id: Optional[str] = None,
    ) -> None:
        table = self.get_table_id(dataset_id, table)

        if self.write:
            errors = self.client.insert_rows(table, rows)
            if errors:
                logging.error(errors)
        else:
            logging.info(f"Skipping writes, would have written {len(rows)} to {table}")
            for row in rows:
                logging.debug(f"  {row}")

    def get_routine(self, routine_id: str) -> bigquery.Routine:
        return self.client.get_routine(routine_id)

    def get_routines(self, dataset_id: str) -> Iterator[bigquery.Routine]:
        for item in self.client.list_routines(dataset_id):
            yield item

    def get_views(self, dataset_id: str) -> Iterator[bigquery.Table]:
        for table_item in self.client.list_tables(dataset_id):
            if table_item.table_type == "VIEW":
                yield self.get_table(table_item.table_id, dataset_id)

    def create_view(
        self,
        view_id: str,
        view_query: str,
        dataset_id: Optional[str] = None,
        description: Optional[str] = None,
    ) -> bigquery.Table:
        view = bigquery.Table(self.get_table_id(dataset_id, view_id))
        view.description = description
        view.view_query = view_query
        if self.write:
            try:
                self.delete_table(view, not_found_ok=True)
                logging.info(f"Creating view {view}")
                self.client.create_table(view)
            except Exception as e:
                logging.warning(
                    f"Failed to create view {view_id}\n{e}\n{view.view_query}"
                )
        else:
            logging.info(
                f"Skiping writes, would create view {view.dataset_id}.{view.table_id}"
            )
            logging.debug(f"Query:\n{view_query}")

        return view

    def query(
        self,
        query: str,
        dataset_id: Optional[str] = None,
        parameters: Optional[Sequence[bigquery.query._AbstractQueryParameter]] = None,
    ) -> bigquery.table.RowIterator:
        """Run a query

        Note that this can't prevent writes in the case that the SQL does writes"""

        job_config = bigquery.QueryJobConfig(
            default_dataset=f"{self.client.project}.{self.get_dataset(dataset_id)}"
        )
        if parameters is not None:
            job_config.query_parameters = parameters

        logging.debug(query)
        return self.client.query(query, job_config=job_config).result()

    def delete_table(
        self, table: bigquery.Table | str, not_found_ok: bool = False
    ) -> None:
        if self.write:
            logging.info(f"Deleting table {table} (if it exists)")
            self.client.delete_table(table, not_found_ok=not_found_ok)
        else:
            logging.info(f"Skipping writes, would delete table {table}")

    def delete_routine(
        self, routine: bigquery.Routine | str, not_found_ok: bool = False
    ) -> None:
        if self.write:
            logging.info(f"Deleting routine {routine} (if it exists)")
            self.client.delete_routine(routine, not_found_ok=not_found_ok)
        else:
            logging.info(f"Skipping writes, would delete table {routine}")

    def temporary_table(
        self,
        schema: Iterable[bigquery.SchemaField],
        rows: Optional[Sequence[Mapping[str, Any]]] = None,
        dataset_id: Optional[str] = None,
    ) -> "TemporaryTable":
        return TemporaryTable(self, schema, rows, dataset_id)

    def current_datetime(self) -> datetime:
        results = list(self.query("""SELECT current_datetime() as current_datetime"""))
        assert len(results) == 1
        return results[0].current_datetime


class TemporaryTable:
    def __init__(
        self,
        client: BigQuery,
        schema: Iterable[bigquery.SchemaField],
        rows: Optional[Sequence[Mapping[str, Any]]] = None,
        dataset_id: Optional[str] = None,
    ):
        self.client = client
        self.schema = schema
        self.rows = rows
        self.dataset_id = dataset_id
        self.name = f"tmp_{uuid.uuid4()}"
        self.table: Optional[bigquery.Table] = None

    def __enter__(self) -> Self:
        self.table = bigquery.Table(
            self.client.get_table_id(self.dataset_id, self.name), schema=self.schema
        )
        self.client.client.create_table(self.table)
        if self.rows is not None:
            self.client.client.load_table_from_json(
                cast(Iterable[dict[str, Any]], self.rows),
                self.table,
            ).result()
            logging.info(f"Wrote {len(self.rows)} records into {self.name}")
        return self

    def __exit__(
        self,
        type_: Optional[type[BaseException]],
        value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        assert self.table is not None
        logging.info(f"Removing temporary table {self.name}")
        self.client.client.delete_table(self.table)
        self.table = None

    def query(
        self,
        query: str,
        parameters: Optional[Sequence[bigquery.query._AbstractQueryParameter]] = None,
    ) -> bigquery.table.RowIterator:
        job_config = bigquery.QueryJobConfig(
            default_dataset=f"{self.client.client.project}.{self.client.get_dataset(self.dataset_id)}"
        )
        if parameters is not None:
            job_config.query_parameters = parameters

        logging.debug(query)
        return self.client.client.query(query, job_config=job_config).result()
