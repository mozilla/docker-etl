import enum
import logging
import uuid
from abc import ABC, abstractmethod
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


@dataclass(frozen=True)
class DatasetId:
    """Id of a bigquery dataset"""

    project: str
    dataset: str

    def __str__(self) -> str:
        assert self.project != ""
        assert self.dataset != ""
        return f"{self.project}.{self.dataset}"

    @classmethod
    def from_str(
        cls,
        ref: str,
        default_project: str,
    ) -> Self:
        """Create a dataset id from a string.

        This is either of the form project:dataset or [project.]dataset
        where parts in square brackets are optional. If the project is omitted
        the default project is used"""
        if ":" in ref:
            # This is the BigQuery API format
            project, dataset = ref.split(":", 1)
            return cls(project, dataset)

        parts = ref.split(".")
        num_parts = len(parts)
        if num_parts == 1:
            project = default_project
            dataset = ref
        elif num_parts == 2:
            project, dataset = parts
        else:
            raise ValueError(f"Invalid id {ref}")
        return cls(project, dataset)


@dataclass(frozen=True)
class SchemaId:
    """Id of a BigQuery schema i.e. a table, view or routine."""

    project: str
    dataset: str
    name: str

    def __str__(self) -> str:
        assert self.project != ""
        assert self.dataset != ""
        assert self.name != ""
        return f"{self.project}.{self.dataset}.{self.name}"

    @property
    def dataset_id(self) -> DatasetId:
        return DatasetId(self.project, self.dataset)

    @classmethod
    def from_str(
        cls,
        ref: str,
        default_project: str,
        default_dataset: str,
    ) -> Self:
        """Create a schema id from a string.

        This is either of the form project:dataset.name or [project.][dataset.]name
        where parts in square brackets are optional. If optional parts are omitted, they
        are assinged to default_project or default_dataset."""
        if ":" in ref:
            # This is the BigQuery API format
            project, rest = ref.split(":", 1)
            parts = rest.split(".")
            if len(parts) != 2:
                raise ValueError(f"Invalid id {ref}")
            return cls(project, parts[0], parts[1])

        parts = ref.split(".")
        num_parts = len(parts)
        if num_parts == 1:
            project = default_project
            dataset = default_dataset
            name = ref
        elif num_parts == 2:
            project = default_project
            dataset, name = parts
        elif num_parts == 3:
            project, dataset, name = parts
        else:
            raise ValueError(f"Invalid id {ref}")
        return cls(project, dataset, name)


class SchemaType(enum.StrEnum):
    table = "table"
    view = "view"
    routine = "routine"


@dataclass
class SchemaField:
    name: str
    type: str
    mode: str = "NULLABLE"


class Schema(ABC):
    type: SchemaType

    def __init__(
        self, id: SchemaId, canonical_id: SchemaId, description: Optional[str] = None
    ):
        self.id = id
        self.canonical_id = canonical_id
        self.description = description or ""
        self._bq_object: Optional[bigquery.Table | bigquery.Routine] = None

    def __str__(self) -> str:
        return str(self.id)

    @abstractmethod
    def bq(self) -> bigquery.Table | bigquery.Routine: ...

    def table(self) -> "TableSchema":
        """Return this as a TableSchema or error

        This is helpful as an assert for typechecking."""
        if not isinstance(self, TableSchema):
            raise ValueError(f"Schema {self.id} is not a TableSchema")
        return self

    def view(self) -> "ViewSchema":
        """Return this as a ViewSchema or error

        This is helpful as an assert for typechecking."""
        if not isinstance(self, ViewSchema):
            raise ValueError(f"Schema {self.id} is not a ViewSchema")
        return self

    def routine(self) -> "RoutineSchema":
        """Return this as a RoutineSchema or error

        This is helpful as an assert for typechecking."""
        if not isinstance(self, RoutineSchema):
            raise ValueError(f"Schema {self.id} is not a RoutineSchema")
        return self


class TableSchema(Schema):
    type = SchemaType.table

    def __init__(
        self,
        id: SchemaId,
        canonical_id: SchemaId,
        fields: list[SchemaField],
        etl: set[str],
        description: Optional[str] = None,
    ):
        super().__init__(id, canonical_id, description=description)
        self.fields = fields
        self.etl_jobs = etl

    @property
    def schema(self) -> Sequence[bigquery.SchemaField]:
        return [
            bigquery.SchemaField(item.name, item.type, mode=item.mode)
            for item in self.fields
        ]

    def bq(self) -> bigquery.Table:
        if self._bq_object is None:
            self._bq_object = bigquery.Table(str(self.id))
        assert isinstance(self._bq_object, bigquery.Table)
        return self._bq_object


class ViewSchema(Schema):
    type = SchemaType.view

    def bq(self) -> bigquery.Table:
        if self._bq_object is None:
            self._bq_object = bigquery.Table(str(self.id))
        assert isinstance(self._bq_object, bigquery.Table)
        return self._bq_object


class RoutineSchema(Schema):
    type = SchemaType.routine

    def bq(self) -> bigquery.Routine:
        if self._bq_object is None:
            self._bq_object = bigquery.Routine(str(self.id))
        assert isinstance(self._bq_object, bigquery.Routine)
        return self._bq_object


class Dataset:
    def __init__(
        self, id: DatasetId, canonical_id: DatasetId, schemas: Iterable[Schema]
    ):
        self.id = id
        self.canonical_id = canonical_id
        self.schemas = {schema.canonical_id.name: schema for schema in schemas}

    def __getitem__(self, name: str) -> Schema:
        try:
            return self.schemas[name]
        except Exception as e:
            raise KeyError(f"No such schema {name}") from e

    def __iter__(self) -> Iterator[Schema]:
        for schema in self.schemas.values():
            yield schema

    def tables(self) -> Iterator[TableSchema]:
        for item in self:
            if isinstance(item, TableSchema):
                yield item

    def views(self) -> Iterator[ViewSchema]:
        for item in self:
            if isinstance(item, ViewSchema):
                yield item

    def routines(self) -> Iterator[RoutineSchema]:
        for item in self:
            if isinstance(item, RoutineSchema):
                yield item


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

    def get_dataset_id(
        self, dataset_id: Optional[str | Dataset | DatasetId]
    ) -> DatasetId:
        if dataset_id is None:
            return DatasetId.from_str(self.default_dataset_id, self.project_id)
        if isinstance(dataset_id, DatasetId):
            return dataset_id
        if isinstance(dataset_id, Dataset):
            return dataset_id.id
        return DatasetId.from_str(dataset_id, self.project_id)

    def get_table_id(
        self,
        dataset_id: Optional[str | DatasetId | Dataset],
        table: bigquery.Table | str | TableSchema | ViewSchema | SchemaId,
    ) -> SchemaId:
        if isinstance(table, SchemaId):
            return table

        if isinstance(table, (TableSchema, ViewSchema)):
            return table.id

        default_dataset = self.get_dataset_id(dataset_id)

        if isinstance(table, bigquery.Table):
            return SchemaId.from_str(
                table.full_table_id, default_dataset.project, default_dataset.dataset
            )

        return SchemaId.from_str(
            table, default_dataset.project, default_dataset.dataset
        )

    def get_routine_id(
        self, routine: bigquery.Routine | str | SchemaId | RoutineSchema
    ) -> SchemaId:
        if isinstance(routine, SchemaId):
            return routine
        if isinstance(routine, RoutineSchema):
            return routine.id
        if isinstance(routine, bigquery.Routine):
            return SchemaId(routine.routine_id, routine.project, routine.dataset_id)
        default_dataset = self.get_dataset_id(None)
        return SchemaId.from_str(
            routine, default_dataset.project, default_dataset.dataset
        )

    def ensure_table(
        self,
        table_id: str | TableSchema | SchemaId,
        schema: Iterable[bigquery.SchemaField],
        dataset_id: Optional[str] = None,
        partition: Optional[RangePartition] = None,
    ) -> bigquery.Table:
        table = bigquery.Table(self.get_table_id(dataset_id, table_id), schema=schema)
        if partition:
            table.range_partitioning = bigquery.table.RangePartitioning(
                bigquery.table.PartitionRange(
                    partition.start, partition.end, partition.interval
                ),
                field=partition.field,
            )

        if self.write:
            table = self.client.create_table(str(table), exists_ok=True)
        else:
            table = self.get_table(table)
        return table

    def get_table(
        self,
        table_id: str | bigquery.Table | SchemaId | TableSchema,
        dataset_id: Optional[str | DatasetId | Dataset] = None,
    ) -> bigquery.Table:
        table = self.get_table_id(dataset_id, table_id)
        return self.client.get_table(str(table))

    def write_table(
        self,
        table: bigquery.Table | str | SchemaId | TableSchema,
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
                str(table),
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
        table: str | bigquery.Table | SchemaId | TableSchema,
        rows: Sequence[Mapping[str, Any]],
        dataset_id: Optional[str] = None,
    ) -> None:
        table = self.get_table_id(dataset_id, table)

        if self.write:
            errors = self.client.insert_rows(str(table), rows)
            if errors:
                logging.error(errors)
        else:
            logging.info(f"Skipping writes, would have written {len(rows)} to {table}")
            for row in rows:
                logging.debug(f"  {row}")

    def get_routine(
        self, routine_id: str | SchemaId | RoutineSchema
    ) -> bigquery.Routine:
        if isinstance(routine_id, SchemaId):
            routine_id = str(routine_id)
        if isinstance(routine_id, RoutineSchema):
            routine_id = str(routine_id.id)
        return self.client.get_routine(routine_id)

    def get_routines(
        self, dataset_id: str | DatasetId | Dataset
    ) -> Iterator[bigquery.Routine]:
        dataset_id = self.get_dataset_id(dataset_id)
        for item in self.client.list_routines(str(dataset_id)):
            yield item

    def get_tables(
        self, dataset_id: str | DatasetId | Dataset
    ) -> Iterator[bigquery.Table]:
        dataset_id = self.get_dataset_id(dataset_id)
        for table_item in self.client.list_tables(str(dataset_id)):
            yield self.get_table(table_item.table_id, dataset_id)

    def get_views(
        self, dataset_id: str | DatasetId | Dataset
    ) -> Iterator[bigquery.Table]:
        dataset_id = self.get_dataset_id(dataset_id)
        for table_item in self.client.list_tables(str(dataset_id)):
            if table_item.table_type == "VIEW":
                yield self.get_table(table_item.table_id, dataset_id)

    def create_view(
        self,
        view_id: str | SchemaId | ViewSchema,
        view_query: str,
        dataset_id: Optional[str] = None,
        description: Optional[str] = None,
    ) -> bigquery.Table:
        view = bigquery.Table(str(self.get_table_id(dataset_id, view_id)))
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
        dataset_id: Optional[str | DatasetId | Dataset] = None,
        parameters: Optional[Sequence[bigquery.query._AbstractQueryParameter]] = None,
    ) -> bigquery.table.RowIterator:
        """Run a query

        Note that this can't prevent writes in the case that the SQL does writes"""

        job_config = bigquery.QueryJobConfig(
            default_dataset=str(self.get_dataset_id(dataset_id))
        )
        if parameters is not None:
            job_config.query_parameters = parameters

        logging.debug(query)
        return self.client.query(query, job_config=job_config).result()

    def delete_table(
        self,
        table: bigquery.Table | str | SchemaId | TableSchema,
        not_found_ok: bool = False,
    ) -> None:
        table = self.get_table_id(self.get_dataset_id(None), table)
        if self.write:
            logging.info(f"Deleting table {table} (if it exists)")
            self.client.delete_table(str(table), not_found_ok=not_found_ok)
        else:
            logging.info(f"Skipping writes, would delete table {table}")

    def delete_routine(
        self,
        routine: bigquery.Routine | str | SchemaId | RoutineSchema,
        not_found_ok: bool = False,
    ) -> None:
        routine = self.get_routine_id(routine)
        if self.write:
            logging.info(f"Deleting routine {routine} (if it exists)")
            self.client.delete_routine(str(routine), not_found_ok=not_found_ok)
        else:
            logging.info(f"Skipping writes, would delete table {routine}")

    def temporary_table(
        self,
        schema: Iterable[bigquery.SchemaField],
        rows: Optional[Sequence[Mapping[str, Any]]] = None,
        dataset_id: Optional[str | DatasetId | Dataset] = None,
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
        dataset_id: Optional[str | Dataset | DatasetId] = None,
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
            default_dataset=str(self.client.get_dataset_id(self.dataset_id))
        )
        if parameters is not None:
            job_config.query_parameters = parameters

        logging.debug(query)
        return self.client.client.query(query, job_config=job_config).result()
