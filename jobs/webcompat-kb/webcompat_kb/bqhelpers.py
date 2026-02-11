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

    def relative_string(self, dataset_id: DatasetId) -> str:
        result = []
        if dataset_id.project != self.project:
            result.append(self.project)
            result.append(self.dataset)
        elif dataset_id.dataset != self.dataset:
            result.append(self.dataset)
        result.append(self.name)
        return ".".join(result)

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
    """Scalar or array field in a table.

    :param str name: - Name of the field
    :param str type: - Datatype of the field e.g. "INTEGER"
    :param str mode: - Validation constraint; either "NULLABLE", "REQUIRED" or "REPEATED"
    :param Optional[int] max_length: - Maximum length of the field, if specified
    """

    name: str
    type: str
    mode: str = "NULLABLE"
    max_length: Optional[int] = None

    def bq(self) -> bigquery.SchemaField:
        """Convert to BigQuery representation"""
        if self.max_length is not None:
            return bigquery.SchemaField(
                self.name, self.type, mode=self.mode, max_length=self.max_length
            )
        return bigquery.SchemaField(self.name, self.type, mode=self.mode)


@dataclass
class SchemaRecordField:
    """Record field in a table.

    Unlike SchemaField this has a list of fields which make up the record."""

    name: str
    type: str
    fields: list[SchemaField | Self]
    mode: str = "NULLABLE"

    def bq(self) -> bigquery.SchemaField:
        """Convert to BigQuery representation"""
        return bigquery.SchemaField(
            self.name,
            self.type,
            mode=self.mode,
            fields=[item.bq() for item in self.fields],
        )


class Schema(ABC):
    type: SchemaType

    def __init__(
        self, id: SchemaId, canonical_id: SchemaId, description: Optional[str] = None
    ):
        """Representation of a specific schema (table/view/routine) in a dataset

        :param SchemaId id: - id of the schema in the current configuration
        :param SchemaId canonical_id: - canonical id of the schema, as used in e.g. the data files.
                                        This typically differs from `id` in staging where this will
                                        point to the prod name, but `id` will point to the staging
                                        name.
        :param Optional[str] description: - Description of the schema.
        """
        self.id = id
        self.canonical_id = canonical_id
        self.description = description or ""
        self._bq_object: Optional[bigquery.Table | bigquery.Routine] = None

    def __str__(self) -> str:
        return str(self.id)

    def __eq__(self, other: Any) -> bool:
        if type(self) is not type(other):
            return False

        return (
            self.id == other.id
            and self.canonical_id == other.canonical_id
            and self.description == other.description
        )

    @abstractmethod
    def bq(self) -> bigquery.Table | bigquery.Routine:
        """Convert to BigQuery representation"""
        ...

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


@dataclass
class RangePartition:
    field: str
    start: int
    end: int
    interval: int = 1


class TableSchema(Schema):
    type = SchemaType.table

    def __init__(
        self,
        id: SchemaId,
        canonical_id: SchemaId,
        fields: list[SchemaField | SchemaRecordField],
        etl: set[str],
        description: Optional[str] = None,
        partition: Optional[RangePartition] = None,
    ):
        super().__init__(id, canonical_id, description=description)
        self.fields = fields
        self.etl_jobs = etl
        self.partition = partition
        if partition is not None and not any(
            field.name == partition.field for field in fields
        ):
            raise ValueError(f"Partition field {partition.field} not found in table")

    def __eq__(self, other: Any) -> bool:
        if not super().__eq__(other):
            return False
        return (
            self.fields == other.fields
            and self.etl_jobs == other.etl_jobs
            and self.partition == other.partition
        )

    @property
    def schema(self) -> Sequence[bigquery.SchemaField]:
        return [item.bq() for item in self.fields]

    def bq(self) -> bigquery.Table:
        if self._bq_object is None:
            self._bq_object = bigquery.Table(str(self.id), schema=self.schema)
            if isinstance(self.partition, RangePartition):
                self._bq_object.range_partitioning = bigquery.table.RangePartitioning(
                    bigquery.table.PartitionRange(
                        self.partition.start,
                        self.partition.end,
                        self.partition.interval,
                    ),
                    field=self.partition.field,
                )
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
        """Representation of a dataset

        :param SchemaId id: - id of the dataset in the current configuration
        :param SchemaId canonical_id: - canonical id of the dataset, as used in e.g. the data files.
                                        This typically differs from `id` in staging where this will
                                        point to the prod name, but `id` will point to the staging
                                        name.
        :param Iterable[Schema] schemas: - Schema objects in the dataset.
        """
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


class BigQuery:
    def __init__(
        self,
        client: bigquery.Client,
        default_dataset_id: DatasetId,
        write: bool,
        write_targets: Optional[set[SchemaId]] = None,
    ):
        self.client = client
        assert default_dataset_id.project == client.project
        self.project_id = client.project
        self.default_dataset_id = default_dataset_id
        self.write = write
        self.write_targets = write_targets

    def get_dataset_id(
        self, dataset_id: Optional[str | Dataset | DatasetId]
    ) -> DatasetId:
        if dataset_id is None:
            return self.default_dataset_id
        if isinstance(dataset_id, DatasetId):
            return dataset_id
        if isinstance(dataset_id, Dataset):
            return dataset_id.id
        return DatasetId.from_str(dataset_id, self.project_id)

    def check_write_target(self, schema_id: SchemaId) -> None:
        if self.write_targets is not None and schema_id not in self.write_targets:
            raise ValueError(f"Trying to write to {schema_id} not permitted")

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
            return SchemaId(table.project, table.dataset_id, table.table_id)

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

    def ensure_dataset(
        self, dataset_id: str | DatasetId, description: Optional[str]
    ) -> None:
        dataset = bigquery.Dataset(str(dataset_id))
        if description is not None:
            dataset.description = description
        if self.write:
            self.client.create_dataset(dataset, exists_ok=True)

    def ensure_table(
        self,
        table_id: str | TableSchema | SchemaId,
        schema: Iterable[bigquery.SchemaField],
        dataset_id: Optional[str] = None,
        partition: Optional[RangePartition] = None,
        update_fields: Optional[bool] = False,
    ) -> bigquery.Table:
        table_id = self.get_table_id(dataset_id, table_id)

        self.check_write_target(table_id)

        table = bigquery.Table(str(table_id), schema=schema)
        if partition:
            table.range_partitioning = bigquery.table.RangePartitioning(
                bigquery.table.PartitionRange(
                    partition.start, partition.end, partition.interval
                ),
                field=partition.field,
            )

        if self.write:
            table = self.client.create_table(table, exists_ok=True)
        else:
            table = self.get_table(table_id)
        if update_fields:
            table = self.add_table_fields(table_id, schema)
        return table

    def _get_new_fields(
        self,
        table_id: SchemaId,
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
        table: str | bigquery.Table | SchemaId | TableSchema,
        schema: Iterable[bigquery.SchemaField],
        dataset_id: Optional[str] = None,
    ) -> bigquery.Table:
        table_id = self.get_table_id(dataset_id, table)

        self.check_write_target(table_id)

        try:
            table = self.get_table(table)
        except Exception:
            if self.write:
                raise
            # If we're not writing the table might not exist
            # but we can pretend that it does
            if not isinstance(table, bigquery.Table):
                table = bigquery.Table(str(table))

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
        self,
        table_id: str | bigquery.Table | SchemaId | TableSchema,
        dataset_id: Optional[str | DatasetId | Dataset] = None,
    ) -> bigquery.Table:
        table = self.get_table_id(dataset_id, table_id)
        return self.client.get_table(str(table))

    def write_table(
        self,
        table: bigquery.Table | str | SchemaId | TableSchema,
        schema: Sequence[bigquery.SchemaField],
        rows: Sequence[Mapping[str, Json]],
        overwrite: bool,
        dataset_id: Optional[str] = None,
    ) -> None:
        table_id = self.get_table_id(dataset_id, table)

        self.check_write_target(table_id)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=schema,
            write_disposition="WRITE_APPEND" if not overwrite else "WRITE_TRUNCATE",
        )

        if self.write:
            job = self.client.load_table_from_json(
                cast(Iterable[dict[str, Any]], rows),
                str(table_id),
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
        table_id = self.get_table_id(dataset_id, table)

        self.check_write_target(table_id)

        table = self.get_table(table_id, dataset_id)

        if isinstance(table, TableSchema):
            table = table.bq()

        if self.write:
            errors = self.client.insert_rows(table, rows)
            if errors:
                logging.error(errors)
        else:
            logging.info(f"Skipping writes, would have written {len(rows)} to {table}")
            for row in rows:
                logging.debug(f"  {row}")

    def insert_query(
        self,
        table: str | bigquery.Table | SchemaId | TableSchema,
        columns: Iterable[str],
        query: str,
        dataset_id: Optional[str] = None,
        parameters: Optional[Sequence[bigquery.query._AbstractQueryParameter]] = None,
    ) -> None:
        table_id = self.get_table_id(dataset_id, table)
        insert_str = f"INSERT `{table_id}` ({', '.join(columns)})"

        self.check_write_target(table_id)

        insert_query = f"""{insert_str}
({query})"""

        if self.write:
            self.query(insert_query, parameters=parameters)
        else:
            logging.info(
                f"Skipping writes, would have run insert with query:\n{insert_query}"
            )
            self.query(query, parameters=parameters)

    def update_query(
        self,
        table: str | bigquery.Table | SchemaId | TableSchema,
        set_columns: Iterable[str],
        from_query: str | SchemaId,
        condition: str,
        dataset_id: Optional[str] = None,
        parameters: Optional[Sequence[bigquery.query._AbstractQueryParameter]] = None,
    ) -> None:
        """Run a UPDATE ... SET ... FROM ... WHERE ... query

        This does not currently support all forms of UPDATE query, only those with a source table,
        where the update maps columns from the source table to the corresponding columns in the
        target table.

        :param table: The table to update. In other clauses this is referred to as "target".
        :param set_columns: A list of column names to set in the target table from the source table.
        :param from_query: A query string representing the source data
        :param condition: A WHERE condition that matches rows in the source table to rows in the
                          target table e.g. "target.col = source.col"
        :param dataset_id: The dataset id to target
        :param parameters: Parameters to pass to the query"""
        table_id = self.get_table_id(dataset_id, table)
        self.check_write_target(table_id)

        set_clause = ",\n  ".join(
            f"target.{column}=source.{column}" for column in set_columns
        )
        update_query = f"""UPDATE `{table_id}` AS target
SET {set_clause}
FROM ({from_query}) AS source
WHERE {condition}
"""
        if self.write:
            result = self.query(update_query)
            if result.num_dml_affected_rows:
                logging.info(
                    f"Updated {result.num_dml_affected_rows} rows in {table_id}"
                )
        else:
            logging.info(f"Would run query:\n{update_query}")
            result = self.query(str(from_query))
            logging.info(f"Would set {set_clause} with:\n{list(result)}")

    def delete_query(
        self,
        table: str | bigquery.Table | SchemaId | TableSchema,
        condition: str,
        dataset_id: Optional[str] = None,
        parameters: Optional[Sequence[bigquery.query._AbstractQueryParameter]] = None,
    ) -> None:
        table_id = self.get_table_id(dataset_id, table)

        self.check_write_target(table_id)

        if self.write:
            query = f"DELETE FROM `{table_id}` WHERE {condition}"
            result = self.query(query, parameters=parameters)
            if result.num_dml_affected_rows:
                logging.info(
                    f"Deleted {result.num_dml_affected_rows} rows from {table_id}"
                )
        else:
            query = f"SELECT COUNT(*) as row_count FROM `{table_id}` WHERE {condition}"
            result = self.query(query, parameters=parameters)
            count = next(result).row_count
            if count:
                logging.info(f"Would delete {count} rows from {table_id}")

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
        table_id = self.get_table_id(dataset_id, view_id)

        self.check_write_target(table_id)

        view = bigquery.Table(str(table_id))
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
            default_dataset=str(self.get_dataset_id(dataset_id)),
        )
        if parameters is not None:
            job_config.query_parameters = parameters

        logging.debug(query)
        return self.client.query(query, job_config=job_config).result()

    def validate_query(
        self, query: str, dataset_id: Optional[str | DatasetId | Dataset] = None
    ) -> bigquery.table.RowIterator:
        """Dry-run a query to check that it is valid"""

        job_config = bigquery.QueryJobConfig(
            default_dataset=str(self.get_dataset_id(dataset_id)), dry_run=True
        )

        logging.debug(query)
        return self.client.query(query, job_config=job_config).result()

    def delete_table(
        self,
        table: bigquery.Table | str | SchemaId | TableSchema,
        not_found_ok: bool = False,
    ) -> None:
        table_id = self.get_table_id(self.get_dataset_id(None), table)

        self.check_write_target(table_id)

        if self.write:
            logging.info(f"Deleting table {table_id} (if it exists)")
            self.client.delete_table(str(table_id), not_found_ok=not_found_ok)
        else:
            logging.info(f"Skipping writes, would delete table {table_id}")

    def delete_routine(
        self,
        routine: bigquery.Routine | str | SchemaId | RoutineSchema,
        not_found_ok: bool = False,
    ) -> None:
        routine = self.get_routine_id(routine)

        self.check_write_target(routine)

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
            str(self.client.get_table_id(self.dataset_id, self.name)),
            schema=self.schema,
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
