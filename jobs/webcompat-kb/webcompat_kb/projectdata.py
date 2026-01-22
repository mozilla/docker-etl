import shutil
import enum
import logging
import os
import pathlib
import tomllib
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    ClassVar,
    Generic,
    Iterable,
    Iterator,
    Literal,
    Mapping,
    Optional,
    Self,
    Sequence,
    TypeVar,
)

import jinja2
import tomli_w
from google.cloud import bigquery
from pydantic import BaseModel, ConfigDict, RootModel, ValidationError

from .config import Config
from .bqhelpers import (
    Dataset,
    DatasetId,
    RoutineSchema,
    Schema,
    SchemaField,
    SchemaId,
    SchemaRecordField,
    SchemaType,
    TableSchema,
    ViewSchema,
)
from .metrics import metrics, ranks


class ReferenceType(enum.StrEnum):
    view = "view"
    routine = "routine"
    table = "table"
    external = "external"


class DatasetMetadata(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    description: Optional[str] = None


class SchemaMetadata(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    description: Optional[str] = None


class SchemaFieldDefinition(BaseModel):
    model_config = ConfigDict(frozen=True)
    type: str
    mode: Optional[Literal["NULLABLE"] | Literal["REQUIRED"] | Literal["REPEATED"]] = (
        None
    )
    max_length: Optional[int] = None

    def to_schema(self, name: str) -> SchemaField:
        if self.type == "RECORD":
            raise ValueError(f"Field {name} of type RECORD has no defined fields")
        return SchemaField(
            name,
            type=self.type,
            mode=self.mode or "NULLABLE",
            max_length=self.max_length,
        )


class SchemaRecordFieldDefinition(BaseModel):
    model_config = ConfigDict(frozen=True)
    type: Literal["RECORD"]
    mode: Optional[Literal["NULLABLE"] | Literal["REQUIRED"] | Literal["REPEATED"]] = (
        None
    )
    fields: Mapping[str, SchemaFieldDefinition | Self]

    def to_schema(self, name: str) -> SchemaRecordField:
        return SchemaRecordField(
            name,
            type=self.type,
            mode=self.mode or "NULLABLE",
            fields=[
                dfn.to_schema(field_name) for field_name, dfn in self.fields.items()
            ],
        )


class RangePartition(BaseModel):
    type: Literal["range"]
    field: str
    start: int
    end: int
    interval: Optional[int] = 1


class TableMetadata(SchemaMetadata):
    model_config = ConfigDict(frozen=True)

    name: str
    description: Optional[str] = None
    etl: Optional[list[str]] = None
    partition: Optional[RangePartition] = None


class TableSchemaDefinition(RootModel):
    root: Mapping[str, SchemaRecordFieldDefinition | SchemaFieldDefinition]


TemplateCls = TypeVar("TemplateCls", bound=BaseModel)


class SchemaTemplate(ABC, Generic[TemplateCls]):
    filename: ClassVar[str]

    def __init__(self, path: os.PathLike, metadata: TemplateCls, template: str):
        """Base class for template files representing some part of the
        schema e.g. a table, a view, or a routine"""
        self.path = path
        self.metadata = metadata
        self.template = template

    @classmethod
    @abstractmethod
    def load_from_dir(cls, path: os.PathLike) -> Optional[Self]: ...

    @classmethod
    def _load_from_dir(
        cls, path: os.PathLike
    ) -> Optional[tuple[Mapping[str, Any], str]]:
        path = os.path.abspath(path)
        if not os.path.isdir(path):
            raise ValueError(f"Expected a directory, got {path}")

        meta_path = os.path.join(path, "meta.toml")
        try:
            with open(meta_path, "rb") as f:
                metadata = tomllib.load(f)
        except OSError:
            logging.warning(f"Failed to find {meta_path}")
            return None

        template_path = os.path.join(path, cls.filename)
        try:
            with open(template_path) as f:
                template = f.read()
        except OSError:
            logging.warning(f"Failed to find {template_path}")
            return None

        return metadata, template


class TableTemplate(SchemaTemplate):
    filename = "table.toml"

    @classmethod
    def load_from_dir(cls, path: os.PathLike) -> Optional[Self]:
        data = cls._load_from_dir(path)
        if data is None:
            return None
        metadata, template = data
        try:
            table_metadata = TableMetadata.model_validate(metadata)
        except ValidationError:
            logging.error(f"Failed to parse metadata {os.path.join(path, 'meta.toml')}")
            raise
        return cls(
            path=path,
            metadata=table_metadata,
            template=template,
        )


class ViewTemplate(SchemaTemplate):
    filename = "view.sql"

    @classmethod
    def load_from_dir(cls, path: os.PathLike) -> Optional[Self]:
        data = cls._load_from_dir(path)
        if data is None:
            return None
        metadata, template = data
        try:
            schema_metadata = SchemaMetadata.model_validate(metadata)
        except ValidationError:
            logging.error(f"Failed to parse metadata {os.path.join(path, 'meta.toml')}")
            raise
        return cls(
            path=path,
            metadata=schema_metadata,
            template=template,
        )


class RoutineTemplate(SchemaTemplate):
    filename = "routine.sql"

    @classmethod
    def load_from_dir(cls, path: os.PathLike) -> Optional[Self]:
        data = cls._load_from_dir(path)
        if data is None:
            return None
        metadata, template = data
        return cls(
            path=path,
            metadata=SchemaMetadata.model_validate(metadata),
            template=template,
        )


class DatasetTemplates:
    def __init__(
        self,
        id: DatasetId,
        description: Optional[str],
        tables: Optional[Iterable[TableTemplate]] = None,
        views: Optional[Iterable[ViewTemplate]] = None,
        routines: Optional[Iterable[RoutineTemplate]] = None,
    ):
        """Metadata and templates for a specific dataset"""
        self.id = id
        self.description = description
        self.tables: list[TableTemplate] = list(tables) if tables is not None else []
        self.views: list[ViewTemplate] = list(views) if views is not None else []
        self.routines: list[RoutineTemplate] = (
            list(routines) if routines is not None else []
        )

    def append(self, template: SchemaTemplate) -> None:
        if isinstance(template, TableTemplate):
            self.tables.append(template)
        elif isinstance(template, ViewTemplate):
            self.views.append(template)
        elif isinstance(template, RoutineTemplate):
            self.routines.append(template)
        else:
            raise ValueError(f"Don't know how to append {template}")

    def remove(self, template: SchemaTemplate) -> None:
        if isinstance(template, TableTemplate):
            self.tables.remove(template)
        elif isinstance(template, ViewTemplate):
            self.views.remove(template)
        elif isinstance(template, RoutineTemplate):
            self.routines.remove(template)
        else:
            raise ValueError(f"Don't know how to remove {template}")


class TemplatesByDataset(dict[DatasetId, DatasetTemplates]):
    def get_schema_template(
        self, schema_type: SchemaType, schema_id: SchemaId
    ) -> TableTemplate | ViewTemplate | RoutineTemplate:
        dataset_id = schema_id.dataset_id
        templates = self[dataset_id]
        target: list[TableTemplate] | list[ViewTemplate] | list[RoutineTemplate]
        if schema_type == SchemaType.table:
            target = templates.tables
        elif schema_type == SchemaType.view:
            target = templates.views
        elif schema_type == SchemaType.routine:
            target = templates.routines

        current_templates = [
            item for item in target if item.metadata.name == schema_id.name
        ]
        if len(current_templates) != 1:
            raise KeyError(f"Failed to find template for {schema_id}")
        return current_templates[0]


@dataclass
class ProjectData:
    """Container for on-disk data used for a project including schema templates"""

    path: os.PathLike
    templates_by_dataset: TemplatesByDataset
    metric_dfns: Sequence[metrics.Metric]
    metric_types: Sequence[metrics.MetricType]
    rank_dfns: Sequence[ranks.RankColumn]

    def get_schema_path(
        self, schema_type: SchemaType, schema_id: SchemaId
    ) -> pathlib.Path:
        return (
            pathlib.Path(self.path)
            / "sql"
            / schema_id.dataset
            / f"{schema_type}s"
            / schema_id.name
        )

    def add_table(
        self,
        schema_id: SchemaId,
        metadata: TableMetadata,
        template_data: str,
        write: bool,
    ) -> None:
        path = self.get_schema_path(SchemaType.table, schema_id)
        template = TableTemplate(path, metadata, template_data)
        self.add_template(schema_id.dataset_id, template, write)

    def add_view(
        self,
        schema_id: SchemaId,
        metadata: SchemaMetadata,
        template_data: str,
        write: bool,
    ) -> None:
        path = self.get_schema_path(SchemaType.view, schema_id)
        template = ViewTemplate(path, metadata, template_data)
        self.add_template(schema_id.dataset_id, template, write)

    def add_routine(
        self,
        schema_id: SchemaId,
        metadata: SchemaMetadata,
        template_data: str,
        write: bool,
    ) -> None:
        path = self.get_schema_path(SchemaType.routine, schema_id)
        template = RoutineTemplate(path, metadata, template_data)
        self.add_template(schema_id.dataset_id, template, write)

    def add_template(
        self,
        dataset_id: DatasetId,
        template: SchemaTemplate,
        write: bool,
    ) -> None:
        path = pathlib.Path(template.path)
        try:
            path.mkdir(parents=True)
        except FileExistsError:
            pass
        meta_file = path / "meta.toml"
        template_file = path / template.filename

        self.templates_by_dataset[dataset_id].append(template)
        metadata = template.metadata.dict(exclude_unset=True)
        if write:
            with open(meta_file, "wb") as f:
                tomli_w.dump(metadata, f, indent=2)

            with open(template_file, "w") as f:
                f.write(template.template)
        else:
            logging.info(
                f"Would write metadata file {meta_file}:\n{tomli_w.dumps(metadata, indent=2)}"
            )
            logging.info(f"Would write template {template_file}:\n{template.template}")

    def remove_table(self, schema_id: SchemaId, write: bool) -> None:
        self.remove_template(SchemaType.table, schema_id, write)

    def remove_view(self, schema_id: SchemaId, write: bool) -> None:
        self.remove_template(SchemaType.view, schema_id, write)

    def remove_routine(self, schema_id: SchemaId, write: bool) -> None:
        self.remove_template(SchemaType.routine, schema_id, write)

    def remove_template(
        self, schema_type: SchemaType, schema_id: SchemaId, write: bool
    ) -> None:
        template = self.templates_by_dataset.get_schema_template(schema_type, schema_id)
        self.templates_by_dataset[schema_id.dataset_id].remove(template)
        path = self.get_schema_path(schema_type, schema_id)
        if write:
            shutil.rmtree(path)
        else:
            logging.info(f"Would remove schema from path {path}")


class SchemaIdMapper:
    def __init__(
        self,
        dataset_mapping: Mapping[DatasetId, DatasetId],
        rewrite_tables: set[SchemaId],
    ):
        self.dataset_mapping = dataset_mapping
        self.rewrite_tables = rewrite_tables

    def __call__(self, ref_type: ReferenceType, input_id: SchemaId) -> SchemaId:
        if ref_type == ReferenceType.external:
            return input_id

        if input_id.dataset_id in self.dataset_mapping and (
            ref_type != ReferenceType.table or input_id in self.rewrite_tables
        ):
            new_dataset = self.dataset_mapping[input_id.dataset_id]
            return SchemaId(new_dataset.project, new_dataset.dataset, input_id.name)
        return input_id


class Project:
    def __init__(
        self,
        project: str,
        project_data: ProjectData,
        datasets: Iterable[Dataset],
        dataset_id_mapper: Callable[[DatasetId], DatasetId],
        schema_id_mapper: Callable[[ReferenceType, SchemaId], SchemaId],
    ):
        self.id = project
        self.data = project_data
        self.datasets = {dataset.canonical_id.dataset: dataset for dataset in datasets}
        self.map_dataset_id = dataset_id_mapper
        self.map_schema_id = schema_id_mapper

    def __getitem__(self, name: str) -> Dataset:
        """Index getter for a dataset.

        This allows writing project["dataset_name"] to access a dataset in the project"""
        try:
            dataset = self.datasets[name]
        except Exception as e:
            raise KeyError(f"No such dataset {name}") from e
        return dataset

    def __iter__(self) -> Iterator[Dataset]:
        for dataset in self.datasets.values():
            yield dataset


class TableSchemaCreator:
    def __init__(
        self,
        project_data: ProjectData,
        schema_id_mapper: Callable[[ReferenceType, SchemaId], SchemaId],
    ):
        """Convert table schema metadata and template into a TableSchema"""
        self.schema_id_mapper = schema_id_mapper
        self.jinja_env = jinja2.Environment()
        self.jinja_env.globals = {
            "metrics": {item.name: item for item in project_data.metric_dfns},
            "metric_types": project_data.metric_types,
            "ranks": project_data.rank_dfns,
        }

    def create_table_schema(
        self, dataset_id: DatasetId, template: SchemaTemplate
    ) -> TableSchema:
        schema_id = SchemaId(
            dataset_id.project, dataset_id.dataset, template.metadata.name
        )
        output = self.render(schema_id, template)

        fields = []
        try:
            schema_data = tomllib.loads(output)
        except Exception as e:
            raise ValueError(f"Failed to load table schema {template.path}") from e
        field_definitions = TableSchemaDefinition.model_validate(schema_data)
        for field_name, field_dfn in field_definitions.root.items():
            try:
                fields.append(field_dfn.to_schema(field_name))
            except ValueError as e:
                raise ValueError(f"Failed creating {schema_id}: {e}")

        output_schema_id = self.schema_id_mapper(
            ReferenceType(SchemaType.table), schema_id
        )
        return TableSchema(
            id=output_schema_id,
            canonical_id=schema_id,
            description=template.metadata.description,
            fields=fields,
            etl=set(template.metadata.etl or []),
            partition=template.metadata.partition,
        )

    def render(self, schema_id: SchemaId, schema_template: SchemaTemplate) -> str:
        try:
            template = self.jinja_env.from_string(schema_template.template)
        except Exception:
            logging.critical(f"Failed loading template for {schema_id}")
            raise
        return template.render()


def load_templates(project: str, root_path: os.PathLike) -> TemplatesByDataset:
    by_dataset = TemplatesByDataset()
    path = pathlib.Path(root_path).resolve()
    sql_path = path / "sql"
    logging.info(f"Loading templates from {sql_path}")
    for dir_name in os.listdir(sql_path):
        dataset_dir = sql_path / dir_name
        if not dataset_dir.is_dir():
            continue
        meta_path = dataset_dir / "meta.toml"
        try:
            with open(meta_path, "rb") as f:
                dataset_data = tomllib.load(f)
        except OSError:
            logging.warning(f"Failed to find {meta_path}")
            continue

        dataset_meta = DatasetMetadata.model_validate(dataset_data)
        dataset = DatasetTemplates(
            DatasetId(project, dataset_meta.name), dataset_meta.description
        )

        for subdir, dest, cls in [
            ("tables", dataset.tables, TableTemplate),
            ("views", dataset.views, ViewTemplate),
            ("routines", dataset.routines, RoutineTemplate),
        ]:
            assert issubclass(cls, SchemaTemplate)
            assert isinstance(dest, list)
            dir_path = dataset_dir / subdir
            if os.path.exists(dir_path):
                for schema_dir in dir_path.iterdir():
                    if not schema_dir.is_dir():
                        continue

                    template = cls.load_from_dir(schema_dir)
                    if template is not None:
                        dest.append(template)

        if not (dataset.tables or dataset.views or dataset.routines):
            logging.warning(f"Failed to find any schema for {dataset.id}")
        by_dataset[dataset.id] = dataset

    return by_dataset


def load_data(project: str, root_path: os.PathLike) -> ProjectData:
    metric_dfns, metric_types = metrics.load(root_path)
    rank_dfns = ranks.load(root_path)
    templates_by_dataset = load_templates(project, root_path)

    return ProjectData(
        path=root_path,
        templates_by_dataset=templates_by_dataset,
        metric_dfns=metric_dfns,
        metric_types=metric_types,
        rank_dfns=rank_dfns,
    )


def stage_dataset(dataset: DatasetId) -> DatasetId:
    """Convert a DatasetId to the name of the equivalent in staging"""
    return DatasetId(project=dataset.project, dataset=dataset.dataset + "_test")


class DatasetMapper:
    def __init__(self, project_data: ProjectData, stage: bool):
        self.dataset_mapping = {
            dataset_id: dataset_id
            for dataset_id in project_data.templates_by_dataset.keys()
        }
        if stage:
            self.dataset_mapping = {
                dataset: stage_dataset(dataset) for dataset in self.dataset_mapping
            }

    def __call__(self, dataset_id: DatasetId) -> DatasetId:
        return self.dataset_mapping.get(dataset_id, dataset_id)


def get_schema_mapper(
    project_id: str,
    project_data: ProjectData,
    etl_jobs: set[str],
    dataset_id_mapper: DatasetMapper,
    known_tables: set[SchemaId],
    config: Config,
) -> Callable[[ReferenceType, SchemaId], SchemaId]:
    rewrite_tables = set()
    if config.stage:
        # If a table is one that we're going to create (i.e. one
        # that's populated by an ETL job that's currently running) or
        # already exists in the target dataset, use that, otherwise
        # reuse the table in the source dataset. This is because we
        # don't always have copies of the tables in the _test datasets
        # for various reasons.
        rewrite_tables |= known_tables
        if config.write:
            # If we're writing assume we want to use the staging version of any tables
            # we might write to
            for dataset, target_dataset in dataset_id_mapper.dataset_mapping.items():
                rewrite_tables |= {
                    SchemaId(project_id, dataset.id.dataset, template.metadata.name)
                    for dataset in project_data.templates_by_dataset.values()
                    for template in dataset.tables
                    if set(template.metadata.etl or []).intersection(etl_jobs)
                }
        logging.debug("\n".join(str(item) for item in rewrite_tables))

    return SchemaIdMapper(dataset_id_mapper.dataset_mapping, rewrite_tables)


def lint_templates(
    etl_jobs: set[str], templates_by_dataset: Iterable[DatasetTemplates]
) -> bool:
    """Basic lint for the input templates.

    Checks:
    * Templates don't use project id directly
    * Templates don't use dataset ids directly"""
    success = True

    for dataset_templates in templates_by_dataset:
        project = dataset_templates.id.project

        for table_template in dataset_templates.tables:
            if table_template.metadata.etl is not None:
                for item in table_template.metadata.etl:
                    if item not in etl_jobs:
                        success = False
                        logging.error(
                            f"Invalid ETL job name {item} in template {table_template.path}"
                        )
        sql_templates = dataset_templates.routines + dataset_templates.views
        for template in sql_templates:
            if project in template.template:
                success = False
                logging.error(f"Found project id in template {template.path}")
            if dataset_templates.id.dataset in template.template:
                success = False
                logging.error(f"Found dataset id in template for {template.path}")

    return success


def create_datasets(
    project: str,
    project_data: ProjectData,
    dataset_id_mapper: Callable[[DatasetId], DatasetId],
    schema_id_mapper: Callable[[ReferenceType, SchemaId], SchemaId],
) -> Sequence[Dataset]:
    """Get a list of Dataset objects for each dataset in the project"""
    creator = TableSchemaCreator(project_data, schema_id_mapper)
    datasets = []
    for dataset_templates in project_data.templates_by_dataset.values():
        schemas: list[Schema] = []
        for template in dataset_templates.tables:
            schemas.append(creator.create_table_schema(dataset_templates.id, template))

        for src, schema_type, cls in [
            (dataset_templates.views, SchemaType.view, ViewSchema),
            (dataset_templates.routines, SchemaType.routine, RoutineSchema),
        ]:
            assert isinstance(src, list)
            for template in src:
                schema_id = SchemaId(
                    project, dataset_templates.id.dataset, template.metadata.name
                )
                output_schema_id = schema_id_mapper(
                    ReferenceType(schema_type), schema_id
                )
                schemas.append(
                    cls(
                        id=output_schema_id,
                        canonical_id=schema_id,
                        description=template.metadata.description,
                    )
                )

        output_dataset_id = dataset_id_mapper(dataset_templates.id)

        datasets.append(
            Dataset(
                id=output_dataset_id, canonical_id=dataset_templates.id, schemas=schemas
            )
        )
    return datasets


def load(
    client: bigquery.Client,
    project: str,
    data_path: os.PathLike,
    etl_jobs: set[str],
    config: Config,
) -> Project:
    """Load project data.

    :param client: - BigQuery client, needed to query which tables already exist in
                     staging mode.
    :param project: - Name of the BigQuery project
    :param data_path: - Path containing the data files for the project
    :param etl_jobs: - List of ETL jobs that are running. This is used in staging mode
                       to decide which table ids to use from the staging data vs from prod.
    :param config: - Global configuration
    """
    project_data = load_data(project, data_path)

    dataset_id_mapper = DatasetMapper(project_data, config.stage)
    known_tables = set()
    if config.stage:
        for dataset in project_data.templates_by_dataset.values():
            target_dataset = dataset_id_mapper(dataset.id)
            try:
                tables = list(client.list_tables(dataset.id.dataset))
            except Exception:
                # If the dataset doesn't exist we don't know about tables it contains
                continue
            for table in tables:
                if table.table_type != "VIEW":
                    schema_id = SchemaId(
                        target_dataset.project, target_dataset.dataset, table.table_id
                    )
                    known_tables.add(schema_id)

    schema_id_mapper = get_schema_mapper(
        project, project_data, etl_jobs, dataset_id_mapper, known_tables, config
    )
    datasets = create_datasets(
        project, project_data, dataset_id_mapper, schema_id_mapper
    )

    return Project(project, project_data, datasets, dataset_id_mapper, schema_id_mapper)
