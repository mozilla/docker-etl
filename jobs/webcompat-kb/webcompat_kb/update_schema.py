import argparse
import difflib
import enum
import json
import logging
import re
import os
import sys
import tomllib
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Iterable, Mapping, Optional, Sequence

import jinja2
from google.cloud import bigquery
from pydantic import BaseModel, ConfigDict

from .base import EtlJob
from .bqhelpers import BigQuery, DatasetId, SchemaId, SchemaType
from .metrics.metrics import metrics, metric_types
from .treehash import hash_tree

here = os.path.dirname(__file__)


@dataclass
class SchemaDefinition:
    id: SchemaId
    type: SchemaType
    sql: str
    depends_on: set[SchemaId]
    description: str = ""


class DatasetMetadata(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    description: Optional[str] = None


class SchemaMetadata(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    description: Optional[str] = None


@dataclass(frozen=True)
class SchemaTemplate:
    path: str
    metadata: SchemaMetadata
    sql_template: str


class DatasetTemplates:
    def __init__(self, id: DatasetId):
        self.id = id
        self.views: list[SchemaTemplate] = []
        self.routines: list[SchemaTemplate] = []


class ReferenceType(enum.StrEnum):
    view = "view"
    routine = "routine"
    table = "table"
    external = "external"


@dataclass
class References:
    views: set[SchemaId]
    routines: set[SchemaId]
    tables: set[SchemaId]
    external: set[SchemaId]


class SchemaIdMapper:
    def __init__(
        self,
        dataset_mapping: Mapping[DatasetId, DatasetId],
        rewrite_tables: set[SchemaId],
    ):
        self.dataset_mapping = dataset_mapping
        self.rewrite_tables = rewrite_tables

    def __call__(self, ref: SchemaId, type: ReferenceType) -> SchemaId:
        if type == ReferenceType.external:
            return ref

        if ref.dataset_id in self.dataset_mapping and (
            type != ReferenceType.table or ref in self.rewrite_tables
        ):
            new_dataset = self.dataset_mapping[ref.dataset_id]
            return SchemaId(new_dataset.project, new_dataset.dataset, ref.name)
        return ref


class ReferenceResolver:
    """Convert a string to a SchemaId

    This acts a function that takes a string like "table_name" and
    converts it to a full schema id. That schema id is then converted
    to an output id using :schema_id_mapper:. In addition the reference is
    classified by type (i.e. table, view, routine, etc.) and added to
    the related property of references. This allows collecting a list of all
    references of a given type that are ever resolved.
    """

    def __init__(
        self,
        schema_id: SchemaId,
        schema_id_mapper: Callable[[SchemaId, ReferenceType], SchemaId],
        view_ids: set[SchemaId],
        routine_ids: set[SchemaId],
    ):
        self.schema_id = schema_id
        self.schema_id_mapper = schema_id_mapper
        self.view_ids = view_ids
        self.routine_ids = routine_ids
        self.references = References(set(), set(), set(), set())

    def __call__(self, name: str) -> SchemaId:
        schema_id = SchemaId.from_str(
            name, self.schema_id.project, self.schema_id.dataset
        )
        if schema_id in self.view_ids:
            ref_type = ReferenceType.view
            dest = self.references.views
        elif schema_id in self.routine_ids:
            ref_type = ReferenceType.routine
            dest = self.references.routines
        elif schema_id.project == self.schema_id.project:
            ref_type = ReferenceType.table
            dest = self.references.tables
        else:
            ref_type = ReferenceType.external
            dest = self.references.external
        output_schema_id = self.schema_id_mapper(schema_id, ref_type)
        if schema_id != self.schema_id:
            # Generally a self-reference is an error, except in routine names or comments,
            # so we could consider handling it better
            dest.add(output_schema_id)
        assert str(output_schema_id).count(".") == 2
        return output_schema_id


def load_schema_template(root_path: str, sql_name: str) -> list[SchemaTemplate]:
    templates = []

    for dir_name in os.listdir(root_path):
        schema_dir = os.path.join(root_path, dir_name)
        if not os.path.isdir(schema_dir):
            continue

        meta_path = os.path.join(schema_dir, "meta.toml")
        try:
            with open(meta_path, "rb") as f:
                schema_data = tomllib.load(f)
        except OSError:
            logging.warning(f"Failed to find {meta_path}")
            continue

        metadata = SchemaMetadata.model_validate(schema_data)

        sql_path = os.path.join(schema_dir, sql_name)
        try:
            with open(sql_path) as f:
                sql_template = f.read()
        except OSError:
            logging.warning(f"Failed to find {sql_path}")
            continue

        templates.append(
            SchemaTemplate(
                path=os.path.abspath(sql_path),
                metadata=metadata,
                sql_template=sql_template,
            )
        )

    return templates


def load_templates(project: str, root_path: os.PathLike) -> list[DatasetTemplates]:
    by_dataset = []
    for dir_name in os.listdir(root_path):
        dataset_dir = os.path.join(root_path, dir_name)
        if not os.path.isdir(dataset_dir):
            continue
        meta_path = os.path.join(dataset_dir, "meta.toml")
        try:
            with open(meta_path, "rb") as f:
                dataset_data = tomllib.load(f)
        except OSError:
            logging.warning(f"Failed to find {meta_path}")
            continue

        dataset_meta = DatasetMetadata.model_validate(dataset_data)
        dataset = DatasetTemplates(DatasetId(project, dataset_meta.name))

        routines_path = os.path.join(dataset_dir, "routines")
        if os.path.exists(routines_path):
            dataset.routines = load_schema_template(routines_path, "routine.sql")

        views_path = os.path.join(dataset_dir, "views")
        if os.path.exists(views_path):
            dataset.views = load_schema_template(views_path, "view.sql")

        by_dataset.append(dataset)

    return by_dataset


def topological_sort(
    nodes: Mapping[SchemaId, SchemaDefinition],
) -> Sequence[SchemaDefinition]:
    """Sort nodes so that dependencies come before their dependents"""
    rv = []

    # Mapping from {node: processing_complete}. Unprocessed nodes are not in the map.
    # During processing complete is False, after processing it's True
    seen_nodes: dict[SchemaId, bool] = {}

    def visit(node: SchemaDefinition) -> None:
        if node.id in seen_nodes:
            if seen_nodes[node.id]:
                return
            else:
                raise ValueError(f"Cyclic dependency in {node.id}")

        seen_nodes[node.id] = False

        for dependent in node.depends_on:
            if dependent in nodes:
                visit(nodes[dependent])
            else:
                raise ValueError(f"Unknown dependency {dependent} from {node.id}")

        seen_nodes[node.id] = True
        rv.append(node)

    for node in nodes.values():
        if node.id in seen_nodes:
            continue
        visit(node)

    return rv


@dataclass
class RenderResult:
    sql: str
    references: References


class SchemaCreator:
    def __init__(
        self,
        project: str,
        schema_id_mapper: Callable[[SchemaId, ReferenceType], SchemaId],
        view_ids: set[SchemaId],
        routine_ids: set[SchemaId],
    ):
        self.project = project
        self.schema_id_mapper = schema_id_mapper
        self.view_ids = view_ids
        self.routine_ids = routine_ids

        self.jinja_env = jinja2.Environment()
        self.jinja_env.globals = {
            "project": project,
            "metrics": {item.name: item for item in metrics},
            "metric_types": metric_types,
        }

    def create_schemas(
        self,
        dataset_templates: DatasetTemplates,
    ) -> Mapping[SchemaId, SchemaDefinition]:
        schemas = {}

        for schema_type, src_templates in [
            (SchemaType.view, dataset_templates.views),
            (SchemaType.routine, dataset_templates.routines),
        ]:
            for template in src_templates:
                schema_id = SchemaId(
                    self.project, dataset_templates.id.dataset, template.metadata.name
                )
                render_result = self.render_sql(schema_id, template)

                output_schema_id = self.schema_id_mapper(
                    schema_id, ReferenceType(schema_type)
                )

                if schema_type == SchemaType.routine:
                    if not validate_routine_sql(output_schema_id, render_result.sql):
                        raise ValueError(
                            f"Invalid SQL for {schema_id} from {template.path}"
                        )
                    if template.metadata.description:
                        render_result.sql = add_routine_options(
                            render_result.sql, description=template.metadata.description
                        )

                depends_on = (
                    render_result.references.views | render_result.references.routines
                )
                logging.debug(f"SQL for {output_schema_id}:\n{render_result.sql}\n----")
                schemas[output_schema_id] = SchemaDefinition(
                    id=output_schema_id,
                    description=template.metadata.description or "",
                    type=schema_type,
                    sql=render_result.sql,
                    depends_on=depends_on,
                )
        return schemas

    def render_sql(
        self,
        schema_id: SchemaId,
        schema: SchemaTemplate,
    ) -> RenderResult:
        try:
            template = self.jinja_env.from_string(schema.sql_template)
        except Exception:
            logging.critical(f"Failed loading template for {schema_id}")
            raise

        ref_mapper = ReferenceResolver(
            schema_id, self.schema_id_mapper, self.view_ids, self.routine_ids
        )

        context = {
            "ref": ref_mapper,
            "dataset": schema_id.dataset,
            "name": schema_id.name,
        }

        try:
            sql = template.render(context)
        except Exception:
            logging.critical(f"Failed rendering template for {schema_id}")
            raise

        return RenderResult(sql=sql, references=ref_mapper.references)


def validate_routine_sql(schema_id: SchemaId, sql: str) -> bool:
    """Some basic validation of the generated SQL for routines

    This is not designed to guard against malicious input, but to ensure the query
    looks basically how we expect."""
    routine_format = re.compile(
        r"^CREATE OR REPLACE FUNCTION `(?P<name>[^`]+)`\((?P<args>[^\)]*)\) RETURNS (?P<return_type>[^\(]+) AS \((?P<body>.*)\);?\s*$",
        re.DOTALL,
    )
    m = routine_format.match(sql)
    if m is None:
        logging.error(f"Invalid SQL for {schema_id}")
        return False

    if m.group("name") != str(schema_id):
        logging.error(
            f"Invalid SQL for {schema_id}, expected function name '{schema_id}' but got '{m.group('name')}'"
        )
        return False

    if "OPTIONS(" in sql:
        logging.error(f"Invalid SQL {schema_id}, OPTIONS not permitted")
        return False

    return True


def add_routine_options(sql: str, description: str) -> str:
    escaped_description = json.dumps(description)
    sql = sql.rstrip(";")
    sql += f"\nOPTIONS(description={escaped_description});"
    return sql


def get_current_schemas(
    client: BigQuery,
    datasets: Iterable[DatasetId],
    dataset_mapping: Mapping[DatasetId, DatasetId],
) -> tuple[Mapping[SchemaId, bigquery.Table], Mapping[SchemaId, bigquery.Routine]]:
    views = {}
    routines = {}
    for dataset in datasets:
        dataset_name = dataset_mapping[dataset].dataset
        for view_table in client.get_views(dataset_name):
            schema_id = SchemaId(dataset.project, dataset_name, view_table.table_id)
            views[schema_id] = view_table
        for routine in client.get_routines(dataset_name):
            schema_id = SchemaId(dataset.project, dataset_name, routine.routine_id)
            routines[schema_id] = routine

    return views, routines


def view_needs_update(
    current_view: Optional[bigquery.Table], new_view: SchemaDefinition
) -> bool:
    if current_view is None:
        logging.info(f"{new_view.id} does not exist")
        return True

    if (
        current_view.description
        and new_view.description
        and current_view.description != new_view.description
    ):
        logging.info(f"{new_view.id} description updated")
        return True

    if current_view.view_query != new_view.sql:
        logging.info(f"{new_view.id} query body updated")
        diff = difflib.unified_diff(
            current_view.view_query.splitlines(), new_view.sql.splitlines()
        )
        logging.info(f"\n{'\n'.join(diff)}")
        return True

    return False


def routine_needs_update(
    current_routine: Optional[bigquery.Routine], new_routine: SchemaDefinition
) -> bool:
    # Diffing routines is complicated by the fact that our input is a CREATE OR REPLACE FUNCTION statement
    # but the Routine object has a parsed representation of the result of that operation.
    # Fow now, just always update routines
    return True


def create_schemas(
    project: str,
    schema_id_mapper: Callable[[SchemaId, ReferenceType], SchemaId],
    templates_by_dataset: Iterable[DatasetTemplates],
) -> tuple[Sequence[SchemaDefinition], set[SchemaId], set[SchemaId]]:
    view_ids = {
        SchemaId(project, dataset.id.dataset, template.metadata.name)
        for dataset in templates_by_dataset
        for template in dataset.views
    }
    routine_ids = {
        SchemaId(project, dataset.id.dataset, template.metadata.name)
        for dataset in templates_by_dataset
        for template in dataset.routines
    }

    creator = SchemaCreator(project, schema_id_mapper, view_ids, routine_ids)
    schemas: dict[SchemaId, SchemaDefinition] = {}
    for dataset_template in templates_by_dataset:
        schemas.update(
            creator.create_schemas(
                dataset_template,
            )
        )

    output_view_ids = {
        schema_id_mapper(schema, ReferenceType.view) for schema in view_ids
    }
    output_routine_ids = {
        schema_id_mapper(schema, ReferenceType.routine) for schema in routine_ids
    }

    schemas_list = topological_sort(schemas)

    return schemas_list, output_view_ids, output_routine_ids


def update_schemas(
    client: BigQuery,
    templates_by_dataset: Iterable[DatasetTemplates],
    dataset_mapping: Mapping[DatasetId, DatasetId],
    schema_id_mapper: Callable[[SchemaId, ReferenceType], SchemaId],
    delete_missing: bool,
) -> None:
    schemas_list, output_view_ids, output_routine_ids = create_schemas(
        client.project_id, schema_id_mapper, templates_by_dataset
    )

    current_views, current_routines = get_current_schemas(
        client, [item.id for item in templates_by_dataset], dataset_mapping
    )

    for schema in schemas_list:
        if schema.type == ReferenceType.routine:
            if routine_needs_update(current_routines.get(schema.id), schema):
                assert schema.id.project == client.project_id
                if client.write:
                    client.query(schema.sql, dataset_id=schema.id.dataset)
                else:
                    logging.info(f"Skipping write, would create routine {schema.id}")
        elif schema.type == ReferenceType.view:
            if view_needs_update(current_views.get(schema.id), schema):
                assert schema.id.project == client.project_id
                client.create_view(
                    schema.id.name,
                    schema.sql,
                    dataset_id=schema.id.dataset,
                    description=schema.description,
                )
        else:
            raise ValueError(f"Schema {schema.id} had unexpected type {schema.type}")

    for item in current_views.keys():
        if item not in output_view_ids:
            logging.info(f"View {item} not found in local definition")
            if delete_missing:
                client.delete_table(str(item))

    for item in current_routines.keys():
        if item not in output_routine_ids:
            logging.info(f"Routine {item} not found in local definition")
            if delete_missing:
                client.delete_routine(str(item))


def lint_templates(templates_by_dataset: list[DatasetTemplates]) -> bool:
    """Basic lint for the input templates.

    Checks:
    * Templates don't use project id directly
    * Templates don't use dataset ids directly"""
    success = True

    for dataset_templates in templates_by_dataset:
        project = dataset_templates.id.project
        templates = dataset_templates.routines + dataset_templates.views
        for template in templates:
            if project in template.sql_template:
                success = False
                logging.error(f"Found project id in template {template.path}")
            if dataset_templates.id.dataset in template.sql_template:
                success = False
                logging.error(f"Found dataset id in template for {template.path}")

    return success


def stage_dataset(dataset: DatasetId) -> DatasetId:
    """Convert a DatasetId to the name of the equivalent in staging"""
    return DatasetId(project=dataset.project, dataset=dataset.dataset + "_test")


def get_last_update(
    client: BigQuery, dataset: DatasetId
) -> tuple[Optional[datetime], Optional[str]]:
    schema = [
        bigquery.SchemaField("run_at", "DATETIME", mode="REQUIRED"),
        bigquery.SchemaField("schema_hash", "STRING", mode="REQUIRED"),
    ]
    client.ensure_table("schema_updates", schema, dataset_id=dataset.dataset)
    try:
        rows = list(
            client.query(f"""SELECT run_at, schema_hash
FROM `{dataset}.schema_updates`
ORDER BY run_at DESC
LIMIT 1""")
        )
    except Exception:
        return None, None
    if not rows:
        return None, None

    row = list(rows)[0]
    return row.run_at, row.schema_hash


def record_update(client: BigQuery, dataset: DatasetId, schema_hash: str) -> None:
    if client.write:
        parameters = [
            bigquery.ScalarQueryParameter("schema_hash", "STRING", schema_hash)
        ]
        client.query(
            f"""
INSERT `{dataset}.schema_updates` (run_at, schema_hash) (
  SELECT CURRENT_DATETIME(), @schema_hash
)""",
            parameters=parameters,
        )


def check_templates() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bq-project-id", action="store", help="BigQuery project ID")
    parser.add_argument(
        "--path",
        action="store",
        default=os.path.join(here, os.pardir, "data", "sql"),
        help="Path to directory containing sql",
    )
    args = parser.parse_args()

    templates_by_dataset = load_templates(args.bq_project_id, args.path)
    if not lint_templates(templates_by_dataset):
        logging.error("Lint failed")
        sys.exit(1)

    schema_id_mapper = SchemaIdMapper({}, set())
    try:
        create_schemas(args.bq_project_id, schema_id_mapper, templates_by_dataset)
    except Exception:
        logging.error("Creating schemas failed")
        raise


def update_schema_if_needed(
    client: BigQuery,
    schema_path: os.PathLike,
    stage: bool,
    recreate: bool,
    delete_extra: bool,
) -> None:
    metadata_dataset = DatasetId(client.project_id, "metadata")
    if stage:
        metadata_dataset = stage_dataset(metadata_dataset)

    src_hash = hash_tree(schema_path).hex()
    last_update_time, last_update_hash = get_last_update(client, metadata_dataset)

    logging.info(f"Templates have hash {src_hash}")
    logging.info(f"Deployed schema have hash {last_update_hash}")

    update_needed = (
        recreate
        or last_update_hash != src_hash
        or (last_update_time and last_update_time.date() < datetime.now().date())
    )
    if not update_needed:
        logging.info("No changes to deploy")
        return

    templates_by_dataset = load_templates(client.project_id, schema_path)
    if not lint_templates(templates_by_dataset):
        raise ValueError("Template lint failed")

    dataset_mapping = {dataset.id: dataset.id for dataset in templates_by_dataset}
    rewrite_tables = set()
    if stage:
        dataset_mapping = {
            dataset: stage_dataset(dataset) for dataset in dataset_mapping
        }
        # If a table is in the target dataset, use that, otherwise reuse the
        # table in the source dataset. This is because we don't always have
        # copies of the tables in the _test datasets for various reasons.
        for dataset, target_dataset in dataset_mapping.items():
            rewrite_tables |= set(
                SchemaId(client.project_id, dataset.dataset, item.table_id)
                for item in client.client.list_tables(target_dataset.dataset)
                if item.table_type != "VIEW"
            )
        logging.debug("\n".join(str(item) for item in rewrite_tables))

    schema_id_mapper = SchemaIdMapper(dataset_mapping, rewrite_tables)
    update_schemas(
        client,
        templates_by_dataset,
        dataset_mapping,
        schema_id_mapper,
        delete_extra,
    )
    record_update(client, metadata_dataset, src_hash)


class UpdateStagingData(EtlJob):
    name = "update-staging-data"
    default = False

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(
            title="Update Staging Data", description="update-staging-data arguments"
        )
        group.add_argument(
            "--update-staging-data-views",
            action="store_true",
            default=False,
            help="Redeploy views to staging",
        )

    def default_dataset(self, args: argparse.Namespace) -> str:
        return args.bq_kb_dataset

    def main(self, client: BigQuery, args: argparse.Namespace) -> None:
        datasets = [DatasetId(client.project_id, args.bq_kb_dataset)]
        dataset_mapping = {dataset: stage_dataset(dataset) for dataset in datasets}

        for dataset in datasets:
            if client.write:
                client.client.create_dataset(dataset.dataset, exists_ok=True)
            tables = list(
                SchemaId(dataset.project, dataset.dataset, item.table_id)
                for item in client.client.list_tables(dataset.dataset)
                if item.table_type != "VIEW"
            )
            schema_id_mapper = SchemaIdMapper(dataset_mapping, set(tables))
            for src_table in tables:
                dest_table = schema_id_mapper(src_table, ReferenceType.table)
                assert dest_table != src_table
                logging.info(f"Creating {dest_table} from {src_table}")
                if args.write:
                    client.delete_table(str(dest_table), not_found_ok=True)
                else:
                    logging.info(f"Would delete table {dest_table}")

                query = f"""
CREATE TABLE `{dest_table}`
CLONE `{src_table}`
"""
                if client.write:
                    logging.info(f"Creating table {dest_table} from {src_table}")
                    try:
                        client.query(query)
                    except Exception:
                        logging.error(f"Creating table {dest_table} failed")
                else:
                    logging.info(f"Would run query:{query}")

        if args.update_staging_data_views:
            logging.info("Updating stage views")
            update_schema_if_needed(
                client,
                schema_path=args.update_schema_path,
                stage=True,
                recreate=True,
                delete_extra=False,
            )


class UpdateSchemaJob(EtlJob):
    name = "update-schema"

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(
            title="Update Schema", description="update-schema arguments"
        )
        group.add_argument(
            "--update-schema-path",
            action="store",
            default=os.path.join(here, os.pardir, "data", "sql"),
            help="Path to directory containing sql to deploy",
        )
        group.add_argument(
            "--update-schema-stage",
            action="store_true",
            help="Write to staging location (currently same project with _test suffix on dataset names)",
        )
        group.add_argument(
            "--update-schema-delete-extra",
            action="store_true",
            dest="update_schema_delete_extra",
            help="Delete remote datasets that aren't in the local schema",
        )
        group.add_argument(
            "--update-schema-recreate",
            action="store_true",
            help="Force update from source files",
        )

    def default_dataset(self, args: argparse.Namespace) -> str:
        return args.bq_kb_dataset

    def main(self, client: BigQuery, args: argparse.Namespace) -> None:
        update_schema_if_needed(
            client,
            schema_path=args.update_schema_path,
            stage=args.update_schema_stage,
            recreate=args.update_schema_recreate,
            delete_extra=args.update_schema_delete_extra,
        )
