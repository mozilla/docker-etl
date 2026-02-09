import argparse
import difflib
import json
import logging
import re
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Iterable, Mapping, Optional, Sequence

import jinja2
from google.cloud import bigquery

from . import metric_rescore
from .base import ALL_JOBS, Context, EtlJob
from .bqhelpers import (
    BigQuery,
    DatasetId,
    SchemaId,
    SchemaType,
    TableSchema,
)
from .projectdata import (
    Project,
    ReferenceType,
    SchemaTemplate,
    TableSchemaCreator,
    lint_templates,
)
from .treehash import hash_tree


here = os.path.dirname(__file__)


@dataclass
class SchemaDefinition:
    id: SchemaId
    type: SchemaType
    sql: str
    depends_on: set[SchemaId]
    description: str = ""


@dataclass
class References:
    views: set[SchemaId] = field(default_factory=set)
    routines: set[SchemaId] = field(default_factory=set)
    tables: set[SchemaId] = field(default_factory=set)
    external: set[SchemaId] = field(default_factory=set)


@dataclass
class RenderResult:
    output: str
    references: References


class SchemaCreator:
    def __init__(
        self,
        project: Project,
    ):
        self.project = project

        self.known_references = {
            schema.canonical_id: schema.type
            for dataset in project
            for schema in dataset
        }

        self.jinja_env = jinja2.Environment()
        self.jinja_env.globals = {
            "project": self.project.id,
            "metrics": {item.name: item for item in project.data.metric_dfns},
            "metric_types": project.data.metric_types,
            "ranks": project.data.rank_dfns,
        }

    def create(
        self, only_schema_ids: Optional[set[SchemaId]] = None
    ) -> Mapping[SchemaId, SchemaDefinition]:
        schemas: dict[SchemaId, SchemaDefinition] = {}

        if only_schema_ids is not None and not only_schema_ids:
            return schemas

        for dataset_templates in self.project.data.templates_by_dataset.values():
            for schema_type, src_templates in [
                (SchemaType.view, dataset_templates.views),
                (SchemaType.routine, dataset_templates.routines),
            ]:
                assert isinstance(src_templates, list)
                for template in src_templates:
                    schema_id = SchemaId(
                        self.project.id,
                        dataset_templates.id.dataset,
                        template.metadata.name,
                    )
                    if only_schema_ids is not None and schema_id not in only_schema_ids:
                        continue
                    render_result = self.render(schema_id, schema_type, template)

                    output_schema_id = self.project.map_schema_id(
                        ReferenceType(schema_type), schema_id
                    )

                    if schema_type == SchemaType.routine:
                        if not validate_routine_sql(
                            output_schema_id, render_result.output
                        ):
                            raise ValueError(
                                f"Invalid SQL for {schema_id} from {template.path}"
                            )
                        if template.metadata.description:
                            render_result.output = add_routine_options(
                                render_result.output,
                                description=template.metadata.description,
                            )

                    depends_on = (
                        render_result.references.views
                        | render_result.references.routines
                    )
                    logging.debug(
                        f"SQL for {output_schema_id}:\n{render_result.output}\n----"
                    )
                    schemas[output_schema_id] = SchemaDefinition(
                        id=output_schema_id,
                        description=template.metadata.description or "",
                        type=schema_type,
                        sql=render_result.output,
                        depends_on=depends_on,
                    )
        return schemas

    def render(
        self,
        schema_id: SchemaId,
        schema_type: SchemaType,
        schema: SchemaTemplate,
    ) -> RenderResult:
        try:
            template = self.jinja_env.from_string(schema.template)
        except Exception:
            logging.critical(f"Failed loading template for {schema_id}")
            raise

        context = {}

        references = References()
        ref_mapper = ReferenceResolver(
            schema_id,
            self.project.map_schema_id,
            self.known_references,
            references,
        )

        context = {
            "ref": ref_mapper,
            "dataset": schema_id.dataset,
            "name": schema_id.name,
        }

        try:
            output = template.render(context)
        except Exception:
            logging.critical(f"Failed rendering template for {schema_id}")
            raise

        return RenderResult(output=output, references=references)


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
        schema_id_mapper: Callable[[ReferenceType, SchemaId], SchemaId],
        known_schema_ids: Mapping[SchemaId, SchemaType],
        references: References,
    ):
        self.schema_id = schema_id
        self.schema_id_mapper = schema_id_mapper
        self.known_schema_ids = known_schema_ids
        self.type_map = {
            SchemaType.view: (ReferenceType.view, references.views),
            SchemaType.table: (ReferenceType.table, references.tables),
            SchemaType.routine: (ReferenceType.routine, references.routines),
            None: (ReferenceType.external, references.external),
        }

    def __call__(self, name: str) -> SchemaId:
        schema_id = SchemaId.from_str(
            name, self.schema_id.project, self.schema_id.dataset
        )
        schema_type = self.known_schema_ids.get(schema_id)
        ref_type, dest = self.type_map[schema_type]
        output_schema_id = self.schema_id_mapper(ref_type, schema_id)
        if schema_id != self.schema_id:
            # Generally a self-reference is an error, except in routine names or comments,
            # so we could consider handling it better
            dest.add(output_schema_id)
        assert str(output_schema_id).count(".") == 2
        return output_schema_id


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


@dataclass
class Schemas:
    tables: dict[SchemaId, bigquery.Table] = field(default_factory=dict)
    views: dict[SchemaId, bigquery.Table] = field(default_factory=dict)
    routines: dict[SchemaId, bigquery.Routine] = field(default_factory=dict)


def get_current_schemas(
    client: BigQuery, datasets: Iterable[DatasetId], need_datasets: set[DatasetId]
) -> Schemas:
    schemas = Schemas()

    for dataset in datasets:
        try:
            tables = list(client.get_tables(dataset.dataset))
        except Exception:
            # If the dataset doesn't exist we don't want to fail here
            if not client.write or dataset not in need_datasets:
                continue
            raise
        for table in tables:
            schema_id = SchemaId(dataset.project, dataset.dataset, table.table_id)
            if table.table_type == "VIEW":
                schemas.views[schema_id] = table
            elif table.view_query is None:
                schemas.tables[schema_id] = table
        for routine in client.get_routines(dataset.dataset):
            schema_id = SchemaId(dataset.project, dataset.dataset, routine.routine_id)
            schemas.routines[schema_id] = routine

    return schemas


def base_table_needs_update(
    current_table: Optional[bigquery.Table], schema: TableSchema | SchemaDefinition
) -> bool:
    if current_table is None:
        logging.info(f"{schema.id} does not exist")
        return True

    if (
        current_table.description
        and schema.description
        and current_table.description != schema.description
    ):
        logging.info(f"{schema.id} description updated")
        return True

    return False


class TableUpdater:
    def __init__(self, current_tables: Mapping[SchemaId, bigquery.Table]):
        self.current_tables = current_tables

    def needs_update(self, schema: TableSchema) -> bool:
        current_table = self.current_tables.get(schema.id)

        if base_table_needs_update(current_table, schema):
            return True

        assert current_table is not None

        if len(schema.fields) != len(current_table.schema):
            logging.info(f"{schema.id} fields added")
            return True

        # We don't handle updates other than additions at the moment, could validate here that
        # only additions are required

        return False

    def update(self, client: BigQuery, schema: TableSchema) -> None:
        assert schema.id.project == client.project_id
        logging.info(f"Updating table definition {schema}")
        client.ensure_table(
            schema,
            schema.schema,
            update_fields=True,
        )


class ViewUpdater:
    def __init__(self, current_views: Mapping[SchemaId, bigquery.Table]):
        self.current_views = current_views

    def needs_update(self, schema: SchemaDefinition) -> bool:
        current_view = self.current_views.get(schema.id)

        if base_table_needs_update(current_view, schema):
            return True

        assert current_view is not None

        if current_view.view_query != schema.sql:
            logging.info(f"{schema.id} query body updated")
            diff = difflib.unified_diff(
                current_view.view_query.splitlines(), schema.sql.splitlines()
            )
            logging.info(f"\n{'\n'.join(diff)}")
            return True

        return False

    def update(self, client: BigQuery, schema: SchemaDefinition) -> None:
        assert schema.id.project == client.project_id
        client.create_view(
            schema.id.name,
            schema.sql,
            dataset_id=schema.id.dataset,
            description=schema.description,
        )


class RoutineUpdater:
    def __init__(self, current_routines: Mapping[SchemaId, bigquery.Routine]):
        self.current_routines = current_routines

    def needs_update(self, schema: SchemaDefinition) -> bool:
        # Diffing routines is complicated by the fact that our input is a CREATE OR REPLACE FUNCTION statement
        # but the Routine object has a parsed representation of the result of that operation.
        # Fow now, just always update routines
        return True

    def update(self, client: BigQuery, schema: SchemaDefinition) -> None:
        assert schema.id.project == client.project_id
        if client.write:
            client.query(schema.sql, dataset_id=schema.id.dataset)
        else:
            logging.info(f"Skipping write, would create routine {schema.id}")


def render_schemas(
    project: Project, schema_ids: Sequence[SchemaId]
) -> Mapping[SchemaId, tuple[SchemaType, str]]:
    """Render the schemas given in schema_ids

    :param Project: The project the schemas are in
    :param schema_ids: The schema ids to render
    :returns: Tuple of (SchemaType, rendered)
    """
    schemas_by_type: dict[SchemaType, list[SchemaId]] = {
        SchemaType.view: [],
        SchemaType.routine: [],
        SchemaType.table: [],
    }
    for schema_id in schema_ids:
        try:
            schema = project[schema_id.dataset][schema_id.name]
        except ValueError:
            logging.error(f"Invalid schema id {schema_id}")
            raise
        schemas_by_type[schema.type].append(schema_id)

    outputs: dict[SchemaId, tuple[SchemaType, str]] = {}
    table_schema_creator = TableSchemaCreator(project.data, project.map_schema_id)

    for schema_id in schemas_by_type[SchemaType.table]:
        dataset_templates = project.data.templates_by_dataset[schema_id.dataset_id]
        for template in dataset_templates.tables:
            if template.metadata.name == schema_id.name:
                outputs[schema_id] = (
                    SchemaType.table,
                    table_schema_creator.render(schema_id, template),
                )
                break
        else:
            # This shouldn't ever happen
            raise ValueError(f"Failed to find template for {schema}")

    creator = SchemaCreator(project)
    for schema_type in [SchemaType.view, SchemaType.routine]:
        type_schema_ids = schemas_by_type[schema_type]
        try:
            for schema_id, dfn in creator.create(set(type_schema_ids)).items():
                outputs[schema_id] = (schema_type, dfn.sql)
        except Exception:
            logging.error("Creating schemas failed")
            raise

    return outputs


def update_schemas(
    client: BigQuery,
    project: Project,
    etl_jobs: set[str],
    delete_missing: bool,
) -> None:
    creator = SchemaCreator(project)
    sql_schemas = creator.create()

    sql_schemas_list = topological_sort(sql_schemas)
    datasets = [
        (project.map_dataset_id(item.id), item.description)
        for item in project.data.templates_by_dataset.values()
    ]

    # Tables needed for a for a job that we'll run
    logging.info(f"Only updating tables used by jobs {', '.join(etl_jobs)}")
    etl_table_schemas = [
        schema
        for dataset in project
        for schema in dataset.tables()
        if schema.etl_jobs.intersection(etl_jobs)
    ]

    # Ensure we have any datasets which either have a view or routine
    # or are needed for a job we'll run
    need_datasets = {schema.id.dataset_id for schema in etl_table_schemas} | {
        dataset.id
        for dataset in project.data.templates_by_dataset.values()
        if dataset.views or dataset.routines
    }

    for dataset_id, description in datasets:
        if dataset_id in need_datasets:
            client.ensure_dataset(dataset_id, description)

    current_schemas = get_current_schemas(
        client, [item[0] for item in datasets], need_datasets
    )

    table_updater = TableUpdater(current_schemas.tables)
    # Only create tables when they're needed for a job that we'll run

    for table_schema in etl_table_schemas:
        if table_updater.needs_update(table_schema):
            table_updater.update(client, table_schema)

    updaters: Mapping[SchemaType, RoutineUpdater | ViewUpdater] = {
        SchemaType.routine: RoutineUpdater(current_schemas.routines),
        SchemaType.view: ViewUpdater(current_schemas.views),
    }

    for schema_dfn in sql_schemas_list:
        updater = updaters[schema_dfn.type]
        if updater.needs_update(schema_dfn):
            updater.update(client, schema_dfn)

    output_view_ids = {schema.id for dataset in project for schema in dataset.views()}
    output_routine_ids = {
        schema.id for dataset in project for schema in dataset.routines()
    }

    for item in current_schemas.views.keys():
        if item not in output_view_ids:
            logging.info(f"View {item} not found in local definition")
            if delete_missing:
                client.delete_table(str(item))

    for item in current_schemas.routines.keys():
        if item not in output_routine_ids:
            logging.info(f"Routine {item} not found in local definition")
            if delete_missing:
                client.delete_routine(str(item))


def get_last_update(
    project: Project, client: BigQuery
) -> tuple[Optional[datetime], Optional[str]]:
    table = project["metadata"]["schema_updates"]
    try:
        rows = list(
            client.query(f"""SELECT run_at, schema_hash
FROM `{table}`
ORDER BY run_at DESC
LIMIT 1""")
        )
    except Exception:
        return None, None
    if not rows:
        return None, None

    row = list(rows)[0]
    return row.run_at, row.schema_hash


def record_update(project: Project, client: BigQuery, schema_hash: str) -> None:
    parameters = [bigquery.ScalarQueryParameter("schema_hash", "STRING", schema_hash)]
    table = project["metadata"]["schema_updates"]
    assert isinstance(table, TableSchema)
    client.insert_query(
        str(table),
        columns=[item.name for item in table.fields],
        query="SELECT CURRENT_DATETIME(), @schema_hash",
        parameters=parameters,
    )


def before_schema_update(client: BigQuery, project: Project) -> None:
    """Lifecycle point to record data that depends on the existing
    schemas, before deploying the update"""

    metric_rescore.record_rescores(project, client)


def update_schema_if_needed(
    project: Project,
    client: BigQuery,
    etl_jobs_enabled: set[str],
    stage: bool,
    recreate: bool,
    delete_extra: bool,
) -> None:
    src_hash = hash_tree(project.data.path).hex()
    last_update_time, last_update_hash = get_last_update(project, client)

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

    etl_jobs = {item for item in ALL_JOBS}
    if not lint_templates(etl_jobs, project.data.templates_by_dataset.values()):
        raise ValueError("Template lint failed")

    before_schema_update(client, project)

    update_schemas(
        client,
        project,
        etl_jobs_enabled,
        delete_extra,
    )
    record_update(project, client, src_hash)


class UpdateSchemaJob(EtlJob):
    name = "update-schema"

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(
            title="Update Schema", description="update-schema arguments"
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

    def default_dataset(self, context: Context) -> str:
        return context.args.bq_kb_dataset

    def write_targets(self, project: Project) -> set[SchemaId]:
        # This job can update any schema
        rv = set()
        for dataset in project:
            for schema in dataset:
                rv.add(schema.id)
        return rv

    def main(self, context: Context) -> None:
        update_schema_if_needed(
            context.project,
            context.bq_client,
            etl_jobs_enabled={item.name for item in context.jobs},
            stage=context.config.stage,
            recreate=context.args.update_schema_recreate,
            delete_extra=context.args.update_schema_delete_extra,
        )
