import difflib
import argparse
import logging
import os
from typing import Mapping, Optional


from .. import projectdata
from .. import redashdata
from ..base import Command
from ..bqhelpers import get_client
from ..config import Config
from ..redash import client
from ..redash.client import RedashClient


class ReferenceResolver:
    def __init__(self, project_id: str):
        self.project_id = project_id

    def __call__(self, name: str) -> str:
        parts = name.split(".")
        if len(parts) == 3:
            return name
        if len(parts) == 2:
            return f"{self.project_id}.{name}"
        raise ValueError(f"Invalid reference {name}")


class ParameterResolver:
    def __init__(self, parameter_names: set[str]):
        self.parameter_names = parameter_names

    def __call__(self, name: str) -> str:
        if name not in self.parameter_names:
            raise ValueError(f"Unknown parameter {name}")
        return f"{{{{ {name} }}}}"


def redash_options(
    parameters: Optional[Mapping[str, redashdata.RedashParameter]],
    current: Optional[client.RedashOptions],
) -> Optional[client.RedashOptions]:
    if parameters is None:
        return None

    query_parameters = []
    if current is not None:
        current_parameters = {item.name: item for item in current.parameters}
    else:
        current_parameters = {}

    for name, parameter in parameters.items():
        current_parameter = current_parameters.get(name)
        query_parameters.append(parameter.to_client(name, current_parameter))

    return client.RedashOptions(parameters=query_parameters)


def create_query(
    redash_client: RedashClient,
    query: redashdata.RedashQueryTemplate,
    parameters: Optional[Mapping[str, redashdata.RedashParameter]],
    sql: str,
    write: bool,
) -> int:
    # Create new query
    if write:
        logging.info(f"Creating query: {query.metadata.name}")
        resp = redash_client.create_query(
            query.metadata.name,
            sql,
            description=query.metadata.description,
            options=redash_options(parameters, None),
        )
        logging.info(f"Created query with ID: {resp.id}")
        query_id = resp.id
    else:
        logging.info(
            f"Would create query: {query.metadata.name} with parameters:\n{redash_options(parameters, None)}\nquery:{sql}"
        )
        query_id = 0

    return query_id


def update_query(
    redash_client: RedashClient,
    query: redashdata.RedashQueryTemplate,
    parameters: Optional[Mapping[str, redashdata.RedashParameter]],
    sql: str,
    write: bool,
) -> bool:
    # Update existing query
    assert query.metadata.id is not None

    current_query = redash_client.get_query(query.metadata.id)
    if (
        current_query.name == query.metadata.name
        and current_query.query == sql
        and current_query.description == query.metadata.description
    ):
        # TODO: This doesn't cover the possibility of just parameters changing
        logging.info(
            f"No updates for query {query.metadata.name} (ID: {query.metadata.id})"
        )
        return False

    if current_query.query != sql:
        logging.info(f"{current_query.name} query body updated")
        diff = difflib.unified_diff(current_query.query.splitlines(), sql.splitlines())
        logging.info(f"\n{'\n'.join(diff)}")

    if write:
        logging.info(f"Updating query: {query.metadata.name} (ID: {query.metadata.id})")
        redash_client.update_query(
            query.metadata.id,
            query.metadata.name,
            sql,
            description=query.metadata.description or "",
            options=redash_options(parameters, current_query.options),
        )
    else:
        logging.info(
            f"Would update query: {query.metadata.name} (ID {query.metadata.id}) with:\n{sql}"
        )
    return True


def update_dashboards(
    project: projectdata.Project,
    redash_client: RedashClient,
    redash_data: redashdata.RedashData,
    dashboards_filter: Optional[set[str]],
    write: bool,
) -> None:
    created = 0
    updated = 0
    errors = 0

    renderer = redashdata.RedashTemplateRenderer(
        project.data.metric_dfns,
        project.data.metric_types,
        ReferenceResolver(project.id),
    )

    for dashboard in redash_data.iter_named(dashboards_filter):
        logging.info(f"Processing dashboard: {dashboard.name}")

        for query in dashboard.queries:
            try:
                parameters = renderer.render_parameters(dashboard, query)
                rendered_sql = renderer.render_query(
                    dashboard,
                    query,
                    ParameterResolver(
                        set(parameters.keys() if parameters is not None else [])
                    ),
                )
                if query.metadata.id:
                    update_required = update_query(
                        redash_client,
                        query,
                        parameters,
                        rendered_sql,
                        write,
                    )
                    if update_required:
                        updated += 1
                else:
                    id = create_query(
                        redash_client,
                        query,
                        parameters,
                        rendered_sql,
                        write,
                    )
                    query.metadata.id = id
                    query.update(write)
                    created += 1
            except Exception as e:
                logging.error(
                    f"Failed to deploy query {query.metadata.name}: {e}",
                    exc_info=True,
                )
                raise
                errors += 1

    logging.info(f"Created: {created}\nUpdated: {updated}\nErrors: {errors}")
    if created > 0 and write:
        logging.info("Please commit updates to newly created templates")


class UpdateRedashCommand(Command):
    """Command to deploy Redash query templates"""

    def argument_parser(self) -> argparse.ArgumentParser:
        parser = super().argument_parser()

        parser.add_argument(
            "--redash-api-key",
            default=os.environ.get("REDASH_API_KEY"),
            help="Redash API key (default: REDASH_API_KEY env var)",
        )

        parser.add_argument(
            "--dashboard",
            action="append",
            dest="dashboards",
            help="Specific dashboard(s) to deploy (can be repeated, default: all)",
        )

        return parser

    def main(self, args: argparse.Namespace) -> Optional[int]:
        if not args.redash_api_key:
            logging.error(
                "Redash API key is required. Set REDASH_API_KEY environment variable "
                "or use --redash-api-key argument."
            )
            return 1

        if not args.bq_project_id:
            logging.error("BigQuery project ID is required. Use --bq-project argument.")
            return 1

        config = Config(write=args.write, stage=args.stage)

        client = get_client(args.bq_project_id)
        project = projectdata.load(
            client, args.bq_project_id, args.data_path, set(), config
        )

        redash_data = redashdata.load(args.data_path)

        redash_client = RedashClient(
            args.redash_api_key,
            default_data_source="Telemetry (BigQuery)",
            allow_updates=args.write,
        )
        update_dashboards(
            project,
            redash_client,
            redash_data,
            set(args.dashboards or []),
            args.write,
        )

        return None


main = UpdateRedashCommand()
