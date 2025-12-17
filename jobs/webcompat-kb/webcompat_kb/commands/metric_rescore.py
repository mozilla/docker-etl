import argparse
import logging
import os
import re
from typing import Callable, Mapping, Optional

from .. import projectdata
from ..base import Command
from ..bqhelpers import BigQuery, DatasetId, SchemaId, SchemaType, get_client
from ..config import Config
from ..metrics import rescores
from ..metric_rescore import conditional_metrics
from ..metrics.rescores import Rescore
from ..projectdata import Project, SchemaMetadata


def copy_view_template(
    project: Project,
    schema_id: SchemaId,
    new_schema_id: SchemaId,
    schema_ids_map: Mapping[SchemaId, SchemaId],
    write: bool,
) -> None:
    template_rewrite = rewrite_refs(
        schema_id.dataset_id, new_schema_id.dataset_id, schema_ids_map
    )
    copy_template(
        project,
        SchemaType.view,
        schema_id,
        new_schema_id,
        write,
        template_rewrite=template_rewrite,
    )


def copy_template(
    project: Project,
    schema_type: SchemaType,
    schema_id: SchemaId,
    new_schema_id: SchemaId,
    write: bool,
    template_rewrite: Optional[Callable[[str], str]] = None,
) -> None:
    logging.info(f"Creating template for {schema_type} {new_schema_id}")
    current_template = project.data.templates_by_dataset.get_schema_template(
        schema_type, schema_id
    )

    new_metadata = current_template.metadata.model_copy(
        update={"name": new_schema_id.name}
    )
    new_template_data = current_template.template
    if template_rewrite is not None:
        new_template_data = template_rewrite(new_template_data)

    if schema_type == SchemaType.view:
        project.data.add_view(new_schema_id, new_metadata, new_template_data, write)
    elif schema_type == SchemaType.routine:
        project.data.add_routine(new_schema_id, new_metadata, new_template_data, write)
    else:
        raise ValueError(f"Can't create template for type {schema_type}")


def validate_rescore(project: Project, bq_client: BigQuery, rescore: Rescore) -> bool:
    # TODO: check that table names are available in the project and the schema
    return True


def rewrite_refs(
    source_dataset: DatasetId,
    dest_dataset: DatasetId,
    update_schema_ids: Mapping[SchemaId, SchemaId],
) -> Callable[[str], str]:
    """Rewrite the ref() calls in a jinja template so they are correct after
    moving the template and changing referenced schema.

    :param source_dataset: - The dataset for the source (pre-rewrite) template
    :param dest_dataset: - The dataset for the destination (post-rewrite) template
    :param update_schema_ids: - Mapping from old schema id to new schema id for
                                schema ids to update.
    :returns: - Function that takes a template string and returns an updated
                template string.
    """
    ref_re = re.compile(r"{{ *ref\((['\"][^\"']*['\"])\) *}}", re.MULTILINE)

    def replace_ref(re_match: re.Match) -> str:
        ref = re_match.group(1)
        if ref[0] != ref[-1]:
            # We expect matching quotes at the start and end
            logging.warning(f"In template rewrite expected reference, got {ref}")
            return ref

        quote_char = ref[0]
        id_str = ref[1:-1]
        schema_id = SchemaId.from_str(
            id_str, source_dataset.project, source_dataset.dataset
        )
        if schema_id in update_schema_ids:
            new_schema = update_schema_ids[schema_id]
        else:
            new_schema = schema_id
        return f"{{{{ ref({quote_char}{new_schema.relative_string(dest_dataset)}{quote_char}) }}}}"

    def rewrite(query: str) -> str:
        return ref_re.sub(replace_ref, query)

    return rewrite


def copy_schema_templates(
    project: Project,
    routine_ids_map: Mapping[SchemaId, SchemaId],
    view_ids_map: Mapping[SchemaId, SchemaId],
    write: bool,
) -> None:
    """Copy templates for views and routines.

    :param routine_ids_map: - Mapping between source routine ids and destination routine ids
    :param view_ids_map: - Mapping between source view ids and destination view ids
    """
    for schema_id, new_schema_id in routine_ids_map.items():
        copy_template(project, SchemaType.routine, schema_id, new_schema_id, write)

    for schema_id, new_schema_id in view_ids_map.items():
        copy_view_template(project, schema_id, new_schema_id, routine_ids_map, write)


def create_delta_template(
    project: Project,
    kb_dataset: DatasetId,
    rescore: Rescore,
    scored_site_reports: SchemaId,
    write: bool,
) -> None:
    """Create a schema template for the diff between the scored site reports after a rescore and the
    existing scored site reports."""

    schema_id = rescore.delta_schema_id(kb_dataset)
    new_scored_site_reports = rescore.staging_schema_id(
        SchemaType.view, scored_site_reports
    )

    score_deltas = {"all": 0}
    scores_query_fields = ["number"]
    query_fields = ["number"]

    score_types = ["old", "new"]

    for score_type in score_types:
        src_table = f"{score_type}_scored_site_reports"
        field_name = f"{score_type}_score"
        scores_query_fields.append(f"{src_table}.score AS {field_name}")
        query_fields.append(field_name)

    for metric in conditional_metrics(project):
        score_deltas[metric.name] = 0
        for score_type, field_name in [
            ("old", metric.is_old_field),
            ("new", metric.is_new_field),
        ]:
            src_table = f"{score_type}_scored_site_reports"
            scores_query_fields.append(f"{metric.condition(src_table)} AS {field_name}")
            query_fields.append(field_name)

    query_fields.append("new_score - old_score AS delta")

    template = f"""
with scores as (
  SELECT
    {",\n    ".join(scores_query_fields)}
  FROM `{{{{ ref('{new_scored_site_reports.relative_string(kb_dataset)}') }}}}` as new_scored_site_reports
  FULL OUTER JOIN `{{{{ ref('{scored_site_reports.relative_string(kb_dataset)}') }}}}` AS old_scored_site_reports USING(number)
  WHERE new_scored_site_reports.resolution = ""
)
SELECT
  {",\n  ".join(query_fields)}
FROM scores
"""
    metadata = SchemaMetadata(
        name=schema_id.name, description=f"Score delta for rescore {schema_id.name}"
    )
    project.data.add_view(schema_id, metadata, template, write)


def create_staging_schemas(
    project: Project, kb_dataset: DatasetId, rescore: Rescore, write: bool
) -> None:
    routine_ids_map = rescore.staging_routine_ids()
    scored_site_reports = SchemaId(
        project.id, "webcompat_knowledge_base", "scored_site_reports"
    )
    new_scored_site_reports = rescore.staging_schema_id(
        SchemaType.view, scored_site_reports
    )
    copy_schema_templates(
        project, routine_ids_map, {scored_site_reports: new_scored_site_reports}, write
    )
    create_delta_template(project, kb_dataset, rescore, scored_site_reports, write)


def create_archive_schemas(
    project: Project, kb_dataset: DatasetId, rescore: Rescore, write: bool
) -> None:
    routine_ids_map = rescore.archive_routine_ids()
    scored_site_reports = SchemaId(
        project.id, "webcompat_knowledge_base", "scored_site_reports"
    )
    new_scored_site_reports = rescore.archive_schema_id(
        SchemaType.view, scored_site_reports
    )
    copy_schema_templates(
        project, routine_ids_map, {scored_site_reports: new_scored_site_reports}, write
    )


def update_prod_schemas(
    project: Project, kb_dataset: DatasetId, rescore: Rescore, write: bool
) -> None:
    """Copy staging schemas to the canonical locations"""
    routine_ids_map = {
        staging_schema_id: schema_id
        for schema_id, staging_schema_id in rescore.staging_routine_ids().items()
    }
    scored_site_reports = SchemaId(
        project.id, "webcompat_knowledge_base", "scored_site_reports"
    )
    staging_scored_site_reports = rescore.staging_schema_id(
        SchemaType.view, scored_site_reports
    )
    copy_schema_templates(
        project,
        routine_ids_map,
        {staging_scored_site_reports: scored_site_reports},
        write,
    )


def remove_archive_schemas(project: Project, rescore: Rescore, write: bool) -> None:
    for routine_id in rescore.staging_routine_ids().values():
        project.data.remove_routine(routine_id, write)
    scored_site_reports = SchemaId(
        project.id, "webcompat_knowledge_base", "scored_site_reports"
    )
    staging_scored_site_reports = rescore.staging_schema_id(
        SchemaType.view, scored_site_reports
    )
    project.data.remove_view(staging_scored_site_reports, write)


def create_schemas(
    project: Project,
    bq_client: BigQuery,
    data_path: os.PathLike,
    kb_dataset: DatasetId,
    name: str,
    reason: str,
    routine_updates: list[str],
    write: bool,
) -> Optional[Rescore]:
    """Create the schema templates for a pending rescore

    This copies the existing schema templates for scored_site_reports and a list of
    routines that will be updated as part of the rescore to provisional names."""
    routine_schema_ids = [
        SchemaId.from_str(name, kb_dataset.project, kb_dataset.dataset)
        for name in routine_updates
    ]
    rescore_dfns = rescores.load(data_path, kb_dataset)
    if name in rescore_dfns:
        logging.error(f"Can't create rescore with name {name}, it already exists")
        return None

    rescore = rescores.Rescore(
        name, reason, routine_updates=routine_schema_ids, stage=True
    )

    if not validate_rescore(project, bq_client, rescore):
        return None

    create_staging_schemas(project, kb_dataset, rescore, write)
    rescores.update(data_path, rescore, write)
    return rescore


def prepare_deploy(
    project: Project,
    bq_client: BigQuery,
    data_path: os.PathLike,
    kb_dataset: DatasetId,
    rescore_name: str,
    write: bool,
) -> Optional[Rescore]:
    """Create the schema templates for deploying a rescore

    This copies the existing scored_site_reports and any updated routines to
    an archive location, and then replaces the canonical scored site reports with
    the updated versions from after the rescore."""
    rescore_dfns = rescores.load(data_path, kb_dataset)
    staged_rescores = {name: item for name, item in rescore_dfns.items() if item.stage}

    rescore = staged_rescores.get(rescore_name)
    if rescore is None:
        logging.error(f"Can't find staged rescore called {rescore_name}")
        return None

    if not validate_rescore(project, bq_client, rescore):
        return None

    create_archive_schemas(project, kb_dataset, rescore, write)
    update_prod_schemas(project, kb_dataset, rescore, write)
    remove_archive_schemas(project, rescore, write)
    rescore.stage = False
    rescores.update(data_path, rescore, write)
    return rescore


class MetricRescore(Command):
    def argument_parser(self) -> argparse.ArgumentParser:
        parser = super().argument_parser()
        parser.add_argument(
            "phase", action="store", choices=["create-schemas", "prepare-deploy"]
        )
        parser.add_argument(
            "name",
            action="store",
            help="Name of the update",
        )
        parser.add_argument(
            "--reason",
            action="store",
            help="Description of reason for updating the score",
        )
        parser.add_argument(
            "--routine",
            dest="routines",
            action="append",
            help="Routine to update",
        )
        return parser

    def main(self, args: argparse.Namespace) -> Optional[int]:
        if args.new_scored_site_reports is None:
            raise ValueError("Missing --new-scored-site-reports")

        if args.reason is None:
            raise ValueError("Missing --reason")

        config = Config(write=args.write, stage=args.stage)

        client = get_client(args.bq_project_id)
        project = projectdata.load(
            client, args.bq_project_id, args.data_path, set(), config
        )

        kb_dataset = project["webcompat_knowledge_base"].id

        bq_client = BigQuery(
            client,
            kb_dataset,
            args.write,
            set(),
        )

        rescore = None
        if args.phase == "create-schemas":
            rescore = create_schemas(
                project,
                bq_client,
                args.data_path,
                kb_dataset,
                args.name,
                args.reason,
                args.routines,
                args.write,
            )
        elif args.phase == "prepare-deploy":
            rescore = prepare_deploy(
                project, bq_client, args.data_path, kb_dataset, args.name, args.write
            )

        return 0 if rescore is not None else 1


main = MetricRescore()
