import os
import shutil
from typing import Dict

import click
import jinja2

from docker_etl.ci_config import update_config
from docker_etl.file_utils import (
    CI_JOB_NAME,
    CI_JOB_TEMPLATE_NAME,
    CI_WORKFLOW_NAME,
    CI_WORKFLOW_TEMPLATE_NAME,
    JOBS_DIR,
    TEMPLATES_DIR,
)


def get_templates() -> Dict[str, str]:
    """Get mappings of template name to template path."""
    return {
        name: os.path.join(TEMPLATES_DIR, name)
        for name in os.listdir(TEMPLATES_DIR)
        if os.path.isdir(os.path.join(TEMPLATES_DIR, name))
    }


def add_ci_config(job_name: str, template_dir: str):
    """Create job CI configs in job directory."""
    template_loader = jinja2.FileSystemLoader(template_dir)
    template_env = jinja2.Environment(loader=template_loader)

    try:
        ci_job_template = template_env.get_template(os.path.join(CI_JOB_TEMPLATE_NAME))
        ci_workflow_template = template_env.get_template(
            os.path.join(CI_WORKFLOW_TEMPLATE_NAME)
        )
    except jinja2.exceptions.TemplateNotFound:
        raise FileNotFoundError(
            f"Both {CI_JOB_TEMPLATE_NAME} and "
            f"{CI_WORKFLOW_TEMPLATE_NAME} must be in {template_dir}"
        )

    ci_job_text = ci_job_template.render(job_name=job_name)
    ci_workflow_text = ci_workflow_template.render(job_name=job_name)

    with open(os.path.join(JOBS_DIR, job_name, CI_JOB_NAME), "w") as f:
        f.write(ci_job_text)
    with open(os.path.join(JOBS_DIR, job_name, CI_WORKFLOW_NAME), "w") as f:
        f.write(ci_workflow_text)


def copy_job_template(job_name: str, template_dir: str):
    """Copy job template files to jobs directory."""
    try:
        new_dir = shutil.copytree(
            src=os.path.join(template_dir, "job"),
            dst=os.path.join(JOBS_DIR, job_name),
        )
    except FileExistsError:
        raise ValueError(f"Job with name {job_name} already exists.")

    # rename module and change setup.py
    if template_dir.endswith("/python"):
        shutil.move(
            src=os.path.join(new_dir, "python_template_job"),
            dst=os.path.join(new_dir, job_name.replace("-", "_")),
        )


@click.command()
@click.option("--job-name", required=True)
@click.option("--template")
def create_job(job_name: str, template: str):
    if template is None:
        template = "default"

    valid_templates = get_templates()

    if template not in valid_templates:
        raise ValueError(
            f"Unrecognized template name {template}.  Valid "
            f"template names: {', '.join(valid_templates.keys())}"
        )

    template_dir = valid_templates[template]
    copy_job_template(job_name, template_dir)

    add_ci_config(job_name, template_dir)
    update_config()


if __name__ == "__main__":
    create_job()
