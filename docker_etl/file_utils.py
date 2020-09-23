import glob
import os
from typing import List

CI_JOB_NAME = "ci_job.yaml"
CI_WORKFLOW_NAME = "ci_workflow.yaml"
CI_JOB_TEMPLATE_NAME = "ci_job.template.yaml"
CI_WORKFLOW_TEMPLATE_NAME = "ci_workflow.template.yaml"

ROOT_DIR = os.path.join(os.path.dirname(__file__), "..")
TEMPLATES_DIR = os.path.join(ROOT_DIR, "templates")
JOBS_DIR = os.path.join(ROOT_DIR, "jobs")


def find_file_in_jobs(filename, recursive=False) -> List[str]:
    """Find all files in job directories matching the given filename."""
    return glob.glob(os.path.join(JOBS_DIR, "*", filename), recursive=recursive)


def get_job_dirs() -> List[str]:
    """Get absolute paths of every directory in the jobs directory"""
    return glob.glob(os.path.join(JOBS_DIR, "*", ""))
