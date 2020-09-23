import os
from typing import List, Set, Tuple

from docker_etl.file_utils import CI_JOB_NAME, CI_WORKFLOW_NAME, get_job_dirs

REQUIRED_FILES = {CI_JOB_NAME, CI_WORKFLOW_NAME, "README.md", "Dockerfile"}


def check_missing_files() -> List[Tuple[str, Set[str]]]:
    """Check all job directories for missing files."""
    failed_jobs = []
    for job_dir in get_job_dirs():
        files = {
            content
            for content in os.listdir(job_dir)
            if os.path.isfile(os.path.join(job_dir, content))
        }
        missing_files = REQUIRED_FILES - files

        if len(missing_files) > 0:
            failed_jobs.append(
                (os.path.basename(os.path.dirname(job_dir)), missing_files)
            )
            print(
                f"{os.path.basename(os.path.dirname(job_dir))}"
                f" missing files: {', '.join(missing_files)}"
            )

    return failed_jobs


if __name__ == "__main__":
    exit(len(check_missing_files()))
