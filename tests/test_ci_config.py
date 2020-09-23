import os
from unittest import TestCase
from unittest.mock import patch

from docker_etl import ci_config


class TestCiConfig(TestCase):
    @patch(
        "docker_etl.file_utils.JOBS_DIR",
        os.path.join(os.path.dirname(__file__), "test_jobs"),
    )
    @patch(
        "docker_etl.ci_config.CI_DIR",
        os.path.join(os.path.dirname(__file__), "test_ci"),
    )
    def test_missing_files_found(self):
        ci_config_text = ci_config.update_config(dry_run=False)
        self.assertTrue(ci_config_text.startswith(ci_config.CI_CONFIG_HEADER))
        self.assertTrue("\n  test_job_1_job\n" in ci_config_text)
        self.assertTrue("\n  test_job_2_job\n" in ci_config_text)
        self.assertTrue("\n  test_job_2_workflow\n" in ci_config_text)
