import argparse
import logging
import os
from typing import Optional

from .. import projectdata, redashdata
from ..base import ALL_JOBS, Command
from ..bqhelpers import get_client
from ..config import Config
from ..projectdata import lint_templates
from ..commands.update_redash import render_dashboards
from ..update_schema import SchemaCreator


here = os.path.dirname(__file__)


class CheckData(Command):
    def main(self, args: argparse.Namespace) -> Optional[int]:
        # This should be unused
        client = get_client("test")

        project = projectdata.load(
            client,
            args.bq_project_id,
            os.path.normpath(args.data_path),
            set(),
            Config(write=False, stage=False),
        )
        if not lint_templates(
            {item.name for item in ALL_JOBS.values()},
            project.data.templates_by_dataset.values(),
        ):
            logging.error("BigQuery templates lint failed")
            return 1

        try:
            creator = SchemaCreator(project)
            creator.create()
        except Exception as e:
            logging.error(f"Creating schemas failed: {e}")
            return 1

        redash_data = redashdata.load(os.path.normpath(args.data_path))
        failures: list[
            tuple[redashdata.RedashDashboard, redashdata.RedashQueryTemplate, Exception]
        ] = []
        render_dashboards(project, redash_data, set(), failures)
        if len(failures):
            logging.error("Redash templates lint failed")
            return 1

        return None


main = CheckData()
