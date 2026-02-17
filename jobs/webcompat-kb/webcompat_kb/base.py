import argparse
import logging
import re
import pathlib
import os
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, MutableMapping, Optional

from google.auth import exceptions as auth_exceptions

from .bqhelpers import BigQuery, SchemaId
from .config import Config
from .projectdata import Project, TableSchema


here = pathlib.Path(os.path.dirname(__file__))

# In the following we assume ascii-only characters for now. That's perhaps wrong,
# but it covers everything we're currently using.

# See https://cloud.google.com/resource-manager/docs/creating-managing-projects#before_you_begin
VALID_PROJECT_ID = re.compile(r"^[a-z](?:[a-z0-9\-]){4,28}[a-z0-9]$")
# See https://cloud.google.com/bigquery/docs/datasets#dataset-naming
VALID_DATASET_ID = re.compile(r"^[a-zA-Z_0-9]{1,1024}$")

# This is automatically populated when EtlJob subclasses are defined
ALL_JOBS: MutableMapping[str, type["EtlJob"]] = {}

DEFAULT_DATA_DIR = (here / os.pardir / "data").absolute()


def project_arg(value: str) -> str:
    if not VALID_PROJECT_ID.match(value):
        raise ValueError(f"{value} is not a valid project id")
    return value


def dataset_arg(value: str) -> str:
    if not VALID_DATASET_ID.match(value):
        raise ValueError(f"{value} is not a valid dataset id")
    return value


class Command(ABC):
    def argument_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--log-level",
            choices=["debug", "info", "warn", "error"],
            default="info",
            help="Log level",
        )

        parser.add_argument(
            "--bq-project",
            dest="bq_project_id",
            type=project_arg,
            help="BigQuery project id",
        )

        parser.add_argument(
            "--data-path",
            action="store",
            type=pathlib.Path,
            default=DEFAULT_DATA_DIR,
            help="Path to directory containing sql to deploy",
        )

        parser.add_argument(
            "--stage",
            action="store_true",
            help="Write to staging location (currently same project with _test suffix on dataset names)",
        )

        parser.add_argument(
            "--no-write",
            dest="write",
            action="store_false",
            default=True,
            help="Don't write updates to BigQuery",
        )

        parser.add_argument(
            "--github-token",
            default=os.environ.get("GH_TOKEN"),
            help="GitHub token",
        )

        parser.add_argument(
            "--pdb", action="store_true", help="Drop into debugger on execption"
        )
        return parser

    @abstractmethod
    def main(self, args: argparse.Namespace) -> Optional[int]: ...

    def __call__(self) -> None:
        parser = self.argument_parser()
        args = parser.parse_args()

        logging.basicConfig()
        log_level = args.log_level.upper() if "log_level" in args else "INFO"
        logging.getLogger().setLevel(logging.getLevelNamesMapping()[log_level])

        rv: Optional[int] = 1
        try:
            rv = self.main(args)
        except auth_exceptions.RefreshError:
            logging.error("""Reauthentication with Google Cloud required. Please run:
gcloud auth login --enable-gdrive-access --update-adc
""")
            sys.exit(1)
        except Exception:
            if "pdb" in args and args.pdb:
                import pdb

                pdb.post_mortem()
            else:
                raise
        if rv:
            sys.exit(rv)


@dataclass
class Context:
    args: argparse.Namespace
    bq_client: BigQuery
    config: Config
    jobs: list["EtlJob"]
    project: Project


class EtlJob(ABC):
    name: str
    default = True

    def __init_subclass__(cls, **kwargs: Any) -> None:
        # Populates ALL_JOBS when a subclass is defined.
        super().__init_subclass__(**kwargs)
        assert cls.name not in ALL_JOBS, f"Got multiple jobs named {cls.name}"
        ALL_JOBS[cls.name] = cls

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None: ...

    def write_targets(self, project: Project) -> set[SchemaId]:
        rv = set()
        for dataset in project:
            for schema in dataset:
                if isinstance(schema, TableSchema) and self.name in schema.etl_jobs:
                    rv.add(schema.id)
        return rv

    def required_args(self) -> set[str | tuple[str, str]]:
        return set()

    @abstractmethod
    def default_dataset(self, context: Context) -> str: ...

    @abstractmethod
    def main(self, context: Context) -> None:
        pass
