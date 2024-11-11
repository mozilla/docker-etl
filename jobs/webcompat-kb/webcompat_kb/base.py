import argparse
import re
from abc import ABC, abstractmethod
from typing import Any, MutableMapping

import google.auth
from google.cloud import bigquery

# In the following we assume ascii-only characters for now. That's perhaps wrong,
# but it covers everything we're currently using.

# See https://cloud.google.com/resource-manager/docs/creating-managing-projects#before_you_begin
VALID_PROJECT_ID = re.compile(r"^[a-z](?:[a-z0-9\-]){4,28}[a-z0-9]$")
# See https://cloud.google.com/bigquery/docs/datasets#dataset-naming
VALID_DATASET_ID = re.compile(r"^[a-zA-Z_0-9]{1,1024}$")

# This is automatically populated when EtlJob subclasses are defined
ALL_JOBS: MutableMapping[str, type["EtlJob"]] = {}


class EtlJob(ABC):
    name: str

    def __init_subclass__(cls, **kwargs: Any) -> None:
        # Populates ALL_JOBS when a subclass is defined.
        super().__init_subclass__(**kwargs)
        ALL_JOBS[cls.name] = cls

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        pass

    def set_default_args(
        self, parser: argparse.ArgumentParser, args: argparse.Namespace
    ) -> None:
        pass

    @abstractmethod
    def main(self, client: bigquery.Client, args: argparse.Namespace) -> None:
        pass


def get_client(bq_project_id: str) -> bigquery.Client:
    credentials, _ = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/bigquery",
        ]
    )

    return bigquery.Client(credentials=credentials, project=bq_project_id)
