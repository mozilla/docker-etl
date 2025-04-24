import argparse
import re
from abc import ABC, abstractmethod
from typing import Any, MutableMapping

from .bqhelpers import BigQuery

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
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None: ...

    def set_default_args(
        self, parser: argparse.ArgumentParser, args: argparse.Namespace
    ) -> None: ...

    @abstractmethod
    def default_dataset(self, args: argparse.Namespace) -> str: ...

    @abstractmethod
    def main(self, bq_client: BigQuery, args: argparse.Namespace) -> None:
        pass
