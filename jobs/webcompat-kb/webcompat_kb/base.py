import argparse
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
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


def project_arg(value: str) -> str:
    if not VALID_PROJECT_ID.match(value):
        raise ValueError(f"{value} is not a valid project id")
    return value


def dataset_arg(value: str) -> str:
    if not VALID_DATASET_ID.match(value):
        raise ValueError(f"{value} is not a valid dataset id")
    return value


@dataclass
class Config:
    write: bool
    stage: bool


@dataclass
class Context:
    args: argparse.Namespace
    bq_client: BigQuery
    config: Config
    jobs: list["EtlJob"]


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

    def required_args(self) -> set[str | tuple[str, str]]:
        return set()

    @abstractmethod
    def default_dataset(self, context: Context) -> str: ...

    @abstractmethod
    def main(self, context: Context) -> None:
        pass
