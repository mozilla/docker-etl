import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, Optional

import dacite


@dataclass(frozen=True)
class PulseExchangeConfig:
    exchange: str
    routing_key: str


@dataclass(frozen=True)
class PulseConfig:
    user: str
    password: str
    host: str = "pulse.mozilla.org"
    port: int = 5671
    queues: ClassVar[dict[str, PulseExchangeConfig]] = {
        "task-completed": PulseExchangeConfig(
            exchange="exchange/taskcluster-queue/v1/task-completed",
            routing_key="#",
        ),
        "task-failed": PulseExchangeConfig(
            exchange="exchange/taskcluster-queue/v1/task-failed",
            routing_key="#",
        ),
        "task-exception": PulseExchangeConfig(
            exchange="exchange/taskcluster-queue/v1/task-exception",
            routing_key="#",
        ),
    }


@dataclass(frozen=True)
class BigQueryConfig:
    project: str
    dataset: str
    credentials: Optional[str] = None


@dataclass(frozen=True)
class MonitoringConfig:
    credentials: Optional[str] = None
    projects: ClassVar[list] = [
        "fxci-production-level1-workers",
        "fxci-production-level3-workers",
    ]


@dataclass(frozen=True)
class StorageConfig:
    project: str
    bucket: str
    credentials: Optional[str] = None


@dataclass(frozen=True)
class Config:
    pulse: PulseConfig
    bigquery: BigQueryConfig
    monitoring: MonitoringConfig
    storage: StorageConfig

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Config":
        return dacite.from_dict(data_class=cls, data=data)

    @classmethod
    def from_file(cls, path: str | Path) -> "Config":
        if isinstance(path, str):
            path = Path(path)

        with path.open("rb") as fh:
            return cls.from_dict(tomllib.load(fh))

    @classmethod
    def from_env(cls) -> "Config":
        envs = {
            k[len("FXCI_ETL_") :]: v
            for k, v in os.environ.items()
            if k.startswith("FXCI_ETL_")
        }

        # Map environment variables to a dict
        config_dict = {}
        for key, value in envs.items():
            parts = key.split("_")
            obj = config_dict
            part = parts.pop(0).lower()
            while parts:
                obj.setdefault(part, {})
                obj = obj[part]
                part = parts.pop(0).lower()
            obj[part] = value

        return cls.from_dict(config_dict)
