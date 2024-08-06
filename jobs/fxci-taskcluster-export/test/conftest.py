from dataclasses import asdict, dataclass
import pytest

from fxci_etl.config import Config


@pytest.fixture
def make_config():
    default_config = {
        "bigquery": {
            "project": "project",
            "dataset": "dataset",
        },
        "monitoring": {},
        "pulse": {
            "user": "user",
            "password": "password",
        },
        "storage": {
            "project": "project",
            "bucket": "bucket",
        },
    }

    def inner(**overrides):
        config = default_config
        config.update(**overrides)
        return Config.from_dict(config)

    return inner
