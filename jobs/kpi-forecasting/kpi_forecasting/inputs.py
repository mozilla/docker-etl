import argparse
import yaml

from dataclasses import dataclass


@dataclass
class CLI:
    """
    Parse command-line arguments. This parser enables users to pass a named
    `config` argument.
    """

    def __post_init__(self) -> None:
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument(
            "-c", "--config", type=str, help="Path to configuration yaml file"
        )
        self.parser.add_argument(
            "--write",
            type=bool,
            help="If true, write results",
            default=True,
            action=argparse.BooleanOptionalAction,
        )
        self.args = self.parser.parse_args()


def load_yaml(filepath: str) -> dict:
    """
    Create a data structure from a YAML config filepath.
    """
    with open(filepath, "r") as f:
        data = yaml.safe_load(f)
    return data
