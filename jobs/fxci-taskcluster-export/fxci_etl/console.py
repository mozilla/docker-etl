import sys
from pathlib import Path

from cleo.application import Application
from cleo.commands.command import Command
from cleo.helpers import option

from fxci_etl.config import Config
from fxci_etl.metric.export import export_metrics
from fxci_etl.pulse.consume import drain

APP_NAME = "fxci-etl"


class ConfigCommand(Command):
    options = [
        option("--config", description="Path to config file to use.", default=None)
    ]

    def parse_config(self, config_path: str | Path | None) -> Config:
        if config_path:
            return Config.from_file(config_path)
        return Config.from_env()


class PulseDrainCommand(ConfigCommand):
    name = "pulse drain"
    description = "Process events in the pulse queues and exit."

    def handle(self):
        config = self.parse_config(self.option("config"))

        for queue in config.pulse.queues:
            self.line(f"Draining queue {queue}")
            drain(config, queue)
        return 0


class MetricExportCommand(ConfigCommand):
    name = "metric export"
    description = "Export configured metrics' timeseries."
    options = ConfigCommand.options + [
        option(
            "--dry-run",
            flag=True,
            description="Print records rather than inserting them into BigQuery",
        )
    ]

    def handle(self):
        config = self.parse_config(self.option("config"))
        return export_metrics(config, dry_run=self.option("dry-run"))


def run():
    application = Application()
    application.add(PulseDrainCommand())
    application.add(MetricExportCommand())
    application.run()


if __name__ == "__main__":
    sys.exit(run())
