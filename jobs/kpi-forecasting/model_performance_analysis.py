import yaml

from kpi_forecasting.inputs import CLI
from kpi_forecasting.results_processing import ModelPerformanceAnalysis


def main() -> None:
    config_file = CLI().args.config
    with open(config_file, "rb") as infile:
        config_data = yaml.safe_load(infile)
    performance_analsis_pull = ModelPerformanceAnalysis(**config_data)
    performance_analsis_pull.write()


if __name__ == "__main__":
    main()
