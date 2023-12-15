import click
import logging
from glean_metrics import run_glean_metrics
from telemetry_probes import run_telemetry_probes
from experiments import run_experiments_metrics


@click.command()
@click.option("--bq_project_id", help="BigQuery project id", required=True)
@click.option("--bq_dataset_id", help="BigQuery dataset id", required=True)
@click.option("--run_glean", is_flag=True, default=False)
@click.option("--run_telemetry", is_flag=True, default=False)
@click.option("--run_experiments", is_flag=True, default=False)
def main(bq_project_id, bq_dataset_id, run_glean, run_telemetry, run_experiments):
    if run_glean:
        run_glean_metrics(bq_project_id, bq_dataset_id)
    if run_telemetry:
        run_telemetry_probes(bq_project_id, bq_dataset_id)
    if run_experiments:
        run_experiments_metrics(bq_project_id, bq_dataset_id)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
