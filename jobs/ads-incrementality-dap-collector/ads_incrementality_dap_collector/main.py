import click
import logging

from helpers import get_experiment, prepare_results_rows, collect_dap_results, write_results_to_bq
from models import BQConfig, DAPConfig

@click.command()
@click.option("--gcp_project", help="GCP project id", required=True)
@click.option(
    "--bq_namespace",
    help="Namespace/BQ Dataset for all dap aggregations",
    required=True)
@click.option("--bq_table", help="Table for incrementality dap aggregations.", required=True)
@click.option(
    "--hpke_token",
    envvar='DIVVIUP_HPKE_TOKEN',
    help="The token defined in the collector credentials, used to authenticate to the leader",
    required=True,
)
@click.option(
    "--hpke_private_key",
    envvar='DIVVIUP_PRIVATE_KEY',
    help="The private key defined in the collector credentials, used to decrypt shares from the leader and helper",
    required=True,
)
# TODO Since this one isn't private if might be possible to keep in the same file as the experiment slugs?
#  If not a new variable will have to be added to Airflow (unless it already exists).
@click.option(
    "--hpke_config",
    envvar='HPKE_CONFIG',
    help="base64url-encoded version of hpke_config defined in the collector credentials. ",
    required=True,
)
@click.option(
    "--batch_start",
    type=int,
    help="Start of the collection interval, as the number of seconds since the Unix epoch",
    required=True,
)
@click.option(
    "--batch_duration",
    type=int,
    help=" Duration of the collection batch interval, in seconds",
    required=True,
)
@click.option("--experiment_slug", "experiment_slugs",
              help="Experiment to collect.  To specify multiple experiments use multiple --experiment_slug options",
              multiple=True, required=True)
def main(gcp_project, bq_namespace, bq_table, hpke_token, hpke_private_key, hpke_config, batch_start, batch_duration,
         experiment_slugs):
    logging.info(f"Starting collector job for experiments: {experiment_slugs}. Using batch_start: {batch_start} duration: {batch_duration}")

    for experiment_slug in experiment_slugs:
        logging.info(f"Processing experiment: {experiment_slug}")
        experiment = get_experiment(experiment_slug)
        logging.info(f"Fetched experiment json: {experiment}")

        tasks_to_collect = prepare_results_rows(experiment)

        dap_config = DAPConfig(hpke_token, hpke_private_key, hpke_config, batch_start, batch_duration)
        collected_tasks = collect_dap_results(tasks_to_collect, dap_config)

        bq_config = BQConfig(gcp_project, bq_namespace, bq_table)
        write_results_to_bq(collected_tasks, bq_config)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
