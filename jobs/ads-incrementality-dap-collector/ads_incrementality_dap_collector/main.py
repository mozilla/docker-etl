import click
import logging

from helpers import get_config, get_experiment, prepare_results_rows, collect_dap_results, write_results_to_bq



@click.command()
@click.option("--gcp_project", help="GCP project id", required=True)
@click.option("--job_config_bucket", help="GCS bucket where the configuration for this job can be found. See example_config.json for format details.", required=True)
@click.option(
    "--hpke_token",
    envvar='DAP_HPKE_TOKEN',
    help="The token defined in the collector credentials, used to authenticate to the leader",
    required=True,
)
@click.option(
    "--hpke_private_key",
    envvar='DAP_PRIVATE_KEY',
    help="The private key defined in the collector credentials, used to decrypt shares from the leader and helper",
    required=True,
)
def main(gcp_project, job_config_bucket, hpke_token, hpke_private_key):
    logging.info(f"Starting collector job with configuration from {job_config_bucket}")
    config = get_config(gcp_project, job_config_bucket, hpke_token, hpke_private_key)
    logging.info(f"Starting collector job for experiments: {config.nimbus.experiment_slugs}.")

    for experiment_slug in config.nimbus.experiment_slugs:
        experiment = get_experiment(experiment_slug, config.nimbus.api_url)

        tasks_to_collect = prepare_results_rows(experiment)

        collected_tasks = collect_dap_results(tasks_to_collect, config.dap)

        write_results_to_bq(collected_tasks, config.bq)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
