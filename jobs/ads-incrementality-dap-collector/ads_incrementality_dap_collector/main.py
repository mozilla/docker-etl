import click
import logging
import traceback

from constants import LOG_FILE_NAME
from helpers import get_config, get_experiment, prepare_results_rows, collect_dap_results, write_job_logs_to_bucket, write_results_to_bq

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
@click.option(
    "--batch_start",
    type=int,
    envvar='BATCH_START',
    help="Start of the collection interval, as the number of seconds since the Unix epoch",
    required=True,
)
def main(gcp_project, job_config_bucket, hpke_token, hpke_private_key, batch_start):
    try:
        logging.info(f"Starting collector job with configuration from gcs bucket: {job_config_bucket}")
        config = get_config(gcp_project, job_config_bucket, hpke_token, hpke_private_key, batch_start)
        logging.info(f"Starting collector job for experiments: {config.nimbus.experiments}.")

        for experiment_config in config.nimbus.experiments:
            experiment = get_experiment(experiment_config.slug, config.nimbus.api_url)

            tasks_to_collect = prepare_results_rows(experiment)

            collected_tasks = collect_dap_results(tasks_to_collect, config.dap, experiment_config)

            write_results_to_bq(collected_tasks, config.bq)

            write_job_logs_to_bucket(gcp_project, job_config_bucket)
    except Exception as e:
        logging.error(f"{e}\n{traceback.format_exc()}")
        write_job_logs_to_bucket(gcp_project, job_config_bucket)


if __name__ == "__main__":
    logging.basicConfig(
        filename=LOG_FILE_NAME,
        filemode='a',
        format='%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO)
    # logging.getLogger().setLevel(logging.INFO)
    main()
