import click
import logging
import traceback

from constants import LOG_FILE_NAME
from helpers import (
    get_config,
    get_experiment,
    prepare_results_rows,
    collect_dap_results,
    write_job_logs_to_bucket,
    write_results_to_bq,
)


@click.command()
@click.option(
    "--job_config_gcp_project",
    help="GCP project id for the GCS bucket where this job will look for a configuration file. ",
    required=True,
)
@click.option(
    "--job_config_bucket",
    help="GCS bucket where the configuration for this job can be found. See example_config.json for format details.",
    required=True,
)
@click.option(
    "--auth_token",
    envvar="DAP_AUTH_TOKEN",
    help="The 'token' defined in the collector credentials, used to authenticate to the leader",
    required=True,
)
@click.option(
    "--hpke_private_key",
    envvar="DAP_PRIVATE_KEY",
    help="The 'private_key' defined in the collector credentials, used to decrypt shares from the leader and helper",
    required=True,
)
@click.option(
    "--process_date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Current processing date (ds)",
    required=True,
)
def main(
    job_config_gcp_project,
    job_config_bucket,
    auth_token,
    hpke_private_key,
    process_date,
):
    try:
        logging.info(
            f"Starting collector job with configuration from gcp project: {job_config_gcp_project} "
            f"and gcs bucket: {job_config_bucket} for process date: {process_date}"
        )
        config = get_config(
            job_config_gcp_project, job_config_bucket, auth_token, hpke_private_key
        )
        logging.info(
            f"Starting collector job for experiments: {config.nimbus.experiments}."
        )

        for experiment_config in config.nimbus.experiments:
            experiment = get_experiment(
                experiment_config, config.nimbus.api_url, process_date.date()
            )

            tasks_to_collect = prepare_results_rows(experiment)
            collected_tasks = collect_dap_results(
                tasks_to_collect, config.dap, experiment_config
            )

            write_results_to_bq(collected_tasks, config.bq)
    except Exception as e:
        logging.error(f"Collector job failed. Error: {e}\n{traceback.format_exc()}")
        raise e
    finally:
        write_job_logs_to_bucket(job_config_gcp_project, job_config_bucket)


if __name__ == "__main__":
    logging.basicConfig(
        filename=LOG_FILE_NAME,
        filemode="a",
        format="%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )
    logging.getLogger().setLevel(logging.INFO)
    main()
