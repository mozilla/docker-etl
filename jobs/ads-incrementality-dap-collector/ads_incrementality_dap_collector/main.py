from google.cloud import bigquery
import click
import datetime
import logging

from helpers import get_experiment, collect_dap_result, create_bq_table_if_not_exists, create_bq_row, insert_into_bq
from models import IncrementalityBranchData

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

    # TODO: Put single try catch around this to catch and log errors thrown from helpers
    for experiment_slug in experiment_slugs:
        logging.info(f"Processing experiment: {experiment_slug}")
        experiment = get_experiment(experiment_slug)
        logging.debug(f"Succeeded fetching experiment json: {experiment}")

        tasks_to_process = {}

        for branch in experiment.branches:
            logging.info(f"Processing branch {branch.slug}")
            features = branch.features

            for feature in features:
                if feature.get("featureId") == "dapTelemetry":
                    visit_counting_experiment_list = feature.get("value").get("visitCountingExperimentList")
                    for visit_counting_list_item in visit_counting_experiment_list:
                        incrementality = IncrementalityBranchData(experiment, branch.slug, visit_counting_list_item)
                        task_id = incrementality.task_id

                        if task_id not in tasks_to_process:
                            tasks_to_process[task_id] = {}
                        tasks_to_process[task_id][incrementality.bucket] = incrementality

            logging.info(f"Succeeded processing branch {branch.slug}, prepared result: {tasks_to_process[task_id]}")

        unique_tasks = list(dict.fromkeys(tasks_to_process))
        for task_id in unique_tasks:
            logging.info(f"Collecting DAP task: {task_id}")
            results = tasks_to_process[task_id]
            # - Need to collect once per task, not bucket.
            # - For now just grabbing the first experiment branch's veclen,
            # as I think it's specified per task, just stored in each branch.
            task_veclen = list(results.values())[0].task_veclen
            collected = collect_dap_result(task_id, task_veclen, hpke_token, hpke_config,
                                           hpke_private_key, batch_start, batch_duration)
            for bucket in results.keys():
                tasks_to_process[task_id][bucket].value_count = collected[bucket]

        records = [v for inner in tasks_to_process.values() for v in inner.values()]

        logging.info(f"Inserting records: {records}")

        bq_client = bigquery.Client(project=gcp_project)
        full_table_id = create_bq_table_if_not_exists(gcp_project, bq_namespace, bq_table, bq_client)

        for record in records:
            row = create_bq_row(start_date=datetime.date.today().isoformat(), end_date=datetime.date.today().isoformat(),
                             country_codes=record.country_codes, advertiser=record.advertiser,
                             experiment_slug=record.experiment_slug, experiment_branch=record.branch,
                             metric=record.metric,
                             value_count=record.value_count)
            insert_into_bq(row, bq_client, full_table_id)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
