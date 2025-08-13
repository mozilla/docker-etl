import ast
import datetime
import logging
import requests
import subprocess
import time

from google.cloud import bigquery
from typing import Optional

from constants import COLLECTOR_RESULTS_SCHEMA, EXPERIMENTER_API_URL_V6, DAP_LEADER, process_timeout, VDAF
from models import BQConfig, DAPConfig, IncrementalityBranchResultsRow, NimbusExperiment

# Nimbus Experimenter helper functions
def get_experiment(experiment_slug: str) -> Optional[NimbusExperiment]:
    """Fetch the experiment from Experimenter API and return the configuration."""
    try:
        nimbus_experiments_json = fetch(f"{EXPERIMENTER_API_URL_V6}{experiment_slug}/")
        nimbus_experiment = NimbusExperiment.from_dict(nimbus_experiments_json)
        return nimbus_experiment
    except Exception as e:
        raise Exception(f"Failed loading experiment {experiment_slug} from {EXPERIMENTER_API_URL_V6}", e)

def prepare_results_rows(experiment: NimbusExperiment) -> dict:
    """Pull info out of the experiment metadata to set up experiment branch results rows. The info
        here will be used to call DAP and get results data for each branch, and  ultimately written
        to BQ."""
    tasks_to_process = {}
    for branch in experiment.branches:
        logging.info(f"Processing experiment branch: {branch.slug}")
        dap_telemetry_features = [f for f in branch.features if f.get("featureId") == "dapTelemetry"]

        for feature in dap_telemetry_features:
            logging.info(f"Processing dapTelemetry experiment feature: {feature}")
            visit_counting_experiment_list = feature.get("value").get("visitCountingExperimentList")

            for visit_counting_list_item in visit_counting_experiment_list:
                incrementality = IncrementalityBranchResultsRow(experiment, branch.slug, visit_counting_list_item)
                task_id = incrementality.task_id

                if task_id not in tasks_to_process:
                    tasks_to_process[task_id] = {}
                tasks_to_process[task_id][incrementality.bucket] = incrementality
                logging.info(f"Prepared intermediate result rows: {tasks_to_process[task_id]}")

        logging.info(f"Finished processing experiment branch: {branch.slug}.")
    return tasks_to_process

# DAP helper functions
def collect_dap_result(task_id: str, vdaf_length: int, hpke_token: str, hpke_config: str, hpke_private_key: str, batch_start: int, duration: int) -> dict:
    command_str = (f"./collect --task-id {task_id} --leader {DAP_LEADER} --vdaf {VDAF} --length {vdaf_length} "
                   f"--authorization-bearer-token {hpke_token} --batch-interval-start {batch_start} "
                   f"--batch-interval-duration {duration} --hpke-config {hpke_config} "
                   f"--hpke-private-key {hpke_private_key}")
    logging.debug(f"command_str: {command_str}")
    logging.info(f"Processing batch_start: {batch_start} for duration: {duration}")
    try:

        result = subprocess.run(["./collect", "--task-id", task_id, "--leader", DAP_LEADER, "--vdaf", VDAF,
                                 "--length", f"{vdaf_length}", "--authorization-bearer-token", hpke_token,
                                 "--batch-interval-start", f"{batch_start}", "--batch-interval-duration", f"{duration}",
                                 "--hpke-config", hpke_config, "--hpke-private-key", hpke_private_key],
                                capture_output=True,
                                text=True,
                                check=True,
                                timeout=process_timeout)

        for line in result.stdout.splitlines():
            if line.startswith("Aggregation result:"):
                entries = parse_histogram(line[21:-1])
                return entries
    except subprocess.CalledProcessError as e:
        logging.error(f"Collection failed for {task_id}, {e.returncode}, stderr: {e.stderr}")
    except subprocess.TimeoutExpired as e:
        logging.error(f"Collection timed out for {task_id}, {e.timeout}, stderr: {e.stderr}")

def collect_dap_results(tasks_to_collect: dict, config: DAPConfig):
    tasks = list(dict.fromkeys(tasks_to_collect))
    logging.info(f"Starting DAP collection for tasks: {tasks}.")
    for task_id in tasks:
        logging.info(f"Collecting DAP task: {task_id}")
        results = tasks_to_collect[task_id]
        # - Need to collect once per task, not bucket.
        # - For now just grabbing the first experiment branch's veclen,
        # as I think it's specified per task, just stored in each branch.
        task_veclen = list(results.values())[0].task_veclen
        collected = collect_dap_result(task_id, task_veclen, config.hpke_token, config.hpke_config,
                                       config.hpke_private_key, config.batch_start, config.batch_duration)
        for bucket in results.keys():
            tasks_to_collect[task_id][bucket].value_count = collected[bucket]
        logging.info(f"Prepared final result rows: {tasks_to_collect[task_id]}")
        logging.info(f"Finished collecting DAP task: {task_id}")
    logging.info("Finished DAP collection for all tasks.")

# TODO Trigger Airflow errors
def correct_wraparound(num: int) -> int:
    field_prime = 340282366920938462946865773367900766209
    field_size = 128
    cutoff = 2 ** (field_size - 1)
    if num > cutoff:
        logging.info(f"Corrected {num} to {num - field_prime} ")
        return num - field_prime
    return num


def parse_histogram(histogram_str: str) -> dict:
    parsed_list = ast.literal_eval(histogram_str)
    return {i: correct_wraparound(val) for i, val in enumerate(parsed_list)}


# BigQuery helper functions
def create_bq_table_if_not_exists(project:str, namespace:str, table: str, bq_client: bigquery.Client):
    data_set = f"{project}.{namespace}"
    bq_client.create_dataset(data_set, exists_ok=True)

    full_table_id = f"{data_set}.{table}"
    table = bigquery.Table(full_table_id, schema=COLLECTOR_RESULTS_SCHEMA)

    try:
        bq_client.create_table(table, exists_ok=True)
        return full_table_id
    except Exception as e:
        raise Exception(f"Failed to create table: {full_table_id} {e}")

def create_bq_row(start_date: str, end_date: str, country_codes: str, experiment_slug: str, experiment_branch: str, advertiser: str,
               metric: str, value_histogram: str = None, value_count: int = None) -> dict:
    row = {"start_date": start_date, "end_date": end_date, "country_codes": country_codes,
           "experiment_slug": experiment_slug, "experiment_branch": experiment_branch, "advertiser": advertiser,
           "metric": metric, "value": {"count": value_count, "histogram": value_histogram}}
    return row

def insert_into_bq(row, bqclient, table_id: str):
    """Inserts the results into BQ. Assumes that they are already in the right format"""
    if row:
        insert_res = bqclient.insert_rows_json(table=table_id, json_rows=[row])
        if len(insert_res) != 0:
            raise Exception(f"Error inserting rows into {table_id}: {insert_res}")

def write_results_to_bq(collected_tasks: dict, config: BQConfig):
    records = [v for inner in collected_tasks.values() for v in inner.values()]
    logging.info(f"Inserting results rows into BQ: {records}")
    bq_client = bigquery.Client(project=config.project)
    full_table_id = create_bq_table_if_not_exists(config.project, config.namespace, config.table, bq_client)
    for record in records:
        row = create_bq_row(start_date=datetime.date.today().isoformat(), end_date=datetime.date.today().isoformat(),
                         country_codes=record.country_codes, advertiser=record.advertiser,
                         experiment_slug=record.experiment_slug, experiment_branch=record.branch,
                         metric=record.metric,
                         value_count=record.value_count)
        insert_into_bq(row, bq_client, full_table_id)
    logging.info("Finished inserting results rows into BQ.")

# General helper functions
def fetch(url: str):
    for _ in range(2):
        try:
            return requests.get(
                url,
                timeout=30,
                headers={"user-agent": "https://github.com/mozilla/docker-etl"},
            ).json()
        except Exception as e:
            last_exception = e
            time.sleep(1)
    raise last_exception
