import ast
from datetime import datetime
import json
import logging
import requests
import subprocess
import time

from google.cloud import bigquery
from google.cloud import storage
from types import SimpleNamespace
from typing import Optional

from constants import (
    COLLECTOR_RESULTS_SCHEMA,
    CONFIG_FILE_NAME,
    DAP_LEADER,
    DEFAULT_BATCH_DURATION,
    LOG_FILE_NAME,
    PROCESS_TIMEOUT,
    VDAF,
)
from models import (
    IncrementalityBranchResultsRow,
    NimbusExperiment,
)


# Nimbus Experimenter helper functions
def get_experiment(
    experiment_config: SimpleNamespace, api_url: str
) -> Optional[NimbusExperiment]:
    """Fetch the experiment from Experimenter API and return the configuration."""
    logging.info(f"Fetching experiment: {experiment_config.slug}")
    try:
        nimbus_experiments_json = fetch(f"{api_url}/{experiment_config.slug}/")
        if not hasattr(experiment_config, "batch_duration"):
            experiment_config.batch_duration = DEFAULT_BATCH_DURATION
        nimbus_experiments_json["batchDuration"] = experiment_config.batch_duration
        nimbus_experiment = NimbusExperiment.from_dict(nimbus_experiments_json)
        logging.info(f"Fetched experiment json: {experiment_config.slug}")
        return nimbus_experiment
    except Exception as e:
        raise Exception(
            f"Failed getting experiment: {experiment_config.slug} from: {api_url}"
        ) from e


def prepare_results_rows(
    experiment: NimbusExperiment,
) -> dict[str, dict[int, IncrementalityBranchResultsRow]]:
    """Pull info out of the experiment metadata to set up experiment branch results rows. The info
    here will be used to call DAP and get results data for each branch, and ultimately written
    to BQ."""
    tasks_to_process: dict[str, dict[int, IncrementalityBranchResultsRow]] = {}
    if not experiment.collect_today():
        logging.info(
            f"Skipping collection for {experiment.slug} today. \
            Next collection date will be {experiment.next_collect_date()}"
        )
        return tasks_to_process

    for branch in experiment.branches:
        logging.info(f"Processing experiment branch: {branch.slug}")
        dap_telemetry_features = [
            f for f in branch.features if f.get("featureId") == "dapTelemetry"
        ]

        for feature in dap_telemetry_features:
            logging.info(f"Processing dapTelemetry experiment feature: {feature}")
            visit_counting_experiment_list = feature.get("value").get(
                "visitCountingExperimentList"
            )

            for visit_counting_list_item in visit_counting_experiment_list:
                incrementality = IncrementalityBranchResultsRow(
                    experiment, branch.slug, visit_counting_list_item
                )
                task_id = incrementality.task_id

                if task_id not in tasks_to_process:
                    tasks_to_process[task_id] = {}
                tasks_to_process[task_id][incrementality.bucket] = incrementality
                logging.info(
                    f"Prepared intermediate result rows: {tasks_to_process[task_id]}"
                )

        logging.info(f"Finished processing experiment branch: {branch.slug}.")
    return tasks_to_process


# DAP helper functions
def collect_dap_result(
    task_id: str,
    vdaf_length: int,
    batch_start: int,
    duration: int,
    auth_token: str,
    hpke_config: str,
    hpke_private_key: str,
) -> dict:
    # Beware! This command string reveals secrets. Uncomment logging below only for debugging in local dev.
    #
    # command_str = (f"./collect --task-id {task_id} --leader {DAP_LEADER} --vdaf {VDAF} --length {vdaf_length} "
    #                f"--authorization-bearer-token {auth_token} --batch-interval-start {batch_start} "
    #                f"--batch-interval-duration {duration} --hpke-config {hpke_config} "
    #                f"--hpke-private-key {hpke_private_key}")
    # logging.debug(f"command_str: {command_str}")
    logging.info(f"Processing batch_start: {batch_start} for duration: {duration}")
    try:
        result = subprocess.run(
            [
                "./collect",
                "--task-id",
                task_id,
                "--leader",
                DAP_LEADER,
                "--vdaf",
                VDAF,
                "--length",
                f"{vdaf_length}",
                "--authorization-bearer-token",
                auth_token,
                "--batch-interval-start",
                f"{batch_start}",
                "--batch-interval-duration",
                f"{duration}",
                "--hpke-config",
                hpke_config,
                "--hpke-private-key",
                hpke_private_key,
            ],
            capture_output=True,
            text=True,
            check=True,
            timeout=PROCESS_TIMEOUT,
        )
        for line in result.stdout.splitlines():
            if line.startswith("Aggregation result:"):
                entries = parse_histogram(line[21:-1])
                return entries
    # Beware! Exceptions thrown by the subprocess reveal secrets.
    # Log them and include traceback only for debugging in local dev.
    except subprocess.CalledProcessError as e:
        raise Exception(
            f"Collection failed for {task_id}, {e.returncode}, stderr: {e.stderr}"
        ) from None
    except subprocess.TimeoutExpired as e:
        raise Exception(
            f"Collection timed out for {task_id}, {e.timeout}, stderr: {e.stderr}"
        ) from None


def collect_dap_results(
    tasks_to_collect: dict[str, dict[int, IncrementalityBranchResultsRow]],
    config: SimpleNamespace,
    experiment_config: SimpleNamespace,
):
    tasks = list(dict.fromkeys(tasks_to_collect))
    logging.info(f"Starting DAP collection for tasks: {tasks}.")
    for task_id in tasks:
        logging.info(f"Collecting DAP task: {task_id}")
        results = tasks_to_collect[task_id]
        # The task vector length and batch duration are specified per-experiment and
        # stored with each branch. So it's okay to just use the first branch
        # to populate these values for all the tasks here.
        firstBranch = list(results.values())[0]
        task_veclen = firstBranch.task_veclen
        batch_start_epoch = int(
            datetime.combine(firstBranch.batch_start, datetime.min.time()).timestamp()
        )
        batch_duration = firstBranch.batch_duration
        collected = collect_dap_result(
            task_id,
            task_veclen,
            batch_start_epoch,
            batch_duration,
            config.auth_token,
            config.hpke_config,
            config.hpke_private_key,
        )
        try:
            for bucket in results.keys():
                tasks_to_collect[task_id][bucket].value_count = collected[bucket]
        except Exception as e:
            raise Exception(
                f"Failed to parse collected DAP results: {collected}"
            ) from e
        logging.info(f"Prepared final result rows: {tasks_to_collect[task_id]}")
        logging.info(f"Finished collecting DAP task: {task_id}")
    logging.info("Finished DAP collection for all tasks.")
    return tasks_to_collect


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
    # Experiment branches are indexed starting from 1, DAP bucket results from 0,
    # so use i + 1 as the key here when parsing the histogram
    return {i + 1: correct_wraparound(val) for i, val in enumerate(parsed_list)}


# BigQuery helper functions
def create_bq_table_if_not_exists(
    project: str, namespace: str, table: str, bq_client: bigquery.Client
):
    data_set = f"{project}.{namespace}"
    bq_client.create_dataset(data_set, exists_ok=True)
    full_table_id = f"{data_set}.{table}"
    table = bigquery.Table(full_table_id, schema=COLLECTOR_RESULTS_SCHEMA)

    try:
        bq_client.create_table(table, exists_ok=True)
        return full_table_id
    except Exception as e:
        raise Exception(f"Failed to create BQ table: {full_table_id}") from e


def create_bq_row(
    collection_start: str,
    collection_end: str,
    country_codes: str,
    experiment_slug: str,
    experiment_branch: str,
    advertiser: str,
    metric: str,
    value_histogram: str = None,
    value_count: int = None,
) -> dict:
    row = {
        "collection_start": collection_start,
        "collection_end": collection_end,
        "country_codes": country_codes,
        "experiment_slug": experiment_slug,
        "experiment_branch": experiment_branch,
        "advertiser": advertiser,
        "metric": metric,
        "value": {"count": value_count, "histogram": value_histogram},
        "created_at": datetime.now(),
    }
    return row


def insert_into_bq(row, bqclient, table_id: str):
    """Inserts the results into BQ. Assumes that they are already in the right format"""
    if row:
        insert_res = bqclient.insert_rows_json(table=table_id, json_rows=[row])
        if len(insert_res) != 0:
            raise Exception(f"Error inserting rows into {table_id}: {insert_res}")


def write_results_to_bq(collected_tasks: dict, config: SimpleNamespace):
    """Takes the collected results for each experiment branch and writes out rows to BQ."""
    records = [v for inner in collected_tasks.values() for v in inner.values()]
    logging.info(f"Inserting results rows into BQ: {records}")
    bq_client = bigquery.Client(project=config.project)
    full_table_id = create_bq_table_if_not_exists(
        config.project, config.namespace, config.table, bq_client
    )
    for record in records:
        row = create_bq_row(
            collection_start=record.batch_start.isoformat(),
            collection_end=record.batch_end.isoformat(),
            country_codes=record.country_codes,
            advertiser=record.advertiser,
            experiment_slug=record.experiment_slug,
            experiment_branch=record.branch,
            metric=record.metric,
            value_count=record.value_count,
        )
        insert_into_bq(row, bq_client, full_table_id)
    logging.info("Finished inserting results rows into BQ.")


# GCS helper functions
def get_config(
    gcp_project: str, config_bucket: str, auth_token: str, hpke_private_key: str
) -> SimpleNamespace:
    """Gets the incrementality job's config from a file in a GCS bucket. See example_config.json for the structure."""
    client = storage.Client(project=gcp_project)
    try:
        bucket = client.get_bucket(config_bucket)
        blob = bucket.blob(CONFIG_FILE_NAME)
        reader = blob.open("rt")
        config = json.load(reader, object_hook=lambda d: SimpleNamespace(**d))
        config.dap.auth_token = auth_token
        config.dap.hpke_private_key = hpke_private_key
        config.bq.project = gcp_project
        return config
    except Exception as e:
        raise Exception(
            f"Failed to get job config file: {CONFIG_FILE_NAME} from GCS bucket: \
            {config_bucket} in project: {gcp_project}."
        ) from e


def write_job_logs_to_bucket(gcp_project: str, config_bucket: str):
    client = storage.Client(project=gcp_project)
    try:
        bucket = client.get_bucket(config_bucket)
        blob = bucket.blob(f"logs/{LOG_FILE_NAME}")
        blob.upload_from_filename(LOG_FILE_NAME)
    except Exception as e:
        raise Exception(
            f"Failed to upload job log file: {LOG_FILE_NAME} to GCS bucket: {config_bucket} in project: {gcp_project}."
        ) from e


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
