from google.cloud import bigquery
import logging
from utils import (
    store_data_in_bigquery,
    get_api_response,
    parse_unix_datetime_to_string,
)

API_BASE_URL_EXPERIMENTS = "https://experimenter.services.mozilla.com"
API_BASE_URL_METRIC_HUB = "https://github.com/mozilla/metric-hub/tree/main/jetstream"
SCHEMA = [
    bigquery.SchemaField("slug", "STRING"),
    bigquery.SchemaField("start_date", "DATE"),
    bigquery.SchemaField("enrollment_end_date", "DATE"),
    bigquery.SchemaField("end_date", "DATE"),
    bigquery.SchemaField("has_config", "BOOLEAN"),
]
TABLE_NAME = "experiments_metrics_external_v1"


def download_experiments_v1(url):
    experiments_v1 = []
    if experiments := get_api_response(f"{url}/api/v1/experiments"):
        for experiment in experiments:
            if experiment["status"] == "Draft":
                continue
            experiments_v1.append(
                {
                    "slug": experiment["slug"],
                    "start_date": parse_unix_datetime_to_string(
                        experiment["start_date"]
                    ),
                    "enrollment_end_date": None,
                    "end_date": parse_unix_datetime_to_string(experiment["end_date"]),
                }
            )
    return experiments_v1


def download_experiments_v6(url):
    experiments_v6 = []
    if experiments := get_api_response(f"{url}/api/v6/experiments"):
        for experiment in experiments:
            experiments_v6.append(
                {
                    "slug": experiment["slug"],
                    "start_date": experiment["startDate"],
                    "enrollment_end_date": experiment["enrollmentEndDate"],
                    "end_date": experiment["endDate"],
                }
            )
    return experiments_v6


def download_metric_hub_files(url):
    metric_files = {}
    if files := get_api_response(url):
        for file in files["payload"]["tree"]["items"]:
            if file["contentType"] != "file":
                continue
            slug = file["name"].replace(".toml", "")
            metric_files[slug] = True
    return metric_files


def run_experiments_metrics(bq_project_id, bq_dataset_id):
    """Download experiments"""
    experiments_v1 = download_experiments_v1(API_BASE_URL_EXPERIMENTS)
    experiments_v6 = download_experiments_v6(API_BASE_URL_EXPERIMENTS)
    experiments = experiments_v1 + experiments_v6
    metric_files = download_metric_hub_files(API_BASE_URL_METRIC_HUB)

    for experiment in experiments:
        if experiment["slug"] in metric_files:
            experiment["has_config"] = True
        else:
            experiment["has_config"] = False
    destination_table_id = f"{bq_project_id}.{bq_dataset_id}.{TABLE_NAME}"
    store_data_in_bigquery(
        data=experiments,
        schema=SCHEMA,
        destination_project=bq_project_id,
        destination_table_id=destination_table_id,
    )
