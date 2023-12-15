from google.cloud import bigquery
import logging
from utils import store_data_in_bigquery, get_api_response

API_BASE_URL = "https://probeinfo.telemetry.mozilla.org"
SCHEMA = [
    bigquery.SchemaField("product", "STRING"),
    bigquery.SchemaField("metric", "STRING"),
    bigquery.SchemaField("type", "STRING"),
    bigquery.SchemaField("first_seen_date", "DATE"),
    bigquery.SchemaField("last_seen_date", "DATE"),
    bigquery.SchemaField("expires", "STRING"),
]
TABLE_NAME = "glean_metrics_external_v1"


def download_glean_metrics(url: str):
    """Download metrics for glean products and parse the data."""
    # get a list of all glean apps
    glean_apps_response = get_api_response(f"{url}/glean/repositories")
    glean_apps = [glean_app["name"] for glean_app in glean_apps_response]

    glean_metrics = []
    for glean_app in glean_apps:
        if metrics := get_api_response(f"{url}/glean/{glean_app}/metrics"):
            for name, metric in metrics.items():
                first_seen = metric["history"][0]["dates"]["first"][:10]
                last_seen = metric["history"][-1]["dates"]["last"][:10]
                expires = metric["history"][0]["expires"]
                glean_metrics.append(
                    {
                        "product": glean_app,
                        "metric": name,
                        "type": metric["history"][0]["type"],
                        "first_seen_date": first_seen,
                        "last_seen_date": last_seen,
                        "expires": expires,
                    }
                )
    return glean_metrics


def run_glean_metrics(bq_project_id, bq_dataset_id):
    destination_table_id = f"{bq_project_id}.{bq_dataset_id}.{TABLE_NAME}"
    glean_metrics = download_glean_metrics(API_BASE_URL)
    store_data_in_bigquery(
        data=glean_metrics,
        schema=SCHEMA,
        destination_project=bq_project_id,
        destination_table_id=destination_table_id,
    )
