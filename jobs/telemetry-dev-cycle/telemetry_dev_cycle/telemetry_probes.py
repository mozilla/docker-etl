import logging
from google.cloud import bigquery
from utils import get_api_response, store_data_in_bigquery


BASE_URL = "https://probeinfo.telemetry.mozilla.org"

TABLE_NAME = "telemetry_probes_external_v1"

SCHEMA = [
    bigquery.SchemaField("channel", "STRING"),
    bigquery.SchemaField("probe", "STRING"),
    bigquery.SchemaField("type", "STRING"),
    bigquery.SchemaField("release_version", "INT64"),
    bigquery.SchemaField("last_version", "INT64"),
    bigquery.SchemaField("expiry_version", "STRING"),
    bigquery.SchemaField("first_added_date", "DATE"),
]

CHANNELS = ["release", "beta", "nightly"]


def download_telemetry_probes(url: str):
    """Download probes for telemetry and parse the data."""
    firefox_metrics = []
    for channel in CHANNELS:
        if probes := get_api_response(f"{url}/firefox/{channel}/main/all_probes"):
            for probe in probes.values():
                release_version = int(
                    probe["history"][channel][-1]["versions"]["first"]
                )
                last_version = int(probe["history"][channel][0]["versions"]["last"])
                expiry_version = probe["history"][channel][0]["expiry_version"]
                first_added_date = probe["first_added"][channel][:10]

                firefox_metrics.append(
                    {
                        "channel": channel,
                        "probe": probe["name"],
                        "type": probe["type"],
                        "release_version": release_version,
                        "last_version": last_version,
                        "expiry_version": expiry_version,
                        "first_added_date": first_added_date,
                    }
                )
    return firefox_metrics


def run_telemetry_probes(bq_project_id, bq_dataset_id):
    destination_table_id = f"{bq_project_id}.{bq_dataset_id}.{TABLE_NAME}"
    telemetry_probes = download_telemetry_probes(BASE_URL)
    store_data_in_bigquery(
        data=telemetry_probes,
        schema=SCHEMA,
        destination_project=bq_project_id,
        destination_table_id=destination_table_id,
    )
