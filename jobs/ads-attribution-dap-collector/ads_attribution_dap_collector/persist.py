from datetime import date, datetime
from typing import Any

from google.cloud import bigquery


NAMESPACE = "ads_dap_derived"
TABLE_NAME = "newtab_attribution_v1"

COLLECTOR_RESULTS_SCHEMA = [
    bigquery.SchemaField(
        "collection_start",
        "DATE",
        mode="REQUIRED",
        description="Start date of the collected time window, inclusive.",
    ),
    bigquery.SchemaField(
        "collection_end",
        "DATE",
        mode="REQUIRED",
        description="End date of the collected time window, inclusive.",
    ),
    bigquery.SchemaField(
        "provider",
        "STRING",
        mode="REQUIRED",
        description="The external service providing the ad.",
    ),
    bigquery.SchemaField(
        "ad_id",
        "INT64",
        mode="REQUIRED",
        description="Id of ad, unique by provider.",
    ),
    bigquery.SchemaField(
        "lookback_window",
        "INT64",
        mode="REQUIRED",
        description="Maximum number of days to attribute an ad.",
    ),
    bigquery.SchemaField(
        "conversion_type",
        "STRING",
        mode="REQUIRED",
        description="Indicates the type of conversion [view, click, default]",
    ),
    bigquery.SchemaField(
        "conversion_count",
        "INT64",
        mode="REQUIRED",
        description="Aggregated number of conversions attributed to the ad_id.",
    ),
    bigquery.SchemaField(
        "created_timestamp",
        "TIMESTAMP",
        mode="REQUIRED",
        description="Timestamp for when this row was created.",
    ),
]


def create_bq_table_if_not_exists(project: str, bq_client: bigquery.Client) -> str:
    data_set = f"{project}.{NAMESPACE}"
    bq_client.create_dataset(data_set, exists_ok=True)

    full_table_id = f"{data_set}.{TABLE_NAME}"
    table = bigquery.Table(full_table_id, schema=COLLECTOR_RESULTS_SCHEMA)
    try:
        bq_client.create_table(table, exists_ok=True)
        return full_table_id
    except Exception as e:
        raise Exception(f"Failed to create BQ table: {full_table_id}") from e


def create_bq_row(
    collection_start: date,
    collection_end: date,
    provider: str,
    ad_id: int,
    lookback_window: int,
    conversion_type: str,
    conversion_count: int,
) -> dict[str, Any]:
    """Creates a BQ row converting date to str where required."""
    row = {
        "collection_start": collection_start.isoformat(),
        "collection_end": collection_end.isoformat(),
        "provider": provider,
        "ad_id": ad_id,
        "lookback_window": lookback_window,
        "conversion_type": conversion_type,
        "conversion_count": conversion_count,
        "created_timestamp": datetime.now().isoformat(),
    }
    return row


def insert_into_bq(row, bqclient, table_id: str):
    """Inserts the results into BQ. Assumes that they are already in the right format"""
    if row:
        insert_res = bqclient.insert_rows_json(table=table_id, json_rows=[row])
        if len(insert_res) != 0:
            raise Exception(f"Error inserting rows into {table_id}: {insert_res}")
