from google.cloud import bigquery


def ensure_table(
    client: bigquery.Client,
    bq_dataset_id: str,
    table_id: str,
    schema: list[bigquery.SchemaField],
    recreate: bool,
) -> None:
    table = bigquery.Table(
        f"{client.project}.{bq_dataset_id}.{table_id}", schema=schema
    )
    if recreate:
        client.delete_table(table, not_found_ok=True)
    client.create_table(table, exists_ok=True)
