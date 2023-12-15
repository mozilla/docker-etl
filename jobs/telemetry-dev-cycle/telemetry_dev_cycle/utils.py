import datetime
import logging
import requests
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery


def store_data_in_bigquery(data, schema, destination_project, destination_table_id):
    """Upload data to Bigquery"""

    client = bigquery.Client(project=destination_project)

    job_config = bigquery.LoadJobConfig(
        create_disposition="CREATE_IF_NEEDED",
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition="WRITE_TRUNCATE",
    )

    load_job = client.load_table_from_json(
        data, destination_table_id, location="US", job_config=job_config
    )

    try:
        load_job.result()
    except BadRequest as ex:
        for e in load_job.errors:
            logging.error(f"Error: {e['message']}")
        raise ex
    stored_table = client.get_table(destination_table_id)
    logging.info(f"Loaded {stored_table.num_rows} rows into {destination_table_id}.")


def get_api_response(url):
    response = requests.get(url)

    if response.status_code != 200:
        logging.error(
            f"Failed to download data from {url}. \nResponse status code {response.status_code}."
        )
        return

    return response.json()


def parse_unix_datetime_to_string(unix_string):
    if not unix_string:
        return None
    return datetime.datetime.fromtimestamp(int(unix_string) // 1000).strftime(
        "%Y-%m-%d"
    )
