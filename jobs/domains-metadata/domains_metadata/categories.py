import logging
import datetime


from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

from domains_metadata.queries import TOP_APEX_DOMAINS
from domains_metadata.cloudflare import domains_categories, content_categories

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


def load_cf_categories(
    destination_project,
    destination_table_id,
):
    """Upload cloudfare category taxonomy to BigQuery."""

    # fetch the formatted categories from cloudflare
    categories = content_categories()

    client = bigquery.Client(project=destination_project)

    job_config = bigquery.LoadJobConfig(
        create_disposition="CREATE_IF_NEEDED",
        schema=[
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("parent_id", "INTEGER"),
            bigquery.SchemaField("name", "STRING"),
        ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition="WRITE_TRUNCATE",
    )

    load_job = client.load_table_from_json(
        categories,
        destination_table_id,
        location="US",
        job_config=job_config,
    )

    try:
        # Catch the exception so that we can print the errors
        # in case of failure.
        load_job.result()
    except BadRequest as ex:
        if load_job.errors:
            for e in load_job.errors:
                logger.error(f"ERROR: {e['message']}")
        # Re-raise the exception to make the job fail.
        raise ex

    stored_table = client.get_table(destination_table_id)
    logger.info(f"Loaded {stored_table.num_rows} rows.")


def load_cf_domain_categories(
    destination_project: str,
    destination_table_id: str,
):
    """Upload cloudfare category taxonomy to BigQuery."""

    # fetch the formatted domain categories from cloudflare
    categorized_domains = _get_domain_categories(destination_project)
    today_as_iso = datetime.date.today().isoformat()

    # Turn the suggestions into dicts and augment them with
    # an insertion date.
    categorized_domains = [
        {**domain_categories, "submission_date": today_as_iso}
        for domain_categories in categorized_domains
    ]
    print(categorized_domains)

    if len(categorized_domains) == 0:
        logger.info("No domains to load. Bailing.")
        return

    client = bigquery.Client(project=destination_project)

    job_config = bigquery.LoadJobConfig(
        create_disposition="CREATE_IF_NEEDED",
        schema=[
            bigquery.SchemaField("submission_date", "DATE"),
            bigquery.SchemaField("domain", "STRING", mode="REQUIRED"),
            bigquery.SchemaField(
                "categories",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("id", "INTEGER"),
                    bigquery.SchemaField("parent_id", "INTEGER"),
                    bigquery.SchemaField("name", "STRING"),
                ],
            ),
        ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition="WRITE_APPEND",
    )

    load_job = client.load_table_from_json(
        categorized_domains,
        destination_table_id,
        location="US",
        job_config=job_config,
    )

    try:
        # Catch the exception so that we can print the errors
        # in case of failure.
        load_job.result()
    except BadRequest as ex:
        if load_job.errors:
            for e in load_job.errors:
                logger.error(f"ERROR: {e['message']}")
        # Re-raise the exception to make the job fail.
        raise ex

    stored_table = client.get_table(destination_table_id)
    logger.info(f"Loaded {stored_table.num_rows} rows.")


def _get_domain_categories(destination_project: str) -> list[dict]:
    logger.info("Fetching top apex domains")
    client = bigquery.Client(project=destination_project)
    query_job = client.query(TOP_APEX_DOMAINS)
    query_job.result()

    # get the temporary destination table if the job succeeded
    domain_accumulator = []
    if query_job.destination:
        destination = client.get_table(query_job.destination)

        logger.info("waiting on results")
        results = client.list_rows(destination, page_size=50)
        i = 0
        for page in results.pages:
            i = i + 1
            print(f"page: {i} num items: {page.num_items}")
            domains = [row['domain'] for row in page]
            if domains:
                try:
                    domain_accumulator.extend(domains_categories(domains))
                except Exception as e:
                    logger.error("error fetching domain categories", exc_info=e)
                    break

    return domain_accumulator
