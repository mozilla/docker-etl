import click
import logging
import traceback

from google.cloud import bigquery, storage

from datetime import datetime

from .parse import get_config, extract_advertisers_with_partners_and_ads
from .collect import get_aggregated_results, current_batch_start, current_batch_end
from .persist import create_bq_table_if_not_exists, create_bq_row, insert_into_bq


LOG_FILE_NAME = f"{datetime.now()}-ads-newtab-attribution-dap-collector.log"


def write_job_logs_to_bucket(gcp_project: str, config_bucket: str):
    client = storage.Client(project=gcp_project)
    try:
        bucket = client.get_bucket(config_bucket)
        blob = bucket.blob(f"logs/{LOG_FILE_NAME}")
        blob.upload_from_filename(LOG_FILE_NAME)
    except Exception as e:
        raise Exception(
            f"Failed to upload job log file: {LOG_FILE_NAME} "
            f"to GCS bucket: {config_bucket} in project: {gcp_project}."
        ) from e


@click.command()
@click.option(
    "--job_config_gcp_project",
    help="GCP project id for the GCS bucket containing the configuration file.",
    required=True,
)
@click.option("--bq_project", help="BigQuery project id", required=True)
@click.option(
    "--job_config_bucket",
    help="GCS bucket where the configuration for this job can be found.",
    required=True,
)
@click.option(
    "--bearer_token",
    envvar="DAP_BEARER_TOKEN",
    help="The 'token' defined in the collector credentials.",
    required=True,
)
@click.option(
    "--hpke_private_key",
    envvar="DAP_PRIVATE_KEY",
    help="The 'private_key' defined in the collector credentials.",
    required=True,
)
@click.option(
    "--process_date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Current processing date (ds)",
    required=True,
)
def main(
    job_config_gcp_project,
    bq_project,
    job_config_bucket,
    bearer_token,
    hpke_private_key,
    process_date,
):
    try:
        process_date = process_date.date()

        logging.info(
            f"Starting collector job with configuration from gcp project: "
            f"{job_config_gcp_project} and gcs bucket: {job_config_bucket} "
            f"for process date: {process_date}"
        )

        bq_client = bigquery.Client(project=bq_project)

        # Step 1 check for the table
        full_table_id = create_bq_table_if_not_exists(bq_project, bq_client)

        # Step 2a Get newtab attribution config
        json_config = get_config(job_config_gcp_project, job_config_bucket)
        hpke_config, config = extract_advertisers_with_partners_and_ads(json_config)

        # Step 2b Get the hpke_config
        for advertiser_config in config:
            #  Step 3 Get processing date range.
            batch_start = current_batch_start(
                process_date,
                advertiser_config.start_date,
                advertiser_config.collector_duration,
            )
            if batch_start is None:
                # The process_date is too early
                logging.info(
                    f"Advertiser start_date: {advertiser_config.start_date} is after "
                    f"process_date: {process_date}, skipping."
                )
                continue
            batch_end = current_batch_end(
                batch_start, advertiser_config.collector_duration
            )

            aggregated_results = get_aggregated_results(
                process_date=process_date,
                batch_start=batch_start,
                batch_end=batch_end,
                task_id=advertiser_config.partner.task_id,
                vdaf_length=advertiser_config.partner.length,
                collector_duration=advertiser_config.collector_duration,
                bearer_token=bearer_token,
                hpke_config=hpke_config,
                hpke_private_key=hpke_private_key,
            )

            if aggregated_results is None:
                logging.info(
                    f"No results available for advertiser: {advertiser_config.name} "
                    f"with start_date: {advertiser_config.start_date} a"
                    f"nd process_date: {process_date}"
                )
                continue
            for ad_config in advertiser_config.ads:
                conversion_count = aggregated_results[ad_config.index]
                row = create_bq_row(
                    collection_start=batch_start,
                    collection_end=batch_end,
                    provider=ad_config.source,
                    ad_id=ad_config.ad_id,
                    lookback_window=advertiser_config.lookback_window,
                    conversion_type=advertiser_config.conversion_type,
                    conversion_count=conversion_count,
                )

                insert_into_bq(row, bq_client, full_table_id)

    except Exception as e:
        logging.error(f"Collector job failed. Error: {e}\n{traceback.format_exc()}")
        raise e
    finally:
        write_job_logs_to_bucket(job_config_gcp_project, job_config_bucket)


if __name__ == "__main__":
    logging.basicConfig(
        filename=LOG_FILE_NAME,
        filemode="a",
        format="%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )
    logging.getLogger().setLevel(logging.INFO)
    main()
