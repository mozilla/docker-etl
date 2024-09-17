import click
from datetime import datetime as dt
from google.cloud import bigquery

@click.command()
@click.option("--project_id", required=True, help="GCP Project ID")
@click.option("--submission_date", default=dt.today().strftime('%Y-%m-%d'), help="Date for the query (default: today, format: YYYY-MM-DD)")
def main(project_id, submission_date):
    # Construct the SQL query
    query = """
        SELECT
            submission_date,
            campaign_id,
            flight_id,
            ad_id,
            creative_id,
            product,
            surface,
            provider,
            country,
            rate_type,
            clicks,
            impressions
        FROM
            `moz-fx-data-shared-prod.ads.consolidated_ad_metrics_daily_pt`
        WHERE
            submission_date = @submission_date
    """

    client = bigquery.Client(project=project_id)

    # Configure the query with the required parameter (the date entered)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("submission_date", "DATE", submission_date)
        ]
    )

    # Execute the query
    query_job = client.query(query, job_config=job_config)
    results = query_job.result().to_dataframe()

    # Save the results to a text file -- For testing for now, will change later to save to shepherd
    results.to_csv('metrics.txt', sep='\t', index=False)
    print("Data saved to metrics.txt")

if __name__ == '__main__':
    main()