from google.cloud import bigquery

DAP_LEADER = "https://dap-09-3.api.divviup.org"
VDAF = "histogram"
process_timeout = 600  # 10 mins

EXPERIMENTER_API_URL_V6 = (
    # "https://experimenter.services.mozilla.com/api/v6/experiments/"
    "https://stage.experimenter.nonprod.webservices.mozgcp.net/api/v6/experiments/"
)

COLLECTOR_RESULTS_SCHEMA = [
    bigquery.SchemaField("start_date", "DATE", mode="REQUIRED", description="Start date of the collected time window, inclusive."),
    bigquery.SchemaField("end_date", "DATE", mode="REQUIRED", description="End date of the collected time window, inclusive."),
    bigquery.SchemaField("country_codes", "JSON", mode="NULLABLE", description="List of 2-char country codes for the experiment"),
    bigquery.SchemaField("experiment_slug", "STRING", mode="REQUIRED", description="Slug indicating the experiment."),
    bigquery.SchemaField("experiment_branch", "STRING", mode="REQUIRED", description="The experiment branch this data is associated with."),
    bigquery.SchemaField("advertiser", "STRING", mode="REQUIRED", description="Advertiser associated with this experiment."),
    bigquery.SchemaField("metric", "STRING", mode="REQUIRED", description="Metric collected for this experiment."),
    bigquery.SchemaField(
        name="value",
        field_type="RECORD",
        mode="REQUIRED",
        fields=[
            bigquery.SchemaField("count", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("histogram", "JSON", mode="NULLABLE"),
        ]
    ),
]
