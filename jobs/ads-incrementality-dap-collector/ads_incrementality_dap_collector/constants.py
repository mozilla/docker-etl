from datetime import datetime

from google.cloud import bigquery

DAP_LEADER = "https://dap-09-3.api.divviup.org"
VDAF = "histogram"
PROCESS_TIMEOUT = 600  # 10 mins

CONFIG_FILE_NAME = "config.json"     # See example_config.json for the contents and structure of the job config file.
LOG_FILE_NAME = f"ads-incrementality-dap-collector-log-{datetime.now()}"

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
