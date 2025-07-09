import ast
import click

from google.cloud import bigquery
from typing import List, Optional
import datetime
import time
import attr
import cattrs
import requests
import pytz
import tldextract
import re
import subprocess
import logging
import json


EXPERIMENTER_API_URL_V6 = (
    # "https://experimenter.services.mozilla.com/api/v6/experiments/"
    "https://stage.experimenter.nonprod.webservices.mozgcp.net/api/v6/experiments/"
)

SCHEMA = [
    bigquery.SchemaField("start_date", "DATE", mode="REQUIRED", description="Start date of the collected time window, inclusive."),
    bigquery.SchemaField("end_date", "DATE", mode="REQUIRED", description="End date of the collected time window, inclusive."),
    bigquery.SchemaField("country_codes", "JSON", mode="NULLABLE", description="List of 2-char country codes for the experiment"),
    bigquery.SchemaField("experiment_slug", "STRING", mode="REQUIRED", description="Slug indicating the experiment."),
    bigquery.SchemaField("branch", "STRING", mode="REQUIRED", description="The experiment branch this data is associated with."),
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


@attr.s(auto_attribs=True)
class Branch:
    """Defines a branch."""

    slug: str
    ratio: int
    features: Optional[dict]


@attr.s(auto_attribs=True)
class NimbusExperiment:
    """Represents a v8 Nimbus experiment from Experimenter."""

    slug: str  # Normandy slug
    startDate: Optional[datetime.datetime]
    endDate: Optional[datetime.datetime]
    enrollmentEndDate: Optional[datetime.datetime]
    proposedEnrollment: int
    branches: List[Branch]
    referenceBranch: Optional[str]
    appName: str
    appId: str
    channel: str
    targeting: str
    bucketConfig: dict
    featureIds: list[str]

    @classmethod
    def from_dict(cls, d) -> "NimbusExperiment":
        """Load an experiment from dict."""
        converter = cattrs.BaseConverter()
        converter.register_structure_hook(
            datetime.datetime,
            lambda num, _: datetime.datetime.fromisoformat(
                num.replace("Z", "+00:00")
            ).astimezone(pytz.utc),
        )
        converter.register_structure_hook(
            Branch,
            lambda b, _: Branch(
                slug=b["slug"], ratio=b["ratio"], features=b["features"]
            ),
        )
        return converter.structure(d, cls)


def create_row(start_date: str, end_date: str, country_codes: str, experiment_slug: str, branch: str, advertiser: str,
               metric: str, value_histogram: str = None, value_count: int = None) -> dict:
    row = {"start_date": start_date, "end_date": end_date, "country_codes": country_codes,
           "experiment_slug": experiment_slug, "branch": branch, "advertiser": advertiser,
           "metric": metric, "value": {"count": value_count, "histogram": value_histogram}}
    return row


def fetch(url: str):
    for _ in range(2):
        try:
            return requests.get(
                url,
                timeout=30,
                headers={"user-agent": "https://github.com/mozilla/docker-etl"},
            ).json()
        except Exception as e:
            last_exception = e
            time.sleep(1)
    raise last_exception


def get_experiment(experiment_slug: str) -> Optional[NimbusExperiment]:
    """Fetch the experiment from Experimenter API and return the configuration."""
    try:
        nimbus_experiments_json = fetch(f"{EXPERIMENTER_API_URL_V6}{experiment_slug}/")
        nimbus_experiment = NimbusExperiment.from_dict(nimbus_experiments_json)
        return nimbus_experiment
    except Exception as e:
        raise Exception(f"Failed loading experiment {experiment_slug} from {EXPERIMENTER_API_URL_V6}", e)


def insert_into_bq(row, bqclient, table_id: str):
    """Inserts the results into BQ. Assumes that they are already in the right format"""
    if row:
        insert_res = bqclient.insert_rows_json(table=table_id, json_rows=[row])
        if len(insert_res) != 0:
            raise Exception(f"Error inserting rows into {table_id}: {insert_res}")


def normalize_url(url: str) -> str:
    # Replace wildcard with a dummy protocol and subdomain so urlparse can handle it
    normalized = re.sub(r'^\*://\*\.?', 'https://', url)
    return normalized


def get_advertiser_from_url(url: str) -> Optional[str]:
    """Parses the advertiser name (domain) from the url"""
    # tldextract cannot handle wildcards, replace with standard values.
    normalized = normalize_url(url)
    ext = tldextract.extract(normalized)
    return ext.domain


def get_country_from_targeting(targeting: str) -> Optional[str]:
    """Parses the region/country from the targeting string and
    returns a JSON formatted list of country codes."""
    # match = re.findall(r"region\s+in\s+(^]+)", targeting)
    match = re.search(r"region\s+in\s+\[([^]]+)]", targeting)

    if match:
        inner = match.group(1)
        regions = [r.strip().strip("'\"") for r in inner.split(',')]
        logging.info("regions: %s", regions)
        return json.dumps(regions)
    return None


LEADER = "https://dap-09-3.api.divviup.org"
VDAF = "histogram"
process_timeout = 600  # 10 mins


# TODO Trigger Airflow errors
def correct_wraparound(num: int) -> int:
    field_prime = 340282366920938462946865773367900766209
    field_size = 128
    cutoff = 2 ** (field_size - 1)
    if num > cutoff:
        logging.info(f"Corrected {num} to {num - field_prime} ")
        return num - field_prime
    return num


def parse_histogram(histogram_str: str) -> dict:
    parsed_list = ast.literal_eval(histogram_str)
    return {i: correct_wraparound(val) for i, val in enumerate(parsed_list)}


def collect_dap_result(task_id: str, vdaf_length: int, auth_token: str, hpke_config: str, hpke_private_key: str, batch_start: int, duration: int) -> dict:
    command_str = (f"./collect --task-id {task_id} --leader {LEADER} --vdaf {VDAF} --length {vdaf_length} "
                   f"--authorization-bearer-token {auth_token} --batch-interval-start {batch_start} "
                   f"--batch-interval-duration {duration} --hpke-config {hpke_config} "
                   f"--hpke-private-key {hpke_private_key}")
    logging.debug(f"command_str: {command_str}")
    logging.info(f"Processing batch_start: {batch_start} for duration: {duration}")
    try:

        result = subprocess.run(["./collect", "--task-id", task_id, "--leader", LEADER, "--vdaf", VDAF,
                                 "--length", f"{vdaf_length}", "--authorization-bearer-token", auth_token,
                                 "--batch-interval-start", f"{batch_start}", "--batch-interval-duration", f"{duration}",
                                 "--hpke-config", hpke_config, "--hpke-private-key", hpke_private_key],
                                capture_output=True,
                                text=True,
                                check=True,
                                timeout=process_timeout)

        for line in result.stdout.splitlines():
            if line.startswith("Aggregation result:"):
                entries = parse_histogram(line[21:-1])
                return entries
    except subprocess.CalledProcessError as e:
        logging.error(f"Collection failed for {task_id}, {e.returncode}, stderr: {e.stderr}")
    except subprocess.TimeoutExpired as e:
        logging.error(f"Collection timed out for {task_id}, {e.timeout}, stderr: {e.stderr}")


@click.command()
@click.option("--project_id", help="GCP project id", required=True)
@click.option(
    "--dataset_id",
    help="Dataset for all dap aggregations",
    required=True)
@click.option("--table_id", help="Table for incrementality dap aggregations.", required=True)
@click.option(
    "--auth_token",
    envvar='AUTH_TOKEN',
    help="HTTP bearer token to authenticate to the leader",
    required=True,
)
@click.option(
    "--hpke_private_key",
    envvar='HPKE_PRIVATE_KEY',
    help="The private key used to decrypt shares from the leader and helper",
    required=True,
)
# TODO Since this one isn't private if might be possible to keep in the same file as the experiment slugs?
#  If not a new variable will have to be added to Airflow (unless it already exists).
@click.option(
    "--hpke_config",
    envvar='HPKE_CONFIG',
    help="base64url-encoded version of hpke_config defined in the collector credentials. ",
    required=True,
)
@click.option(
    "--batch_start",
    type=int,
    help="Start of the collection interval, as the number of seconds since the Unix epoch",
    required=True,
)
@click.option(
    "--batch_duration",
    type=int,
    help=" Duration of the collection batch interval, in seconds",
    required=True,
)
@click.option("--experiment_slug", "experiment_slugs",
              help="Experiment to collect.  To specify multiple experiments use multiple --experiment_slug options",
              multiple=True, required=True)
def main(project_id, dataset_id, table_id, auth_token, hpke_private_key, hpke_config, batch_start, batch_duration,
         experiment_slugs):
    logging.info(f"Starting collector job for batch_start: {batch_start} duration: {batch_duration}")
    data_set = f"{project_id}.{dataset_id}"
    bqclient = bigquery.Client(project=project_id)
    bqclient.create_dataset(data_set, exists_ok=True)

    full_table_id = f"{data_set}.{table_id}"
    table = bigquery.Table(full_table_id, schema=SCHEMA)
    try:
        bqclient.create_table(table, exists_ok=True)
    except Exception as e:
        raise Exception(f"Failed to create table: {full_table_id} {e}")

    for experiment_slug in experiment_slugs:
        logging.info(f"Processing experiment: {experiment_slug}")
        try:
            experiment = get_experiment(experiment_slug)
        except:
            raise Exception(f"Cannot load experiment {experiment_slug} from: ")
        branches = experiment.branches

        targeting = experiment.targeting
        country_codes = get_country_from_targeting(targeting)
        tasks_to_process = {}
        collector_results = {}
        for branch_metadata in branches:
            branch = branch_metadata.slug
            logging.info(f"Processing branch {branch}")
            features = branch_metadata.features
            for feature in features:
                if feature.get("featureId") == "dapTelemetry":
                    visit_counting_experiment_list = feature.get("value").get("visitCountingExperimentList")
                    for visitCountList in visit_counting_experiment_list:
                        task_id = visitCountList.get("task_id")
                        if task_id not in collector_results:
                            collector_results[task_id] = {}
                        # Store the length associated with the task, needed for collector process.
                        tasks_to_process[task_id] = visitCountList.get("task_veclen")
                        bucket = visitCountList.get("bucket")
                        urls = visitCountList.get("urls")
                        # default to the first url in the list to determine the advertiser.
                        advertiser = "not_set"
                        if len(urls) > 0:
                            advertiser = get_advertiser_from_url(urls[0])

                        result = {"experiment_slug": experiment_slug, "branch": branch, "advertiser": advertiser,
                                  "metrics": "unique_client_organic_visits", "country_codes": country_codes}
                        collector_results[task_id][bucket] = result
                        logging.info(collector_results)

        unique_tasks = list(dict.fromkeys(tasks_to_process))
        for task_id in unique_tasks:
            logging.info(f"Collecting task_id: {task_id}")
            experiment_metadata = collector_results[task_id]
            # need to collect once per task, not bucket.
            collected = collect_dap_result(task_id, tasks_to_process.get(task_id), auth_token, hpke_config,
                                           hpke_private_key, batch_start, batch_duration)
            for bucket in experiment_metadata.keys():
                collector_results[task_id][bucket]["value_count"] = collected[bucket]

        records = [v for inner in collector_results.values() for v in inner.values()]

        logging.info(f"Inserting records: {records}")
        for record in records:
            row = create_row(start_date=datetime.date.today().isoformat(), end_date=datetime.date.today().isoformat(),
                             country_codes=record["country_codes"], advertiser=record["advertiser"],
                             experiment_slug=record["experiment_slug"], branch=record["branch"],
                             metric="unique_client_organic_visits",
                             value_count=record["value_count"])
            insert_into_bq(row, bqclient, full_table_id)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
