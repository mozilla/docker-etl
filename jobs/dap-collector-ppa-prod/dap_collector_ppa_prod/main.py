import asyncio
import click
import datetime
import math
import time

from google.cloud import bigquery
import requests

LEADER = "https://dap-09-3.api.divviup.org"
CMD = f"./collect --task-id {{task_id}} --leader {LEADER} --vdaf {{vdaf}} {{vdaf_args}} --authorization-bearer-token {{auth_token}} --batch-interval-start {{timestamp}} --batch-interval-duration {{duration}} --hpke-config {{hpke_config}} --hpke-private-key {{hpke_private_key}}"
MINUTES_IN_DAY = 1440
ADS_SCHEMA = [
    bigquery.SchemaField("collection_time", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("placement_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ad_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("conversion_key", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("task_size", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("task_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("task_index", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("conversion_count", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("advertiser_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("advertiser_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("campaign_id", "STRING", mode="REQUIRED"),
]
REPORT_SCHEMA = [
    bigquery.SchemaField("collection_time", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("collection_duration", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("task_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("metric_type", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("slot_start", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("report_count", "INTEGER"),
    bigquery.SchemaField("error", "STRING"),
    bigquery.SchemaField("value", "INTEGER", mode="REPEATED"),
]

ads = {}

def read_json(config_url):
    """Read configuration from Google Cloud bucket."""

    resp = requests.get(config_url)
    return resp.json()


def toh(timestamp):
    """Turn a timestamp into a datetime object which prints human readably."""
    return datetime.datetime.fromtimestamp(timestamp, datetime.timezone.utc)


async def collect_once(task, timestamp, duration, hpke_private_key, auth_token):
    """Runs collection for a single time interval.

    This uses the Janus collect binary. The result is formatted to fit the BQ table.
    """
    collection_time = str(datetime.datetime.now(datetime.timezone.utc).timestamp())
    print(f"{collection_time} Collecting {toh(timestamp)} - {toh(timestamp+duration)}")

    # Prepare output
    res = {}
    res["reports"] = []
    res["counts"] = []

    rpt = build_base_report(task["task_id"], timestamp, task["metric_type"], collection_time)

    # Convert VDAF description to string for command line use
    vdaf_args = ""
    for k, v in task["vdaf_args_structured"].items():
        vdaf_args += f" --{k} {v}"

    cmd = CMD.format(
        timestamp=timestamp,
        duration=duration,
        hpke_private_key=hpke_private_key,
        auth_token=auth_token,
        task_id=task["task_id"],
        vdaf=task["vdaf"],
        vdaf_args=vdaf_args,
        hpke_config=task["hpke_config"],
    )

    # How long an individual collection can take before it is killed.
    timeout = 100
    start_counter = time.perf_counter()
    try:
        proc = await asyncio.wait_for(
            asyncio.create_subprocess_shell(
                cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            ),
            timeout,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout)
        stdout = stdout.decode()
        stderr = stderr.decode()
    except asyncio.exceptions.TimeoutError:
        rpt["collection_duration"] = time.perf_counter() - start_counter
        rpt["error"] = f"TIMEOUT"

        res["reports"].append(rpt)
        return res

    print(f"{timestamp} Result: code {proc.returncode}")
    rpt["collection_duration"] = time.perf_counter() - start_counter

    # Parse the output of the collect binary
    if proc.returncode == 1:
        if (
            stderr
            == "Error: HTTP response status 400 Bad Request - The number of reports included in the batch is invalid.\n"
        ):
            rpt["error"] = "BATCH TOO SMALL"
        else:
            rpt["error"] = f"UNHANDLED ERROR: {stderr}"
    else:
        for line in stdout.splitlines():
            if line.startswith("Aggregation result:"):
                entries = line[21:-1]
                entries = list(map(int, entries.split(",")))

                rpt["value"] = entries

                for i in range(5):
                    ad = get_ad(task["task_id"], i)
                    print(task["task_id"], i, ad)
                    if ad is not None:
                        cnt = {}
                        cnt["collection_time"] = timestamp
                        cnt["placement_id"] = ad["advertiserInfo"]["placementId"]
                        cnt["advertiser_id"] = ad["advertiserInfo"]["advertiserId"]
                        cnt["advertiser_name"] = ad["advertiserInfo"]["advertiserName"]
                        cnt["ad_id"] = ad["advertiserInfo"]["adId"]
                        cnt["conversion_key"] = ad["advertiserInfo"]["conversionKey"]
                        cnt["task_id"] = task["task_id"]
                        cnt["task_index"] = i
                        cnt["task_size"] = task["task_size"]
                        cnt["campaign_id"] = ad["advertiserInfo"]["campaignId"]
                        cnt["conversion_count"] = entries[i]

                        res["counts"].append(cnt)
            elif line.startswith("Number of reports:"):
                rpt["report_count"] = int(line.split()[-1].strip())
            elif (
                line.startswith("Interval start:")
                or line.startswith("Interval end:")
                or line.startswith("Interval length:")
            ):
                # irrelevant since we are using time interval queries
                continue
            else:
                print(f"UNHANDLED OUTPUT LINE: {line}")
                raise NotImplementedError
    res["reports"].append(rpt)

    return res


def build_base_report(task_id, timestamp, metric_type, collection_time):
    row = {}
    row["task_id"] = task_id
    row["slot_start"] = timestamp
    row["metric_type"] = metric_type
    row["collection_time"] = collection_time
    return row


def build_error_result(task_id, timestamp, metric_type, error):
    collection_time = str(datetime.datetime.now(datetime.timezone.utc).timestamp())

    results = {}
    results["counts"] = []
    results["reports"] = []

    rpt = build_base_report(task_id, timestamp, metric_type, collection_time)
    rpt["collection_duration"] = 0
    rpt["error"] = error

    results["reports"].append(rpt)

    return results


def get_ad(task_id, index):
    global ads
    for ad in ads:
        if ad["taskId"] == task_id and ad["taskIndex"] == index:
            return ad


async def process_queue(q: asyncio.Queue, results: dict):
    """Worker for parallelism. Processes items from the queue until it is empty."""
    while not q.empty():
        job = q.get_nowait()
        res = await collect_once(*job)
        results["reports"] += res["reports"]
        results["counts"] += res["counts"]


async def collect_many(
    task, time_from, time_until, interval_length, hpke_private_key, auth_token
):
    """Collects data for a given time interval.

    Creates a configurable amount of workers which process jobs from a queue
    for parallelism.
    """
    time_from = int(time_from.timestamp())
    time_until = int(time_until.timestamp())
    start = math.ceil(time_from // interval_length) * interval_length
    jobs = asyncio.Queue(288)
    results = {}
    results["reports"] = []
    results["counts"] = []
    while start + interval_length <= time_until:
        await jobs.put((task, start, interval_length, hpke_private_key, auth_token))
        start += interval_length
    workers = []
    for _ in range(10):
        workers.append(process_queue(jobs, results))
    await asyncio.gather(*workers)

    return results


def check_collection_date(date):
    # collector should collect through to the beginning of a day
    if date.hour != 0 or date.minute != 0 or date.second != 0:
        return f"Collection date is not at beginning of a day {date}"
    else:
        return None


def check_time_precision(time_precision_minutes, end_collection_date):
    """Check that a given time precision is valid for the collection date
    """
    end_collection_date_seconds = int(end_collection_date.timestamp())

    if time_precision_minutes is None:
        # task is missing a time precision setting
        return f"Task missing time time_precision_minutes value"
    elif time_precision_minutes < MINUTES_IN_DAY:
        if MINUTES_IN_DAY % time_precision_minutes > 0:
            # time precision has to evenly divide a day in order for this collector code to query all aggregations
            return f"Task has time precision that does not evenly divide a day"
    else:
        if time_precision_minutes % MINUTES_IN_DAY != 0:
            # time precision is a day or longer, but is not a multiple of a day
            return f"Task has time precision that is not an even multiple of a day"
        elif end_collection_date_seconds % (time_precision_minutes*60) != 0:
            # time precision is a multiple of day, but the end does not align with this task's buckets
            return f"{end_collection_date} does not align with task aggregation buckets"

    return None


async def collect_task(task, auth_token, hpke_private_key, date):
    """Collects data for the given task through to the given day.
        For tasks with time precision smaller than a day, will collect data for aggregations from the day prior to date.
        For tasks with time precision a day or multiple of day, will collect data for the aggregation that ends on date.
            If date does not align with the end of an aggregation, it will not collect anything.
    """
    end_collection_date = datetime.datetime.fromisoformat(date)
    end_collection_date = end_collection_date.replace(tzinfo=datetime.timezone.utc)
    time_precision_minutes = task["time_precision_minutes"]

    err = check_collection_date(end_collection_date)
    if err is not None:
        return build_error_result(task["task_id"], end_collection_date, task["metric_type"], err)

    err = check_time_precision(time_precision_minutes, end_collection_date)
    if err is not None:
        return build_error_result(task["task_id"], end_collection_date, task["metric_type"], err)

    # task precision and date are valid
    if time_precision_minutes < MINUTES_IN_DAY:
        # time precision is shorter than daily
        # query for the last day of aggregations
        start_collection_date = end_collection_date - datetime.timedelta(days=1)
    else:
        # time precision is a multiple of a day
        # query for the aggregation that ends at end_collection_date
        aggregation_days = time_precision_minutes/MINUTES_IN_DAY
        start_collection_date = end_collection_date - datetime.timedelta(days=aggregation_days)

    return await collect_many(
        task, start_collection_date, end_collection_date, time_precision_minutes * 60, hpke_private_key, auth_token
    )


def ensure_table(bqclient, table_id, schema):
    """Checks if the table exists in BQ and creates it otherwise.
    Fails if the table exists but has the wrong schema.
    """
    table = bigquery.Table(table_id, schema=schema)
    print(f"Making sure the table {table_id} exists.")
    table = bqclient.create_table(table, exists_ok=True)


def store_data(results, bqclient, table_id):
    """Inserts the results into BQ. Assumes that they are already in the right format"""
    if results:
        insert_res = bqclient.insert_rows_json(table=table_id, json_rows=results)
        if len(insert_res) != 0:
            print(insert_res)
            assert len(insert_res) == 0


@click.command()
@click.option("--project", help="GCP project id", required=True)
@click.option(
    "--ad-table-id",
    help="The aggregated DAP measurements will be stored in this table.",
    required=True,
)
@click.option(
    "--report-table-id",
    help="The aggregated DAP measurements will be stored in this table.",
    required=True,
)
@click.option(
    "--auth-token",
    envvar='AUTH_TOKEN',
    help="HTTP bearer token to authenticate to the leader",
    required=True,
)
@click.option(
    "--hpke-private-key",
    envvar='HPKE_PRIVATE_KEY',
    help="The private key used to decrypt shares from the leader and helper.",
    required=True,
)
@click.option(
    "--date",
    help="Date at which the backfill will start, going backwards (YYYY-MM-DD)",
    required=True,
)
@click.option(
    "--task-config-url",
    help="URL where a JSON definition of the tasks to be collected can be found.",
    required=True,
)
@click.option(
    "--ad-config-url",
    help="URL where a JSON definition of the ads to task map can be found.",
    required=True,
)
def main(project, ad_table_id, report_table_id, auth_token, hpke_private_key, date, task_config_url, ad_config_url):
    global ads
    ad_table_id = project + "." + ad_table_id
    report_table_id = project + "." + report_table_id
    bqclient = bigquery.Client(project=project)
    ads = read_json(ad_config_url)
    ensure_table(bqclient, ad_table_id, ADS_SCHEMA)
    ensure_table(bqclient, report_table_id, REPORT_SCHEMA)
    for task in read_json(task_config_url):
        print(f"Now processing task: {task['task_id']}")
        results = asyncio.run(collect_task(task, auth_token, hpke_private_key, date))
        store_data(results["reports"], bqclient, report_table_id)
        store_data(results["counts"], bqclient, ad_table_id)

if __name__ == "__main__":
    main()
