import asyncio
import click
import datetime
import math
import time

from google.cloud import bigquery
import requests
from jsonschema import validate, ValidationError



LEADER = "https://dap-09-3.api.divviup.org"
CMD = f"./collect --task-id {{task_id}} --leader {LEADER} --vdaf {{vdaf}} {{vdaf_args}} --authorization-bearer-token {{auth_token}} --batch-interval-start {{timestamp}} --batch-interval-duration {{duration}} --hpke-config {{hpke_config}} --hpke-private-key {{hpke_private_key}}"
MINUTES_IN_DAY = 1440
UNIX_EPOCH_WEEKDAY = datetime.datetime.fromtimestamp(0, tz=datetime.timezone.utc).weekday()

# The modulo prime for the field for Prio3SumVec, and its size in bits. We use these to detect and counteract negative
# conversion counts (as a result of differential privacy noise being added) wrapping around.
#
# Note that these values are specific to the data type we use for our tasks. If we start using a different type (e.g.
# Prio3Histogram), the values will need to be adjusted.
#
# https://github.com/divviup/libprio-rs/blob/a85d271ddee087f13dfd847a7170786f35abd0b9/src/vdaf/prio3.rs#L88
# https://github.com/divviup/libprio-rs/blob/a85d271ddee087f13dfd847a7170786f35abd0b9/src/fp.rs#L87
FIELD_PRIME = 340282366920938462946865773367900766209
FIELD_SIZE = 128

ADS_SCHEMA = [
    bigquery.SchemaField("collection_time", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("placement_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ad_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("task_size", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("task_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("task_index", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("conversion_count", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("advertiser_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("advertiser_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("campaign_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("time_precision_minutes", "INTEGER"),
]

AD_CONFIG_JSON_SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "required": ["taskId", "taskIndex", "advertiserInfo"],
        "properties": {
            "taskId": {"type": "string"},
            "taskIndex": {"type": "integer"},
            "advertiserInfo": {
                "type": "object",
                "required": ["advertiserId", "adId", "placementId", "campaignId", "extraInfo"],
                "properties": {
                    "advertiserId": {"type": "string"},
                    "adId": {"type": "string"},
                    "placementId": {"type": "string"},
                    "campaignId": {"type": "string"},
                    "extraInfo": {
                        "type": "object",
                        "required": ["spend", "budget"],
                        "properties": {
                            "spend": {"type": "integer"},
                            "budget": {"type": "integer"}
                        }
                    }
                }
            }
        }
    }
}

REPORT_SCHEMA = [
    bigquery.SchemaField("collection_time", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("collection_duration", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("task_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("metric_type", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("slot_start", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("report_count", "INTEGER"),
    bigquery.SchemaField("error", "STRING"),
    bigquery.SchemaField("value", "INTEGER", mode="REPEATED"),
    bigquery.SchemaField("time_precision_minutes", "INTEGER"),
]

TASK_CONFIG_JSON_SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "required": [
            "task_id", 
            "time_precision_minutes", 
            "vdaf_args_structured", 
            "vdaf", 
            "hpke_config"
        ],
        "properties": {
            "task_id": {"type": "string"},
            "time_precision_minutes": {"type": "integer"},
            "start_date": {"type": "string"},
            "end_date": {"type": "string"},
            "vdaf_args_structured": {
                "type": "object",
                "required": ["length", "bits"],
                "properties": {
                    "length": {"type": "integer"},
                    "bits": {"type": "integer"}
                }
            },
            "vdaf": {"type": "string"},
            "hpke_config": {"type": "string"}
        }
    }
}

ads = {}

def read_json(config_url):
    """Read configuration from Google Cloud bucket."""
    resp = requests.get(config_url)
    return resp.json()

def json_array_find_duplicates(json_array, keys):
    duplicates = {key: [] for key in keys}
    seen_values = {key: set() for key in keys}

    for item in json_array:
        for key in keys:
            if key in item:
                value = item[key]
                if value in seen_values[key]:
                    duplicates[key].append(value)
                else:
                    seen_values[key].add(value)
    return {key: val for key, val in duplicates.items() if val}

def validate_json_config(data, unique_keys, json_schema):
    if unique_keys:
        if not isinstance(data, list):
            raise ValueError(f"uniqueKeys supplied but data not a list")
        
        duplicates  = json_array_find_duplicates(data, ["taskId", "taskIndex"])
        if duplicates:
            raise ValueError(f"data contains duplicates for unique_keys, duplicates: {duplicates}")

    try:
        validate(instance=data, schema=json_schema)
    except ValidationError as e:
        raise ValueError(f"schema validation failed: {e.message}")

def validate_task_data(taskdata):
    time_precision_minutes = taskdata["time_precision_minutes"]
    if time_precision_minutes == 0:
        return f"time_precision_minutes can not be zero"
    if time_precision_minutes < MINUTES_IN_DAY:
        if MINUTES_IN_DAY % time_precision_minutes !=0:
            return f"Task has time precision that does not evenly divide a day"
        else:
            return None
    if time_precision_minutes % MINUTES_IN_DAY !=0:
        return f"time_precision_minutes is longer than a day but is not a whole multiple of a day"

    start_date=datetime.datetime.strptime(taskdata["start_date"], "%Y-%b-%d")
    end_date=datetime.datetime.strptime(taskdata["end_date"], "%Y-%b-%d")
    ttl_minutes=(end_date-start_date)/60
    ttl_minutes = (end_date - start_date).total_seconds() / 60

    if ttl_minutes % time_precision_minutes !=0:
        return f"time_precision_minutes ({time_precision_minutes}) does not allow a full cocverage between {start_date} and end_date: {end_date} ({ttl_minutes} minutes )"

    return None

def get_ad_config(ad_config_url):
    data = read_json(ad_config_url)
    validate_json_config(data, ["taskId", "taskIndex"], AD_CONFIG_JSON_SCHEMA)
    return data    

def get_task_config(task_config_url):
    data = read_json(task_config_url)
    validate_json_config(data, ["task_id"], TASK_CONFIG_JSON_SCHEMA)
    validate_task_data(data)
    return data

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

    rpt = build_base_report(task, timestamp, collection_time)

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
        rpt["error"] = "TIMEOUT"

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
                entries = parse_vector(line[21:-1])

                rpt["value"] = entries

                for i, entry in enumerate(entries):
                    ad = get_ad(task["task_id"], i)
                    print(task["task_id"], i, ad)
                    if ad is not None:
                        cnt = {}
                        cnt["collection_time"] = timestamp
                        cnt["placement_id"] = ad["advertiserInfo"]["placementId"]
                        cnt["advertiser_id"] = ad["advertiserInfo"]["advertiserId"]
                        cnt["advertiser_name"] = ad["advertiserInfo"]["advertiserName"]
                        cnt["ad_id"] = ad["advertiserInfo"]["adId"]
                        cnt["task_id"] = task["task_id"]
                        cnt["task_index"] = i
                        cnt["task_size"] = task["vdaf_args_structured"]["length"]
                        cnt["campaign_id"] = ad["advertiserInfo"]["campaignId"]
                        cnt["conversion_count"] = entry

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


def parse_vector(histogram_str):
    count_strs = histogram_str.split(",")
    return [
        correct_wraparound(int(count_str))
        for count_str in count_strs
    ]


def correct_wraparound(num):
    cutoff = 2 ** (FIELD_SIZE - 1)

    if num > cutoff:
        return num - FIELD_PRIME

    return num


def build_base_report(task, timestamp, collection_time):
    row = {}
    row["task_id"] = task["task_id"]
    row["slot_start"] = timestamp
    row["metric_type"] = task["vdaf"]
    row["collection_time"] = collection_time
    row["time_precision_minutes"] = task["time_precision_minutes"]
    return row


def build_error_result(task, timestamp, error):
    collection_time = str(datetime.datetime.now(datetime.timezone.utc).timestamp())

    results = {}
    results["counts"] = []
    results["reports"] = []
    slot_start = int(timestamp.timestamp())

    rpt = build_base_report(task, slot_start, collection_time)
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


def check_collection_dates(start_collection_date, end_collection_date, task):
    """
    Check if the collection dates are valid based on the task's ad start and end dates.

    The function validates:
    1. If the `start_collection_date` is after or equal to the task's ad start date.
    2. If the `end_collection_date` is before or equal to the task's ad end date.
    3. For task with time_precision_minutes longer than a day -- If the `start_collection_date` is aligned with the `time_precision_minutes` defined in the task.
    
    Args:
        start_collection_date (datetime): The start date of the collection.
        end_collection_date (datetime): The end date of the collection.
        task (dict): The task dictionary containing 'start_date', 'end_date', and 'time_precision_minutes'.

    Returns:
        str: An error message if a validation fails, otherwise None.
    """

    ad_start_date = datetime.datetime.strptime(task["start_date"], "%Y-%b-%d").replace(tzinfo=datetime.timezone.utc)
    ad_end_date=datetime.datetime.strptime(task["end_date"], "%Y-%b-%d").replace(tzinfo=datetime.timezone.utc)

    if start_collection_date < ad_start_date:
        return f"start_collection_date {start_collection_date} is before ad_start_date {ad_start_date}"
    if end_collection_date > ad_end_date:
        return f"end_collection_date {end_collection_date} is after ad_end_date {ad_end_date}"

    time_precision_minutes = task["time_precision_minutes"]

    if time_precision_minutes < MINUTES_IN_DAY:
        return None

    minutes_after_ad_start = (start_collection_date - ad_start_date).total_seconds()/60
    if minutes_after_ad_start % time_precision_minutes != 0:
        return f"start_collection_date is not aligned with the time_precision_minutes of {time_precision_minutes}."

    return None

async def collect_task(task, auth_token, hpke_private_key, date):
    """Collects data for the given task through to the given day.
        For tasks with time precision smaller than a day, will collect data for aggregations from the day prior to {date}.
        For tasks with time precision a day or multiple of day, will collect data for the aggregation that ends on {date}.
            will not collect anything if
                {date} does not align with the end of an aggregation.
                ~~~~~~~~~~~~~~~~~~~~~~~~~
    """
    end_collection_date = datetime.datetime.fromisoformat(date).replace(tzinfo=datetime.timezone.utc)
    err = check_collection_date(end_collection_date)
    if err is not None:
        return build_error_result(task, end_collection_date, err)
    
    time_precision_minutes = task["time_precision_minutes"]

    if time_precision_minutes < MINUTES_IN_DAY:
        start_collection_date = end_collection_date - datetime.timedelta(days=1)
    else:
        aggregation_days = time_precision_minutes/MINUTES_IN_DAY
        start_collection_date = end_collection_date - datetime.timedelta(days=aggregation_days)
    

    err = check_collection_dates(start_collection_date, end_collection_date, task)
    if err is not None:
        return build_error_result(task, end_collection_date, err)

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
    ads = get_ad_config(ad_config_url)
    ensure_table(bqclient, ad_table_id, ADS_SCHEMA)
    ensure_table(bqclient, report_table_id, REPORT_SCHEMA)

    reports = []
    counts = []
    for task in get_task_config(task_config_url):
        print(f"Now processing task: {task['task_id']}")
        results = asyncio.run(collect_task(task, auth_token, hpke_private_key, date))
        reports += results["reports"]
        counts += results["counts"]

    store_data(reports, bqclient, report_table_id)
    store_data(counts, bqclient, ad_table_id)

if __name__ == "__main__":
    main()
