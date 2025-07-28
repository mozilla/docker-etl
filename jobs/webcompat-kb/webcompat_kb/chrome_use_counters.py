import argparse
import logging
from datetime import date, datetime, timedelta
from typing import Mapping, Optional, Sequence
from dataclasses import asdict, dataclass

import pydantic
from google.cloud import bigquery

from .base import EtlJob, dataset_arg
from .bqhelpers import BigQuery
from .httphelpers import get_json


class WebFeaturePopularity(pydantic.BaseModel):
    bucket_id: int
    date: date
    day_percentage: float
    property_name: str


@dataclass
class UseCounter:
    feature: str
    use_counter_name: str
    bucket_id: int
    date: date
    day_percentage: float


def get_use_counter_table(client: BigQuery) -> bigquery.Table:
    schema = [
        bigquery.SchemaField("feature", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("use_counter_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("bucket_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("day_percentage", "FLOAT", mode="REQUIRED"),
    ]
    return client.ensure_table("use_counters", schema)


def get_last_import(
    client: BigQuery,
) -> tuple[bigquery.Table, Optional[datetime]]:
    runs_table = client.ensure_table(
        "import_runs",
        [bigquery.SchemaField("run_at", "TIMESTAMP", mode="REQUIRED")],
    )
    query = "SELECT run_at FROM import_runs ORDER BY run_at DESC LIMIT 1"
    result = list(client.query(query))
    if len(result):
        row = result[0]
        return runs_table, row.run_at
    return runs_table, None


def get_use_counters_list() -> Sequence[WebFeaturePopularity]:
    logging.info("Getting webfeaturepopularity data")
    data = get_json("https://chromestatus.com/data/webfeaturepopularity")
    assert isinstance(data, list)
    return [WebFeaturePopularity.model_validate(item) for item in data]


def get_use_counter_historic(bucket_id: int) -> Sequence[WebFeaturePopularity]:
    logging.info(f"Getting webfeaturepopularity timeline data for bucket {bucket_id}")
    data = get_json(
        f"https://chromestatus.com/data/timeline/webfeaturepopularity?bucket_id={bucket_id}"
    )
    assert isinstance(data, list)
    return [WebFeaturePopularity.model_validate(item) for item in data]


def get_current_use_counter_data(
    client: BigQuery, web_features_dataset: str
) -> Mapping[str, tuple[str, Optional[date]]]:
    rv = {}
    query = f"""
WITH feature_name_map AS (
  SELECT feature, REPLACE(INITCAP(feature), "-", "") as use_counter_name
  FROM {client.project_id}.{web_features_dataset}.features_latest
)
SELECT feature, feature_name_map.use_counter_name, MAX(date) as last_record_date
FROM feature_name_map
LEFT JOIN use_counters USING(feature)
GROUP BY feature, feature_name_map.use_counter_name
"""
    for row in client.query(query):
        rv[row.use_counter_name] = (row.feature, row.last_record_date)
    return rv


def update_chrome_use_counters(
    client: BigQuery, web_features_dataset: str, recreate: bool
) -> None:
    if recreate and client.write:
        client.delete_table("use_counters")
    use_counter_table = get_use_counter_table(client)
    last_import_table, last_updated_at = get_last_import(client)
    if last_updated_at is not None and last_updated_at.date() == datetime.now().date():
        logging.info("Already updated use counter data today")
        return

    updates = []
    use_counters = get_use_counters_list()
    use_counter_data = get_current_use_counter_data(client, web_features_dataset)
    for use_counter in use_counters:
        if use_counter.property_name not in use_counter_data:
            logging.warning(
                f"Web feature not found for use counter {use_counter.property_name}"
            )
            continue
        feature, last_date = use_counter_data[use_counter.property_name]
        if last_date is None or use_counter.date != last_date + timedelta(days=1):
            new_data = [
                item
                for item in get_use_counter_historic(use_counter.bucket_id)
                if last_date is None or item.date > last_date
            ]
        else:
            new_data = [use_counter]

        for row in new_data:
            updates.append(
                UseCounter(
                    feature=feature,
                    use_counter_name=row.property_name,
                    bucket_id=row.bucket_id,
                    date=row.date,
                    day_percentage=row.day_percentage,
                )
            )

    updates_json = []
    for item in updates:
        item_dict = asdict(item)
        item_dict["date"] = item_dict["date"].strftime("%Y-%m-%d")
        updates_json.append(item_dict)

    logging.info(f"Adding {len(updates)} new use counter entries")
    # write_table appears to be much faster than insert_rows for large updates,
    # but means manually converting to JSON
    client.write_table(
        use_counter_table, use_counter_table.schema, updates_json, overwrite=recreate
    )
    logging.info("Updating last import time")
    client.insert_rows(last_import_table, [{"run_at": datetime.now()}])


class ChromeUseCountersJob(EtlJob):
    name = "chrome-use-counters"

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(
            title="Chrome Use Counters", description="Chrome use counters arguments"
        )
        group.add_argument(
            "--bq-chrome-use-counters-dataset",
            type=dataset_arg,
            help="BigQuery Chrome use counters dataset id",
        )
        group.add_argument(
            "--chrome-use-counters-recreate",
            action="store_true",
            help="Recreate Chrome use counters data",
        )

    def default_dataset(self, args: argparse.Namespace) -> str:
        return args.bq_chrome_use_counters_dataset

    def required_args(self) -> set[str | tuple[str, str]]:
        return {"bq_chrome_use_counters_dataset", "bq_web_features_dataset"}

    def main(self, client: BigQuery, args: argparse.Namespace) -> None:
        update_chrome_use_counters(
            client, args.bq_web_features_dataset, args.chrome_use_counters_recreate
        )
