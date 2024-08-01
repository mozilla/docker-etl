import base64
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pprint import pprint

import pytz
from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.cloud.monitoring_v3 import (
    Aggregation,
    ListTimeSeriesRequest,
    MetricServiceClient,
    TimeInterval,
)
from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp

from fxci_etl.config import Config
from fxci_etl.loaders.bigquery import BigQueryLoader, BigQueryTypes as t, Record

METRIC = "compute.googleapis.com/instance/uptime"
DEFAULT_INTERVAL = 3600 * 6
MIN_BUFFER_TIME = 10  # minutes


@dataclass
class WorkerUptime(Record):
    instance_id: t.STRING
    project: t.STRING
    zone: t.STRING
    uptime: t.FLOAT
    interval_start_time: t.TIMESTAMP
    interval_end_time: t.TIMESTAMP

    def __str__(self):
        return f"worker {self.instance_id}"


class MetricExporter:
    def __init__(self, config):
        self.config = config

        if config.storage.credentials:
            self.storage_client = storage.Client.from_service_account_info(
                json.loads(base64.b64decode(config.storage.credentials).decode("utf8"))
            )
        else:
            self.storage_client = storage.Client()

        bucket = self.storage_client.bucket(config.storage.bucket)
        self.last_export = bucket.blob("last_uptime_export_interval.json")

        if config.monitoring.credentials:
            self.metric_client = MetricServiceClient.from_service_account_info(
                json.loads(base64.b64decode(config.monitoring.credentials).decode("utf8"))
            )
        else:
            self.metric_client = MetricServiceClient()

    def get_timeseries(self, project: str, interval: TimeInterval):
        metric_filter = f'metric.type="{METRIC}"'

        aggregation = Aggregation(
            alignment_period=Duration(
                seconds=int(interval.end_time.timestamp())  # type: ignore
                - int(interval.start_time.timestamp())  # type: ignore
            ),
            per_series_aligner=Aggregation.Aligner.ALIGN_SUM,
            cross_series_reducer=Aggregation.Reducer.REDUCE_SUM,
            group_by_fields=[
                "metric.labels.instance_name",
                "resource.labels.instance_id",
                "resource.labels.zone",
            ],
        )

        results = self.metric_client.list_time_series(
            request={
                "name": f"projects/{project}",
                "filter": metric_filter,
                "interval": interval,
                "view": ListTimeSeriesRequest.TimeSeriesView.FULL,
                "aggregation": aggregation,
            }
        )

        return results

    def get_time_interval(self) -> TimeInterval:
        """Return the time interval to query metrics over.

        This will grab metrics all metrics from the last end time, up until
        11:59:59 of yesterday. Ideally the metric export runs in a daily cron
        task, such that it exports a days worth of data at a time.
        """
        utc = pytz.UTC
        now = datetime.now(utc)
        yesterday = now.date() - timedelta(days=1)
        end_time = utc.localize(datetime.combine(yesterday, datetime.max.time()))

        # Ensure end_time is at least 10 minutes in the past to ensure Cloud
        # Monitoring has finished adding metrics for the prior day.
        if now <= end_time + timedelta(minutes=MIN_BUFFER_TIME):
            raise Exception(f"Abort: metric export ran too close to {end_time}! "
                            f"It must run at least {MIN_BUFFER_TIME} minutes after this time.")

        try:
            start_time = json.loads(self.last_export.download_as_string())["end_time"]
        except NotFound:
            start_time = int(utc.localize(datetime.combine(yesterday, datetime.min.time())).timestamp())

        end_time = int(end_time.timestamp())

        if start_time >= end_time:
            raise Exception(f"Abort: metric export already ran for {yesterday}!")

        return TimeInterval(
            end_time=Timestamp(seconds=end_time),
            start_time=Timestamp(seconds=start_time),
        )

    def set_last_end_time(self, end_time: int):
        self.last_export.upload_from_string(json.dumps({"end_time": end_time}))


def export_metrics(config: Config, dry_run: bool = False) -> int:
    exporter = MetricExporter(config)

    interval = exporter.get_time_interval()

    records = []
    for project in config.monitoring.projects:
        for ts in exporter.get_timeseries(project, interval):
            if dry_run:
                pprint(ts)
                continue

            records.append(
                WorkerUptime.from_dict(
                    config.bigquery.tables.metrics,
                    {
                        "project": ts.resource.labels["project_id"],
                        "zone": ts.resource.labels["zone"],
                        "instance_id": ts.resource.labels["instance_id"],
                        "uptime": round(ts.points[0].value.double_value, 2),
                        "interval_start_time": ts.points[
                            0
                        ].interval.start_time.timestamp(),
                        "interval_end_time": ts.points[0].interval.end_time.timestamp(),
                    }
                )
            )

    if dry_run:
        return 0

    if not records:
        raise Exception("Abort: No records retrieved!")

    exporter.set_last_end_time(int(interval.end_time.timestamp()))

    loader = BigQueryLoader(config)
    loader.insert(records)
    return 0
