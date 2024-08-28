import base64
import json
from datetime import datetime, timedelta, timezone
from pprint import pprint

from google.cloud import storage
from google.cloud.monitoring_v3 import (
    Aggregation,
    ListTimeSeriesRequest,
    MetricServiceClient,
    TimeInterval,
)
from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp

from fxci_etl.config import Config
from fxci_etl.loaders.bigquery import BigQueryLoader
from fxci_etl.schemas import Metrics

METRIC = "compute.googleapis.com/instance/uptime"
DEFAULT_INTERVAL = 3600 * 6
MIN_BUFFER_TIME = 10  # minutes


class MetricExporter:
    def __init__(self, config):
        self.config = config

        if config.storage.credentials:
            self.storage_client = storage.Client.from_service_account_info(
                json.loads(base64.b64decode(config.storage.credentials).decode("utf8"))
            )
        else:
            self.storage_client = storage.Client(project=config.storage.project)

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

    def get_time_interval(self, date: str) -> TimeInterval:
        """Return the time interval for the specified date."""
        now = datetime.now(timezone.utc)
        date_obj = datetime.strptime(date, "%Y-%m-%d")
        start_time = datetime.combine(date_obj, datetime.min.time()).replace(tzinfo=timezone.utc)
        end_time = datetime.combine(date_obj, datetime.max.time()).replace(tzinfo=timezone.utc)

        # Ensure end_time is at least 10 minutes in the past to ensure Cloud
        # Monitoring has finished adding metrics for the prior day.
        if now <= end_time + timedelta(minutes=MIN_BUFFER_TIME):
            raise Exception(f"Abort: metric export ran too close to {end_time}! "
                            f"It must run at least {MIN_BUFFER_TIME} minutes after this time.")

        return TimeInterval(
            end_time=Timestamp(seconds=int(end_time.timestamp())),
            start_time=Timestamp(seconds=int(start_time.timestamp())),
        )



def export_metrics(config: Config, date: str, dry_run: bool = False) -> int:
    exporter = MetricExporter(config)

    interval = exporter.get_time_interval(date)

    records = []
    for project in config.monitoring.projects:
        for ts in exporter.get_timeseries(project, interval):
            if dry_run:
                pprint(ts)
                continue

            records.append(
                Metrics.from_dict(
                    {
                        "project": ts.resource.labels["project_id"],
                        "zone": ts.resource.labels["zone"],
                        "instance_id": ts.resource.labels["instance_id"],
                        "uptime": round(ts.points[0].value.double_value, 2),
                        "interval_start_time": ts.points[
                            0
                        ].interval.start_time.timestamp(),  # type: ignore
                        "interval_end_time": ts.points[0].interval.end_time.timestamp(),  # type: ignore
                    }
                )
            )

    if dry_run:
        return 0

    if not records:
        raise Exception("Abort: No records retrieved!")

    loader = BigQueryLoader(config, "metrics")
    loader.replace(date, records)
    return 0
