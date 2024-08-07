from datetime import datetime, timedelta
from math import floor
from unittest.mock import call

import pytest
import pytz
from freezegun import freeze_time
from google.cloud.monitoring_v3 import Aggregation, ListTimeSeriesRequest, TimeInterval
from google.protobuf.timestamp_pb2 import Timestamp

from fxci_etl.metric import export
from fxci_etl.schemas import Metrics


@pytest.fixture(autouse=True)
def patch_gcp_clients(mocker):
    mocker.patch.object(export, "storage", mocker.Mock())
    mocker.patch.object(export, "MetricServiceClient", mocker.Mock())


def test_metric_exporter_get_timeseries(make_config):
    # constants
    project = "proj"
    start_time = datetime.now()
    end_time = int((start_time + timedelta(hours=1)).timestamp())
    start_time = int(start_time.timestamp())
    interval = TimeInterval(
        start_time=Timestamp(seconds=start_time),
        end_time=Timestamp(seconds=end_time),
    )

    # test
    config = make_config()
    exporter = export.MetricExporter(config)
    exporter.get_timeseries(project, interval)

    # assert
    exporter.metric_client.list_time_series.assert_called_once()  # type: ignore
    request = exporter.metric_client.list_time_series.call_args.kwargs["request"]  # type: ignore
    assert request["name"] == f"projects/{project}"
    assert request["view"] == ListTimeSeriesRequest.TimeSeriesView.FULL
    assert request["filter"] == f'metric.type="{export.METRIC}"'
    assert request["interval"].start_time.timestamp() == start_time
    assert request["interval"].end_time.timestamp() == end_time
    assert request["aggregation"].alignment_period.seconds == 3600
    assert request["aggregation"].per_series_aligner == Aggregation.Aligner.ALIGN_SUM
    assert request["aggregation"].cross_series_reducer == Aggregation.Reducer.REDUCE_SUM
    assert request["aggregation"].group_by_fields == [
        "metric.labels.instance_name",
        "resource.labels.instance_id",
        "resource.labels.zone",
    ]


@freeze_time("2024-08-02 00:15:00")
def test_metric_exporter_get_time_interval(make_config):
    # constants
    utc = pytz.UTC
    date_str = "2024-08-01"
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    expected_start_time = utc.localize(datetime.combine(date_obj, datetime.min.time()))
    expected_end_time = utc.localize(datetime.combine(date_obj, datetime.max.time()))

    config = make_config()
    exporter = export.MetricExporter(config)
    result = exporter.get_time_interval(date_str)
    assert isinstance(result, TimeInterval)
    assert result.start_time.timestamp() == floor(expected_start_time.timestamp())  # type: ignore
    assert result.end_time.timestamp() == floor(expected_end_time.timestamp())  # type: ignore


@freeze_time("2024-08-02 00:05:00")
def test_metric_exporter_get_time_interval_too_close_to_midnight(make_config):
    config = make_config()
    exporter = export.MetricExporter(config)
    with pytest.raises(Exception):
        exporter.get_time_interval("2024-08-01")


def test_export_metrics(mocker, make_config):
    # constants
    date_str = "2024-08-01"
    start_time = 123
    end_time = 456
    uptime = 10.0
    project = "proj"
    zone = "zone"
    instance_id = "instance_id"

    # mocks
    mock_interval = mocker.Mock()
    mock_interval.end_time.timestamp.return_value = end_time

    mock_point = mocker.Mock()
    mock_point.value.double_value = uptime
    mock_point.interval.start_time.timestamp.return_value = start_time
    mock_point.interval.end_time.timestamp.return_value = end_time

    mock_ts = mocker.Mock()
    mock_ts.resource.labels = {
        "project_id": project,
        "zone": zone,
        "instance_id": instance_id,
    }
    mock_ts.points = [mock_point]

    mock_exporter = mocker.Mock()
    mock_exporter.get_time_interval.return_value = mock_interval
    mock_exporter.get_timeseries.return_value = [mock_ts]

    mock_loader = mocker.Mock()
    mock_loader.insert.return_value = None
    mock_loader.replace.return_value = None

    mocker.patch.object(
        export, "MetricExporter", mocker.Mock(return_value=mock_exporter)
    )
    mocker.patch.object(export, "BigQueryLoader", mocker.Mock(return_value=mock_loader))

    config = make_config()

    # test dry_run
    result = export.export_metrics(config, date=date_str, dry_run=True)
    assert result == 0
    mock_exporter.get_time_interval.assert_called_once_with(date_str)
    assert mock_exporter.get_timeseries.call_count == 2
    mock_exporter.get_timeseries.assert_has_calls(
        [
            call("fxci-production-level1-workers", mock_interval),
            call("fxci-production-level3-workers", mock_interval),
        ]
    )
    assert mock_loader.replace.called is False

    mock_exporter.reset_mock()

    # test non dry_run
    result = export.export_metrics(config, date=date_str, dry_run=False)
    assert result == 0
    mock_exporter.get_time_interval.assert_called_once_with(date_str)
    assert mock_exporter.get_timeseries.call_count == 2
    mock_exporter.get_timeseries.assert_has_calls(
        [
            call("fxci-production-level1-workers", mock_interval),
            call("fxci-production-level3-workers", mock_interval),
        ]
    )
    record = Metrics.from_dict(
        {
            "submission_date": date_str,
            "instance_id": instance_id,
            "project": project,
            "zone": zone,
            "uptime": uptime,
            "interval_start_time": start_time,
            "interval_end_time": end_time,
        }
    )
    mock_loader.replace.assert_called_once_with(date_str, [record, record])
    assert mock_loader.insert.called is False
