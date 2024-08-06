from datetime import datetime, timedelta
from unittest.mock import call

from freezegun import freeze_time
from google.cloud.exceptions import NotFound
from google.cloud.monitoring_v3 import Aggregation, ListTimeSeriesRequest, TimeInterval
from google.protobuf.timestamp_pb2 import Timestamp

from fxci_etl.metric import export



@freeze_time("2024-08-01")
def test_metric_exporter_get_timeseries(mocker, make_config):
    # constants
    project = "proj"
    start_time = datetime.now()
    end_time = int((start_time + timedelta(hours=1)).timestamp())
    start_time = int(start_time.timestamp())
    interval = TimeInterval(
        start_time=Timestamp(seconds=start_time),
        end_time=Timestamp(seconds=end_time),
    )

    # mock
    mocker.patch.object(export, "storage", mocker.Mock())
    mocker.patch.object(export, "MetricServiceClient", mocker.Mock())

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



@freeze_time("2024-08-01")
def test_metric_exporter_get_time_interval(mocker, make_config):
    # constants
    now = datetime.now()
    prev_end_time = now - timedelta(hours=1)

    # mock
    mocker.patch.object(export, "storage", mocker.Mock())
    mocker.patch.object(export, "MetricServiceClient", mocker.Mock())

    config = make_config()
    exporter = export.MetricExporter(config)

    # test common case
    exporter.last_export.download_as_string.return_value = (  # type: ignore
        f'{{"end_time": {int(prev_end_time.timestamp())}}}'
    )
    result = exporter.get_time_interval()
    assert isinstance(result, TimeInterval)
    assert result.start_time.timestamp() == prev_end_time.timestamp()  # type: ignore
    assert result.end_time.timestamp() == (now - timedelta(minutes=10)).timestamp()  # type: ignore

    # test last_end_time not found
    exporter.last_export.download_as_string.side_effect = NotFound("")  # type: ignore
    result = exporter.get_time_interval()
    assert isinstance(result, TimeInterval)
    assert (
        result.start_time.timestamp()  # type: ignore
        == (
            now - timedelta(minutes=10) - timedelta(seconds=export.MINIMUM_INTERVAL)
        ).timestamp()
    )
    assert result.end_time.timestamp() == (now - timedelta(minutes=10)).timestamp()  # type: ignore


def test_export_metrics(mocker, make_config):
    # constants
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
    mock_exporter.set_last_end_time.return_value = None
    mock_exporter.get_timeseries.return_value = [mock_ts]

    mock_loader = mocker.Mock()
    mock_loader.insert.return_value = None

    mocker.patch.object(
        export, "MetricExporter", mocker.Mock(return_value=mock_exporter)
    )
    mocker.patch.object(export, "BigQueryLoader", mocker.Mock(return_value=mock_loader))

    config = make_config()

    # test dry_run
    result = export.export_metrics(config, dry_run=True)
    assert result == 0
    mock_exporter.get_time_interval.assert_called_once_with()
    assert mock_exporter.get_timeseries.call_count == 2
    mock_exporter.get_timeseries.assert_has_calls(
        [
            call("fxci-production-level1-workers", mock_interval),
            call("fxci-production-level3-workers", mock_interval),
        ]
    )
    assert mock_exporter.set_last_end_time.called is False
    assert mock_loader.insert.called is False

    mock_exporter.reset_mock()

    # test non dry_run
    result = export.export_metrics(config, dry_run=False)
    assert result == 0
    mock_exporter.get_time_interval.assert_called_once_with()
    assert mock_exporter.get_timeseries.call_count == 2
    mock_exporter.get_timeseries.assert_has_calls(
        [
            call("fxci-production-level1-workers", mock_interval),
            call("fxci-production-level3-workers", mock_interval),
        ]
    )
    assert mock_exporter.set_last_end_time.called is True
    record = export.WorkerUptime.from_dict(
        "foo",
        {
            "submission_date": "2024-08-01",
            "instance_id": instance_id,
            "project": project,
            "zone": zone,
            "uptime": uptime,
            "interval_start_time": start_time,
            "interval_end_time": end_time,
        },
    )
    mock_loader.insert.assert_called_once_with([record, record])
