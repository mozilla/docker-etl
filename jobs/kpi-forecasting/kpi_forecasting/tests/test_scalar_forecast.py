import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from kpi_forecasting.models.scalar_forecast import ScalarForecast


@pytest.fixture
def setup_forecast():
    observed_df = pd.DataFrame(
        {
            "submission_date": pd.date_range(start="2020-01-01", periods=6, freq="M"),
            "value": [100, 150, 200, 250, 300, 350],
            "segment": ["A", "A", "A", "B", "B", "B"],
        }
    )
    dates_to_predict = pd.DataFrame(
        {"submission_date": pd.date_range(start="2020-07-01", periods=6, freq="M")}
    )
    metric_hub = MagicMock()
    metric_hub.slug = "metric_slug"
    metric_hub.segments = {"segment": ["A", "B"]}
    metric_hub.alias = "metric_alias"
    metric_hub.app_name = "app_name"
    metric_hub.min_date = "2019-01-01"
    metric_hub.max_date = "2020-06-01"

    start_date = "2020-07-01"
    end_date = "2020-12-31"
    scalar_adjustments = [MagicMock()]
    parameters = MagicMock()
    parameters.formula = "metric:YOY + metric2:MOM"

    forecast = ScalarForecast(
        model_type="scalar",
        parameters=parameters,
        use_holidays=False,
        start_date=start_date,
        end_date=end_date,
        metric_hub=metric_hub
    )

    forecast.observed_df = observed_df
    forecast.scalar_adjustments = scalar_adjustments

    return forecast


def test_post_init(setup_forecast):
    forecast = setup_forecast
    assert forecast.start_date == "2020-07-01"
    assert len(forecast.scalar_adjustments) == 1
    assert list(forecast.combination_df.columns) == ["segment"]


def test_period_names_map(setup_forecast):
    forecast = setup_forecast
    assert forecast.period_names_map == {
        "YOY": pd.DateOffset(years=1),
        "MOM": pd.DateOffset(months=1),
    }


def test_parse_formula_for_over_period_changes(setup_forecast):
    forecast = setup_forecast
    result = forecast._parse_formula_for_over_period_changes()
    assert result == {"metric": "YOY", "metric2": "MOM"}


def test_add_scalar_columns(setup_forecast):
    forecast = setup_forecast
    forecast.forecast
    _df = forecast.dates_to_predict.merge(
        forecast.combination_df, how="cross"
    )
    forecast._add_scalar_columns()
    assert "scalar_mock" in forecast.forecast_df.columns


def test_fit(setup_forecast):
    forecast = setup_forecast
    with patch.object(
        forecast, "_parse_formula_for_over_period_changes", return_value=None
    ), patch.object(forecast, "_add_scalar_columns"):
        forecast._fit()
        assert forecast.metric_hub.alias in forecast.forecast_df.columns
        assert not forecast.forecast_df[forecast.metric_hub.alias].isnull().any()


def test_predict(setup_forecast):
    forecast = setup_forecast
    with patch.object(forecast, "_set_seed"), patch.object(forecast, "_predict"):
        forecast.predict()
        assert forecast.predicted_at is not None


def test_summarize(setup_forecast):
    forecast = setup_forecast
    with patch.object(
        forecast, "_summarize", return_value=pd.DataFrame()
    ), patch.object(forecast, "_add_summary_metadata"):
        forecast.summarize(requires_summarization=False)
        assert forecast.summary_df is not None


@patch("bigquery.Client")
def test_write_results(mock_client, setup_forecast):
    forecast = setup_forecast
    mock_client_instance = mock_client.return_value
    mock_load_job = MagicMock()
    mock_client_instance.load_table_from_dataframe.return_value = mock_load_job
    mock_load_job.result.return_value = None

    forecast.summary_df = pd.DataFrame(
        {"submission_date": ["2020-07-01"], "value": [100]}
    )

    forecast.write_results("project", "dataset", "table")
    mock_client_instance.load_table_from_dataframe.assert_called_once()
