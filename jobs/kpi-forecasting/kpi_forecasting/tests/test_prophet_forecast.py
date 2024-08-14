from datetime import date
from dateutil.relativedelta import relativedelta

import pandas as pd
from dotmap import DotMap
import numpy as np
import pytest
import collections


from kpi_forecasting.models.prophet_forecast import ProphetForecast

# Arbitrarily choose some date to use for the tests
TEST_DATE = date(2024, 1, 1)
TEST_DATE_STR = TEST_DATE.strftime("%Y-%m-%d")
TEST_DATE_NEXT_DAY = date(2024, 1, 1)
TEST_DATE_NEXT_DAY_STR = TEST_DATE_NEXT_DAY.strftime("%Y-%m-%d")


@pytest.fixture
def forecast():
    A1_start_date = TEST_DATE_STR
    parameter_dict = {
        "model_setting_split_dim": "a",
        "segment_settings": {
            "A1": {
                "start_date": A1_start_date,
                "end_date": None,
                "holidays": [],
                "regressors": [],
                "grid_parameters": {"param1": [1, 2], "param2": [20, 10]},
                "cv_settings": {},
            },
        },
    }

    parameter_dotmap = DotMap(parameter_dict)
    predict_start_date = TEST_DATE_NEXT_DAY_STR
    # arbitarily set it a couple months in the future
    predict_end_date = (TEST_DATE + relativedelta(months=2)).strftime("%Y-%m-%d")
    return ProphetForecast(
        model_type="test",
        parameters=parameter_dotmap,
        use_holidays=None,
        start_date=predict_start_date,
        end_date=predict_end_date,
        metric_hub=None,
    )


class MockModel:
    """Used in place of prophet.Prophet for testing purposes"""

    def __init__(self, param1=0, param2=0, **kwargs):
        self.value = param1 * param2
        self.history = None

    def fit(self, df, *args, **kwargs):
        self.history = df
        return None

    def predict(self, dates_to_predict):
        output = dates_to_predict.copy()

        output[
            [
                "yhat",
                "trend",
                "trend_upper",
                "trend_lower",
                "weekly",
                "weekly_upper",
                "weekly_lower",
                "yearly",
                "yearly_upper",
                "yearly_lower",
            ]
        ] = 0  # some dummy value so it has the right shape

        return output

    def predictive_samples(self, dates_to_predict):
        # prophet function outputs dict of numpy arrays
        # only element we care about is `yhat`
        output = np.arange(len(dates_to_predict)) * self.value
        return {"yhat": {0: output}}


def mock_build_model(parameters):
    """mocks the FunnelForecast build_model method"""
    return MockModel(
        **parameters,
    )


def mock_aggregate_forecast_observed(
    forecast_df, observed_df, period, numpy_aggregations, percentiles
):
    """Mocks the aggregate_forecast_observed function defined in ProphetForecast
    and inherited in FunnelForecast.
    This function is tested extensively in test_prophet_forecast
    so we can make dummy outputs for tests related to it"""

    # add dummy columns where aggregated metrics woudl go
    percentile_columns = [f"p{el}" for el in percentiles]
    output_forecast_df = forecast_df.copy()
    output_forecast_df[numpy_aggregations + percentile_columns] = 0
    return output_forecast_df, observed_df.copy()


def test_under_fit(forecast, mocker):
    """test the _fit method"""

    observed_data = pd.DataFrame(
        {
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
        }
    )
    mocker.patch.object(forecast, "_build_model", mock_build_model)

    forecast._fit(observed_data)

    # checking that history is set in the mocked Model ensures fit was called on it
    pd.testing.assert_frame_equal(
        observed_data.rename(columns={"submission_date": "ds"}), forecast.model.history
    )


def test_fit(forecast, mocker):
    """test the fit function.  It is inherited from BaseForecast
    and calls _fit with the proper object attributes.  Test looks very
    similar to that for _fit"""
    observed_data = pd.DataFrame(
        {
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
        }
    )
    mocker.patch.object(forecast, "_build_model", mock_build_model)

    forecast.observed_df = observed_data
    forecast.fit()

    # checking that history is set in the mocked Model ensures fit was called on it
    pd.testing.assert_frame_equal(
        observed_data.rename(columns={"submission_date": "ds"}), forecast.model.history
    )

    assert forecast.trained_at is not None


def test_combine_forecast_observed(mocker, forecast):
    """tests the _combine_forecast_observed method"""
    # forecast predictions are set with the
    # mock_aggregate_forecast_observed function so they
    # can be ommited here
    forecast_df = pd.DataFrame(
        {
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
        }
    )

    # rows with negative values are those expected to be removed
    # by filters in summarize
    observed_df = pd.DataFrame(
        {
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
            "value": [10, 20],
        }
    )

    mocker.patch.object(
        forecast, "_aggregate_forecast_observed", mock_aggregate_forecast_observed
    )

    numpy_aggregations = ["mean"]
    percentiles = [10, 50, 90]
    output_df = forecast._combine_forecast_observed(
        forecast_df,
        observed_df,
        period="period",
        numpy_aggregations=numpy_aggregations,
        percentiles=percentiles,
    )
    observed_expected_df = pd.DataFrame(
        {
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
            "value": [10, 20],
            "measure": ["observed", "observed"],
            "source": ["historical", "historical"],
        }
    )

    # 4x2 columns, 4 metrics (mean, p10, p50, p90)
    forecast_expected_df = pd.DataFrame(
        {
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
            "measure": ["mean", "mean", "p10", "p10", "p50", "p50", "p90", "p90"],
            "value": [0] * 8,
            "source": ["forecast"] * 8,
        }
    )

    # concat in same order to make our lives easier
    expected = pd.concat([observed_expected_df, forecast_expected_df]).sort_values(
        ["submission_date", "measure"]
    )
    assert set(expected.columns) == set(output_df.columns)
    # force value columns to be floats in both cases to make check easier
    numeric_cols = ["value", "value_low", "value_mid", "value_high"]
    pd.testing.assert_frame_equal(
        output_df.sort_values(["submission_date", "measure"]).reset_index(drop=True),
        expected[output_df.columns].reset_index(drop=True),
    )

    # should not be any nulls outside the metric column
    non_metric_columns = [el for el in output_df.columns if el not in numeric_cols]
    assert not pd.isna(output_df[non_metric_columns]).any(axis=None)


def test_under_summarize(mocker, forecast):
    """testing _summarize"""
    # forecast predictions are set with the
    # mock_aggregate_forecast_observed function so they
    # can be ommited here
    forecast_df = pd.DataFrame(
        {
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
        }
    )

    # rows with negative values are those expected to be removed
    # by filters in summarize
    observed_df = pd.DataFrame(
        {
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
            "value": [10, 20],
        }
    )

    mocker.patch.object(
        forecast, "_aggregate_forecast_observed", mock_aggregate_forecast_observed
    )

    numpy_aggregations = ["mean"]
    percentiles = [10, 50, 90]
    output_df = forecast._summarize(
        forecast_df,
        observed_df,
        period="period",
        numpy_aggregations=numpy_aggregations,
        percentiles=percentiles,
    )
    observed_expected_df = pd.DataFrame(
        {
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
            "value": [10, 20],
            "measure": ["observed", "observed"],
            "source": ["historical", "historical"],
        }
    )

    # 4x2 columns, 4 metrics (mean, p10, p50, p90)
    forecast_expected_df = pd.DataFrame(
        {
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
            "measure": ["mean", "mean", "p10", "p10", "p50", "p50", "p90", "p90"],
            "value": [0] * 8,
            "source": ["forecast"] * 8,
        }
    )

    # concat in same order to make our lives easier
    expected = pd.concat([observed_expected_df, forecast_expected_df]).sort_values(
        ["submission_date", "measure"]
    )
    expected["aggregation_period"] = "period"

    assert set(expected.columns) == set(output_df.columns)
    # force value columns to be floats in both cases to make check easier
    numeric_cols = ["value", "value_low", "value_mid", "value_high"]
    pd.testing.assert_frame_equal(
        output_df.sort_values(["submission_date", "measure"]).reset_index(drop=True),
        expected[output_df.columns].reset_index(drop=True),
    )

    # should not be any nulls outside the metric column
    non_metric_columns = [el for el in output_df.columns if el not in numeric_cols]
    assert not pd.isna(output_df[non_metric_columns]).any(axis=None)


def test_summarize(mocker, forecast):
    """testing summarize"""
    # create dummy metric hub object to when meta data from
    # it is added we don't get an error
    MetricHub = collections.namedtuple(
        "MetricHub",
        ["alias", "app_name", "slug", "min_date", "max_date"],
    )

    dummy_metric_hub = MetricHub("", "", "", TEST_DATE_STR, TEST_DATE_STR)

    # forecast predictions are set with the
    # mock_aggregate_forecast_observed function so they
    # can be ommited here
    forecast_df = pd.DataFrame(
        {
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
        }
    )

    # rows with negative values are those expected to be removed
    # by filters in summarize
    observed_df = pd.DataFrame(
        {
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
            "value": [10, 20],
        }
    )

    mocker.patch.object(
        forecast, "_aggregate_forecast_observed", mock_aggregate_forecast_observed
    )

    numpy_aggregations = ["mean"]
    percentiles = [10, 50, 90]

    forecast.observed_df = observed_df
    forecast.forecast_df = forecast_df
    forecast.metric_hub = dummy_metric_hub

    #  timestamp attributes created by fit and predict
    # must be added manuall
    forecast.collected_at = ""
    forecast.trained_at = ""
    forecast.predicted_at = ""
    forecast.metadata_params = ""

    numpy_aggregations = ["mean"]
    percentiles = [10, 50, 90]
    forecast.summarize(
        periods=["period1", "period2"],
        numpy_aggregations=numpy_aggregations,
        percentiles=percentiles,
    )

    output_df = forecast.summary_df

    observed_expected_df = pd.DataFrame(
        {
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
            "value": [10, 20],
            "measure": ["observed", "observed"],
            "source": ["historical", "historical"],
        }
    )

    # 4x2 columns, 4 metrics (mean, p10, p50, p90)
    forecast_expected_df = pd.DataFrame(
        {
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
            "measure": ["mean", "mean", "p10", "p10", "p50", "p50", "p90", "p90"],
            "value": [0] * 8,
            "source": ["forecast"] * 8,
        }
    )

    # concat in same order to make our lives easier
    expected = pd.concat([observed_expected_df, forecast_expected_df]).sort_values(
        ["submission_date", "measure"]
    )
    expected1 = expected.copy()
    expected2 = expected.copy()
    expected1["aggregation_period"] = "period1"
    expected2["aggregation_period"] = "period2"

    expected = pd.concat([expected1, expected2])

    # not going to check all the metadata columns
    # in assert_frame_equal.  Just make sure they're there
    metadata_columns = {
        "metric_alias",
        "metric_hub_app_name",
        "metric_hub_slug",
        "metric_start_date",
        "metric_end_date",
        "metric_collected_at",
        "forecast_start_date",
        "forecast_end_date",
        "forecast_trained_at",
        "forecast_predicted_at",
        "forecast_parameters",
    }
    assert set(expected.columns) | metadata_columns == set(output_df.columns)
    # force value columns to be floats in both cases to make check easier
    numeric_cols = ["value", "value_low", "value_mid", "value_high"]
    pd.testing.assert_frame_equal(
        output_df.sort_values(["submission_date", "aggregation_period", "measure"])[
            expected.columns
        ].reset_index(drop=True),
        expected.sort_values(
            ["submission_date", "aggregation_period", "measure"]
        ).reset_index(drop=True),
    )

    # should not be any nulls outside the metric column
    non_metric_columns = [el for el in output_df.columns if el not in numeric_cols]
    assert not pd.isna(output_df[non_metric_columns]).any(axis=None)


def test_under_predict(mocker, forecast):
    """testing _predict"""
    # this ensures forecast is using MockModel
    mocker.patch.object(forecast, "_build_model", mock_build_model)

    observed_df = pd.DataFrame(
        {
            "y": [0, 1],
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
        }
    )

    dates_to_predict = pd.DataFrame(
        {
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ]
        }
    )
    forecast.observed_df = observed_df
    forecast.parameters = {"param1": 1, "param2": 2}
    forecast.fit()
    out = forecast._predict(dates_to_predict).reset_index(drop=True)

    # in MockModel, the predictive_samples method sets the output to
    # np.arange(len(dates_to_predict)) * self.value for one column called 0
    # this helps ensure the forecast_df in segment_models is set properly
    expected = pd.DataFrame(
        {
            0: [0, 2],
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
        }
    )

    pd.testing.assert_frame_equal(out, expected)

    # test predict while we're here

    forecast.dates_to_predict = dates_to_predict
    forecast.number_of_simulations = 1  # so that _validate doesn't break
    forecast.predict()

    out = forecast.forecast_df

    # in MockModel, the predictive_samples method sets the output to
    # np.arange(len(dates_to_predict)) * self.value for one column called 0
    # this helps ensure the forecast_df in segment_models is set properly
    expected = pd.DataFrame(
        {
            0: [0, 2],
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
        }
    )

    pd.testing.assert_frame_equal(out, expected)
    assert forecast.predicted_at is not None


def test_summarize_non_overlapping_day():
    observed_start_date = TEST_DATE_STR
    observed_end_date = (TEST_DATE + relativedelta(months=1)).strftime("%Y-%m-%d")

    predict_start_date = (TEST_DATE + relativedelta(months=1, days=1)).strftime(
        "%Y-%m-%d"
    )
    predict_end_date = (TEST_DATE + relativedelta(months=2)).strftime("%Y-%m-%d")

    forecast = ProphetForecast(
        model_type="test",
        parameters=DotMap(),
        use_holidays=None,
        start_date=predict_start_date,
        end_date=predict_end_date,
        metric_hub=None,
    )
    observed_submission_dates = pd.date_range(
        pd.to_datetime(observed_start_date), pd.to_datetime(observed_end_date)
    ).date
    predict_submission_dates = forecast.dates_to_predict["submission_date"].values

    observed_df = pd.DataFrame(
        {
            "submission_date": observed_submission_dates,
            "value": range(len(observed_submission_dates)),
        }
    )

    # there are the samples generated
    # the mean and median are the aggregates used
    test_samples = np.array([1, 1, 2, 3, 5, 8, 13])
    test_mean = np.mean(test_samples)
    test_median = np.median(test_samples)

    # mean and median scale with a factor
    # so a factor is multiplied on to make sure the aggregation is working
    # across rows properly
    forecast_array = np.stack(
        [test_samples * i for i in range(1, 1 + len(predict_submission_dates))],
        axis=0,
    )
    forecast_data = {str(i): forecast_array[:, i] for i in range(len(test_samples))}
    forecast_df = pd.DataFrame(
        dict(**{"submission_date": predict_submission_dates}, **forecast_data)
    )

    output_df = forecast._combine_forecast_observed(
        forecast_df, observed_df, "day", ["mean", "median"], [50]
    )

    expected_observed_df = observed_df.copy()
    expected_observed_df["source"] = "historical"
    expected_observed_df["measure"] = "observed"
    expected_observed_df["submission_date"] = (
        pd.to_datetime(expected_observed_df["submission_date"].values)
        .to_period("d")
        .to_timestamp()
    )

    forecast_mean_df = pd.DataFrame(
        {
            "submission_date": pd.to_datetime(forecast_df["submission_date"].values)
            .to_period("d")
            .to_timestamp(),
            "value": [
                test_mean * i for i in range(1, 1 + len(predict_submission_dates))
            ],
            "source": ["forecast"] * len(predict_submission_dates),
            "measure": ["mean"] * len(predict_submission_dates),
        }
    )

    forecast_median_df = pd.DataFrame(
        {
            "submission_date": pd.to_datetime(forecast_df["submission_date"].values)
            .to_period("d")
            .to_timestamp(),
            "value": [
                test_median * i for i in range(1, 1 + len(predict_submission_dates))
            ],
            "source": ["forecast"] * len(predict_submission_dates),
            "measure": ["median"] * len(predict_submission_dates),
        }
    )

    forecast_p50_df = forecast_median_df.copy()
    forecast_p50_df["measure"] = "p50"

    expected_df = pd.concat(
        [expected_observed_df, forecast_mean_df, forecast_median_df, forecast_p50_df]
    )

    assert set(expected_df.columns) == set(output_df.columns)
    columns = expected_df.columns
    expected_df_compare = (
        expected_df[columns]
        .sort_values(["submission_date", "source", "measure"])
        .reset_index(drop=True)
    )
    output_df_compare = (
        output_df[columns]
        .sort_values(["submission_date", "source", "measure"])
        .reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(
        expected_df_compare, output_df_compare, check_exact=False
    )


def test_summarize_non_overlapping_month():
    # choose arbitrary year for the start and end dates
    # two full months (Jan and Feb )
    # are in the observed data, the number of days (31 and 28 days respectively)
    # in each month is used in the checks
    observed_start_date = "2124-01-01"
    observed_end_date = "2124-02-28"

    # two full months (April and May )
    # are in the observed data, the number of days (28 and 31 days respectively)
    # in each month is used in the checks
    predict_start_date = "2124-04-01"
    predict_end_date = "2124-05-31"

    print(observed_start_date, observed_end_date)
    print(predict_start_date, predict_end_date)

    forecast = ProphetForecast(
        model_type="test",
        parameters=DotMap(),
        use_holidays=None,
        start_date=predict_start_date,
        end_date=predict_end_date,
        metric_hub=None,
    )
    observed_submission_dates = pd.date_range(
        pd.to_datetime(observed_start_date), pd.to_datetime(observed_end_date)
    ).date
    predict_submission_dates = forecast.dates_to_predict["submission_date"].values

    observed_df = pd.DataFrame(
        {
            "submission_date": observed_submission_dates,
            "value": [1] * len(observed_submission_dates),
        }
    )

    test_samples = np.array([1, 1, 2, 3, 5, 8, 13])
    test_mean = np.mean(test_samples)
    test_median = np.median(test_samples)

    forecast_array = np.stack(
        [test_samples] * len(predict_submission_dates),
        axis=0,
    )
    forecast_data = {str(i): forecast_array[:, i] for i in range(len(test_samples))}
    forecast_df = pd.DataFrame(
        dict(**{"submission_date": predict_submission_dates}, **forecast_data)
    )

    output_df = forecast._combine_forecast_observed(
        forecast_df, observed_df, "month", ["mean", "median"], [50]
    )

    expected_observed_dates = sorted(
        pd.to_datetime(observed_df["submission_date"].values)
        .to_period("m")
        .to_timestamp()
        .unique()
    )
    expected_observed_df = pd.DataFrame(
        {
            "submission_date": expected_observed_dates,
            "source": ["historical", "historical"],
            "measure": ["observed", "observed"],
            "value": [31, 28],  # number of days in each month
        }
    )

    forecast_observed_dates = sorted(
        pd.to_datetime(forecast_df["submission_date"].values)
        .to_period("m")
        .to_timestamp()
        .unique()
    )
    forecast_mean_df = pd.DataFrame(
        {
            "submission_date": forecast_observed_dates,
            "source": ["forecast", "forecast"],
            "measure": ["mean", "mean"],
            "value": [test_mean * 30, test_mean * 31],  # number of days in each month
        }
    )

    forecast_median_df = pd.DataFrame(
        {
            "submission_date": forecast_observed_dates,
            "source": ["forecast", "forecast"],
            "measure": ["median", "median"],
            "value": [
                test_median * 30,
                test_median * 31,
            ],  # number of days in each month
        }
    )

    forecast_p50_df = pd.DataFrame(
        {
            "submission_date": forecast_observed_dates,
            "source": ["forecast", "forecast"],
            "measure": ["p50", "p50"],
            "value": [
                test_median * 30,
                test_median * 31,
            ],  # number of days in each month
        }
    )

    expected_df = pd.concat(
        [expected_observed_df, forecast_mean_df, forecast_median_df, forecast_p50_df]
    )

    assert set(expected_df.columns) == set(output_df.columns)
    columns = expected_df.columns
    expected_df_compare = (
        expected_df[columns]
        .sort_values(["submission_date", "source", "measure"])
        .reset_index(drop=True)
    )
    output_df_compare = (
        output_df[columns]
        .sort_values(["submission_date", "source", "measure"])
        .reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(
        expected_df_compare, output_df_compare, check_exact=False
    )


def test_summarize_overlapping_day():
    observed_start_date = TEST_DATE_STR
    observed_end_date = (TEST_DATE + relativedelta(months=1)).strftime("%Y-%m-%d")

    predict_start_date = TEST_DATE_STR
    predict_end_date = (TEST_DATE + relativedelta(months=1)).strftime("%Y-%m-%d")

    forecast = ProphetForecast(
        model_type="test",
        parameters=DotMap(),
        use_holidays=None,
        start_date=predict_start_date,
        end_date=predict_end_date,
        metric_hub=None,
    )
    observed_submission_dates = pd.date_range(
        pd.to_datetime(observed_start_date), pd.to_datetime(observed_end_date)
    ).date
    predict_submission_dates = forecast.dates_to_predict["submission_date"].values

    observed_df = pd.DataFrame(
        {
            "submission_date": observed_submission_dates,
            "value": [1] * len(observed_submission_dates),
        }
    )

    # there are the samples generated
    # the mean and median are the aggregates used
    test_samples = np.array([1, 1, 2, 3, 5, 8, 13])
    test_mean = np.mean(test_samples)
    test_median = np.median(test_samples)

    # mean and median scale with a factor
    # so a factor is multiplied on to make sure the aggregation is working
    # across rows properly
    forecast_array = np.stack(
        [test_samples * i for i in range(1, 1 + len(predict_submission_dates))],
        axis=0,
    )
    forecast_data = {str(i): forecast_array[:, i] for i in range(len(test_samples))}
    forecast_df = pd.DataFrame(
        dict(**{"submission_date": predict_submission_dates}, **forecast_data)
    )

    output_df = forecast._combine_forecast_observed(
        forecast_df, observed_df, "day", ["mean", "median"], [50]
    )

    expected_observed_df = observed_df.copy()
    expected_observed_df["source"] = "historical"
    expected_observed_df["measure"] = "observed"
    expected_observed_df["submission_date"] = (
        pd.to_datetime(expected_observed_df["submission_date"].values)
        .to_period("d")
        .to_timestamp()
    )

    # value has + 1 due to observed (which has value=1) being added
    # due to overlap
    forecast_mean_df = pd.DataFrame(
        {
            "submission_date": pd.to_datetime(forecast_df["submission_date"].values)
            .to_period("d")
            .to_timestamp(),
            "value": [
                test_mean * i + 1 for i in range(1, 1 + len(predict_submission_dates))
            ],
            "source": ["forecast"] * len(predict_submission_dates),
            "measure": ["mean"] * len(predict_submission_dates),
        }
    )

    forecast_median_df = pd.DataFrame(
        {
            "submission_date": pd.to_datetime(forecast_df["submission_date"].values)
            .to_period("d")
            .to_timestamp(),
            "value": [
                test_median * i + 1 for i in range(1, 1 + len(predict_submission_dates))
            ],
            "source": ["forecast"] * len(predict_submission_dates),
            "measure": ["median"] * len(predict_submission_dates),
        }
    )

    forecast_p50_df = forecast_median_df.copy()
    forecast_p50_df["measure"] = "p50"

    expected_df = pd.concat(
        [expected_observed_df, forecast_mean_df, forecast_median_df, forecast_p50_df]
    )

    assert set(expected_df.columns) == set(output_df.columns)
    columns = expected_df.columns
    expected_df_compare = (
        expected_df[columns]
        .sort_values(["submission_date", "source", "measure"])
        .reset_index(drop=True)
    )
    output_df_compare = (
        output_df[columns]
        .sort_values(["submission_date", "source", "measure"])
        .reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(
        expected_df_compare, output_df_compare, check_exact=False
    )


def test_summarize_overlapping_month():
    # choose arbitrary year for the start and end dates
    # two full months (Jan and Feb )
    # are in the observed data, the number of days (31 and 28 days respectively)
    # in each month is used in the checks
    observed_start_date = "2124-01-01"
    observed_end_date = "2124-02-28"

    predict_start_date = "2124-01-01"
    predict_end_date = "2124-02-28"

    forecast = ProphetForecast(
        model_type="test",
        parameters=DotMap(),
        use_holidays=None,
        start_date=predict_start_date,
        end_date=predict_end_date,
        metric_hub=None,
    )
    observed_submission_dates = pd.date_range(
        pd.to_datetime(observed_start_date), pd.to_datetime(observed_end_date)
    ).date
    predict_submission_dates = forecast.dates_to_predict["submission_date"].values

    observed_df = pd.DataFrame(
        {
            "submission_date": observed_submission_dates,
            "value": [1] * len(observed_submission_dates),
        }
    )

    # there are the samples generated
    # the mean and median are the aggregates used
    test_samples = np.array([1, 1, 2, 3, 5, 8, 13])
    test_mean = np.mean(test_samples)
    test_median = np.median(test_samples)

    # mean and median scale with a factor
    # so a factor is multiplied on to make sure the aggregation is working
    # across rows properly
    forecast_array = np.stack(
        [test_samples] * len(predict_submission_dates),
        axis=0,
    )
    forecast_data = {str(i): forecast_array[:, i] for i in range(len(test_samples))}
    forecast_df = pd.DataFrame(
        dict(**{"submission_date": predict_submission_dates}, **forecast_data)
    )

    output_df = forecast._combine_forecast_observed(
        forecast_df, observed_df, "month", ["mean", "median"], [50]
    )

    expected_observed_dates = sorted(
        pd.to_datetime(observed_df["submission_date"].values)
        .to_period("m")
        .to_timestamp()
        .unique()
    )
    expected_observed_df = pd.DataFrame(
        {
            "submission_date": expected_observed_dates,
            "source": ["historical", "historical"],
            "measure": ["observed", "observed"],
            "value": [31, 28],  # number of days in each month
        }
    )

    forecast_observed_dates = sorted(
        pd.to_datetime(forecast_df["submission_date"].values)
        .to_period("m")
        .to_timestamp()
        .unique()
    )

    # add extra length of month for aggregated value column that gets added
    # due to overlap
    forecast_mean_df = pd.DataFrame(
        {
            "submission_date": forecast_observed_dates,
            "source": ["forecast", "forecast"],
            "measure": ["mean", "mean"],
            "value": [
                test_mean * 31 + 31,
                test_mean * 28 + 28,
            ],  # number of days in each month
        }
    )

    forecast_median_df = pd.DataFrame(
        {
            "submission_date": forecast_observed_dates,
            "source": ["forecast", "forecast"],
            "measure": ["median", "median"],
            "value": [
                test_median * 31 + 31,
                test_median * 28 + 28,
            ],  # number of days in each month
        }
    )

    forecast_p50_df = pd.DataFrame(
        {
            "submission_date": forecast_observed_dates,
            "source": ["forecast", "forecast"],
            "measure": ["p50", "p50"],
            "value": [
                test_median * 31 + 31,
                test_median * 28 + 28,
            ],  # number of days in each month
        }
    )

    expected_df = pd.concat(
        [expected_observed_df, forecast_mean_df, forecast_median_df, forecast_p50_df]
    )

    assert set(expected_df.columns) == set(output_df.columns)
    columns = expected_df.columns
    expected_df_compare = (
        expected_df[columns]
        .sort_values(["submission_date", "source", "measure"])
        .reset_index(drop=True)
    )
    output_df_compare = (
        output_df[columns]
        .sort_values(["submission_date", "source", "measure"])
        .reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(
        expected_df_compare, output_df_compare, check_exact=False
    )
