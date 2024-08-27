"""tests for the funnel forecast module"""

from datetime import date
from dateutil.relativedelta import relativedelta

import pandas as pd
import pytest
import numpy as np
import json


from kpi_forecasting.models.funnel_forecast import (
    ProphetAutotunerForecast,
    FunnelForecast,
    combine_forecast_observed,
    summarize_with_parameters,
    summarize,
)
from kpi_forecasting.models.prophet_forecast import ProphetForecast

# Arbitrarily choose some date to use for the tests
TEST_DATE = date(2024, 1, 1)
TEST_DATE_STR = TEST_DATE.strftime("%Y-%m-%d")
TEST_DATE_NEXT_DAY = date(2024, 1, 2)
TEST_DATE_NEXT_DAY_STR = TEST_DATE_NEXT_DAY.strftime("%Y-%m-%d")
TEST_PREDICT_END = TEST_DATE + relativedelta(months=2)
TEST_PREDICT_END_STR = TEST_PREDICT_END.strftime("%Y-%m-%d")


class MockModel:
    """Used in place of prophet.Prophet for testing purposes"""

    def __init__(self, seasonality_prior_scale=0, holidays_prior_scale=0, growth=None):
        # arbitrarily choose a few parameters from ProphetForecast to use
        self.seasonality_prior_scale = seasonality_prior_scale
        self.holidays_prior_scale = holidays_prior_scale
        self.value = seasonality_prior_scale * holidays_prior_scale
        self.history = None
        self.growth = growth

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


def mock_build_model(self):
    """mocks the FunnelForecast build_model method"""
    return MockModel(
        seasonality_prior_scale=self.seasonality_prior_scale,
        holidays_prior_scale=self.holidays_prior_scale,
        growth=self.growth,
    )


def mock_get_crossvalidation_metric(self, m, *args, **kwargs):
    """mocks the FunnelForecast get_crossvalidation_metric
    method, meant to be used with MockModel"""
    return m.model.value  # value atrribute in MockModel


def test_combine_forecast_observed():
    """tests the _combine_forecast_observed method"""

    forecast_df = pd.DataFrame(
        [
            {
                "submission_date": TEST_DATE,
                "a": "A1",
                "forecast_parameters": "blah",
                "value": 0,
                "value_low": 0,
                "value_mid": 0,
                "value_high": 0,
            },
            {
                "submission_date": TEST_DATE_NEXT_DAY,
                "a": "A1",
                "forecast_parameters": "blah",
                "value": 0,
                "value_low": 0,
                "value_mid": 0,
                "value_high": 0,
            },
        ]
    )

    observed_df = pd.DataFrame(
        [
            {
                "submission_date": TEST_DATE - relativedelta(days=2),
                "value": 5,
                "a": "A1",
            },
            {
                "submission_date": TEST_DATE - relativedelta(days=1),
                "value": 6,
                "a": "A1",
            },
        ]
    )

    output_df = combine_forecast_observed(
        forecast_df,
        observed_df,
    )

    expected_df = pd.DataFrame(
        [
            {
                "submission_date": TEST_DATE,
                "a": "A1",
                "forecast_parameters": "blah",
                "value": 0,
                "value_low": 0.0,
                "value_mid": 0.0,
                "value_high": 0.0,
                "source": "forecast",
            },
            {
                "submission_date": TEST_DATE_NEXT_DAY,
                "a": "A1",
                "forecast_parameters": "blah",
                "value": 0,
                "value_low": 0.0,
                "value_mid": 0.0,
                "value_high": 0.0,
                "source": "forecast",
            },
            {
                "submission_date": TEST_DATE - relativedelta(days=2),
                "a": "A1",
                "forecast_parameters": np.nan,
                "value": 5,
                "value_low": np.nan,
                "value_mid": np.nan,
                "value_high": np.nan,
                "source": "historical",
            },
            {
                "submission_date": TEST_DATE - relativedelta(days=1),
                "a": "A1",
                "forecast_parameters": np.nan,
                "value": 6,
                "value_low": np.nan,
                "value_mid": np.nan,
                "value_high": np.nan,
                "source": "historical",
            },
        ]
    )

    assert set(expected_df.columns) == set(output_df.columns)

    pd.testing.assert_frame_equal(
        expected_df.sort_values(["source", "submission_date"]).reset_index(drop=True),
        output_df[expected_df.columns]
        .sort_values(["source", "submission_date"])
        .reset_index(drop=True),
    )


def test_summarize_with_parameters_no_overlap():
    """testing summarize_with_parameters"""
    forecast_df = pd.DataFrame(
        {
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
        }
    )

    test_date_samples_A1 = np.arange(1000)
    test_date_samples_A2 = np.arange(1000) * 10
    test_next_date_samples_A1 = np.arange(1000) * 2
    test_next_date_samples_A2 = np.arange(1000) * 20
    forecast_df = pd.DataFrame(
        [
            {  # this element will be filtered out because it occurs before the observed_data ends
                **{
                    "submission_date": TEST_DATE - relativedelta(days=2),
                    "a": "A1",
                    "forecast_parameters": "A1",
                },
                **{i: 0 for i in range(1000)},
            },
            {
                **{
                    "submission_date": TEST_DATE,
                    "a": "A1",
                    "forecast_parameters": "A1",
                },
                **{i: el for i, el in enumerate(test_date_samples_A1)},
            },
            {
                **{
                    "submission_date": TEST_DATE_NEXT_DAY,
                    "a": "A1",
                    "forecast_parameters": "A1",
                },
                **{i: el for i, el in enumerate(test_next_date_samples_A1)},
            },
            {
                **{
                    "submission_date": TEST_DATE,
                    "a": "A2",
                    "forecast_parameters": "A2",
                },
                **{i: el for i, el in enumerate(test_date_samples_A2)},
            },
            {
                **{
                    "submission_date": TEST_DATE_NEXT_DAY,
                    "a": "A2",
                    "forecast_parameters": "A2",
                },
                **{i: el for i, el in enumerate(test_next_date_samples_A2)},
            },
        ]
    )

    # rows with negative values are those expected to be removed
    # by filters in summarize
    observed_df = pd.DataFrame(
        {
            "submission_date": [
                TEST_DATE - relativedelta(days=2),
                TEST_DATE - relativedelta(days=1),
                TEST_DATE - relativedelta(days=2),
                TEST_DATE - relativedelta(days=1),
            ],
            "a": ["A1", "A1", "A2", "A2"],
            "value": [20, 30, 40, 50],
        }
    )

    numpy_aggregations = ["mean"]
    percentiles = [10, 50, 90]
    output_df = summarize_with_parameters(
        forecast_df=forecast_df,
        observed_df=observed_df,
        period="day",
        numpy_aggregations=numpy_aggregations,
        percentiles=percentiles,
        segment_cols=["a"],
    )
    observed_expected_df = pd.DataFrame(
        {
            "submission_date": [
                TEST_DATE - relativedelta(days=2),
                TEST_DATE - relativedelta(days=1),
                TEST_DATE - relativedelta(days=2),
                TEST_DATE - relativedelta(days=1),
            ],
            "a": ["A1", "A1", "A2", "A2"],
            "value": [20, 30, 40, 50],
            "value_low": [np.nan, np.nan, np.nan, np.nan],
            "value_mid": [np.nan, np.nan, np.nan, np.nan],
            "value_high": [np.nan, np.nan, np.nan, np.nan],
            "source": ["historical", "historical", "historical", "historical"],
        }
    )

    forecast_summarized_expected_df = pd.DataFrame(
        [
            {
                "submission_date": TEST_DATE,
                "a": "A1",
                "forecast_parameters": "A1",
                "value": np.mean(test_date_samples_A1),
                "value_low": np.percentile(test_date_samples_A1, 10),
                "value_mid": np.percentile(test_date_samples_A1, 50),
                "value_high": np.percentile(test_date_samples_A1, 90),
                "source": "forecast",
            },
            {
                "submission_date": TEST_DATE_NEXT_DAY,
                "a": "A1",
                "forecast_parameters": "A1",
                "value": np.mean(test_next_date_samples_A1),
                "value_low": np.percentile(test_next_date_samples_A1, 10),
                "value_mid": np.percentile(test_next_date_samples_A1, 50),
                "value_high": np.percentile(test_next_date_samples_A1, 90),
                "source": "forecast",
            },
            {
                "submission_date": TEST_DATE,
                "a": "A2",
                "forecast_parameters": "A2",
                "value": np.mean(test_date_samples_A2),
                "value_low": np.percentile(test_date_samples_A2, 10),
                "value_mid": np.percentile(test_date_samples_A2, 50),
                "value_high": np.percentile(test_date_samples_A2, 90),
                "source": "forecast",
            },
            {
                "submission_date": TEST_DATE_NEXT_DAY,
                "a": "A2",
                "forecast_parameters": "A2",
                "value": np.mean(test_next_date_samples_A2),
                "value_low": np.percentile(test_next_date_samples_A2, 10),
                "value_mid": np.percentile(test_next_date_samples_A2, 50),
                "value_high": np.percentile(test_next_date_samples_A2, 90),
                "source": "forecast",
            },
        ]
    )

    # concat in same order to make our lives easier
    expected = pd.concat([observed_expected_df, forecast_summarized_expected_df])
    expected["aggregation_period"] = "day"
    expected["submission_date"] = pd.to_datetime(expected["submission_date"])

    assert set(expected.columns) == set(output_df.columns)

    pd.testing.assert_frame_equal(
        expected.sort_values(["source", "a", "submission_date"]).reset_index(drop=True),
        output_df[expected.columns]
        .sort_values(["source", "a", "submission_date"])
        .reset_index(drop=True),
    )


def test_summarize_with_parameters_month_overlap():
    """testing summarize_with_parameters"""
    test_date_samples_A1 = np.arange(1000)
    test_date_samples_A2 = np.arange(1000) * 10
    test_next_date_samples_A1 = np.arange(1000) * 2
    test_next_date_samples_A2 = np.arange(1000) * 20
    # add a week to all the dates so they're in the same month as the observed
    # but occur after so they won't get filtered out
    forecast_df = pd.DataFrame(
        [
            {  # this element will be filtered out because it occurs before the observed_data ends
                **{
                    "submission_date": TEST_DATE - relativedelta(days=2),
                    "a": "A1",
                    "forecast_parameters": "A1",
                },
                **{i: 0 for i in range(1000)},
            },
            {
                **{
                    "submission_date": TEST_DATE + relativedelta(days=7),
                    "a": "A1",
                    "forecast_parameters": "A1",
                },
                **{i: el for i, el in enumerate(test_date_samples_A1)},
            },
            {
                **{
                    "submission_date": TEST_DATE_NEXT_DAY + relativedelta(days=7),
                    "a": "A1",
                    "forecast_parameters": "A1",
                },
                **{i: el for i, el in enumerate(test_next_date_samples_A1)},
            },
            {
                **{
                    "submission_date": TEST_DATE + relativedelta(days=7),
                    "a": "A2",
                    "forecast_parameters": "A2",
                },
                **{i: el for i, el in enumerate(test_date_samples_A2)},
            },
            {
                **{
                    "submission_date": TEST_DATE_NEXT_DAY + relativedelta(days=7),
                    "a": "A2",
                    "forecast_parameters": "A2",
                },
                **{i: el for i, el in enumerate(test_next_date_samples_A2)},
            },
        ]
    )

    # rows with negative values are those expected to be removed
    # by filters in summarize
    observed_df = pd.DataFrame(
        {
            "submission_date": [TEST_DATE, TEST_DATE_NEXT_DAY],
            "a": ["A1", "A2"],
            "value": [20, 30],
        }
    )

    numpy_aggregations = ["mean"]
    percentiles = [10, 50, 90]
    output_df = summarize_with_parameters(
        forecast_df=forecast_df,
        observed_df=observed_df,
        period="month",
        numpy_aggregations=numpy_aggregations,
        percentiles=percentiles,
        segment_cols=["a"],
    )
    observed_expected_df = pd.DataFrame(
        {
            "submission_date": [TEST_DATE, TEST_DATE],
            "a": ["A1", "A2"],
            "value": [20, 30],
            "value_low": [np.nan, np.nan],
            "value_mid": [np.nan, np.nan],
            "value_high": [np.nan, np.nan],
            "source": ["historical", "historical"],
        }
    )

    forecast_summarized_expected_df = pd.DataFrame(
        [
            {
                "submission_date": TEST_DATE,
                "a": "A1",
                "forecast_parameters": "A1",
                "value": np.mean(test_date_samples_A1 + test_next_date_samples_A1 + 20),
                "value_low": np.percentile(
                    test_date_samples_A1 + test_next_date_samples_A1 + 20, 10
                ),
                "value_mid": np.percentile(
                    test_date_samples_A1 + test_next_date_samples_A1 + 20, 50
                ),
                "value_high": np.percentile(
                    test_date_samples_A1 + test_next_date_samples_A1 + 20, 90
                ),
                "source": "forecast",
            },
            {
                "submission_date": TEST_DATE,
                "a": "A2",
                "forecast_parameters": "A2",
                "value": np.mean(test_date_samples_A2 + test_next_date_samples_A2 + 30),
                "value_low": np.percentile(
                    test_date_samples_A2 + test_next_date_samples_A2 + 30, 10
                ),
                "value_mid": np.percentile(
                    test_date_samples_A2 + test_next_date_samples_A2 + 30, 50
                ),
                "value_high": np.percentile(
                    test_date_samples_A2 + test_next_date_samples_A2 + 30, 90
                ),
                "source": "forecast",
            },
        ]
    )

    # concat in same order to make our lives easier
    expected = pd.concat([observed_expected_df, forecast_summarized_expected_df])
    expected["aggregation_period"] = "month"
    expected["submission_date"] = pd.to_datetime(expected["submission_date"])

    assert set(expected.columns) == set(output_df.columns)

    pd.testing.assert_frame_equal(
        expected.sort_values(["source", "a", "submission_date"]).reset_index(drop=True),
        output_df[expected.columns]
        .sort_values(["source", "a", "submission_date"])
        .reset_index(drop=True),
    )


def test_summarize():
    """testing summarize"""
    # create dummy metric hub object to when meta data from
    # it is added we don't get an error
    test_date_samples_A1 = np.arange(1000)
    test_date_samples_A2 = np.arange(1000) * 10
    test_next_date_samples_A1 = np.arange(1000) * 2
    test_next_date_samples_A2 = np.arange(1000) * 20
    # add a week to all the dates so they're in the same month as the observed
    # but occur after so they won't get filtered out
    forecast_df = pd.DataFrame(
        [
            {  # this element will be filtered out because it occurs before the observed_data ends
                **{
                    "submission_date": TEST_DATE - relativedelta(days=2),
                    "a": "A1",
                    "forecast_parameters": "A1",
                },
                **{i: 0 for i in range(1000)},
            },
            {
                **{
                    "submission_date": TEST_DATE + relativedelta(days=7),
                    "a": "A1",
                    "forecast_parameters": "A1",
                },
                **{i: el for i, el in enumerate(test_date_samples_A1)},
            },
            {
                **{
                    "submission_date": TEST_DATE_NEXT_DAY + relativedelta(days=7),
                    "a": "A1",
                    "forecast_parameters": "A1",
                },
                **{i: el for i, el in enumerate(test_next_date_samples_A1)},
            },
            {
                **{
                    "submission_date": TEST_DATE + relativedelta(days=7),
                    "a": "A2",
                    "forecast_parameters": "A2",
                },
                **{i: el for i, el in enumerate(test_date_samples_A2)},
            },
            {
                **{
                    "submission_date": TEST_DATE_NEXT_DAY + relativedelta(days=7),
                    "a": "A2",
                    "forecast_parameters": "A2",
                },
                **{i: el for i, el in enumerate(test_next_date_samples_A2)},
            },
        ]
    )

    # rows with negative values are those expected to be removed
    # by filters in summarize
    observed_df = pd.DataFrame(
        {
            "submission_date": [TEST_DATE, TEST_DATE_NEXT_DAY],
            "a": ["A1", "A2"],
            "value": [20, 30],
        }
    )

    numpy_aggregations = ["mean"]
    percentiles = [10, 50, 90]
    output_df = summarize(
        forecast_df=forecast_df,
        observed_df=observed_df,
        periods=["day", "month"],
        numpy_aggregations=numpy_aggregations,
        percentiles=percentiles,
        segment_cols=["a"],
    )
    observed_month_expected_df = pd.DataFrame(
        {
            "submission_date": [TEST_DATE, TEST_DATE],
            "a": ["A1", "A2"],
            "value": [20, 30],
            "value_low": [np.nan, np.nan],
            "value_mid": [np.nan, np.nan],
            "value_high": [np.nan, np.nan],
            "source": ["historical", "historical"],
            "aggregation_period": "month",
        }
    )
    observed_day_expected_df = pd.DataFrame(
        {
            "submission_date": [TEST_DATE, TEST_DATE_NEXT_DAY],
            "a": ["A1", "A2"],
            "value": [20, 30],
            "value_low": [np.nan, np.nan],
            "value_mid": [np.nan, np.nan],
            "value_high": [np.nan, np.nan],
            "source": ["historical", "historical"],
            "aggregation_period": "day",
        }
    )

    forecast_month_summarized_expected_df = pd.DataFrame(
        [
            {
                "submission_date": TEST_DATE,
                "a": "A1",
                "forecast_parameters": "A1",
                "value": np.mean(test_date_samples_A1 + test_next_date_samples_A1 + 20),
                "value_low": np.percentile(
                    test_date_samples_A1 + test_next_date_samples_A1 + 20, 10
                ),
                "value_mid": np.percentile(
                    test_date_samples_A1 + test_next_date_samples_A1 + 20, 50
                ),
                "value_high": np.percentile(
                    test_date_samples_A1 + test_next_date_samples_A1 + 20, 90
                ),
                "source": "forecast",
                "aggregation_period": "month",
            },
            {
                "submission_date": TEST_DATE,
                "a": "A2",
                "forecast_parameters": "A2",
                "value": np.mean(test_date_samples_A2 + test_next_date_samples_A2 + 30),
                "value_low": np.percentile(
                    test_date_samples_A2 + test_next_date_samples_A2 + 30, 10
                ),
                "value_mid": np.percentile(
                    test_date_samples_A2 + test_next_date_samples_A2 + 30, 50
                ),
                "value_high": np.percentile(
                    test_date_samples_A2 + test_next_date_samples_A2 + 30, 90
                ),
                "source": "forecast",
                "aggregation_period": "month",
            },
        ]
    )

    forecast_day_summarized_expected_df = pd.DataFrame(
        [
            {
                "submission_date": TEST_DATE + relativedelta(days=7),
                "a": "A1",
                "forecast_parameters": "A1",
                "value": np.mean(test_date_samples_A1),
                "value_low": np.percentile(test_date_samples_A1, 10),
                "value_mid": np.percentile(test_date_samples_A1, 50),
                "value_high": np.percentile(test_date_samples_A1, 90),
                "source": "forecast",
                "aggregation_period": "day",
            },
            {
                "submission_date": TEST_DATE_NEXT_DAY + relativedelta(days=7),
                "a": "A1",
                "forecast_parameters": "A1",
                "value": np.mean(test_next_date_samples_A1),
                "value_low": np.percentile(test_next_date_samples_A1, 10),
                "value_mid": np.percentile(test_next_date_samples_A1, 50),
                "value_high": np.percentile(test_next_date_samples_A1, 90),
                "source": "forecast",
                "aggregation_period": "day",
            },
            {
                "submission_date": TEST_DATE + relativedelta(days=7),
                "a": "A2",
                "forecast_parameters": "A2",
                "value": np.mean(test_date_samples_A2),
                "value_low": np.percentile(test_date_samples_A2, 10),
                "value_mid": np.percentile(test_date_samples_A2, 50),
                "value_high": np.percentile(test_date_samples_A2, 90),
                "source": "forecast",
                "aggregation_period": "day",
            },
            {
                "submission_date": TEST_DATE_NEXT_DAY + relativedelta(days=7),
                "a": "A2",
                "forecast_parameters": "A2",
                "value": np.mean(test_next_date_samples_A2),
                "value_low": np.percentile(test_next_date_samples_A2, 10),
                "value_mid": np.percentile(test_next_date_samples_A2, 50),
                "value_high": np.percentile(test_next_date_samples_A2, 90),
                "source": "forecast",
                "aggregation_period": "day",
            },
        ]
    )

    # concat in same order to make our lives easier
    expected = pd.concat(
        [
            forecast_day_summarized_expected_df,
            forecast_month_summarized_expected_df,
            observed_day_expected_df,
            observed_month_expected_df,
        ]
    )
    expected["submission_date"] = pd.to_datetime(expected["submission_date"])

    assert set(expected.columns) == set(output_df.columns)

    pd.testing.assert_frame_equal(
        expected.sort_values(
            ["source", "a", "submission_date", "aggregation_period"]
        ).reset_index(drop=True),
        output_df[expected.columns]
        .sort_values(["source", "a", "submission_date", "aggregation_period"])
        .reset_index(drop=True),
    )


def test_auto_tuning(mocker):
    """test the auto_tuning function"""

    mocker.patch.object(ProphetForecast, "_build_model", mock_build_model)
    # mock_get_crossvalidation_metric will choose the parameters that
    # have the lowest absolute product
    mocker.patch.object(
        ProphetAutotunerForecast,
        "_get_crossvalidation_metric",
        mock_get_crossvalidation_metric,
    )
    forecast = ProphetAutotunerForecast(
        growth="testval",
        grid_parameters={
            "seasonality_prior_scale": [1, 2],
            "holidays_prior_scale": [20, 10],
        },
    )

    observed_df = pd.DataFrame(
        {
            "a": ["A1", "A1"],
            "b": ["B1", "B2"],
            "submission_date": [
                TEST_DATE,
                TEST_DATE,
            ],
        }
    )

    best_model = forecast._auto_tuning(observed_df)

    # in the mocked class the two params get multiplied and the lowest combo gets select
    assert best_model.seasonality_prior_scale == 1
    assert best_model.holidays_prior_scale == 10

    # make sure growth got written to new class
    assert best_model.growth == "testval"

    # check to make sure it's fit
    pd.testing.assert_frame_equal(
        best_model.history, forecast._build_train_dataframe(observed_df)
    )


def test_autotuner_predict(mocker):
    """testing _predict"""
    mocker.patch.object(ProphetForecast, "_build_model", mock_build_model)
    # mock_get_crossvalidation_metric will choose the parameters that
    # have the lowest absolute product
    mocker.patch.object(
        ProphetAutotunerForecast,
        "_get_crossvalidation_metric",
        mock_get_crossvalidation_metric,
    )

    forecast = ProphetAutotunerForecast(
        growth="testval",
        grid_parameters={
            "seasonality_prior_scale": [1, 2],
            "holidays_prior_scale": [20, 10],
        },
    )

    observed_df = pd.DataFrame(
        {
            "a": ["A1", "A1"],
            "b": ["B1", "B2"],
            "submission_date": pd.to_datetime(
                [
                    TEST_DATE,
                    TEST_DATE_NEXT_DAY,
                ]
            ),
            "y": [1, 2],
        }
    )

    forecast.fit(observed_df)

    dates_to_predict = pd.DataFrame(
        {
            "submission_date": pd.to_datetime(
                [
                    TEST_DATE,
                    TEST_DATE_NEXT_DAY,
                ]
            )
        }
    )

    out = forecast.predict(dates_to_predict).reset_index(drop=True)

    # in MockModel, the predictive_samples method sets the output to
    # np.arange(len(dates_to_predict)) * self.value for one column called 0
    # this helps ensure the forecast_df in segment_models is set properly
    model_value = forecast.model.value
    expected = pd.DataFrame(
        {
            0: [0, model_value],
            "submission_date": pd.to_datetime(
                [
                    TEST_DATE,
                    TEST_DATE_NEXT_DAY,
                ]
            ),
        }
    )

    pd.testing.assert_frame_equal(out, expected)

    # check the components
    expected_components = observed_df[["submission_date", "y"]].copy()
    expected_components["submission_date"] = pd.to_datetime(
        expected_components["submission_date"]
    )
    expected_components[
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
    ] = 0

    components_df = forecast.components_df
    assert set(expected_components.columns) == set(components_df.columns)
    pd.testing.assert_frame_equal(
        components_df, expected_components[components_df.columns]
    )


def test_funnelforecast_fit(mocker):
    """test the fit method, and implicitly the set_segment_models method"""
    # arbitrarily choose number_of_simulations as a parameter
    # to set in order to check the test
    parameter_list = [
        {
            "segment": {"a": "A1"},
            "parameters": {
                "growth": "logistic",
                "grid_parameters": {
                    "seasonality_prior_scale": [1, 2],
                    "holidays_prior_scale": [20, 10],
                },
            },
        },
        {
            "segment": {"a": "A2"},
            "parameters": {
                "growth": "A2",
                "grid_parameters": {
                    "seasonality_prior_scale": [3, 4],
                    "holidays_prior_scale": [40, 30],
                },
            },
        },
    ]

    mocker.patch.object(ProphetForecast, "_build_model", mock_build_model)
    mocker.patch.object(
        ProphetAutotunerForecast,
        "_get_crossvalidation_metric",
        mock_get_crossvalidation_metric,
    )
    ensemble_object = FunnelForecast(parameters=parameter_list, segments=["a", "b"])

    observed_data = pd.DataFrame(
        {
            "a": ["A1", "A1", "A2", "A2", "A2"],
            "b": ["B1", "B2", "B1", "B2", "B2"],
            "submission_date": [
                TEST_DATE_STR,
                TEST_DATE_STR,
                TEST_DATE_STR,
                TEST_DATE_STR,
                TEST_DATE_STR,
            ],
            "value": [1, 2, 3, 4, 5],
        }
    )

    ensemble_object.fit(observed_data)

    segment_models = ensemble_object.segment_models

    # put the segments and the start date in the same dictionary to make
    # comparison easier
    # the important things to check is that all possible combinations
    # of segments are present and that each has the parameters set properly
    # start_date is a stand-in for these parameters and
    # is determined by the value of a as specified in parameter_dict
    check_segment_models = [
        dict(
            **el["segment"],
            **{"value": el["model"].model.value, "growth": el["model"].growth},
        )
        for el in segment_models
    ]

    expected = [
        {"a": "A1", "b": "B1", "growth": "logistic", "value": 10},
        {"a": "A1", "b": "B2", "growth": "logistic", "value": 10},
        {"a": "A2", "b": "B1", "growth": "A2", "value": 90},
        {"a": "A2", "b": "B2", "growth": "A2", "value": 90},
    ]

    # can't make a set of dicts for comparison
    # so sort the lists and compare each element
    compare_sorted = zip(
        sorted(check_segment_models, key=lambda x: (x["a"], x["b"])),
        sorted(expected, key=lambda x: (x["a"], x["b"])),
    )

    for checkval, expectedval in compare_sorted:
        assert checkval == expectedval

    # test that the seed was set for all models during fitting
    assert all([el["model"]._set_seed for el in segment_models])

    # test that the fit was applied properly to all models
    # to do this check the is_fit attribute, which will equal
    # A1_start_date for A1 segments and A2_start_date for A2 segments

    # check that it fit by making sure model.history is not null
    for segment in segment_models:
        subset = observed_data[
            (observed_data["a"] == segment["segment"]["a"])
            & (observed_data["b"] == segment["segment"]["b"])
        ]
        subset = subset.rename(columns={"submission_date": "ds", "value": "y"})
        if segment["segment"]["a"] == "A1":
            if segment["segment"]["b"] == "B1":
                floor = 0.5 * 1
                cap = 1.5 * 1
            else:
                floor = 0.5 * 2
                cap = 1.5 * 2
            subset["floor"] = floor
            subset["cap"] = cap
        pd.testing.assert_frame_equal(subset, segment["model"].model.history)


def test_funnelforecast_fit_multiple(mocker):
    """test the set_segment_models method
    with segments on multiple columns"""
    # arbitrarily choose number_of_simulations as a parameter
    # to set in order to check the test
    parameter_list = [
        {
            "segment": {"a": "A1", "b": "B1"},
            "parameters": {
                "growth": "logistic",
                "grid_parameters": {
                    "seasonality_prior_scale": [1, 2],
                    "holidays_prior_scale": [20, 10],
                },
            },
        },
        {
            "segment": {"a": "A2", "b": "B1"},
            "parameters": {
                "growth": "A2B1",
                "grid_parameters": {
                    "seasonality_prior_scale": [3, 4],
                    "holidays_prior_scale": [40, 30],
                },
            },
        },
        {
            "segment": {"a": "A1", "b": "B2"},
            "parameters": {
                "growth": "logistic",
                "grid_parameters": {
                    "seasonality_prior_scale": [10, 20],
                    "holidays_prior_scale": [200, 100],
                },
            },
        },
        {
            "segment": {"a": "A2", "b": "B2"},
            "parameters": {
                "growth": "A2B2",
                "grid_parameters": {
                    "seasonality_prior_scale": [30, 40],
                    "holidays_prior_scale": [400, 300],
                },
            },
        },
    ]

    mocker.patch.object(ProphetForecast, "_build_model", mock_build_model)
    mocker.patch.object(
        ProphetAutotunerForecast,
        "_get_crossvalidation_metric",
        mock_get_crossvalidation_metric,
    )
    ensemble_object = FunnelForecast(parameters=parameter_list, segments=["a", "b"])

    observed_data = pd.DataFrame(
        {
            "a": ["A1", "A1", "A2", "A2", "A2"],
            "b": ["B1", "B2", "B1", "B2", "B2"],
            "submission_date": [
                TEST_DATE_STR,
                TEST_DATE_STR,
                TEST_DATE_STR,
                TEST_DATE_STR,
                TEST_DATE_STR,
            ],
            "value": [1, 2, 3, 4, 5],
        }
    )

    ensemble_object.fit(observed_data)

    segment_models = ensemble_object.segment_models

    # put the segments and the start date in the same dictionary to make
    # comparison easier
    # the important things to check is that all possible combinations
    # of segments are present and that each has the parameters set properly
    # start_date is a stand-in for these parameters and
    # is determined by the value of a as specified in parameter_dict
    check_segment_models = [
        dict(
            **el["segment"],
            **{"value": el["model"].model.value, "growth": el["model"].growth},
        )
        for el in segment_models
    ]

    expected = [
        {"a": "A1", "b": "B1", "growth": "logistic", "value": 10},
        {"a": "A1", "b": "B2", "growth": "logistic", "value": 1000},
        {"a": "A2", "b": "B1", "growth": "A2B1", "value": 90},
        {"a": "A2", "b": "B2", "growth": "A2B2", "value": 9000},
    ]

    # can't make a set of dicts for comparison
    # so sort the lists and compare each element
    compare_sorted = zip(
        sorted(check_segment_models, key=lambda x: (x["a"], x["b"])),
        sorted(expected, key=lambda x: (x["a"], x["b"])),
    )

    for checkval, expectedval in compare_sorted:
        assert checkval == expectedval

    # test that the seed was set for all models during fitting
    assert all([el["model"]._set_seed for el in segment_models])

    # test that the fit was applied properly to all models
    # to do this check the is_fit attribute, which will equal
    # A1_start_date for A1 segments and A2_start_date for A2 segments

    # check that it fit by making sure model.history is not null
    for segment in segment_models:
        subset = observed_data[
            (observed_data["a"] == segment["segment"]["a"])
            & (observed_data["b"] == segment["segment"]["b"])
        ]
        subset = subset.rename(columns={"submission_date": "ds", "value": "y"})
        if segment["segment"]["a"] == "A1":
            if segment["segment"]["b"] == "B1":
                floor = 0.5 * 1
                cap = 1.5 * 1
            else:
                floor = 0.5 * 2
                cap = 1.5 * 2
            subset["floor"] = floor
            subset["cap"] = cap
        pd.testing.assert_frame_equal(subset, segment["model"].model.history)


def test_funnel_predict(mocker):
    """test the predict method.  This is similar to test_under_predict
    but multiple segments are acted upon"""

    # arbitrarily choose number_of_simulations as a parameter
    # to set in order to check the test
    parameter_list = [
        {
            "segment": {"a": "A1"},
            "parameters": {
                "growth": "logistic",
                "grid_parameters": {
                    "seasonality_prior_scale": [1, 2],
                    "holidays_prior_scale": [20, 10],
                },
            },
        },
        {
            "segment": {"a": "A2"},
            "parameters": {
                "growth": "A2",
                "grid_parameters": {
                    "seasonality_prior_scale": [3, 4],
                    "holidays_prior_scale": [40, 30],
                },
            },
        },
    ]

    mocker.patch.object(ProphetForecast, "_build_model", mock_build_model)
    mocker.patch.object(
        ProphetAutotunerForecast,
        "_get_crossvalidation_metric",
        mock_get_crossvalidation_metric,
    )
    ensemble_object = FunnelForecast(parameters=parameter_list, segments=["a", "b"])

    observed_data = pd.DataFrame(
        {
            "a": ["A1", "A1", "A2", "A2", "A2"] * 2,
            "b": ["B1", "B2", "B1", "B2", "B2"] * 2,
            "submission_date": pd.to_datetime(
                [
                    TEST_DATE,
                    TEST_DATE,
                    TEST_DATE,
                    TEST_DATE,
                    TEST_DATE,
                    TEST_DATE_NEXT_DAY,
                    TEST_DATE_NEXT_DAY,
                    TEST_DATE_NEXT_DAY,
                    TEST_DATE_NEXT_DAY,
                    TEST_DATE_NEXT_DAY,
                ]
            ),
            "value": [1, 2, 3, 4, 5] * 2,
        }
    )

    ensemble_object.fit(observed_data)

    dates_to_predict = pd.DataFrame(
        {
            "submission_date": pd.to_datetime(
                [
                    TEST_DATE,
                    TEST_DATE_NEXT_DAY,
                ]
            )
        }
    )

    out = ensemble_object.predict(dates_to_predict).reset_index(drop=True)

    for segment in ensemble_object.segment_models:
        # in MockModel, the predictive_samples method sets the output to
        # np.arange(len(dates_to_predict)) * self.value for one column called 0
        # this helps ensure the forecast_df in segment_models is set properly
        out_subset = out[
            (out["a"] == segment["segment"]["a"])
            & (out["b"] == segment["segment"]["b"])
        ]
        model_value = segment["model"].model.value
        expected = pd.DataFrame(
            {
                0: [0, model_value],
                "submission_date": pd.to_datetime(
                    [
                        TEST_DATE,
                        TEST_DATE_NEXT_DAY,
                    ]
                ),
                "a": [segment["segment"]["a"], segment["segment"]["a"]],
                "b": [segment["segment"]["b"], segment["segment"]["b"]],
                "forecast_parameters": [json.dumps(segment["model"]._get_parameters())]
                * 2,
            }
        )

        pd.testing.assert_frame_equal(
            out_subset.reset_index(drop=True), expected.reset_index(drop=True)
        )

        # check the components
        expected_components = (
            observed_data.loc[
                (observed_data["a"] == segment["segment"]["a"])
                & (observed_data["b"] == segment["segment"]["b"]),
                ["submission_date", "value"],
            ]
            .rename(columns={"value": "y"})
            .copy()
        )
        expected_components[
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
        ] = 0

        components_df = segment["model"].components_df
        assert set(expected_components.columns) == set(components_df.columns)
        pd.testing.assert_frame_equal(
            components_df.reset_index(drop=True),
            expected_components[components_df.columns].reset_index(drop=True),
        )


def test_funnel_predict_growth(mocker):
    """test the predict method when growth is set in the
    grid parameters.  Extra attributes need to be updated with this one"""

    # arbitrarily choose number_of_simulations as a parameter
    # to set in order to check the test
    parameter_list = [
        {
            "segment": {"a": "A1"},
            "parameters": {
                "grid_parameters": {
                    "seasonality_prior_scale": [1, 2],
                    "holidays_prior_scale": [20, 10],
                    "growth": "logistic",
                },
            },
        },
        {
            "segment": {"a": "A2"},
            "parameters": {
                "growth": "A2",
                "grid_parameters": {
                    "seasonality_prior_scale": [3, 4],
                    "holidays_prior_scale": [40, 30],
                },
            },
        },
    ]

    mocker.patch.object(ProphetForecast, "_build_model", mock_build_model)
    mocker.patch.object(
        ProphetAutotunerForecast,
        "_get_crossvalidation_metric",
        mock_get_crossvalidation_metric,
    )
    ensemble_object = FunnelForecast(parameters=parameter_list, segments=["a", "b"])

    observed_data = pd.DataFrame(
        {
            "a": ["A1", "A1", "A2", "A2", "A2"] * 2,
            "b": ["B1", "B2", "B1", "B2", "B2"] * 2,
            "submission_date": pd.to_datetime(
                [
                    TEST_DATE,
                    TEST_DATE,
                    TEST_DATE,
                    TEST_DATE,
                    TEST_DATE,
                    TEST_DATE_NEXT_DAY,
                    TEST_DATE_NEXT_DAY,
                    TEST_DATE_NEXT_DAY,
                    TEST_DATE_NEXT_DAY,
                    TEST_DATE_NEXT_DAY,
                ]
            ),
            "value": [1, 2, 3, 4, 5] * 2,
        }
    )

    ensemble_object.fit(observed_data)

    dates_to_predict = pd.DataFrame(
        {
            "submission_date": pd.to_datetime(
                [
                    TEST_DATE,
                    TEST_DATE_NEXT_DAY,
                ]
            )
        }
    )

    out = ensemble_object.predict(dates_to_predict).reset_index(drop=True)

    for segment in ensemble_object.segment_models:
        # in MockModel, the predictive_samples method sets the output to
        # np.arange(len(dates_to_predict)) * self.value for one column called 0
        # this helps ensure the forecast_df in segment_models is set properly
        out_subset = out[
            (out["a"] == segment["segment"]["a"])
            & (out["b"] == segment["segment"]["b"])
        ]
        model_value = segment["model"].model.value
        expected = pd.DataFrame(
            {
                0: [0, model_value],
                "submission_date": pd.to_datetime(
                    [
                        TEST_DATE,
                        TEST_DATE_NEXT_DAY,
                    ]
                ),
                "a": [segment["segment"]["a"], segment["segment"]["a"]],
                "b": [segment["segment"]["b"], segment["segment"]["b"]],
                "forecast_parameters": [json.dumps(segment["model"]._get_parameters())]
                * 2,
            }
        )

        pd.testing.assert_frame_equal(
            out_subset.reset_index(drop=True), expected.reset_index(drop=True)
        )

        # check that the growth attributes were set
        if segment["segment"]["a"] == "A1":
            if segment["segment"]["b"] == "B1":
                assert segment["model"].logistic_growth_floor == 0.5
                assert segment["model"].logistic_growth_cap == 1.5
            elif segment["segment"]["b"] == "B2":
                assert segment["model"].logistic_growth_floor == 1.0
                assert segment["model"].logistic_growth_cap == 3.0

        # check the components
        expected_components = (
            observed_data.loc[
                (observed_data["a"] == segment["segment"]["a"])
                & (observed_data["b"] == segment["segment"]["b"]),
                ["submission_date", "value"],
            ]
            .rename(columns={"value": "y"})
            .copy()
        )
        expected_components[
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
        ] = 0

        components_df = segment["model"].components_df
        assert set(expected_components.columns) == set(components_df.columns)
        pd.testing.assert_frame_equal(
            components_df.reset_index(drop=True),
            expected_components[components_df.columns].reset_index(drop=True),
        )


def test_set_segment_models_exception(mocker):
    """test the exception for segment_models where
    and exception is raised if a model_setting_split_dim
    is specified that isn't in the data"""
    # arbitrarily choose number_of_simulations as a parameter
    # to set in order to check the test
    parameter_list = [
        {
            "segment": {"c": "A1"},
            "parameters": {
                "growth": "logistic",
                "grid_parameters": {
                    "seasonality_prior_scale": [1, 2],
                    "holidays_prior_scale": [20, 10],
                },
            },
        },
        {
            "segment": {"c": "A2"},
            "parameters": {
                "growth": "A2",
                "grid_parameters": {
                    "seasonality_prior_scale": [3, 4],
                    "holidays_prior_scale": [40, 30],
                },
            },
        },
    ]

    mocker.patch.object(ProphetForecast, "_build_model", mock_build_model)
    mocker.patch.object(
        ProphetAutotunerForecast,
        "_get_crossvalidation_metric",
        mock_get_crossvalidation_metric,
    )
    ensemble_object = FunnelForecast(parameters=parameter_list, segments=["a", "b"])

    observed_data = pd.DataFrame(
        {
            "a": ["A1", "A1", "A2", "A2", "A2"],
            "b": ["B1", "B2", "B1", "B2", "B2"],
            "submission_date": [
                TEST_DATE_STR,
                TEST_DATE_STR,
                TEST_DATE_STR,
                TEST_DATE_STR,
                TEST_DATE_STR,
            ],
            "value": [1, 2, 3, 4, 5],
        }
    )

    with pytest.raises(
        ValueError,
        match="Segment keys missing from metric hub segments: c",
    ):
        ensemble_object.fit(observed_df=observed_data)


def test_build_model():
    """test build_model
    just runs the function and ensures no error is raised"""
    regressor_list = ["post_esr_migration", "in_covid", "ad_click_bug"]

    # use holidays from holiday config file
    holiday_list = {
        "easter": {
            "name": "easter",
            "ds": [
                "2016-03-27",
                "2017-04-16",
                "2018-04-01",
                "2019-04-21",
                "2020-04-12",
                "2021-04-04",
                "2022-04-17",
                "2023-04-09",
                "2024-03-31",
                "2025-04-20",
            ],
            "lower_window": -2,
            "upper_window": 1,
        },
        "covid_sip1": {
            "name": "covid_sip1",
            "ds": ["2020-03-14"],
            "lower_window": 0,
            "upper_window": 45,
        },
        "covid_sip11": {
            "name": "covid_sip11",
            "ds": ["2020-03-14"],
            "lower_window": -14,
            "upper_window": 30,
        },
    }

    grid_parameters = {
        "changepoint_prior_scale": [0.01, 0.1, 0.15, 0.2],
        "changepoint_range": [0.8, 0.9, 1],
        "n_changepoints": [30],
        "weekly_seasonality": True,
        "yearly_seasonality": True,
        "growth": "logistic",
    }
    cv_settings = {
        "initial": "366 days",
        "period": "30 days",
        "horizon": "30 days",
        "parallel": "processes",
    }
    forecast = ProphetAutotunerForecast(
        holidays=holiday_list.keys(),
        regressors=regressor_list,
        grid_parameters=grid_parameters,
        cv_settings=cv_settings,
    )

    _ = forecast._build_model()

    holiday_df = forecast.holidays
    expected_holidays = pd.concat(
        [
            pd.DataFrame(
                {
                    "holiday": h["name"],
                    "ds": pd.to_datetime(h["ds"]),
                    "lower_window": h["lower_window"],
                    "upper_window": h["upper_window"],
                }
            )
            for h in holiday_list.values()
        ],
        ignore_index=True,
    )
    pd.testing.assert_frame_equal(holiday_df, expected_holidays)
