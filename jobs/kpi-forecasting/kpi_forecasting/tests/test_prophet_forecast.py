from datetime import date
from dateutil.relativedelta import relativedelta

import pandas as pd
import numpy as np
import pytest


from kpi_forecasting.models.prophet_forecast import (
    ProphetForecast,
    combine_forecast_observed,
    aggregate_forecast_observed,
    summarize,
)
from kpi_forecasting.configs.model_inputs import ProphetRegressor


# Arbitrarily choose some date to use for the tests
TEST_DATE = date(2024, 1, 1)
TEST_DATE_STR = TEST_DATE.strftime("%Y-%m-%d")
TEST_DATE_NEXT_DAY = date(2024, 1, 2)
TEST_DATE_NEXT_DAY_STR = TEST_DATE_NEXT_DAY.strftime("%Y-%m-%d")
TEST_PREDICT_END = TEST_DATE + relativedelta(months=2)
TEST_PREDICT_END_STR = TEST_PREDICT_END.strftime("%Y-%m-%d")


class MockModel:
    """Used in place of prophet.Prophet for testing purposes"""

    def __init__(self, **kwargs):
        self.value = 2
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


def mock_build_model(self):
    """mocks the FunnelForecast build_model method"""
    return MockModel(holidays=self.holidays, regressors=self.regressors)


@pytest.fixture
def forecast(mocker):
    parameter_dict = {"number_of_simulations": 1}

    mocker.patch.object(ProphetForecast, "_build_model", mock_build_model)

    # arbitarily set it a couple months in the future
    return ProphetForecast(**parameter_dict)


def test_predict(forecast):
    """testing _predict"""

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

    forecast.fit(observed_df)

    # to make sure the validation works set the number of simulations
    out = forecast.predict(dates_to_predict).reset_index(drop=True)

    # in MockModel, the predictive_samples method sets the output to
    # np.arange(len(dates_to_predict)) * self.value for one column called 0
    # this helps ensure the forecast_df in segment_models is set properly
    # self.value is 2
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


def test_fit(forecast):
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

    forecast.fit(observed_data)

    # checking that history is set in the mocked Model ensures fit was called on it
    pd.testing.assert_frame_equal(
        observed_data.rename(columns={"submission_date": "ds"}), forecast.model.history
    )


def test_aggregate_forecast_to_day():
    """tests the aggregate_forecast_observed method in the case
    where the observed and forecasted have no overlap and the aggregation
    happens at the day level"""
    test_date_samples = np.arange(1000)
    test_next_date_samples = np.arange(1000) * 2
    forecast_df = pd.DataFrame(
        [
            {
                **{"submission_date": TEST_DATE},
                **{i: el for i, el in enumerate(test_date_samples)},
            },
            {
                **{"submission_date": TEST_DATE_NEXT_DAY},
                **{i: el for i, el in enumerate(test_next_date_samples)},
            },
        ]
    )

    # rows with negative values are those expected to be removed
    # by filters in summarize
    # arbitrarily subtract 1 month so there's not overlap
    observed_df = pd.DataFrame(
        {
            "submission_date": [
                TEST_DATE - relativedelta(months=1),
                TEST_DATE_NEXT_DAY - relativedelta(months=1),
            ],
            "value": [10, 20],
        }
    )

    numpy_aggregations = ["mean"]
    percentiles = [10, 50, 90]
    forecast_summarized_output, observed_summarized_output = (
        aggregate_forecast_observed(
            forecast_df,
            observed_df,
            period="day",
            numpy_aggregations=numpy_aggregations,
            percentiles=percentiles,
        )
    )
    observed_summarized_expected_df = pd.DataFrame(
        {
            "submission_date": [
                pd.to_datetime(TEST_DATE - relativedelta(months=1)),
                pd.to_datetime(TEST_DATE_NEXT_DAY - relativedelta(months=1)),
            ],
            "value": [10, 20],
        }
    )

    forecast_summarized_expected_df = pd.DataFrame(
        [
            {
                "submission_date": pd.to_datetime(TEST_DATE),
                "mean": np.mean(test_date_samples),
                "p10": np.percentile(test_date_samples, 10),
                "p50": np.percentile(test_date_samples, 50),
                "p90": np.percentile(test_date_samples, 90),
            },
            {
                "submission_date": pd.to_datetime(TEST_DATE_NEXT_DAY),
                "mean": np.mean(test_next_date_samples),
                "p10": np.percentile(test_next_date_samples, 10),
                "p50": np.percentile(test_next_date_samples, 50),
                "p90": np.percentile(test_next_date_samples, 90),
            },
        ]
    )

    pd.testing.assert_frame_equal(
        forecast_summarized_output, forecast_summarized_expected_df
    )

    pd.testing.assert_frame_equal(
        observed_summarized_output, observed_summarized_expected_df
    )


def test_aggregate_forecast_to_month():
    """tests the aggregate_forecast_observed method in the case
    where the observed and forecasted have no overlap and the aggregation
    happens at the day level"""
    test_date_samples = np.arange(1000)
    test_next_date_samples = np.arange(1000) * 2
    forecast_df = pd.DataFrame(
        [
            {
                **{"submission_date": TEST_DATE, "forecast_parameters": "test_month"},
                **{i: el for i, el in enumerate(test_date_samples)},
            },
            {
                **{
                    "submission_date": TEST_DATE_NEXT_DAY,
                    "forecast_parameters": "test_month",
                },
                **{i: el for i, el in enumerate(test_next_date_samples)},
            },
        ]
    )

    # rows with negative values are those expected to be removed
    # by filters in summarize
    # arbitrarily subtract 1 month so there's not overlap
    observed_df = pd.DataFrame(
        {
            "submission_date": [
                TEST_DATE - relativedelta(months=1),
                TEST_DATE_NEXT_DAY - relativedelta(months=1),
            ],
            "value": [10, 20],
        }
    )

    numpy_aggregations = ["mean"]
    percentiles = [10, 50, 90]
    forecast_summarized_output, observed_summarized_output = (
        aggregate_forecast_observed(
            forecast_df,
            observed_df,
            period="month",
            numpy_aggregations=numpy_aggregations,
            percentiles=percentiles,
        )
    )

    # TEST_DATE should be the first of the month
    observed_summarized_expected_df = pd.DataFrame(
        {
            "submission_date": [
                pd.to_datetime(TEST_DATE - relativedelta(months=1)),
            ],
            "value": [30],
        }
    )

    forecast_summarized_expected_df = pd.DataFrame(
        [
            {
                "submission_date": pd.to_datetime(TEST_DATE),
                "mean": np.mean(test_date_samples + test_next_date_samples),
                "p10": np.percentile(test_date_samples + test_next_date_samples, 10),
                "p50": np.percentile(test_date_samples + test_next_date_samples, 50),
                "p90": np.percentile(test_date_samples + test_next_date_samples, 90),
                "forecast_parameters": "test_month",
            },
        ]
    )

    pd.testing.assert_frame_equal(
        forecast_summarized_output, forecast_summarized_expected_df
    )

    pd.testing.assert_frame_equal(
        observed_summarized_output, observed_summarized_expected_df
    )


def test_aggregate_forecast_to_month_extra_agg_col():
    """tests the aggregate_forecast_observed method in the case
    where the observed and forecasted have no overlap and the aggregation
    happens at the day level"""
    test_date_samples = np.arange(1000)
    test_next_date_samples = np.arange(1000) * 2
    forecast_df = pd.DataFrame(
        [
            {
                **{
                    "submission_date": TEST_DATE,
                    "a": "A1",
                    "forecast_parameters": "A1",
                },
                **{i: el for i, el in enumerate(test_date_samples)},
            },
            {
                **{
                    "submission_date": TEST_DATE_NEXT_DAY,
                    "a": "A1",
                    "forecast_parameters": "A1",
                },
                **{i: el for i, el in enumerate(test_next_date_samples)},
            },
            {
                **{
                    "submission_date": TEST_DATE,
                    "a": "A2",
                    "forecast_parameters": "A2",
                },
                **{i: el for i, el in enumerate(2 * test_date_samples)},
            },
            {
                **{
                    "submission_date": TEST_DATE_NEXT_DAY,
                    "a": "A2",
                    "forecast_parameters": "A2",
                },
                **{i: el for i, el in enumerate(2 * test_next_date_samples)},
            },
        ]
    )

    # rows with negative values are those expected to be removed
    # by filters in summarize
    # arbitrarily subtract 1 month so there's not overlap
    observed_df = pd.DataFrame(
        {
            "submission_date": [
                TEST_DATE - relativedelta(months=1),
                TEST_DATE_NEXT_DAY - relativedelta(months=1),
            ],
            "value": [10, 20],
            "a": ["A1", "A1"],
        }
    )

    numpy_aggregations = ["mean"]
    percentiles = [10, 50, 90]
    forecast_summarized_output, observed_summarized_output = (
        aggregate_forecast_observed(
            forecast_df,
            observed_df,
            period="month",
            numpy_aggregations=numpy_aggregations,
            percentiles=percentiles,
            additional_aggregation_columns=["a"],
        )
    )

    # TEST_DATE should be the first of the month
    observed_summarized_expected_df = pd.DataFrame(
        {
            "submission_date": [
                pd.to_datetime(TEST_DATE - relativedelta(months=1)),
            ],
            "value": [30],
            "a": ["A1"],
        }
    )

    forecast_summarized_expected_df = pd.DataFrame(
        [
            {
                "submission_date": pd.to_datetime(TEST_DATE),
                "mean": np.mean(test_date_samples + test_next_date_samples),
                "p10": np.percentile(test_date_samples + test_next_date_samples, 10),
                "p50": np.percentile(test_date_samples + test_next_date_samples, 50),
                "p90": np.percentile(test_date_samples + test_next_date_samples, 90),
                "a": "A1",
                "forecast_parameters": "A1",
            },
            {
                "submission_date": pd.to_datetime(TEST_DATE),
                "mean": 2 * np.mean(test_date_samples + test_next_date_samples),
                "p10": 2
                * np.percentile(test_date_samples + test_next_date_samples, 10),
                "p50": 2
                * np.percentile(test_date_samples + test_next_date_samples, 50),
                "p90": 2
                * np.percentile(test_date_samples + test_next_date_samples, 90),
                "a": "A2",
                "forecast_parameters": "A2",
            },
        ]
    )

    assert set(forecast_summarized_output.columns) == set(
        forecast_summarized_output.columns
    )
    pd.testing.assert_frame_equal(
        forecast_summarized_output[forecast_summarized_expected_df.columns],
        forecast_summarized_expected_df,
    )

    assert set(observed_summarized_output.columns) == set(
        observed_summarized_expected_df.columns
    )
    pd.testing.assert_frame_equal(
        observed_summarized_output[observed_summarized_expected_df.columns],
        observed_summarized_expected_df,
    )


def test_aggregate_forecast_observed_overlap_to_day():
    """tests the aggregate_forecast_observed method in the case
    where the observed and forecasted overlap and the aggregation
    happens at the day level"""
    test_date_samples = np.arange(1000)
    test_next_date_samples = np.arange(1000) * 2
    forecast_df = pd.DataFrame(
        [
            {
                **{"submission_date": TEST_DATE},
                **{i: el for i, el in enumerate(test_date_samples)},
            },
            {
                **{"submission_date": TEST_DATE_NEXT_DAY},
                **{i: el for i, el in enumerate(test_next_date_samples)},
            },
        ]
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

    numpy_aggregations = ["mean"]
    percentiles = [10, 50, 90]
    forecast_summarized_output, observed_summarized_output = (
        aggregate_forecast_observed(
            forecast_df,
            observed_df,
            period="day",
            numpy_aggregations=numpy_aggregations,
            percentiles=percentiles,
        )
    )
    observed_summarized_expected_df = pd.DataFrame(
        {
            "submission_date": [
                pd.to_datetime(TEST_DATE),
                pd.to_datetime(TEST_DATE_NEXT_DAY),
            ],
            "value": [10, 20],
        }
    )

    # add values from observed because of overlap
    forecast_summarized_expected_df = pd.DataFrame(
        [
            {
                "submission_date": pd.to_datetime(TEST_DATE),
                "mean": np.mean(test_date_samples + 10),
                "p10": np.percentile(test_date_samples + 10, 10),
                "p50": np.percentile(test_date_samples + 10, 50),
                "p90": np.percentile(test_date_samples + 10, 90),
            },
            {
                "submission_date": pd.to_datetime(TEST_DATE_NEXT_DAY),
                "mean": np.mean(test_next_date_samples + 20),
                "p10": np.percentile(test_next_date_samples + 20, 10),
                "p50": np.percentile(test_next_date_samples + 20, 50),
                "p90": np.percentile(test_next_date_samples + 20, 90),
            },
        ]
    )

    pd.testing.assert_frame_equal(
        forecast_summarized_output, forecast_summarized_expected_df
    )

    pd.testing.assert_frame_equal(
        observed_summarized_output, observed_summarized_expected_df
    )


def test_aggregate_forecast_observed_overlap_to_day_with_additional():
    """tests the aggregate_forecast_observed method in the case
    where the observed and forecasted overlap and the aggregation
    happens at the day level"""
    test_date_samples = np.arange(1000)
    test_next_date_samples = np.arange(1000) * 2
    forecast_df = pd.DataFrame(
        [
            {
                **{
                    "submission_date": TEST_DATE,
                    "a": "A1",
                    "forecast_parameters": "A1",
                },
                **{i: el for i, el in enumerate(test_date_samples)},
            },
            {
                **{
                    "submission_date": TEST_DATE_NEXT_DAY,
                    "a": "A1",
                    "forecast_parameters": "A1",
                },
                **{i: el for i, el in enumerate(test_next_date_samples)},
            },
            {
                **{
                    "submission_date": TEST_DATE,
                    "a": "A2",
                    "forecast_parameters": "A2",
                },
                **{i: el for i, el in enumerate(2 * test_date_samples)},
            },
            {
                **{
                    "submission_date": TEST_DATE_NEXT_DAY,
                    "a": "A2",
                    "forecast_parameters": "A2",
                },
                **{i: el for i, el in enumerate(2 * test_next_date_samples)},
            },
        ]
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
            "a": ["A1", "A2"],
        }
    )

    numpy_aggregations = ["mean"]
    percentiles = [10, 50, 90]
    forecast_summarized_output, observed_summarized_output = (
        aggregate_forecast_observed(
            forecast_df,
            observed_df,
            period="day",
            numpy_aggregations=numpy_aggregations,
            percentiles=percentiles,
            additional_aggregation_columns=["a"],
        )
    )
    observed_summarized_expected_df = pd.DataFrame(
        {
            "submission_date": [
                pd.to_datetime(TEST_DATE),
                pd.to_datetime(TEST_DATE_NEXT_DAY),
            ],
            "value": [10, 20],
            "a": ["A1", "A2"],
        }
    )

    # add values from observed because of overlap
    forecast_summarized_expected_df = pd.DataFrame(
        [
            {
                "submission_date": pd.to_datetime(TEST_DATE),
                "a": "A1",
                "forecast_parameters": "A1",
                "mean": np.mean(test_date_samples + 10),
                "p10": np.percentile(test_date_samples + 10, 10),
                "p50": np.percentile(test_date_samples + 10, 50),
                "p90": np.percentile(test_date_samples + 10, 90),
            },
            {
                "submission_date": pd.to_datetime(TEST_DATE_NEXT_DAY),
                "a": "A1",
                "forecast_parameters": "A1",
                "mean": np.mean(test_next_date_samples),
                "p10": np.percentile(test_next_date_samples, 10),
                "p50": np.percentile(test_next_date_samples, 50),
                "p90": np.percentile(test_next_date_samples, 90),
            },
            {
                "submission_date": pd.to_datetime(TEST_DATE),
                "a": "A2",
                "forecast_parameters": "A2",
                "mean": np.mean(2 * test_date_samples),
                "p10": np.percentile(2 * test_date_samples, 10),
                "p50": np.percentile(2 * test_date_samples, 50),
                "p90": np.percentile(2 * test_date_samples, 90),
            },
            {
                "submission_date": pd.to_datetime(TEST_DATE_NEXT_DAY),
                "a": "A2",
                "forecast_parameters": "A2",
                "mean": np.mean(2 * test_next_date_samples + 20),
                "p10": np.percentile(2 * test_next_date_samples + 20, 10),
                "p50": np.percentile(2 * test_next_date_samples + 20, 50),
                "p90": np.percentile(2 * test_next_date_samples + 20, 90),
            },
        ]
    )

    assert set(forecast_summarized_expected_df.columns) == set(
        forecast_summarized_output.columns
    )
    pd.testing.assert_frame_equal(
        forecast_summarized_output[forecast_summarized_expected_df.columns]
        .sort_values(["submission_date", "a"])
        .reset_index(drop=True),
        forecast_summarized_expected_df.sort_values(
            ["submission_date", "a"]
        ).reset_index(drop=True),
    )

    assert set(observed_summarized_expected_df.columns) == set(
        observed_summarized_output.columns
    )
    pd.testing.assert_frame_equal(
        observed_summarized_output[observed_summarized_expected_df.columns]
        .sort_values(["submission_date", "a"])
        .reset_index(drop=True),
        observed_summarized_expected_df.sort_values(
            ["submission_date", "a"]
        ).reset_index(drop=True),
    )


def test_aggregate_forecast_observed_overlap_to_month():
    """tests the aggregate_forecast_observed method in the case
    where the observed and forecasted overlap and the aggregation
    happens at the day level"""
    test_date_samples = np.arange(1000)
    test_next_date_samples = np.arange(1000) * 2
    forecast_df = pd.DataFrame(
        [
            {
                **{"submission_date": TEST_DATE},
                **{i: el for i, el in enumerate(test_date_samples)},
            },
            {
                **{"submission_date": TEST_DATE_NEXT_DAY},
                **{i: el for i, el in enumerate(test_next_date_samples)},
            },
        ]
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

    numpy_aggregations = ["mean"]
    percentiles = [10, 50, 90]
    forecast_summarized_output, observed_summarized_output = (
        aggregate_forecast_observed(
            forecast_df,
            observed_df,
            period="month",
            numpy_aggregations=numpy_aggregations,
            percentiles=percentiles,
        )
    )
    observed_summarized_expected_df = pd.DataFrame(
        {
            "submission_date": [
                pd.to_datetime(TEST_DATE),
            ],
            "value": [30],
        }
    )

    # add values from observed because of overlap
    forecast_summarized_expected_df = pd.DataFrame(
        [
            {
                "submission_date": pd.to_datetime(TEST_DATE),
                "mean": np.mean(test_date_samples + test_next_date_samples + 30),
                "p10": np.percentile(
                    test_date_samples + test_next_date_samples + 30, 10
                ),
                "p50": np.percentile(
                    test_date_samples + test_next_date_samples + 30, 50
                ),
                "p90": np.percentile(
                    test_date_samples + test_next_date_samples + 30, 90
                ),
            },
        ]
    )

    pd.testing.assert_frame_equal(
        forecast_summarized_output, forecast_summarized_expected_df
    )

    pd.testing.assert_frame_equal(
        observed_summarized_output, observed_summarized_expected_df
    )


def test_aggregate_forecast_observed_overlap_to_month_with_additional():
    """tests the aggregate_forecast_observed method in the case
    where the observed and forecasted overlap and the aggregation
    happens at the day level"""
    test_date_samples = np.arange(1000)
    test_next_date_samples = np.arange(1000) * 2
    forecast_df = pd.DataFrame(
        [
            {
                **{
                    "submission_date": TEST_DATE,
                    "forecast_parameters": "A1",
                    "a": "A1",
                },
                **{i: el for i, el in enumerate(test_date_samples)},
            },
            {
                **{
                    "submission_date": TEST_DATE_NEXT_DAY,
                    "forecast_parameters": "A1",
                    "a": "A1",
                },
                **{i: el for i, el in enumerate(test_next_date_samples)},
            },
            {
                **{
                    "submission_date": TEST_DATE,
                    "forecast_parameters": "A2",
                    "a": "A2",
                },
                **{i: el for i, el in enumerate(2 * test_date_samples)},
            },
            {
                **{
                    "submission_date": TEST_DATE_NEXT_DAY,
                    "forecast_parameters": "A2",
                    "a": "A2",
                },
                **{i: el for i, el in enumerate(2 * test_next_date_samples)},
            },
        ]
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
            "a": ["A1", "A2"],
        }
    )

    numpy_aggregations = ["mean"]
    percentiles = [10, 50, 90]
    forecast_summarized_output, observed_summarized_output = (
        aggregate_forecast_observed(
            forecast_df,
            observed_df,
            period="month",
            numpy_aggregations=numpy_aggregations,
            percentiles=percentiles,
            additional_aggregation_columns=["a"],
        )
    )
    observed_summarized_expected_df = pd.DataFrame(
        {
            "submission_date": [
                pd.to_datetime(TEST_DATE),
                pd.to_datetime(TEST_DATE),
            ],
            "value": [10, 20],
            "a": ["A1", "A2"],
        }
    )

    # add values from observed because of overlap
    forecast_summarized_expected_df = pd.DataFrame(
        [
            {
                "submission_date": pd.to_datetime(TEST_DATE),
                "forecast_parameters": "A1",
                "a": "A1",
                "mean": np.mean(test_date_samples + test_next_date_samples + 10),
                "p10": np.percentile(
                    test_date_samples + test_next_date_samples + 10, 10
                ),
                "p50": np.percentile(
                    test_date_samples + test_next_date_samples + 10, 50
                ),
                "p90": np.percentile(
                    test_date_samples + test_next_date_samples + 10, 90
                ),
            },
            {
                "submission_date": pd.to_datetime(TEST_DATE),
                "forecast_parameters": "A2",
                "a": "A2",
                "mean": np.mean(
                    2 * test_date_samples + 2 * test_next_date_samples + 20
                ),
                "p10": np.percentile(
                    2 * test_date_samples + 2 * test_next_date_samples + 20, 10
                ),
                "p50": np.percentile(
                    2 * test_date_samples + 2 * test_next_date_samples + 20, 50
                ),
                "p90": np.percentile(
                    2 * test_date_samples + 2 * test_next_date_samples + 20, 90
                ),
            },
        ]
    )

    assert set(forecast_summarized_expected_df.columns) == set(
        forecast_summarized_output.columns
    )
    pd.testing.assert_frame_equal(
        forecast_summarized_output[forecast_summarized_expected_df.columns]
        .sort_values(["submission_date", "a"])
        .reset_index(drop=True),
        forecast_summarized_expected_df.sort_values(
            ["submission_date", "a"]
        ).reset_index(drop=True),
    )

    assert set(observed_summarized_expected_df.columns) == set(
        observed_summarized_output.columns
    )
    pd.testing.assert_frame_equal(
        observed_summarized_output[observed_summarized_expected_df.columns]
        .sort_values(["submission_date", "a"])
        .reset_index(drop=True),
        observed_summarized_expected_df.sort_values(
            ["submission_date", "a"]
        ).reset_index(drop=True),
    )


def test_combine_forecast_observed():
    """tests the combine_forecast_observed method"""
    forecast_df = pd.DataFrame(
        {
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
            "mean": [0, 0],
            "p10": [0, 0],
            "p50": [0, 0],
            "p90": [0, 0],
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

    output_df = combine_forecast_observed(forecast_df, observed_df)
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

    pd.testing.assert_frame_equal(
        output_df.sort_values(["submission_date", "measure"]).reset_index(drop=True),
        expected[output_df.columns].reset_index(drop=True),
    )

    assert not pd.isna(output_df).any(axis=None)


def test_summarize():
    """testing _summarize"""
    test_date_samples = np.arange(1000)
    test_next_date_samples = np.arange(1000) * 2
    forecast_df = pd.DataFrame(
        [
            {
                **{"submission_date": TEST_DATE},
                **{i: el for i, el in enumerate(test_date_samples)},
            },
            {
                **{"submission_date": TEST_DATE_NEXT_DAY},
                **{i: el for i, el in enumerate(test_next_date_samples)},
            },
        ]
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

    numpy_aggregations = ["mean"]
    percentiles = [10, 50, 90]
    output_df = summarize(
        forecast_df,
        observed_df,
        period="day",
        numpy_aggregations=numpy_aggregations,
        percentiles=percentiles,
    )

    observed_expected_df = pd.DataFrame(
        {
            "submission_date": [
                pd.to_datetime(TEST_DATE),
                pd.to_datetime(TEST_DATE_NEXT_DAY),
            ],
            "value": [10, 20],
            "measure": ["observed", "observed"],
            "source": ["historical", "historical"],
        }
    )

    # add values from observed because of overlap
    forecast_expected_df = pd.DataFrame(
        [
            {
                "submission_date": pd.to_datetime(TEST_DATE),
                "measure": "mean",
                "value": np.mean(test_date_samples + 10),
                "source": "forecast",
            },
            {
                "submission_date": pd.to_datetime(TEST_DATE_NEXT_DAY),
                "measure": "mean",
                "value": np.mean(test_next_date_samples + 20),
                "source": "forecast",
            },
            {
                "submission_date": pd.to_datetime(TEST_DATE),
                "measure": "p10",
                "value": np.percentile(test_date_samples + 10, 10),
                "source": "forecast",
            },
            {
                "submission_date": pd.to_datetime(TEST_DATE_NEXT_DAY),
                "measure": "p10",
                "value": np.percentile(test_next_date_samples + 20, 10),
                "source": "forecast",
            },
            {
                "submission_date": pd.to_datetime(TEST_DATE),
                "measure": "p50",
                "value": np.percentile(test_date_samples + 10, 50),
                "source": "forecast",
            },
            {
                "submission_date": pd.to_datetime(TEST_DATE_NEXT_DAY),
                "measure": "p50",
                "value": np.percentile(test_next_date_samples + 20, 50),
                "source": "forecast",
            },
            {
                "submission_date": pd.to_datetime(TEST_DATE),
                "measure": "p90",
                "value": np.percentile(test_date_samples + 10, 90),
                "source": "forecast",
            },
            {
                "submission_date": pd.to_datetime(TEST_DATE_NEXT_DAY),
                "measure": "p90",
                "value": np.percentile(test_next_date_samples + 20, 90),
                "source": "forecast",
            },
        ]
    )

    # concat in same order to make our lives easier
    expected = pd.concat([observed_expected_df, forecast_expected_df]).sort_values(
        ["submission_date", "measure"]
    )
    expected["aggregation_period"] = "day"

    assert set(expected.columns) == set(output_df.columns)
    pd.testing.assert_frame_equal(
        output_df.sort_values(["submission_date", "measure"]).reset_index(drop=True),
        expected[output_df.columns].reset_index(drop=True),
    )

    assert not pd.isna(output_df).any(axis=None)


def test_summarize_non_overlapping_day():
    observed_start_date = TEST_DATE_STR
    observed_end_date = (TEST_DATE + relativedelta(months=1)).strftime("%Y-%m-%d")

    predict_start_date = (TEST_DATE + relativedelta(months=1, days=1)).strftime(
        "%Y-%m-%d"
    )
    predict_end_date = (TEST_DATE + relativedelta(months=2)).strftime("%Y-%m-%d")

    observed_submission_dates = pd.date_range(
        pd.to_datetime(observed_start_date), pd.to_datetime(observed_end_date)
    ).date
    predict_submission_dates = pd.date_range(
        pd.to_datetime(predict_start_date), pd.to_datetime(predict_end_date)
    ).date

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

    output_df = summarize(forecast_df, observed_df, "day", ["mean", "median"], [50])

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

    expected_df["aggregation_period"] = "day"

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

    observed_submission_dates = pd.date_range(
        pd.to_datetime(observed_start_date), pd.to_datetime(observed_end_date)
    ).date
    predict_submission_dates = pd.date_range(
        pd.to_datetime(predict_start_date), pd.to_datetime(predict_end_date)
    ).date

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

    output_df = summarize(forecast_df, observed_df, "month", ["mean", "median"], [50])

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

    expected_df["aggregation_period"] = "month"

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

    observed_submission_dates = pd.date_range(
        pd.to_datetime(observed_start_date), pd.to_datetime(observed_end_date)
    ).date
    predict_submission_dates = pd.date_range(
        pd.to_datetime(predict_start_date), pd.to_datetime(predict_end_date)
    ).date
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

    output_df = summarize(forecast_df, observed_df, "day", ["mean", "median"], [50])

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

    expected_df["aggregation_period"] = "day"

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

    observed_submission_dates = pd.date_range(
        pd.to_datetime(observed_start_date), pd.to_datetime(observed_end_date)
    ).date
    predict_submission_dates = pd.date_range(
        pd.to_datetime(predict_start_date), pd.to_datetime(predict_end_date)
    ).date
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

    output_df = summarize(forecast_df, observed_df, "month", ["mean", "median"], [50])

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

    expected_df["aggregation_period"] = "month"

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


def test_add_regressors(forecast):
    """test add regressors
    test case for each element of regressor_list_raw is indicated in name"""

    # choose arbitrary dates for dates
    # name indicates the relationship of the window
    # to the timeframe of the data as defined in the ds
    # column of df below
    regressor_list_raw = [
        {
            "name": "all_in",
            "description": "it's all in",
            "start_date": "2124-01-01",
            "end_date": "2124-01-06",
        },
        {
            "name": "all_out",
            "description": "it's all out",
            "start_date": "2124-02-01",
            "end_date": "2124-02-06",
        },
        {
            "name": "just_end",
            "description": "just the second half",
            "start_date": "2124-01-03",
            "end_date": "2124-02-06",
        },
        {
            "name": "just_middle",
            "description": "just the middle two",
            "start_date": "2124-01-02",
            "end_date": "2124-01-03",
        },
    ]

    regressor_list = [ProphetRegressor(**r) for r in regressor_list_raw]

    df = pd.DataFrame(
        {
            "ds": [
                pd.to_datetime("2124-01-01").date(),
                pd.to_datetime("2124-01-02").date(),
                pd.to_datetime("2124-01-03").date(),
                pd.to_datetime("2124-01-04").date(),
            ],
        }
    )

    output_df = forecast._add_regressors(df, regressors=regressor_list)

    expected_df = pd.DataFrame(
        {
            "ds": [
                pd.to_datetime("2124-01-01").date(),
                pd.to_datetime("2124-01-02").date(),
                pd.to_datetime("2124-01-03").date(),
                pd.to_datetime("2124-01-04").date(),
            ],
            "all_in": [0, 0, 0, 0],
            "all_out": [1, 1, 1, 1],
            "just_end": [1, 1, 0, 0],
            "just_middle": [1, 0, 0, 1],
        }
    )

    assert set(output_df.columns) == set(expected_df.columns)
    pd.testing.assert_frame_equal(output_df, expected_df[output_df.columns])


def test_add_regressors_partial(forecast):
    """test add regressors when some fields aren't set
    test case for each element of regressor_list_raw is indicated in name"""

    # choose arbitrary dates for dates
    # name indicates the relationship of the window
    # to the timeframe of the data as defined in the ds
    # column of df below
    regressor_list_raw = [
        {
            "name": "just_end",
            "description": "just the second half",
            "start_date": "2124-01-03",
        },
        {
            "name": "just_start",
            "description": "just the beginning",
            "end_date": "2124-01-03",
        },
    ]

    regressor_list = [ProphetRegressor(**r) for r in regressor_list_raw]

    df = pd.DataFrame(
        {
            "ds": [
                pd.to_datetime("2124-01-01").date(),
                pd.to_datetime("2124-01-02").date(),
                pd.to_datetime("2124-01-03").date(),
                pd.to_datetime("2124-01-04").date(),
            ],
        }
    )

    output_df = forecast._add_regressors(df, regressors=regressor_list)

    expected_df = pd.DataFrame(
        {
            "ds": [
                pd.to_datetime("2124-01-01").date(),
                pd.to_datetime("2124-01-02").date(),
                pd.to_datetime("2124-01-03").date(),
                pd.to_datetime("2124-01-04").date(),
            ],
            "just_end": [1, 1, 0, 0],
            "just_start": [0, 0, 0, 1],
        }
    )

    assert set(output_df.columns) == set(expected_df.columns)
    pd.testing.assert_frame_equal(output_df, expected_df[output_df.columns])


def test_build_train_dataframe_no_regressors(forecast):
    """test _build_train_dataframe with no regressors"""
    # only the growth and regressors attributes matter for train_dataframe
    # so they can be manually set here
    regressor_list = []
    forecast.regressors = regressor_list

    observed_df = pd.DataFrame(
        {
            "a": [1, 1],
            "b": [2, 2],
            "y": [3, 4],
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
        }
    )

    output_train_df = forecast._build_train_dataframe(observed_df)
    expected_train_df = pd.DataFrame(
        {
            "a": [1, 1],
            "b": [2, 2],
            "y": [3, 4],
            "ds": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
        }
    )
    pd.testing.assert_frame_equal(
        output_train_df.reset_index(drop=True), expected_train_df
    )

    # test again but with add_logistic_growth_cols set to true
    forecast.growth = "logistic"
    output_train_wlog_df = forecast._build_train_dataframe(observed_df)
    expected_train_wlog_df = pd.DataFrame(
        {
            "a": [1, 1],
            "b": [2, 2],
            "y": [3, 4],
            "ds": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
            "floor": [1.5, 1.5],
            "cap": [6.0, 6.0],
        }
    )

    assert set(output_train_wlog_df.columns) == set(expected_train_wlog_df.columns)
    pd.testing.assert_frame_equal(
        output_train_wlog_df.reset_index(drop=True),
        expected_train_wlog_df[output_train_wlog_df.columns],
    )


def test_build_train_dataframe(forecast):
    """test _build_train_dataframe and include regressors"""
    regressor_list = [
        {
            "name": "all_in",
            "description": "it's all in",
            "start_date": TEST_DATE_STR,
            "end_date": (TEST_DATE + relativedelta(days=6)).strftime("%Y-%m-%d"),
        },
        {
            "name": "all_out",
            "description": "it's all in",
            "start_date": (TEST_DATE + relativedelta(months=1)).strftime("%Y-%m-%d"),
            "end_date": (TEST_DATE + relativedelta(months=1, days=6)).strftime(
                "%Y-%m-%d"
            ),
        },
        {
            "name": "just_end",
            "description": "just the second one",
            "start_date": (TEST_DATE + relativedelta(days=1)).strftime("%Y-%m-%d"),
            "end_date": (TEST_DATE + relativedelta(months=1, days=6)).strftime(
                "%Y-%m-%d"
            ),
        },
    ]
    # only the growth and regressors attributes matter for train_dataframe
    # so they can be manually set here
    forecast.regressors = [ProphetRegressor(**r) for r in regressor_list]

    observed_df = pd.DataFrame(
        {
            "a": [1, 1],
            "b": [2, 2],
            "y": [3, 4],
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
        }
    )

    output_train_df = forecast._build_train_dataframe(observed_df)
    expected_train_df = pd.DataFrame(
        {
            "a": [1, 1],
            "b": [2, 2],
            "y": [3, 4],
            "ds": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
            "all_in": [0, 0],
            "all_out": [
                1,
                1,
            ],
            "just_end": [1, 0],
        }
    )
    pd.testing.assert_frame_equal(
        output_train_df.reset_index(drop=True), expected_train_df
    )

    # now with logistic growth set
    forecast.growth = "logistic"
    output_train_wlog_df = forecast._build_train_dataframe(observed_df)
    expected_train_wlog_df = pd.DataFrame(
        {
            "a": [1, 1],
            "b": [2, 2],
            "y": [3, 4],
            "ds": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
            "all_in": [0, 0],
            "all_out": [1, 1],
            "just_end": [1, 0],
            "floor": [1.5, 1.5],
            "cap": [6.0, 6.0],
        }
    )

    assert set(output_train_wlog_df.columns) == set(expected_train_wlog_df.columns)
    pd.testing.assert_frame_equal(
        output_train_wlog_df.reset_index(drop=True),
        expected_train_wlog_df[output_train_wlog_df.columns],
    )


def test_build_predict_dataframe_no_regressors(forecast):
    """test _build_predict with no regressors"""
    # only the growth and regressors attributes matter for train_dataframe
    # so they can be manually set here
    regressor_list = []
    forecast.regressors = regressor_list

    # manually set trained_parameters, normally this would happen during training
    forecast.logistic_growth_floor = -1.0
    forecast.logistic_growth_cap = 10.0

    dates_to_predict = pd.DataFrame(
        {
            "submission_date": [
                TEST_DATE - relativedelta(months=1),
                TEST_DATE_NEXT_DAY - relativedelta(months=1),
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
        }
    )

    output_predict_df = forecast._build_predict_dataframe(dates_to_predict)
    expected_predict_df = pd.DataFrame(
        {
            "ds": [
                TEST_DATE - relativedelta(months=1),
                TEST_DATE_NEXT_DAY - relativedelta(months=1),
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
        }
    )
    pd.testing.assert_frame_equal(
        output_predict_df.reset_index(drop=True), expected_predict_df
    )

    # test against but with add_logistic_growth_cols set to true
    forecast.growth = "logistic"
    output_predict_wlog_df = forecast._build_predict_dataframe(dates_to_predict)
    expected_predict_wlog_df = pd.DataFrame(
        {
            "ds": [
                TEST_DATE - relativedelta(months=1),
                TEST_DATE_NEXT_DAY - relativedelta(months=1),
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
            "floor": [-1.0, -1.0, -1.0, -1.0, -1.0, -1.0],
            "cap": [10.0, 10.0, 10.0, 10.0, 10.0, 10.0],
        }
    )

    assert set(output_predict_wlog_df.columns) == set(expected_predict_wlog_df.columns)
    pd.testing.assert_frame_equal(
        output_predict_wlog_df.reset_index(drop=True),
        expected_predict_wlog_df[output_predict_wlog_df.columns],
    )


def test_build_predict_dataframe(forecast):
    """test _build_predict_dataframe including regressors"""
    regressor_list = [
        {
            "name": "all_in",
            "description": "it's all in",
            "start_date": TEST_DATE_STR,
            "end_date": (TEST_DATE + relativedelta(days=6)).strftime("%Y-%m-%d"),
        },
        {
            "name": "all_out",
            "description": "it's all in",
            "start_date": (TEST_DATE + relativedelta(months=1)).strftime("%Y-%m-%d"),
            "end_date": (TEST_DATE + relativedelta(months=1, days=6)).strftime(
                "%Y-%m-%d"
            ),
        },
        {
            "name": "just_end",
            "description": "just the second one",
            "start_date": (TEST_DATE + relativedelta(days=1)).strftime("%Y-%m-%d"),
            "end_date": (TEST_DATE + relativedelta(months=1, days=6)).strftime(
                "%Y-%m-%d"
            ),
        },
    ]

    # only the growth and regressors attributes matter for train_dataframe
    # so they can be manually set here
    forecast.regressors = [ProphetRegressor(**r) for r in regressor_list]

    # manually set trained_parameters, normally this would happen during training
    forecast.logistic_growth_floor = -1.0
    forecast.logistic_growth_cap = 10.0

    dates_to_predict = pd.DataFrame(
        {
            "submission_date": [TEST_DATE, TEST_DATE_NEXT_DAY],
        }
    )

    output_train_df = forecast._build_predict_dataframe(dates_to_predict)
    expected_train_df = pd.DataFrame(
        {
            "ds": [TEST_DATE, TEST_DATE_NEXT_DAY],
            "all_in": [0, 0],
            "all_out": [1, 1],
            "just_end": [1, 0],
        }
    )
    pd.testing.assert_frame_equal(
        output_train_df.reset_index(drop=True), expected_train_df
    )

    # test again but with add_logistic_growth_cols set to true
    forecast.growth = "logistic"
    output_train_wlog_df = forecast._build_predict_dataframe(dates_to_predict)
    expected_train_wlog_df = pd.DataFrame(
        {
            "ds": [TEST_DATE, TEST_DATE_NEXT_DAY],
            "all_in": [0, 0],
            "all_out": [1, 1],
            "just_end": [1, 0],
            "floor": [-1.0, -1.0],
            "cap": [10.0, 10.0],
        }
    )

    assert set(output_train_wlog_df.columns) == set(expected_train_wlog_df.columns)
    pd.testing.assert_frame_equal(
        output_train_wlog_df.reset_index(drop=True),
        expected_train_wlog_df[output_train_wlog_df.columns],
    )
