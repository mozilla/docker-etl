import pandas as pd
from dotmap import DotMap
import numpy as np


from kpi_forecasting.models.prophet_forecast import ProphetForecast


def test_summarize_non_overlapping_day():
    observed_start_date = "2124-01-01"
    observed_end_date = "2124-02-01"

    predict_start_date = "2124-02-02"
    predict_end_date = "2124-03-01"

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

    test_samples = np.array([1, 1, 2, 3, 5, 8, 13])
    test_mean = np.mean(test_samples)
    test_median = np.median(test_samples)

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
    observed_start_date = "2124-01-01"
    observed_end_date = "2124-02-28"

    predict_start_date = "2124-04-01"
    predict_end_date = "2124-05-31"

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
    observed_start_date = "2124-01-01"
    observed_end_date = "2124-02-01"

    predict_start_date = "2124-01-01"
    predict_end_date = "2124-02-01"

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
