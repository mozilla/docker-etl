import re

import pandas as pd
from dotmap import DotMap
import pytest


from kpi_forecasting.configs.model_inputs import ProphetRegressor, ProphetHoliday
from kpi_forecasting.models.funnel_forecast import SegmentModelSettings, FunnelForecast


@pytest.fixture()
def forecast():
    predict_start_date = "2124-01-01"
    predict_end_date = "2124-03-01"

    forecast = FunnelForecast(
        model_type="test",
        parameters=DotMap(),
        use_holidays=None,
        start_date=predict_start_date,
        end_date=predict_end_date,
        metric_hub=None,
    )
    return forecast


def test_fill_regressor_dates(forecast):
    regressor_info = {
        "name": "only_start",
        "description": "only has a start",
        "start_date": "2020-08-15",
    }
    regressor = ProphetRegressor(**regressor_info)
    forecast._fill_regressor_dates(regressor)
    assert regressor.start_date == pd.to_datetime("2020-08-15")
    assert regressor.end_date == pd.to_datetime("2124-03-01")

    regressor_info = {
        "name": "only_end",
        "description": "only has a end",
        "end_date": "2125-08-15",
    }
    regressor = ProphetRegressor(**regressor_info)
    forecast._fill_regressor_dates(regressor)
    assert regressor.start_date == pd.to_datetime("2124-01-01")
    assert regressor.end_date == pd.to_datetime("2125-08-15")

    regressor_info = {
        "name": "both",
        "description": "only has a start",
        "start_date": "2020-08-15",
        "end_date": "2020-09-15",
    }
    regressor = ProphetRegressor(**regressor_info)
    forecast._fill_regressor_dates(regressor)
    assert regressor.start_date == pd.to_datetime("2020-08-15")
    assert regressor.end_date == pd.to_datetime("2020-09-15")

    regressor_info = {
        "name": "neither",
        "description": "nothin to see here",
    }
    regressor = ProphetRegressor(**regressor_info)
    forecast._fill_regressor_dates(regressor)
    assert regressor.start_date == pd.to_datetime("2124-01-01")
    assert regressor.end_date == pd.to_datetime("2124-03-01")

    regressor_info = {
        "name": "out_of_order",
        "description": "best better break",
        "start_date": "2020-08-15",
        "end_date": "2000-09-15",
    }
    regressor = ProphetRegressor(**regressor_info)
    with pytest.raises(
        Exception,
        match="Regressor out_of_order start date comes after end date",
    ):
        forecast._fill_regressor_dates(regressor)


def test_add_regressors(forecast):
    regressor_list_raw = [
        {
            "name": "all_in",
            "description": "it's all in",
            "start_date": "2124-01-01",
            "end_date": "2124-01-06",
        },
        {
            "name": "all_out",
            "description": "it's all in",
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


def test_build_model_dataframe_exception(forecast):
    regressor_list = []

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
    segment_settings = SegmentModelSettings(
        segment={"a": 1, "b": 2},
        start_date="2124-01-01",
        end_date="2124-02-01",
        holidays=[],
        regressors=[ProphetRegressor(**r) for r in regressor_list],
        grid_parameters=grid_parameters,
        cv_settings=cv_settings,
    )

    observed_df = pd.DataFrame(
        {
            "a": [1, 1, 1, 1, 3, 3],
            "b": [1, 1, 2, 2, 2, 2],
            "y": [1, 2, 3, 4, 5, 6],
            "submission_date": [
                pd.to_datetime("2124-12-01").date(),
                pd.to_datetime("2124-12-02").date(),
                pd.to_datetime("2124-01-01").date(),
                pd.to_datetime("2124-01-02").date(),
                pd.to_datetime("2123-01-01").date(),
                pd.to_datetime("2123-01-02").date(),
            ],
        }
    )

    forecast.observed_df = observed_df

    with pytest.raises(ValueError, match="task set to test, must be train or predict"):
        _ = forecast._build_model_dataframe(
            segment_settings=segment_settings, task="test"
        )


def test_build_model_dataframe_no_regressors_train(forecast):
    regressor_list = []

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
    segment_settings = SegmentModelSettings(
        segment={"a": 1, "b": 2},
        start_date="2124-01-01",
        end_date="2124-02-01",
        holidays=[],
        regressors=[ProphetRegressor(**r) for r in regressor_list],
        grid_parameters=grid_parameters,
        cv_settings=cv_settings,
    )

    observed_df = pd.DataFrame(
        {
            "a": [1, 1, 1, 1, 3, 3],
            "b": [1, 1, 2, 2, 2, 2],
            "y": [1, 2, 3, 4, 5, 6],
            "submission_date": [
                pd.to_datetime("2124-12-01").date(),
                pd.to_datetime("2124-12-02").date(),
                pd.to_datetime("2124-01-01").date(),
                pd.to_datetime("2124-01-02").date(),
                pd.to_datetime("2123-01-01").date(),
                pd.to_datetime("2123-01-02").date(),
            ],
        }
    )

    forecast.observed_df = observed_df

    output_train_df = forecast._build_model_dataframe(
        segment_settings=segment_settings, task="train"
    )
    expected_train_df = pd.DataFrame(
        {
            "a": [1, 1],
            "b": [2, 2],
            "y": [3, 4],
            "ds": [
                pd.to_datetime("2124-01-01").date(),
                pd.to_datetime("2124-01-02").date(),
            ],
        }
    )
    pd.testing.assert_frame_equal(
        output_train_df.reset_index(drop=True), expected_train_df
    )

    output_train_wlog_df = forecast._build_model_dataframe(
        segment_settings=segment_settings, task="train", add_logistic_growth_cols=True
    )
    expected_train_wlog_df = pd.DataFrame(
        {
            "a": [1, 1],
            "b": [2, 2],
            "y": [3, 4],
            "ds": [
                pd.to_datetime("2124-01-01").date(),
                pd.to_datetime("2124-01-02").date(),
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


def test_build_model_dataframe_train(forecast):
    regressor_list = [
        {
            "name": "all_in",
            "description": "it's all in",
            "start_date": "2124-01-01",
            "end_date": "2124-01-06",
        },
        {
            "name": "all_out",
            "description": "it's all in",
            "start_date": "2124-02-01",
            "end_date": "2124-02-06",
        },
        {
            "name": "just_end",
            "description": "just the second one",
            "start_date": "2124-01-02",
            "end_date": "2124-02-06",
        },
    ]

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
    segment_settings = SegmentModelSettings(
        segment={"a": 1, "b": 2},
        start_date="2124-01-01",
        end_date="2124-02-01",
        holidays=[],
        regressors=[ProphetRegressor(**r) for r in regressor_list],
        grid_parameters=grid_parameters,
        cv_settings=cv_settings,
    )

    observed_df = pd.DataFrame(
        {
            "a": [1, 1, 1, 1, 3, 3],
            "b": [1, 1, 2, 2, 2, 2],
            "y": [1, 2, 3, 4, 5, 6],
            "submission_date": [
                pd.to_datetime("2124-12-01").date(),
                pd.to_datetime("2124-12-02").date(),
                pd.to_datetime("2124-01-01").date(),
                pd.to_datetime("2124-01-02").date(),
                pd.to_datetime("2123-01-01").date(),
                pd.to_datetime("2123-01-02").date(),
            ],
        }
    )

    forecast.observed_df = observed_df

    output_train_df = forecast._build_model_dataframe(
        segment_settings=segment_settings, task="train"
    )
    expected_train_df = pd.DataFrame(
        {
            "a": [1, 1],
            "b": [2, 2],
            "y": [3, 4],
            "ds": [
                pd.to_datetime("2124-01-01").date(),
                pd.to_datetime("2124-01-02").date(),
            ],
            "all_in": [0, 0],
            "all_out": [1, 1],
            "just_end": [1, 0],
        }
    )
    pd.testing.assert_frame_equal(
        output_train_df.reset_index(drop=True), expected_train_df
    )

    output_train_wlog_df = forecast._build_model_dataframe(
        segment_settings=segment_settings, task="train", add_logistic_growth_cols=True
    )
    expected_train_wlog_df = pd.DataFrame(
        {
            "a": [1, 1],
            "b": [2, 2],
            "y": [3, 4],
            "ds": [
                pd.to_datetime("2124-01-01").date(),
                pd.to_datetime("2124-01-02").date(),
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


def test_build_model_dataframe_no_regressors_predict(forecast):
    regressor_list = []

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
    segment_settings = SegmentModelSettings(
        segment={"a": 1, "b": 2},
        start_date="2124-01-01",
        end_date="2124-02-01",
        holidays=[],
        regressors=[ProphetRegressor(**r) for r in regressor_list],
        grid_parameters=grid_parameters,
        cv_settings=cv_settings,
    )

    segment_settings.trained_parameters = {"floor": -1.0, "cap": 10.0}

    dates_to_predict = pd.DataFrame(
        {
            "submission_date": [
                pd.to_datetime("2124-12-01").date(),
                pd.to_datetime("2124-12-02").date(),
                pd.to_datetime("2124-01-01").date(),
                pd.to_datetime("2124-01-02").date(),
                pd.to_datetime("2123-01-01").date(),
                pd.to_datetime("2123-01-02").date(),
            ],
        }
    )

    forecast.dates_to_predict = dates_to_predict

    output_predict_df = forecast._build_model_dataframe(
        segment_settings=segment_settings, task="predict"
    )
    expected_predict_df = pd.DataFrame(
        {
            "ds": [
                pd.to_datetime("2124-12-01").date(),
                pd.to_datetime("2124-12-02").date(),
                pd.to_datetime("2124-01-01").date(),
                pd.to_datetime("2124-01-02").date(),
                pd.to_datetime("2123-01-01").date(),
                pd.to_datetime("2123-01-02").date(),
            ],
        }
    )
    pd.testing.assert_frame_equal(
        output_predict_df.reset_index(drop=True), expected_predict_df
    )

    output_predict_wlog_df = forecast._build_model_dataframe(
        segment_settings=segment_settings, task="predict", add_logistic_growth_cols=True
    )
    expected_predict_wlog_df = pd.DataFrame(
        {
            "ds": [
                pd.to_datetime("2124-12-01").date(),
                pd.to_datetime("2124-12-02").date(),
                pd.to_datetime("2124-01-01").date(),
                pd.to_datetime("2124-01-02").date(),
                pd.to_datetime("2123-01-01").date(),
                pd.to_datetime("2123-01-02").date(),
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


def test_build_model_dataframe_predict(forecast):
    regressor_list = [
        {
            "name": "all_in",
            "description": "it's all in",
            "start_date": "2124-01-01",
            "end_date": "2124-01-06",
        },
        {
            "name": "all_out",
            "description": "it's all in",
            "start_date": "2124-02-01",
            "end_date": "2124-02-06",
        },
        {
            "name": "just_end",
            "description": "just the second one",
            "start_date": "2124-01-02",
            "end_date": "2124-02-06",
        },
    ]

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
    segment_settings = SegmentModelSettings(
        segment={"a": 1, "b": 2},
        start_date="2124-01-01",
        end_date="2124-02-01",
        holidays=[],
        regressors=[ProphetRegressor(**r) for r in regressor_list],
        grid_parameters=grid_parameters,
        cv_settings=cv_settings,
    )

    observed_df = pd.DataFrame(
        {
            "a": [1, 1, 1, 1, 3, 3],
            "b": [1, 1, 2, 2, 2, 2],
            "y": [1, 2, 3, 4, 5, 6],
            "submission_date": [
                pd.to_datetime("2124-12-01").date(),
                pd.to_datetime("2124-12-02").date(),
                pd.to_datetime("2124-01-01").date(),
                pd.to_datetime("2124-01-02").date(),
                pd.to_datetime("2123-01-01").date(),
                pd.to_datetime("2123-01-02").date(),
            ],
        }
    )

    forecast.observed_df = observed_df

    output_train_df = forecast._build_model_dataframe(
        segment_settings=segment_settings, task="train"
    )
    expected_train_df = pd.DataFrame(
        {
            "a": [1, 1],
            "b": [2, 2],
            "y": [3, 4],
            "ds": [
                pd.to_datetime("2124-01-01").date(),
                pd.to_datetime("2124-01-02").date(),
            ],
            "all_in": [0, 0],
            "all_out": [1, 1],
            "just_end": [1, 0],
        }
    )
    pd.testing.assert_frame_equal(
        output_train_df.reset_index(drop=True), expected_train_df
    )

    output_train_wlog_df = forecast._build_model_dataframe(
        segment_settings=segment_settings, task="train", add_logistic_growth_cols=True
    )
    expected_train_wlog_df = pd.DataFrame(
        {
            "a": [1, 1],
            "b": [2, 2],
            "y": [3, 4],
            "ds": [
                pd.to_datetime("2124-01-01").date(),
                pd.to_datetime("2124-01-02").date(),
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


def test_build_model(forecast):
    regressor_list = [
        {
            "name": "all_in",
            "description": "it's all in",
            "start_date": "2124-01-01",
            "end_date": "2124-01-06",
        },
        {
            "name": "all_out",
            "description": "it's all in",
            "start_date": "2124-02-01",
            "end_date": "2124-02-06",
        },
        {
            "name": "just_end",
            "description": "just the second one",
            "start_date": "2124-01-02",
            "end_date": "2124-02-06",
        },
    ]

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
    segment_settings = SegmentModelSettings(
        segment={"a": 1, "b": 2},
        start_date="2124-01-01",
        end_date="2124-02-01",
        holidays=[ProphetHoliday(**h) for h in holiday_list.values()],
        regressors=[ProphetRegressor(**r) for r in regressor_list],
        grid_parameters=grid_parameters,
        cv_settings=cv_settings,
    )

    model = forecast._build_model(
        segment_settings=segment_settings,
        parameters={
            "changepoint_prior_scale": 0.01,
            "changepoint_range": 0.8,
            "n_changepoints": 30,
            "weekly_seasonality": True,
            "yearly_seasonality": True,
            "growth": "logistic",
        },
    )

    holiday_df = model.holidays
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
