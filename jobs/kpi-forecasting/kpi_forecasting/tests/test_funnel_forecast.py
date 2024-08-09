"""tests for the funnel forecast module"""

import collections
from datetime import date, datetime
from dateutil.relativedelta import relativedelta

import pandas as pd
import pytest
import numpy as np


from kpi_forecasting.configs.model_inputs import ProphetRegressor, ProphetHoliday
from kpi_forecasting.models.funnel_forecast import SegmentModelSettings, FunnelForecast

# Arbitrarily choose some date to use for the tests
TEST_DATE = date(2024, 1, 1)
TEST_DATE_STR = TEST_DATE.strftime("%Y-%m-%d")
TEST_DATE_NEXT_DAY = date(2024, 1, 2)
TEST_DATE_NEXT_DAY_STR = TEST_DATE_NEXT_DAY.strftime("%Y-%m-%d")
TEST_PREDICT_END = TEST_DATE + relativedelta(months=2)
TEST_PREDICT_END_STR = TEST_PREDICT_END.strftime("%Y-%m-%d")


@pytest.fixture()
def forecast():
    """This mocks a generic forecast object"""
    # 2024-01-01 is arbitarily chosen as a future date
    predict_start_date = TEST_DATE_STR
    predict_end_date = TEST_PREDICT_END_STR

    forecast = FunnelForecast(
        model_type="test",
        parameters={},
        use_holidays=None,
        start_date=predict_start_date,
        end_date=predict_end_date,
        metric_hub=None,
    )
    return forecast


@pytest.fixture()
def segment_info_fit_tests():
    """This fixture creates segment info dictionaries
    that mimic the content of the config file and are used
    in the functions that test fit methods"""

    # 2024-01-01 is arbitarily chosen as a future date
    A1_start_date = TEST_DATE_STR
    A2_start_date = TEST_DATE_NEXT_DAY_STR

    segment_info_dict = {
        "A1": {
            "start_date": A1_start_date,
            "grid_parameters": {"param1": [1, 2], "param2": [20, 10]},
            "min_param_value": 10,
        },
        "A2": {
            "start_date": A2_start_date,
            "grid_parameters": {"param1": [-1, -2], "param2": [3, 4]},
            "min_param_value": -3,  # closest to zero
        },
    }
    return segment_info_dict


@pytest.fixture()
def funnel_forecast_for_fit_tests(segment_info_fit_tests, mocker):
    """This method creates a forecast object from the segment dict
    created in the segment_info_fit_tests fixture.  It also
    mocks some of the object methods to enable easier testing"""
    parameter_list = [
        {
            "segment": {"a": "A1"},
            "start_date": segment_info_fit_tests["A1"]["start_date"],
            "end_date": None,
            "holidays": [],
            "regressors": [],
            "grid_parameters": segment_info_fit_tests["A1"]["grid_parameters"],
            "cv_settings": {},
        },
        {
            "segment": {"a": "A2"},
            "start_date": segment_info_fit_tests["A2"]["start_date"],
            "end_date": None,
            "holidays": [],
            "regressors": [],
            "grid_parameters": segment_info_fit_tests["A2"]["grid_parameters"],
            "cv_settings": {},
        },
    ]

    predict_start_date = TEST_DATE_STR
    predict_end_date = TEST_DATE_NEXT_DAY_STR

    forecast = FunnelForecast(
        model_type="test",
        parameters=parameter_list,
        use_holidays=None,
        start_date=predict_start_date,
        end_date=predict_end_date,
        metric_hub=None,
    )

    mocker.patch.object(forecast, "_build_model", mock_build_model)
    mocker.patch.object(
        forecast, "_get_crossvalidation_metric", mock_get_crossvalidation_metric
    )

    return forecast


class MockModel:
    """Used in place of prophet.Prophet for testing purposes"""

    def __init__(self, param1=0, param2=0):
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


def mock_build_model(segment_settings, parameters):
    """mocks the FunnelForecast build_model method"""
    return MockModel(
        **parameters,
    )


def mock_get_crossvalidation_metric(m, *args, **kwargs):
    """mocks the FunnelForecast get_crossvalidation_metric
    method, meant to be used with MockModel"""
    return m.value  # value atrribute in MockModel


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


def test_combine_forecast_observed(mocker, forecast):
    """tests the _combine_forecast_observed method"""
    mocker.patch.object(
        forecast, "_aggregate_forecast_observed", mock_aggregate_forecast_observed
    )

    forecast_df = pd.DataFrame(
        {
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
        }
    )

    observed_df = pd.DataFrame(
        {
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
            "a": ["A1", "A1"],
            "value": [5, 6],
        }
    )

    numpy_aggregations = ["mean"]
    percentiles = [10, 50, 90]

    output_df = forecast._combine_forecast_observed(
        forecast_df=forecast_df,
        observed_df=observed_df,
        period="period",
        numpy_aggregations=numpy_aggregations,
        percentiles=percentiles,
        segment={"a": "A1"},
    )

    # mean was renamed to value, percentiles to high, medium, low
    forecast_df[["value", "value_low", "value_mid", "value_high"]] = 0
    forecast_df["a"] = "A1"  # this column is already present in observed

    forecast_df["source"] = "forecast"
    observed_df["source"] = "historical"

    # concat in same order to make our lives easier
    expected = pd.concat([observed_df, forecast_df])
    assert set(expected.columns) == set(output_df.columns)
    pd.testing.assert_frame_equal(output_df, expected[output_df.columns])

    # should not be any nulls outside the metric column
    non_metric_columns = [
        el
        for el in output_df.columns
        if el not in ["value", "value_low", "value_mid", "value_high"]
    ]
    assert not pd.isna(output_df[non_metric_columns]).any(axis=None)


def test_under_summarize(mocker, forecast):
    """testing _summarize"""
    # 2024-01-01 is chosen as an arbitrary date to center the tests around

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
                TEST_DATE - relativedelta(months=1),
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
            "a": ["A1", "A1", "A1", "A2", "A2"],
            "value": [10, 20, 30, 40, 50],
        }
    )

    SegmentSettings = collections.namedtuple(
        "SegmentSettings",
        ["start_date", "forecast_df", "segment", "trained_parameters"],
    )
    dummy_segment_settings = SegmentSettings(
        start_date=TEST_DATE_STR,
        forecast_df=forecast_df.copy(),
        segment={"a": "A1"},
        trained_parameters={"trained_parameters": "yes"},
    )

    mocker.patch.object(
        forecast, "_aggregate_forecast_observed", mock_aggregate_forecast_observed
    )

    forecast.observed_df = observed_df

    numpy_aggregations = ["mean"]
    percentiles = [10, 50, 90]
    output_df = forecast._summarize(
        segment_settings=dummy_segment_settings,
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
            "a": ["A1", "A1"],
            "value": [20, 30],
        }
    )

    # percentile numeric values changed to names
    # mean gets mapped to value
    forecast_df[["value", "value_low", "value_mid", "value_high"]] = 0

    forecast_df["a"] = "A1"  # this column is already present in observed

    forecast_df["source"] = "forecast"
    observed_expected_df["source"] = "historical"

    # concat in same order to make our lives easier
    expected = pd.concat([observed_expected_df, forecast_df])
    expected["forecast_parameters"] = '{"trained_parameters": "yes"}'
    expected["aggregation_period"] = "period"

    assert set(expected.columns) == set(output_df.columns)
    # force value columns to be floats in both cases to make check easier
    numeric_cols = ["value", "value_low", "value_mid", "value_high"]
    expected[numeric_cols] = expected[numeric_cols].astype(float)
    output_df[numeric_cols] = output_df[numeric_cols].astype(float)
    pd.testing.assert_frame_equal(
        output_df.reset_index(drop=True),
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
                TEST_DATE - relativedelta(months=1),
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
            "a": ["A1", "A1", "A1", "A2", "A2"],
            "value": [10, 20, 30, 40, 50],
        }
    )

    SegmentSettings = collections.namedtuple(
        "SegmentSettings",
        ["start_date", "forecast_df", "segment", "trained_parameters", "components_df"],
    )

    # for the components_df the contents aren't important here
    # we're only testing that it is concatenated properly
    # with the segment data added
    dummy_segment_settings_A1 = SegmentSettings(
        start_date=TEST_DATE_STR,
        forecast_df=forecast_df.copy(),
        segment={"a": "A1"},
        trained_parameters={"trained_parameters": "yes"},
        components_df=pd.DataFrame({"testcol": [1]}),
    )

    dummy_segment_settings_A2 = SegmentSettings(
        start_date=TEST_DATE_STR,
        forecast_df=forecast_df.copy(),
        segment={"a": "A2"},
        trained_parameters={"trained_parameters": "yes"},
        components_df=pd.DataFrame({"testcol": [2]}),
    )

    segment_models = [dummy_segment_settings_A1, dummy_segment_settings_A2]

    mocker.patch.object(
        forecast, "_aggregate_forecast_observed", mock_aggregate_forecast_observed
    )

    forecast.observed_df = observed_df
    forecast.segment_models = segment_models
    forecast.metric_hub = dummy_metric_hub

    #  timestamp attributes created by fit and predict
    # must be added manuall
    forecast.collected_at = ""
    forecast.trained_at = ""
    forecast.predicted_at = ""

    numpy_aggregations = ["mean"]
    percentiles = [10, 50, 90]
    forecast.summarize(
        periods=["period"],
        numpy_aggregations=numpy_aggregations,
        percentiles=percentiles,
    )

    output_df = forecast.summary_df

    # time filter removes first element of observed_df
    observed_expected_df = pd.DataFrame(
        {
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
            "a": ["A1", "A1", "A2", "A2"],
            "value": [20, 30, 40, 50],
        }
    )

    # doubled because there are two segments in the observed data
    forecast_df = pd.concat([forecast_df, forecast_df])

    forecast_df[["value", "value_low", "value_mid", "value_high"]] = 0
    forecast_df["source"] = "forecast"

    # segment data column is already present in observed
    # needs to be added manually for forecast
    forecast_df["a"] = [
        "A1",
        "A1",
        "A2",
        "A2",
    ]

    observed_expected_df["source"] = "historical"

    # concat in same order to make our lives easier
    expected = pd.concat([observed_expected_df, forecast_df])
    expected["forecast_parameters"] = '{"trained_parameters": "yes"}'
    expected["aggregation_period"] = "period"

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
    }
    assert set(expected.columns) | metadata_columns == set(output_df.columns)
    # force value columns to be floats in both cases to make check easier
    numeric_cols = ["value", "value_low", "value_mid", "value_high"]
    expected[numeric_cols] = expected[numeric_cols].astype(float)
    output_df[numeric_cols] = output_df[numeric_cols].astype(float)
    pd.testing.assert_frame_equal(
        output_df.sort_values(["a", "submission_date"])[expected.columns].reset_index(
            drop=True
        ),
        expected.sort_values(["a", "submission_date"]).reset_index(drop=True),
    )

    # should not be any nulls outside the metric column
    non_metric_columns = [el for el in output_df.columns if el not in numeric_cols]
    assert not pd.isna(output_df[non_metric_columns]).any(axis=None)

    # check components
    # only checking that concatenation happened properly
    # with segment data added
    output_components = forecast.components_df
    expected_components = pd.DataFrame({"testcol": [1, 2], "a": ["A1", "A2"]})
    pd.testing.assert_frame_equal(expected_components, output_components)


def test_under_predict(mocker):
    """testing _predict"""
    # set segment models

    A1_start_date = TEST_DATE_STR
    parameter_list = [
        {
            "segment": {"a": "A1"},
            "start_date": A1_start_date,
            "end_date": None,
            "holidays": [],
            "regressors": [],
            "grid_parameters": {"param1": [1, 2], "param2": [20, 10]},
            "cv_settings": {},
        }
    ]

    predict_start_date = TEST_DATE_NEXT_DAY_STR
    predict_end_date = TEST_PREDICT_END_STR

    forecast = FunnelForecast(
        model_type="test",
        parameters=parameter_list,
        use_holidays=None,
        start_date=predict_start_date,
        end_date=predict_end_date,
        metric_hub=None,
    )

    # this ensures forecast is using MockModel
    mocker.patch.object(forecast, "_build_model", mock_build_model)
    # the optimization is just using the value attribute of MockModel,
    # which is the product of the parameteres passed.  The crossvalidation
    # will choose the parameters where the absolute value of the product is smallest
    mocker.patch.object(
        forecast, "_get_crossvalidation_metric", mock_get_crossvalidation_metric
    )

    observed_df = pd.DataFrame(
        {
            "a": ["A1", "A1"],
            "b": ["B1", "B2"],
            "y": [0, 1],
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
        }
    )

    segment_list = ["a"]

    # manually set segment_models attribute here instead of in __post_init__
    # which is bypassed to avoid a metric hub call
    forecast._set_segment_models(
        observed_df=observed_df, segment_column_list=segment_list
    )
    # check that we only have one element here
    assert len(forecast.segment_models) == 1
    # because of the check above we can use the first element
    # and know that's all the segments present
    segment_settings = forecast.segment_models[0]

    dates_to_predict = pd.DataFrame(
        {
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ]
        }
    )
    forecast.observed_df = observed_df
    forecast.fit()
    out = forecast._predict(dates_to_predict, segment_settings).reset_index(drop=True)

    # in MockModel, the predictive_samples method sets the output to
    # np.arange(len(dates_to_predict)) * self.value for one column called 0
    # this helps ensure the forecast_df in segment_models is set properly
    model_value = forecast.segment_models[0].segment_model.value
    expected = pd.DataFrame(
        {
            0: [0, model_value],
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
        }
    )

    # time filter corresponds to the start time of the object
    # as opposed to the segment
    expected_time_filter = (
        expected["submission_date"] >= pd.to_datetime(forecast.start_date).date()
    )
    expected = expected[expected_time_filter].reset_index(drop=True)

    pd.testing.assert_frame_equal(out, expected)

    # check the components
    expected_components = observed_df[["submission_date", "y"]].copy()
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

    components_df = forecast.segment_models[0].components_df
    assert set(expected_components.columns) == set(components_df.columns)
    pd.testing.assert_frame_equal(
        components_df, expected_components[components_df.columns]
    )


def test_predict(funnel_forecast_for_fit_tests, segment_info_fit_tests):
    """test the predict method.  This is similar to test_under_predict
    but multiple segments are acted upon"""

    observed_data = pd.DataFrame(
        {
            "a": ["A1", "A1", "A2", "A2"],
            "b": ["B1", "B2", "B1", "B2"],
            "y": [-1, 1, -1, 1],
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
        }
    )

    segment_list = ["a"]

    funnel_forecast_for_fit_tests._set_segment_models(
        observed_df=observed_data, segment_column_list=segment_list
    )
    funnel_forecast_for_fit_tests.observed_df = observed_data
    funnel_forecast_for_fit_tests.fit()
    funnel_forecast_for_fit_tests.predict()

    for segment in funnel_forecast_for_fit_tests.segment_models:
        key = segment.segment["a"]

        model_value = segment_info_fit_tests[key]["min_param_value"]

        # in MockModel, the predictive_samples method sets the output to
        # np.arange(len(dates_to_predict)) * self.value for one column called 0
        # this helps ensure the forecast_df in segment_models is set properly
        expected_raw = pd.DataFrame(
            {
                0: [0, model_value],
                "submission_date": [
                    TEST_DATE,
                    TEST_DATE_NEXT_DAY,
                ],
            }
        )

        # filter in predict happens against object start_date not
        # segment start_date
        expected_time_filter = (
            expected_raw["submission_date"]
            >= pd.to_datetime(funnel_forecast_for_fit_tests.start_date).date()
        )
        expected = expected_raw[expected_time_filter].reset_index(drop=True)

        forecast_df = segment.forecast_df
        pd.testing.assert_frame_equal(forecast_df, expected)

        # check the components
        expected_components = expected_raw[["submission_date"]].copy()
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

        # because of time filtereing of training data, if the history has one
        # element, y will but [0, 1].  The first element is turned into a NULL
        # and then becomes a 0 because of fillna(0)
        # if it has two it will have both elements and be [-1,1]

        if len(segment.segment_model.history) == 2:
            expected_components["y"] = [-1, 1]
        else:
            expected_components["y"] = [0, 1]

        components_df = segment.components_df

        # there is weird stuff going on with the types but it shouldn't matter
        # so coerce the type
        expected_components["y"] = expected_components["y"].astype(
            components_df["y"].dtype
        )
        assert set(expected_components.columns) == set(components_df.columns)
        pd.testing.assert_frame_equal(
            components_df,
            expected_components[components_df.columns],
            check_column_type=False,
        )


def test_auto_tuning(forecast, mocker):
    """test the auto_tuning function"""

    # set one segment with two sets of grid parameters
    segment_settings = SegmentModelSettings(
        segment={"a": "A1"},
        start_date=TEST_DATE_STR,
        end_date=TEST_PREDICT_END_STR,
        holidays=[],
        regressors=[],
        grid_parameters={"param1": [1, 2], "param2": [20, 10]},
        cv_settings={},
    )

    mocker.patch.object(forecast, "_build_model", mock_build_model)

    # mock_get_crossvalidation_metric will choose the parameters that
    # have the lowest absolute product
    mocker.patch.object(
        forecast, "_get_crossvalidation_metric", mock_get_crossvalidation_metric
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

    forecast.segment_models = [segment_settings]

    best_params = forecast._auto_tuning(observed_df, segment_settings)

    # in the mocked class the two params get multiplied and the lowest combo gets select
    assert best_params == {"param1": 1, "param2": 10}


def test_under_fit(funnel_forecast_for_fit_tests, segment_info_fit_tests):
    """test the _fit method"""

    observed_data = pd.DataFrame(
        {
            "a": ["A1", "A1", "A2", "A2"],
            "b": ["B1", "B2", "B1", "B2"],
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
        }
    )

    segment_list = ["a"]

    funnel_forecast_for_fit_tests._set_segment_models(
        observed_df=observed_data, segment_column_list=segment_list
    )
    funnel_forecast_for_fit_tests._fit(observed_data)

    # _fit iterates though all the segments in segment_modles
    # iterate through them and check based on the value in
    # segment_info_fit_tests defined in the fixture of the same name
    for segment in funnel_forecast_for_fit_tests.segment_models:
        key = segment.segment["a"]

        assert segment.start_date == segment_info_fit_tests[key]["start_date"]
        assert segment.grid_parameters == segment_info_fit_tests[key]["grid_parameters"]
        segment_model = segment.segment_model
        assert segment_model.value == segment_info_fit_tests[key]["min_param_value"]

        # the history attribute is used in the components output so check it is set properly
        expected_training = observed_data[
            (observed_data["a"] == key)
            & (
                observed_data["submission_date"]
                >= pd.to_datetime(segment_info_fit_tests[key]["start_date"]).date()
            )
        ].rename(columns={"submission_date": "ds"})

        pd.testing.assert_frame_equal(segment_model.history, expected_training)


def test_fit(funnel_forecast_for_fit_tests, segment_info_fit_tests):
    """test the fit function.  It is inherited from BaseForecast
    and calls _fit with the proper object attributes.  Test looks very
    similar to that for _fit"""
    observed_data = pd.DataFrame(
        {
            "a": ["A1", "A1", "A2", "A2"],
            "b": ["B1", "B2", "B1", "B2"],
            "submission_date": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
        }
    )

    segment_list = ["a"]

    funnel_forecast_for_fit_tests._set_segment_models(
        observed_df=observed_data, segment_column_list=segment_list
    )
    funnel_forecast_for_fit_tests.observed_df = observed_data
    funnel_forecast_for_fit_tests.fit()

    # _fit is called by fit and iterates though all the segments in segment_modles
    # iterate through them and check based on the value in
    # segment_info_fit_tests defined in the fixture of the same name
    for segment in funnel_forecast_for_fit_tests.segment_models:
        key = segment.segment["a"]

        assert segment.start_date == segment_info_fit_tests[key]["start_date"]
        assert segment.grid_parameters == segment_info_fit_tests[key]["grid_parameters"]
        segment_model = segment.segment_model
        assert segment_model.value == segment_info_fit_tests[key]["min_param_value"]

        # check history attribute
        expected_training = observed_data[
            (observed_data["a"] == key)
            & (
                observed_data["submission_date"]
                >= pd.to_datetime(segment_info_fit_tests[key]["start_date"]).date()
            )
        ].rename(columns={"submission_date": "ds"})
        pd.testing.assert_frame_equal(segment_model.history, expected_training)


def test_set_segment_models():
    """test the set_segment_models method"""
    A1_start_date = "2018-01-01"
    A2_start_date = "2020-02-02"
    parameter_list = [
        {
            "segment": {"a": "A1"},
            "start_date": A1_start_date,
            "end_date": None,
            "holidays": [],
            "regressors": [],
            "grid_parameters": {},
            "cv_settings": {},
        },
        {
            "segment": {"a": "A2"},
            "start_date": A2_start_date,
            "end_date": None,
            "holidays": [],
            "regressors": [],
            "grid_parameters": {},
            "cv_settings": {},
        },
    ]

    predict_start_date = TEST_DATE_STR
    predict_end_date = TEST_PREDICT_END_STR

    forecast = FunnelForecast(
        model_type="test",
        parameters=parameter_list,
        use_holidays=None,
        start_date=predict_start_date,
        end_date=predict_end_date,
        metric_hub=None,
    )

    observed_data = pd.DataFrame(
        {"a": ["A1", "A1", "A2", "A2", "A2"], "b": ["B1", "B2", "B1", "B2", "B2"]}
    )

    segment_list = ["a", "b"]

    forecast._set_segment_models(
        observed_df=observed_data, segment_column_list=segment_list
    )

    # put the segments and the start date in the same dictionary to make
    # comparison easier
    # the important things to check is that all possible combinations
    # of segments are present and that each has the parameters set properly
    # start_date is a stand-in for these parameters and
    # is determined by the value of a as specified in parameter_dict
    check_segment_models = [
        dict(**el.segment, **{"start_date": el.start_date})
        for el in forecast.segment_models
    ]
    expected = [
        {"a": "A1", "b": "B1", "start_date": A1_start_date},
        {"a": "A1", "b": "B2", "start_date": A1_start_date},
        {"a": "A2", "b": "B1", "start_date": A2_start_date},
        {"a": "A2", "b": "B2", "start_date": A2_start_date},
    ]

    # can't make a set of dicts for comparison
    # so sort the lists and compare each element
    compare_sorted = zip(
        sorted(check_segment_models, key=lambda x: (x["a"], x["b"])),
        sorted(expected, key=lambda x: (x["a"], x["b"])),
    )

    for checkval, expectedval in compare_sorted:
        assert checkval == expectedval


def test_set_segment_models_exception():
    """test the exception for segment_models where
    and exception is raised if a model_setting_split_dim
    is specified that isn't in the data"""
    A1_start_date = "2018-01-01"
    A2_start_date = "2020-02-02"
    parameter_list = [
        {
            "segment": {"c": "A1"},
            "start_date": A1_start_date,
            "end_date": None,
            "holidays": [],
            "regressors": [],
            "grid_parameters": {},
            "cv_settings": {},
        },
        {
            "segment": {"c": "A2"},
            "start_date": A2_start_date,
            "end_date": None,
            "holidays": [],
            "regressors": [],
            "grid_parameters": {},
            "cv_settings": {},
        },
    ]

    predict_start_date = TEST_DATE_STR
    predict_end_date = TEST_PREDICT_END_STR

    forecast = FunnelForecast(
        model_type="test",
        parameters=parameter_list,
        use_holidays=None,
        start_date=predict_start_date,
        end_date=predict_end_date,
        metric_hub=None,
    )

    observed_data = pd.DataFrame(
        {"a": ["A1", "A1", "A2", "A2", "A2"], "b": ["B1", "B2", "B1", "B2", "B2"]}
    )

    segment_list = ["a", "b"]

    with pytest.raises(
        ValueError,
        match="Segment keys missing from metric hub segments: c",
    ):
        forecast._set_segment_models(
            observed_df=observed_data, segment_column_list=segment_list
        )


def test_fill_regressor_dates(forecast):
    """test _fill_regressor_dates
    the name in the regressor info indicates which case is being tested
    Dates are chosen arbitrarily"""
    # get the set start and end dates for the forecast fixture
    # as datetime objects
    default_start_datetime = datetime(TEST_DATE.year, TEST_DATE.month, TEST_DATE.day)
    default_end_datetime = datetime(
        TEST_PREDICT_END.year, TEST_PREDICT_END.month, TEST_PREDICT_END.day
    )

    # set the start date with an arbitrary date
    regressor_info = {
        "name": "only_start",
        "description": "only has a start",
        "start_date": "2020-08-15",
    }
    regressor = ProphetRegressor(**regressor_info)
    forecast._fill_regressor_dates(regressor)
    assert regressor.start_date == pd.to_datetime("2020-08-15")

    # this is the end dat for the forecast fixture
    assert regressor.end_date == default_end_datetime

    # set the end date with an arbitrary date
    regressor_info = {
        "name": "only_end",
        "description": "only has a end",
        "end_date": "2125-08-15",
    }
    regressor = ProphetRegressor(**regressor_info)
    forecast._fill_regressor_dates(regressor)
    # the start date for the forecast fixture is TEST_DATE
    assert regressor.start_date == default_start_datetime
    assert regressor.end_date == pd.to_datetime("2125-08-15")

    # set both the start and end dates to arbitrary dates
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

    # use the defaults for both
    regressor_info = {
        "name": "neither",
        "description": "nothin to see here",
    }
    regressor = ProphetRegressor(**regressor_info)
    forecast._fill_regressor_dates(regressor)
    assert regressor.start_date == default_start_datetime
    assert regressor.end_date == default_end_datetime

    # use arbitrary out of order dates to set
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
            "all_in": [1, 1, 1, 1],
            "all_out": [0, 0, 0, 0],
            "just_end": [0, 0, 1, 1],
            "just_middle": [0, 1, 1, 0],
        }
    )

    assert set(output_df.columns) == set(expected_df.columns)
    pd.testing.assert_frame_equal(output_df, expected_df[output_df.columns])


def test_build_train_dataframe_no_regressors(forecast):
    """test _build_train_dataframe with no regressors"""
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
        start_date=TEST_DATE_STR,
        end_date=TEST_PREDICT_END_STR,
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
                TEST_DATE - relativedelta(months=1),
                TEST_DATE_NEXT_DAY - relativedelta(months=1),
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
                TEST_DATE + relativedelta(months=1),
                TEST_DATE_NEXT_DAY + relativedelta(months=1),
            ],
        }
    )

    output_train_df = forecast._build_train_dataframe(
        observed_df, segment_settings=segment_settings
    )
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
    output_train_wlog_df = forecast._build_train_dataframe(
        observed_df, segment_settings=segment_settings, add_logistic_growth_cols=True
    )
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
        start_date=TEST_DATE_STR,
        end_date=(TEST_DATE + relativedelta(months=1)).strftime("%Y-%m-%d"),
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
                TEST_DATE - relativedelta(months=1),
                TEST_DATE_NEXT_DAY - relativedelta(months=1),
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
                TEST_DATE + relativedelta(months=1),
                TEST_DATE_NEXT_DAY + relativedelta(months=1),
            ],
        }
    )
    output_train_df = forecast._build_train_dataframe(
        observed_df, segment_settings=segment_settings
    )
    expected_train_df = pd.DataFrame(
        {
            "a": [1, 1],
            "b": [2, 2],
            "y": [3, 4],
            "ds": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
            "all_in": [1, 1],
            "all_out": [0, 0],
            "just_end": [0, 1],
        }
    )
    pd.testing.assert_frame_equal(
        output_train_df.reset_index(drop=True), expected_train_df
    )

    output_train_wlog_df = forecast._build_train_dataframe(
        observed_df, segment_settings=segment_settings, add_logistic_growth_cols=True
    )
    expected_train_wlog_df = pd.DataFrame(
        {
            "a": [1, 1],
            "b": [2, 2],
            "y": [3, 4],
            "ds": [
                TEST_DATE,
                TEST_DATE_NEXT_DAY,
            ],
            "all_in": [1, 1],
            "all_out": [0, 0],
            "just_end": [0, 1],
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
        start_date=TEST_DATE_STR,
        end_date=TEST_PREDICT_END_STR,
        holidays=[],
        regressors=[ProphetRegressor(**r) for r in regressor_list],
        grid_parameters=grid_parameters,
        cv_settings=cv_settings,
    )

    # manually set trained_parameters, normally this would happen during training
    segment_settings.trained_parameters = {"floor": -1.0, "cap": 10.0}

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

    output_predict_df = forecast._build_predict_dataframe(
        dates_to_predict, segment_settings=segment_settings
    )
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
    output_predict_wlog_df = forecast._build_predict_dataframe(
        dates_to_predict,
        segment_settings=segment_settings,
        add_logistic_growth_cols=True,
    )
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
        start_date=TEST_DATE_STR,
        end_date=TEST_PREDICT_END_STR,
        holidays=[],
        regressors=[ProphetRegressor(**r) for r in regressor_list],
        grid_parameters=grid_parameters,
        cv_settings=cv_settings,
    )

    # set training_parameters, which is usually done in the fit method
    segment_settings.trained_parameters = {"floor": -1.0, "cap": 10.0}

    dates_to_predict = pd.DataFrame(
        {
            "submission_date": [TEST_DATE, TEST_DATE_NEXT_DAY],
        }
    )

    output_train_df = forecast._build_predict_dataframe(
        dates_to_predict,
        segment_settings=segment_settings,
    )
    expected_train_df = pd.DataFrame(
        {
            "ds": [TEST_DATE, TEST_DATE_NEXT_DAY],
            "all_in": [1, 1],
            "all_out": [0, 0],
            "just_end": [0, 1],
        }
    )
    pd.testing.assert_frame_equal(
        output_train_df.reset_index(drop=True), expected_train_df
    )

    # test again but with add_logistic_growth_cols set to true
    output_train_wlog_df = forecast._build_predict_dataframe(
        dates_to_predict,
        segment_settings=segment_settings,
        add_logistic_growth_cols=True,
    )
    expected_train_wlog_df = pd.DataFrame(
        {
            "ds": [TEST_DATE, TEST_DATE_NEXT_DAY],
            "all_in": [1, 1],
            "all_out": [0, 0],
            "just_end": [0, 1],
            "floor": [-1.0, -1.0],
            "cap": [10.0, 10.0],
        }
    )

    assert set(output_train_wlog_df.columns) == set(expected_train_wlog_df.columns)
    pd.testing.assert_frame_equal(
        output_train_wlog_df.reset_index(drop=True),
        expected_train_wlog_df[output_train_wlog_df.columns],
    )


def test_build_model(forecast):
    """test build_model
    just runs the function and ensures no error is raised"""
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
    segment_settings = SegmentModelSettings(
        segment={"a": 1, "b": 2},
        start_date=TEST_DATE_STR,
        end_date=TEST_PREDICT_END_STR,
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
