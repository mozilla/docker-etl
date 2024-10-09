from datetime import date
from dateutil.relativedelta import relativedelta
from dataclasses import dataclass


import pytest
import pandas as pd
import numpy as np


from kpi_forecasting.models.base_forecast import BaseForecast, BaseEnsembleForecast

# Arbitrarily choose some date to use for the tests
TEST_DATE = date(2024, 1, 1)
TEST_DATE_STR = TEST_DATE.strftime("%Y-%m-%d")
TEST_DATE_NEXT_DAY = date(2024, 1, 2)
TEST_DATE_NEXT_DAY_STR = TEST_DATE_NEXT_DAY.strftime("%Y-%m-%d")
TEST_PREDICT_END = TEST_DATE + relativedelta(months=2)
TEST_PREDICT_END_STR = TEST_PREDICT_END.strftime("%Y-%m-%d")
TEST_OBSERVED_START = date(2023, 1, 1)


class BadClass(BaseForecast):
    pass


@pytest.fixture()
def good_class():
    class GoodModel:
        def __init__(self, id, factor):
            self.id = id
            self.is_fit = False
            self.factor = factor

        def fit(self, observed_data):
            self.is_fit = min(observed_data["submission_date"])

        def predict(self, forecast_data):
            forecast_data = forecast_data.copy()
            start_at = 2 - len(forecast_data)
            forecast_data["value"] = np.array([1, 2])[start_at:] * self.factor
            return forecast_data

    @dataclass
    class GoodClass(BaseForecast):
        id: str = None
        seed_set: bool = False
        factor: int = 1

        # overwrite _get_observed_data
        def _set_seed(self):
            self.seed_set = True
            return

        def fit(self, observed_df: pd.DataFrame) -> None:
            # takes array as input to simplify tests
            self.model = GoodModel(self.id, self.factor)
            self.model.fit(observed_df)

        def predict(self, dates_to_predict: pd.DataFrame) -> pd.DataFrame:
            # takes array as input to simplify tests
            return self.model.predict(dates_to_predict)

        def _validate_forecast_df(self, forecast_df: pd.DataFrame) -> None:
            # takes array as input to simplify tests
            # check that all are even after _predict runs
            assert np.all(forecast_df % 2 == 0)

        def _get_parameters(self):
            return {"id": self.id, "factor": self.factor}

    return GoodClass


def test_forecast_not_implemented():
    with pytest.raises(
        TypeError,
        match="Can't instantiate abstract class BadClass with abstract methods _set_seed, _validate_forecast_df, fit, predict",
    ):
        _ = BadClass()


def test_fit(good_class):
    """test the fit method, and implicitly the set_segment_models method"""
    A1_start_date = "2018-01-01"
    A2_start_date = "2020-02-02"
    parameter_list = [
        {"segment": {"a": "A1"}, "parameters": {"id": "This is A1"}},
        {"segment": {"a": "A2"}, "parameters": {"id": "This is A2"}},
    ]

    EnsembleObject = BaseEnsembleForecast(
        model_class=good_class, parameters=parameter_list, segments=["a", "b"]
    )

    observed_data = pd.DataFrame(
        {
            "a": ["A1", "A1", "A2", "A2", "A2"],
            "b": ["B1", "B2", "B1", "B2", "B2"],
            "submission_date": [
                A1_start_date,
                A1_start_date,
                A2_start_date,
                A2_start_date,
                A2_start_date,
            ],
        }
    )

    EnsembleObject.fit(observed_data)

    segment_models = EnsembleObject.segment_models

    # put the segments and the start date in the same dictionary to make
    # comparison easier
    # the important things to check is that all possible combinations
    # of segments are present and that each has the parameters set properly
    # start_date is a stand-in for these parameters and
    # is determined by the value of a as specified in parameter_dict
    check_segment_models = [
        dict(**el["segment"], **{"id": el["model"].id}) for el in segment_models
    ]

    expected = [
        {"a": "A1", "b": "B1", "id": "This is A1"},
        {"a": "A1", "b": "B2", "id": "This is A1"},
        {"a": "A2", "b": "B1", "id": "This is A2"},
        {"a": "A2", "b": "B2", "id": "This is A2"},
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
    assert all([el["model"].seed_set for el in segment_models])

    # test that the fit was applied properly to all models
    # to do this check the is_fit attribute, which will equal
    # A1_start_date for A1 segments and A2_start_date for A2 segments

    for segment in segment_models:
        if segment["segment"]["a"] == "A1":
            assert segment["model"].model.is_fit == A1_start_date
        else:
            assert segment["model"].model.is_fit == A2_start_date


def test_fit_multiple(good_class):
    """test the fit method
    with segments on multiple columns.
    Implicitly testing set_segment_models with multiple
    segments as well"""
    # set arbitrary dates
    # they're only used to make sure segments are set correctly
    A1B1_start_date = "2018-01-01"
    A1B2_start_date = "2019-01-01"
    A2B1_start_date = "2020-02-02"
    A2B2_start_date = "2021-02-02"
    parameter_list = [
        {
            "segment": {"a": "A1", "b": "B1"},
            "parameters": {"id": "This is A1B1"},
        },
        {
            "segment": {"a": "A1", "b": "B2"},
            "parameters": {"id": "This is A1B2"},
        },
        {
            "segment": {"a": "A2", "b": "B1"},
            "parameters": {"id": "This is A2B1"},
        },
        {
            "segment": {"a": "A2", "b": "B2"},
            "parameters": {"id": "This is A2B2"},
        },
    ]

    EnsembleObject = BaseEnsembleForecast(
        model_class=good_class, parameters=parameter_list, segments=["a", "b"]
    )

    observed_data = pd.DataFrame(
        {
            "a": ["A1", "A1", "A2", "A2", "A2"],
            "b": ["B1", "B2", "B1", "B2", "B2"],
            "submission_date": [
                A1B1_start_date,
                A1B2_start_date,
                A2B1_start_date,
                A2B2_start_date,
                A2B2_start_date,
            ],
        }
    )

    EnsembleObject.fit(observed_data)

    segment_models = EnsembleObject.segment_models

    # put the segments and the start date in the same dictionary to make
    # comparison easier
    # the important things to check is that all possible combinations
    # of segments are present and that each has the parameters set properly
    # start_date is a stand-in for these parameters and
    # is determined by the value of a as specified in parameter_dict
    check_segment_models = [
        dict(**el["segment"], **{"id": el["model"].id}) for el in segment_models
    ]
    expected = [
        {"a": "A1", "b": "B1", "id": "This is A1B1"},
        {"a": "A1", "b": "B2", "id": "This is A1B2"},
        {"a": "A2", "b": "B1", "id": "This is A2B1"},
        {"a": "A2", "b": "B2", "id": "This is A2B2"},
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
    assert all([el["model"].seed_set for el in segment_models])

    # test that the fit was applied properly to all models
    # to do this check the is_fit attribute, which will equal
    # A1_start_date for A1 segments and A2_start_date for A2 segments

    for segment in segment_models:
        if segment["segment"]["a"] == "A1" and segment["segment"]["b"] == "B1":
            assert segment["model"].model.is_fit == A1B1_start_date
        elif segment["segment"]["a"] == "A1" and segment["segment"]["b"] == "B2":
            assert segment["model"].model.is_fit == A1B2_start_date
        elif segment["segment"]["a"] == "A2" and segment["segment"]["b"] == "B1":
            assert segment["model"].model.is_fit == A2B1_start_date
        else:
            assert segment["model"].model.is_fit == A2B2_start_date


def test_fit_multiple_with_start(good_class):
    """test the fit method
    with segments on multiple columns.
    Implicitly testing set_segment_models with multiple
    segments as well"""
    parameter_list = [
        {
            "segment": {"a": "A1", "b": "B1"},
            "parameters": {"id": "This is A1B1"},
        },
        {
            "segment": {"a": "A1", "b": "B2"},
            "parameters": {"id": "This is A1B2"},
            "start_date": TEST_DATE_NEXT_DAY_STR,
        },
        {
            "segment": {"a": "A2", "b": "B1"},
            "parameters": {"id": "This is A2B1"},
        },
        {
            "segment": {"a": "A2", "b": "B2"},
            "parameters": {"id": "This is A2B2"},
            "start_date": TEST_DATE_NEXT_DAY_STR,
        },
    ]

    EnsembleObject = BaseEnsembleForecast(
        model_class=good_class, parameters=parameter_list, segments=["a", "b"]
    )

    # every segment has two days, TEST_DATE and TEST_DATE_NEXT_DAY
    observed_data = pd.DataFrame(
        [
            {"a": "A1", "b": "B1", "submission_date": TEST_DATE},
            {"a": "A1", "b": "B1", "submission_date": TEST_DATE_NEXT_DAY},
            {"a": "A1", "b": "B2", "submission_date": TEST_DATE},
            {"a": "A1", "b": "B2", "submission_date": TEST_DATE_NEXT_DAY},
            {"a": "A2", "b": "B1", "submission_date": TEST_DATE},
            {"a": "A2", "b": "B1", "submission_date": TEST_DATE_NEXT_DAY},
            {"a": "A2", "b": "B2", "submission_date": TEST_DATE},
            {"a": "A2", "b": "B2", "submission_date": TEST_DATE_NEXT_DAY},
        ]
    )

    EnsembleObject.fit(observed_data)

    segment_models = EnsembleObject.segment_models

    # put the segments and the start date in the same dictionary to make
    # comparison easier
    # the important things to check is that all possible combinations
    # of segments are present and that each has the parameters set properly
    # start_date is a stand-in for these parameters and
    # is determined by the value of a as specified in parameter_dict
    check_segment_models = [
        dict(**el["segment"], **{"id": el["model"].id}) for el in segment_models
    ]
    expected = [
        {"a": "A1", "b": "B1", "id": "This is A1B1"},
        {"a": "A1", "b": "B2", "id": "This is A1B2"},
        {"a": "A2", "b": "B1", "id": "This is A2B1"},
        {"a": "A2", "b": "B2", "id": "This is A2B2"},
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
    assert all([el["model"].seed_set for el in segment_models])

    # test that the fit was applied properly to the time-filtered data
    # to do this check the is_fit attribute, which will equal
    # the earliest date.  For B1 it is TEST_DATE
    # B2 has start_date set to TEST_DATE_NEXT_DAY, so it will have that value

    for segment in segment_models:
        if segment["segment"]["b"] == "B1":
            assert segment["model"].model.is_fit == TEST_DATE
        else:
            assert segment["model"].model.is_fit == TEST_DATE_NEXT_DAY


def test_set_segment_models_exception(mocker):
    """test the exception for segment_models where
    and exception is raised if a model_setting_split_dim
    is specified that isn't in the data"""
    A1_start_date = "2018-01-01"
    A2_start_date = "2020-02-02"
    parameter_list = [
        {"segment": {"c": "A1"}, "parameters": {"id": "This is A1"}},
        {"segment": {"c": "A2"}, "parameters": {"id": "This is A2"}},
    ]
    EnsembleObject = BaseEnsembleForecast(
        model_class=good_class, parameters=parameter_list, segments=["a", "b"]
    )

    observed_data = pd.DataFrame(
        {
            "a": ["A1", "A1", "A2", "A2", "A2"],
            "b": ["B1", "B2", "B1", "B2", "B2"],
            "submission_date": [
                A1_start_date,
                A1_start_date,
                A2_start_date,
                A2_start_date,
                A2_start_date,
            ],
        }
    )

    with pytest.raises(
        ValueError,
        match="Segment keys missing from metric hub segments: c",
    ):
        EnsembleObject.fit(observed_data)


def test_predict(good_class):
    """test the predict"""
    parameter_list = [
        {
            "segment": {"a": "A1", "b": "B1"},
            "parameters": {"id": "This is A1B1", "factor": 4},
        },
        {
            "segment": {"a": "A1", "b": "B2"},
            "parameters": {"id": "This is A1B2", "factor": 6},
        },
        {
            "segment": {"a": "A2", "b": "B1"},
            "parameters": {"id": "This is A2B1", "factor": 8},
        },
        {
            "segment": {"a": "A2", "b": "B2"},
            "parameters": {"id": "This is A2B2", "factor": 10},
        },
    ]

    EnsembleObject = BaseEnsembleForecast(
        model_class=good_class, parameters=parameter_list, segments=["a", "b"]
    )

    # submission date doesn't matter here
    observed_data = pd.DataFrame(
        {
            "a": ["A1", "A1", "A2", "A2", "A2"],
            "b": ["B1", "B2", "B1", "B2", "B2"],
            "submission_date": [
                TEST_DATE_NEXT_DAY,
                TEST_DATE_NEXT_DAY,
                TEST_DATE_NEXT_DAY,
                TEST_DATE_NEXT_DAY,
                TEST_DATE_NEXT_DAY,
            ],
        }
    )

    EnsembleObject.fit(observed_data)

    # pass submission_date as a float for the purpose of testing
    # this is fine because no time filtering happens in the predict of
    # BaseEnsembleForecast or the dummy class and model
    predict_df = pd.DataFrame({"submission_date": [TEST_DATE, TEST_DATE_NEXT_DAY]})
    output_df = EnsembleObject.predict(predict_df)

    expected_df = pd.DataFrame(
        [
            {"a": "A1", "b": "B1", "value": 1 * 4, "submission_date": TEST_DATE},
            {
                "a": "A1",
                "b": "B1",
                "value": 2 * 4,
                "submission_date": TEST_DATE_NEXT_DAY,
            },
            {"a": "A1", "b": "B2", "value": 1 * 6, "submission_date": TEST_DATE},
            {
                "a": "A1",
                "b": "B2",
                "value": 2 * 6,
                "submission_date": TEST_DATE_NEXT_DAY,
            },
            {"a": "A2", "b": "B1", "value": 1 * 8, "submission_date": TEST_DATE},
            {
                "a": "A2",
                "b": "B1",
                "value": 2 * 8,
                "submission_date": TEST_DATE_NEXT_DAY,
            },
            {"a": "A2", "b": "B2", "value": 1 * 10, "submission_date": TEST_DATE},
            {
                "a": "A2",
                "b": "B2",
                "value": 2 * 10,
                "submission_date": TEST_DATE_NEXT_DAY,
            },
        ]
    )

    pd.testing.assert_frame_equal(
        output_df[["a", "b", "value", "submission_date"]].reset_index(drop=True),
        expected_df,
    )


def test_predict_with_start(good_class):
    """test the predict"""
    # set B2 parameters to filter out TEST_DATE
    parameter_list = [
        {
            "segment": {"a": "A1", "b": "B1"},
            "parameters": {"id": "This is A1B1", "factor": 4},
        },
        {
            "segment": {"a": "A1", "b": "B2"},
            "parameters": {
                "id": "This is A1B2",
                "factor": 6,
            },
            "start_date": TEST_DATE_NEXT_DAY_STR,
        },
        {
            "segment": {"a": "A2", "b": "B1"},
            "parameters": {"id": "This is A2B1", "factor": 8},
        },
        {
            "segment": {"a": "A2", "b": "B2"},
            "parameters": {"id": "This is A2B2", "factor": 10},
            "start_date": TEST_DATE_NEXT_DAY_STR,
        },
    ]

    EnsembleObject = BaseEnsembleForecast(
        model_class=good_class, parameters=parameter_list, segments=["a", "b"]
    )

    observed_data = pd.DataFrame(
        {
            "a": ["A1", "A1", "A2", "A2", "A2"],
            "b": ["B1", "B2", "B1", "B2", "B2"],
            "submission_date": [
                TEST_DATE_NEXT_DAY,
                TEST_DATE_NEXT_DAY,
                TEST_DATE_NEXT_DAY,
                TEST_DATE_NEXT_DAY,
                TEST_DATE_NEXT_DAY,
            ],
        }
    )

    EnsembleObject.fit(observed_data)

    # pass submission_date as a float for the purpose of testing
    # this is fine because no time filtering happens in the predict of
    # BaseEnsembleForecast or the dummy class and model
    predict_df = pd.DataFrame({"submission_date": [TEST_DATE, TEST_DATE_NEXT_DAY]})
    output_df = EnsembleObject.predict(predict_df)

    expected_df = pd.DataFrame(
        [
            {"a": "A1", "b": "B1", "value": 1 * 4, "submission_date": TEST_DATE},
            {
                "a": "A1",
                "b": "B1",
                "value": 2 * 4,
                "submission_date": TEST_DATE_NEXT_DAY,
            },
            {
                "a": "A1",
                "b": "B2",
                "value": 2 * 6,
                "submission_date": TEST_DATE_NEXT_DAY,
            },
            {"a": "A2", "b": "B1", "value": 1 * 8, "submission_date": TEST_DATE},
            {
                "a": "A2",
                "b": "B1",
                "value": 2 * 8,
                "submission_date": TEST_DATE_NEXT_DAY,
            },
            {
                "a": "A2",
                "b": "B2",
                "value": 2 * 10,
                "submission_date": TEST_DATE_NEXT_DAY,
            },
        ]
    )
    pd.testing.assert_frame_equal(
        output_df[["a", "b", "value", "submission_date"]].reset_index(drop=True),
        expected_df,
    )
