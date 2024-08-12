from typing import List
import collections
from datetime import date, datetime
from dateutil.relativedelta import relativedelta

import pytest
import pandas as pd
from dotmap import DotMap
import numpy as np
from datetime import timedelta, timezone


from kpi_forecasting.models.base_forecast import BaseForecast

# Arbitrarily choose some date to use for the tests
TEST_DATE = date(2024, 1, 1)
TEST_DATE_STR = TEST_DATE.strftime("%Y-%m-%d")
TEST_DATE_NEXT_DAY = date(2024, 1, 2)
TEST_DATE_NEXT_DAY_STR = TEST_DATE_NEXT_DAY.strftime("%Y-%m-%d")
TEST_PREDICT_END = TEST_DATE + relativedelta(months=2)
TEST_PREDICT_END_STR = TEST_PREDICT_END.strftime("%Y-%m-%d")


class BadClass(BaseForecast):
    pass


@pytest.fixture()
def good_class():
    class GoodModel:
        def __init__(self):
            self.is_fit = False

        def fit(self, observed_data):
            self.is_fit = max(observed_data["submission_date"])

    class GoodClass(BaseForecast):
        # overwrite _get_observed_data
        def _get_observed_data(self):
            self.observed_df = pd.DataFrame(
                {
                    "submission_date": [
                        TEST_DATE,
                        TEST_DATE
                        - relativedelta(years=1),  # just an arbitrary date in the past
                    ]
                }
            )

        def _fit(self, observed_df: np.array) -> None:
            # takes array as input to simplify tests
            self.model = GoodModel()
            self.model.fit(observed_df)

        def _predict(self, dates_to_predict: np.array) -> pd.DataFrame:
            # takes array as input to simplify tests
            return dates_to_predict * 2

        def _validate_forecast_df(self, forecast_df: np.array) -> None:
            # takes array as input to simplify tests
            # check that all are even after _predict runs
            assert np.all(forecast_df % 2 == 0)

        def _summarize(
            self,
            forecast_df: np.array,
            observed_df: np.array,
            period: str,
            numpy_aggregations: List[str],
            percentiles: List[str],
        ) -> pd.DataFrame:
            # input types changes to simplify test
            np_func = getattr(np, numpy_aggregations[0])
            agg_val = np_func(forecast_df + observed_df)
            return pd.DataFrame(
                [{"number": agg_val, "period": period, "percentiles": percentiles[0]}]
            )

    return GoodClass


def test_not_implemented():
    with pytest.raises(
        TypeError,
        match="Can't instantiate abstract class BadClass with abstract methods _fit, _predict, _summarize, _validate_forecast_df",
    ):
        _ = BadClass()


def test_post_init(good_class):
    start_date = TEST_DATE_STR
    end_date = TEST_PREDICT_END_STR
    good_class = good_class(
        model_type="test",
        parameters=DotMap(),
        use_holidays=None,
        start_date=start_date,
        end_date=end_date,
        metric_hub=None,
    )
    dates_to_predict_expected = pd.DataFrame(
        {
            "submission_date": pd.date_range(
                pd.to_datetime(start_date), pd.to_datetime(end_date)
            ).date
        }
    )
    assert good_class.dates_to_predict.equals(dates_to_predict_expected)


def test_post_init_default_dates(good_class):
    # check default start and end time
    good_class = good_class(
        model_type="test",
        parameters=DotMap(),
        use_holidays=None,
        start_date="",
        end_date="",
        metric_hub=None,
    )
    # this is the max date of the self.observed_data['submission_date'] plus one day
    # from the object definion
    start_date = TEST_DATE_NEXT_DAY
    end_date = (
        datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(weeks=78)
    ).date()
    dates_to_predict_expected = pd.DataFrame(
        {"submission_date": pd.date_range(start_date, end_date).date}
    )
    assert good_class.dates_to_predict.equals(dates_to_predict_expected)


def test_fit(good_class):
    good_class = good_class(
        model_type="test",
        parameters=DotMap(),
        use_holidays=None,
        start_date=TEST_DATE_STR,
        end_date=TEST_PREDICT_END_STR,
        metric_hub=None,
    )
    good_class.fit()
    assert good_class.model

    # model sets is_fit to the largest day in the observed data
    assert good_class.model.is_fit == TEST_DATE


def test_predict_and_validate(good_class):
    good_class = good_class(
        model_type="test",
        parameters=DotMap(),
        use_holidays=None,
        start_date=TEST_DATE_STR,
        end_date=TEST_PREDICT_END_STR,
        metric_hub=None,
    )
    # overwrite date range set in __post_init__
    good_class.dates_to_predict = np.arange(10)
    good_class.predict()
    assert np.all(good_class.forecast_df == good_class.dates_to_predict * 2)


def test_summarize(good_class):
    good_class = good_class(
        model_type="test",
        parameters=DotMap(),
        use_holidays=None,
        start_date=TEST_DATE_STR,
        end_date=TEST_PREDICT_END_STR,
        metric_hub=None,
    )
    good_class.forecast_df = np.array([1, 2])
    good_class.observed_df = np.array([3, 4])
    MetricHub = collections.namedtuple(
        "MetricHub",
        ["alias", "app_name", "slug", "min_date", "max_date"],
    )

    dummy_metric_hub = MetricHub("", "", "", TEST_DATE_STR, TEST_DATE_STR)

    # add it here rather than in __init__ so it doesn't try to load data
    good_class.metric_hub = dummy_metric_hub
    good_class.trained_at = ""
    good_class.predicted_at = ""

    number_val = 10
    output = good_class.summarize(
        periods=["a", "b", "c"], numpy_aggregations=["sum"], percentiles=["percentiles"]
    )
    expected_output = pd.DataFrame(
        [
            {"number": number_val, "period": el, "percentiles": "percentiles"}
            for el in ["a", "b", "c"]
        ]
    )
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
    assert set(expected_output.columns) | metadata_columns == set(output.columns)

    pd.testing.assert_frame_equal(
        output[expected_output.columns].reset_index(drop=True), expected_output
    )
    pd.testing.assert_frame_equal(
        good_class.summary_df[expected_output.columns].reset_index(drop=True),
        expected_output,
    )
