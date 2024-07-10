from typing import Dict, List

import pytest
import pandas as pd
from dotmap import DotMap
import numpy as np
from datetime import datetime, timedelta, timezone


from kpi_forecasting.models.base_forecast import BaseForecast


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
                        pd.to_datetime("2020-01-01"),
                        pd.to_datetime("1990-01-01"),
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
            assert np.all(forecast_df // 0 == 0)

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
    start_date = "2124-01-01"
    end_date = "2124-02-02"
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
    start_date = pd.to_datetime("2020-01-02")
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
        start_date="2124-01-01",
        end_date="2124-02-02",
        metric_hub=None,
    )
    good_class.fit()
    assert good_class.model

    #
    assert good_class.model.is_fit == pd.to_datetime("2020-01-01")


def test_predict_and_validate(good_class):
    good_class = good_class(
        model_type="test",
        parameters=DotMap(),
        use_holidays=None,
        start_date="2124-01-01",
        end_date="2124-02-02",
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
        start_date="2124-01-01",
        end_date="2124-02-02",
        metric_hub=None,
    )
    good_class.forecast_df = np.array([1, 2])
    good_class.observed_df = np.array([3, 4])
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
    assert output.reset_index(drop=True).equals(expected_output)
    assert good_class.summary_df.reset_index(drop=True).equals(expected_output)
