from typing import Dict, List

import pytest
import pandas as pd
from dotmap import DotMap


from kpi_forecasting.models.base_forecast import BaseForecast


class BadClass(BaseForecast):
    pass


@pytest.fixture()
def good_class():
    class GoodModel:
        def __init__(self):
            self.is_fit = False

        def fit(self, observed_data):
            self.is_fit = max(observed_data)

    class GoodClass(BaseForecast):
        # overwrite _get_observed_data
        def _get_observed_data(self):
            self.observed_df = range(10)

        def _fit(self, observed_df: pd.DataFrame) -> None:
            self.model = GoodModel()
            self.model.fit(observed_df)

        def _predict(self, dates_to_predict: pd.DataFrame) -> pd.DataFrame:
            pass

        def _validate_forecast_df(self, forecast_df: pd.DataFrame) -> None:
            pass

        def _summarize(
            self,
            forecast_df: pd.DataFrame,
            observed_df: pd.DataFrame,
            period: str,
            numpy_aggregations: List[str],
            percentiles: List[int],
        ) -> pd.DataFrame:
            pass

    return GoodClass


def test_not_implemented():
    with pytest.raises(
        TypeError,
        match="Can't instantiate abstract class BadClass with abstract methods _fit, _predict, _summarize, _validate_forecast_df",
    ):
        _ = BadClass()


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
    assert good_class.model.is_fit == 9
