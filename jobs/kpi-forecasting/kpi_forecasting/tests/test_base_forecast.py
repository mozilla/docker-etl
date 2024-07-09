import pytest

from kpi_forecasting.models.base_forecast import BaseForecast


class BadClass(BaseForecast):
    pass


def test_fit_not_implemented():
    with pytest.raises(
        TypeError,
        match="Can't instantiate abstract class BadClass with abstract methods _fit, _predict, _summarize",
    ):
        _ = BadClass()
