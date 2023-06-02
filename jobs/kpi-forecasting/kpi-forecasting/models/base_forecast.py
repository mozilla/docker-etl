import numpy as np
import pandas as pd
import pandas_extras as pdx
import uuid

from dataclasses import dataclass
from datetime import datetime, timedelta
from metric_hub import MetricHub
from typing import Dict, List, Tuple


@dataclass
class BaseForecast:
    """
    A base class for fitting, forecasting, and aggregating. This class should not
    be invoked directly; it should be inherited by a child class. The child class
    needs to implement `_fit`, `_forecast`, and `_aggregate` methods in order to
    work.

    Args:
        model_type (str): The name of the forecasting model that's being used.
        parameters (Dict): Parameters that should be passed to the forecasting model.
        use_holidays (bool): Whether or not the forecasting model should use holidays.
            The base model does not apply holiday logic; that logic needs to be built
            in the child class.
        start_date (str): A 'YYYY-MM-DD' formatted-string that specifies the first
            date that should be forecsted.
        end_date (str): A 'YYYY-MM-DD' formatted-string that specifies the last
            date the metric should be queried.
        aggregates (List): A list of time periods that the forecast output should
            aggregated over. For example, "week", "month", etc. The forecast always
            aggregates over "day". This is important to consider because some of the
            model outputs (e.g. posterior distributions of confidence intervalas) are
            not saved and cannot be recreated outside of the forecasting context.
        metric_hub (MetricHub): A MetricHub object that provides details about the
            metric to be forecasted.
    """

    model_type: str
    parameters: Dict
    use_holidays: bool
    start_date: str
    end_date: str
    metric_hub: MetricHub

    def __post_init__(self) -> None:
        # fetch observed observed data
        self.collected_at = datetime.utcnow()
        self.observed_df = self.metric_hub.fetch()

        # use default start/end dates if the user doesn't specify them
        self.start_date = self.start_date or self._default_start_date
        self.end_date = self.end_date or self._default_end_date

        # model-specific attributes
        self.model = None
        self.forecast_df = None

        # metadata
        self.model_id = str(uuid.uuid4())
        self.metric_id = str(uuid.uuid4())
        self.prediction_id = str(uuid.uuid4())

    def _fit(self) -> None:
        """
        Fit a forecasting model using `self.observed_df` that was generated using
        Metric Hub data. This method should update `self.model`.
        """
        raise NotImplementedError

    def _predict(self) -> None:
        """
        Forecast using `self.model`. This method should update `self.forecast_df`.
        """
        raise NotImplementedError

    def _predict_legacy(self) -> None:
        """
        Forecast using `self.model`, adhering to the legacy data format. This
        method should eventually be removed.
        """
        raise NotImplementedError

    @property
    def _default_start_date(self) -> str:
        """The first day after the last date in the observed dataset."""
        return str(self.observed_df["submission_date"].max() + timedelta(days=1))

    @property
    def _default_end_date(self) -> str:
        """64 weeks (16 months) ahead of the current UTC date."""
        return str((datetime.utcnow() + timedelta(weeks=64)).date())

    def _set_seed(self) -> None:
        """Set random seed to ensure that fits and predictions are reproducible."""
        np.random.seed(42)

    def fit(self) -> None:
        """Fit a model using historic metric data provided by `metric_hub`."""
        print(f"Fitting {self.model_type} model.", flush=True)
        self.trained_at = datetime.utcnow()
        self._set_seed()
        self._fit()

    def predict(self) -> None:
        """Generate a forecast from `start_date` to `end_date`."""
        print(f"Forecasting from {self.start_date} to {self.end_date}.", flush=True)
        self.predicted_at = datetime.utcnow()
        self._set_seed()
        self._predict()
        self._predict_legacy()

    def _summarize(self, period: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        quantiles = [5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95]

        # aggregate
        observed_summarized = pdx.aggregate_to_period(self.observed_df, period)
        forecast_agg = pdx.aggregate_to_period(self.forecast_df, period)

        # find periods of overlap between observed and forecasted data
        overlap = forecast_agg.join(
            observed_summarized, on="submission_date", how="left"
        ).fillna(0)

        # add observed data samples in any overlapping forecasted period
        forecast_agg += overlap[["value"]].values

        # calculate summary metrics for forecast samples
        forecast_summarized = forecast_agg.agg(
            [np.mean, *[pdx.quantile(i) for i in quantiles]],
            axis=1,
        )
        return observed_summarized, forecast_summarized

    def _summarize_legacy(
        self,
        period: str,
        observed_summarized,
        forecast_summarized,
    ) -> pd.DataFrame:
        forecast_summarized.rename(columns={"mean", "value"}, inplace=True)

        observed_summarized["type"] = "actual"
        forecast_summarized["type"] = "forecast"

        all_aggregated = pd.merge(
            observed_summarized,
            forecast_summarized,
            on=["submission_date", "type", "value"],
            how="outer",
        )
        all_aggregated["target"] = self.metric_hub.alias
        all_aggregated["unit"] = period
        all_aggregated["asofdate"] = self.forecast_df["submission_date"].max()

        return all_aggregated

    def summarize(
        self,
        periods: List[str],
    ) -> None:
        """Aggregate observed and forecasted data."""
        legacy_dfs = []
        observed_dfs = []
        forecast_dfs = []
        for period in periods:
            observed, forecast = self._summarize(period)
            if period != "day":
                legacy_dfs.append(self.summarize_legacy(period, observed, forecast))
            # add details


@dataclass
class BQ:
    def write_df(self, df: pd.DataFrame):
        pass

    def write_model(self, forecast: BaseForecast, forecast_parameters: Dict) -> None:
        df = pd.DataFrame(
            {
                "id": forecast.model_id,
                "trained_at": forecast.trained_at,
                "metric_id": forecast.metric_id,
                "params": forecast_parameters,
            }
        )
        self.write_df(df)

    def write_metric(self, forecast: BaseForecast, forecast_parameters: Dict) -> None:
        forecast.forecast_df
        df = pd.DataFrame({})
        self.write_df()
