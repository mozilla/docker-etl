import numpy as np
import pandas as pd
import pandas_extras as pdx
import uuid

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from metric_hub import MetricHub
from pandas.api import types as pd_types
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
        self.dates_to_predict = pd.DataFrame(
            {"submission_date": pd.date_range(self.start_date, self.end_date).date}
        )

        # model-specific attributes
        self.model = None
        self.forecast_df = None
        self.number_of_simulations = 1000

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

    def _validate_forecast_df(self) -> None:
        df = self.forecast_df
        columns = df.columns
        expected_shape = (len(self.dates_to_predict), 1 + self.number_of_simulations)
        numeric_columns = df.drop(columns="submission_date").columns

        if "submission_date" not in columns:
            raise ValueError("forecast_df must contain a 'submission_date' column.")

        if df.shape != expected_shape:
            raise ValueError(
                f"Expected forecast_df to have shape {expected_shape}, but it has shape {df.shape}."
            )

        if df["submission_date"] != self.dates_to_predict:
            raise ValueError(
                "forecast_df['submission_date'] does not match dates_to_predict."
            )

        for i in numeric_columns:
            if not pd_types.is_numeric_dtype(self.forecast_df[i]):
                raise ValueError(
                    "All forecast_df columns except 'submission_date' must be numeric,"
                    f" but column {i} has type {df[i].dtypes}."
                )

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
        self._validate_forecast_df()

        self._predict_legacy()

    def _summarize_to_period(
        self,
        period: str,
        numpy_aggregations: List[str],
        quantiles: List[int],
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        # build a list of all functions that we'll summarize the data by
        aggregations = [getattr(np, i) for i in numpy_aggregations]
        aggregations.extend([pdx.quantile(i) for i in quantiles])

        # aggregate metric to the correct date period (day, month, year)
        observed_summarized = pdx.aggregate_to_period(self.observed_df, period)
        forecast_agg = pdx.aggregate_to_period(self.forecast_df, period)

        # find periods of overlap between observed and forecasted data
        overlap = forecast_agg.join(
            observed_summarized, on="submission_date", how="left"
        ).fillna(0)

        # Add observed data samples to any overlapping forecasted period. This
        # ensures that any forecast made partway through a period accounts for
        # previously observed data within the period. For example, when a monthly
        # forecast is generated in the middle of the month.
        forecast_agg += overlap[["value"]].values

        # calculate summary values for forecast samples
        forecast_summarized = forecast_agg.drop(columns="submission_date").agg(
            aggregations, axis=1
        )
        forecast_summarized["submission_date"] = forecast_agg["submission_date"]

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
        periods: List[str] = field(default_factory=list),
        numpy_aggregations: List[str] = field(default_factory=list),
        quantiles: List[str] = field(default_factory=list),
    ) -> None:
        """Aggregate observed and forecasted data."""
        legacy_dfs = []
        observed_dfs = []
        forecast_dfs = []
        for period in periods:
            observed, forecast = self._summarize_to_period(
                period,
                numpy_aggregations,
                quantiles,
            )
            if period != "day":
                legacy_dfs.append(self.summarize_legacy(period, observed, forecast))
            # add details
