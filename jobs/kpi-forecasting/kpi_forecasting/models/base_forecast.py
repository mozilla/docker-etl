import json
import numpy as np
import pandas as pd
import abc


from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from kpi_forecasting.metric_hub import MetricHub
from typing import Dict, List


@dataclass
class BaseForecast(abc.ABC):
    """
    A base class for fitting, forecasting, and summarizing forecasts. This class
    should not be invoked directly; it should be inherited by a child class. The
    child class needs to implement `_fit` and `_forecast` methods in order to work.

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
        metric_hub (MetricHub): A MetricHub object that provides details about the
            metric to be forecasted.
        number_of_simulations (int): The number of simulated timeseries that the forecast
            should generate. Since many forecast models are probablistic, this enables the
            measurement of variation across a range of possible outcomes.
    """

    model_type: str
    parameters: Dict
    use_holidays: bool
    start_date: str
    end_date: str
    metric_hub: MetricHub
    number_of_simulations: int = 1000

    def _get_observed_data(self):
        if self.metric_hub:
            # the columns in this dataframe
            # are "value" for the metric, submission_date
            # and any segments where the column name
            # is the name of the segment
            self.observed_df = self.metric_hub.fetch()

    def __post_init__(self) -> None:
        # fetch observed observed data
        self.collected_at = datetime.now(timezone.utc).replace(tzinfo=None)
        self._get_observed_data()

        # use default start/end dates if the user doesn't specify them
        self.start_date = pd.to_datetime(self.start_date or self._default_start_date)
        self.end_date = pd.to_datetime(self.end_date or self._default_end_date)
        self.dates_to_predict = pd.DataFrame(
            {"submission_date": pd.date_range(self.start_date, self.end_date).date}
        )

        # initialize unset attributes
        self.model = None
        self.forecast_df = None
        self.summary_df = None

        # metadata
        self.metadata_params = json.dumps(
            {
                "model_type": self.model_type.lower(),
                "model_params": self.parameters.toDict(),
                "use_holidays": self.use_holidays,
            }
        )

    @abc.abstractmethod
    def _fit(self, observed_df: pd.DataFrame) -> None:
        """Fit a forecasting model using `observed_df.` This will typically
        be the data that was generated using
        Metric Hub in `__post_init__`.
        This method should update (and potentially set) `self.model`.

        Args:
            observed_df (pd.DataFrame): observed data used to fit the model
        """
        raise NotImplementedError

    @abc.abstractmethod
    def _predict(self, dates_to_predict: pd.DataFrame) -> pd.DataFrame:
        """Forecast using `self.model` on dates in `dates_to_predict`.
        This method should return a dataframe that will
        be validated by `_validate_forecast_df`.

        Args:
            dates_to_predict (pd.DataFrame): dataframe of dates to forecast for

        Returns:
            pd.DataFrame: dataframe of predictions
        """
        raise NotImplementedError

    @abc.abstractmethod
    def _validate_forecast_df(self, forecast_df: pd.DataFrame) -> None:
        """Method to validate reults produced by _predict

        Args:
            forecast_df (pd.DataFrame): dataframe produced by `_predict`"""
        raise NotImplementedError

    @abc.abstractmethod
    def _summarize(
        self,
        forecast_df: pd.DataFrame,
        observed_df: pd.DataFrame,
        period: str,
        numpy_aggregations: List[str],
        percentiles: List[int],
    ) -> pd.DataFrame:
        """Calculate summary metrics for `forecast_df` over a given period, and
        add metadata.

        Args:
            forecast_df (pd.DataFrame): forecast dataframe created by `predict`
            observed_df (pd.DataFrame): observed data used to generate prediction
            period (str): aggregation period up to which metrics are aggregated
            numpy_aggregations (List[str]): List of numpy aggregation names
            percentiles (List[int]): List of percentiles to aggregate up to

        Returns:
            pd.DataFrame: dataframe containing metrics listed in numpy_aggregations
                and percentiles
        """
        raise NotImplementedError

    @property
    def _default_start_date(self) -> str:
        """The first day after the last date in the observed dataset."""
        return self.observed_df["submission_date"].max() + timedelta(days=1)

    @property
    def _default_end_date(self) -> str:
        """78 weeks (18 months) ahead of the current UTC date."""
        return (
            datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(weeks=78)
        ).date()

    def _set_seed(self) -> None:
        """Set random seed to ensure that fits and predictions are reproducible."""
        np.random.seed(42)

    def fit(self) -> None:
        """Fit a model using historic metric data provided by `metric_hub`."""
        print(f"Fitting {self.model_type} model.", flush=True)
        self._set_seed()
        self.trained_at = datetime.now(timezone.utc).replace(tzinfo=None)
        self._fit(self.observed_df)

    def predict(self) -> None:
        """Generate a forecast from `start_date` to `end_date`.
        Result is set to `self.forecast_df`"""
        print(f"Forecasting from {self.start_date} to {self.end_date}.", flush=True)
        self._set_seed()
        self.predicted_at = datetime.now(timezone.utc).replace(tzinfo=None)
        self.forecast_df = self._predict(self.dates_to_predict)
        self._validate_forecast_df(self.forecast_df)

    def summarize(
        self,
        periods: List[str] = ["day", "month"],
        numpy_aggregations: List[str] = ["mean"],
        percentiles: List[int] = [10, 50, 90],
    ) -> pd.DataFrame:
        """
        Calculate summary metrics for `forecast_df` and add metadata.
        The dataframe returned here will be reported in Big Query when
        `write_results` is called.

        Args:
            periods (List[str]): A list of the time periods that the data should be aggregated and
                summarized by. For example ["day", "month"]
            numpy_aggregations (List[str]): A list of numpy methods (represented as strings) that can
                be applied to summarize numeric values in a numpy dataframe. For example, ["mean"].
            percentiles (List[int]): A list of integers representing the percentiles that should be reported
                in the summary. For example [50] would calculate the 50th percentile (i.e. the median).

        Returns:
            pd.DataFrame: metric dataframe for all metrics and aggregations
        """
        summary_df = pd.concat(
            [
                self._summarize(
                    self.forecast_df,
                    self.observed_df,
                    i,
                    numpy_aggregations,
                    percentiles,
                )
                for i in periods
            ]
        )

        # add Metric Hub metadata columns
        summary_df["metric_alias"] = self.metric_hub.alias.lower()
        summary_df["metric_hub_app_name"] = self.metric_hub.app_name.lower()
        summary_df["metric_hub_slug"] = self.metric_hub.slug.lower()
        summary_df["metric_start_date"] = pd.to_datetime(self.metric_hub.min_date)
        summary_df["metric_end_date"] = pd.to_datetime(self.metric_hub.max_date)
        summary_df["metric_collected_at"] = self.collected_at

        # add forecast model metadata columns
        summary_df["forecast_start_date"] = self.start_date
        summary_df["forecast_end_date"] = self.end_date
        summary_df["forecast_trained_at"] = self.trained_at
        summary_df["forecast_predicted_at"] = self.predicted_at
        summary_df["forecast_parameters"] = self.metadata_params

        self.summary_df = summary_df

        return self.summary_df
