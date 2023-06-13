import json
import numpy as np
import pandas as pd
import pandas_extras as pdx

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
        self.metadata_params = json.dumps(
            {
                "model_type": self.model_type.lower(),
                "model_params": self.parameters.toDict(),
                "use_holidays": self.use_holidays,
            }
        )

    def _fit(self) -> None:
        """
        Fit a forecasting model using `self.observed_df` that was generated using
        Metric Hub data. This method should update `self.model`.
        """
        raise NotImplementedError

    def _predict(self) -> pd.DataFrame:
        """
        Forecast using `self.model`. This method should return a dataframe that will
        be validated by `_validate_forecast_df`.
        """
        raise NotImplementedError

    def _predict_legacy(self) -> pd.DataFrame:
        """
        Forecast using `self.model`, adhering to the legacy data format.
        """
        # TODO: This method should be removed once the forecasting data model is updated:
        # https://docs.google.com/document/d/18esfJraogzUf1gbZv25vgXkHigCLefazyvzly9s-1k0.
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

        if not df["submission_date"].equals(self.dates_to_predict["submission_date"]):
            raise ValueError(
                "forecast_df['submission_date'] does not match dates_to_predict['submission_date']."
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

        self.forecast_df = self._predict()
        self._validate_forecast_df()

        # TODO: This line should be removed once the forecasting data model is updated:
        # https://docs.google.com/document/d/18esfJraogzUf1gbZv25vgXkHigCLefazyvzly9s-1k0.
        self.forecast_df_legacy = self._predict_legacy()

    def _summarize(
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
        overlap = forecast_agg.merge(
            observed_summarized, on="submission_date", how="left"
        ).fillna(0)

        forecast_summarized = (
            forecast_agg.set_index("submission_date")
            # Add observed data samples to any overlapping forecasted period. This
            # ensures that any forecast made partway through a period accounts for
            # previously observed data within the period. For example, when a monthly
            # forecast is generated in the middle of the month.
            .add(overlap[["value"]].values)
            # calculate summary values, aggregating by submission_date,
            .agg(aggregations, axis=1).reset_index()
            # "melt" the df from wide-format to long-format.
            .melt(id_vars="submission_date", var_name="measure")
        )

        # add datasource-specific metadata columns
        forecast_summarized["type"] = "forecast"
        observed_summarized["type"] = "historical"
        observed_summarized["measure"] = "observed"

        # create a single dataframe that contains observed and forecasted data
        df = pd.concat([observed_summarized, forecast_summarized])

        # add forecast model metadata columns
        df["forecast_trained_at"] = self.trained_at
        df["forecast_predicted_at"] = self.predicted_at
        df["forecast_parameters"] = self.metadata_params
        df["forecast_start_date"] = self.start_date
        df["forecast_end_date"] = self.end_date

        # add metric hub metadata columns
        df["metric_collected_at"] = self.collected_at
        df["metric_alias"] = self.metric_hub.alias.lower()
        df["metric_hub_slug"] = self.metric_hub.slug.lower()
        df["metric_app_name"] = self.metric_hub.app_name.lower()
        df["metric_start_date"] = self.metric_hub.min_date
        df["metric_end_date"] = self.metric_hub.max_date

        # add summary metadata columns
        df["summary_aggregation_level"] = period.lower()

        return df

    def _summarize_legacy(self) -> pd.DataFrame:
        """
        Converts a summarized dataframe to the legacy format.
        """
        # TODO: This method should be removed once the forecasting data model is updated:
        # https://docs.google.com/document/d/18esfJraogzUf1gbZv25vgXkHigCLefazyvzly9s-1k0.

        df = self.summary_df.copy(deep=True)

        # rename columns to legacy values
        df.rename(
            columns={
                "forecast_end_date": "asofdate",
                "submission_date": "date",
                "metric_alias": "target",
                "summary_aggregation_level": "unit",
            },
            inplace=True,
        )
        df["forecast_date"] = df["forecast_predicted_at"].dt.date
        df["type"] = df["type"].replace("historical", "actual")
        df["measure"] = df["measure"].replace("observed", "value")

        # pivot the df from "long" to "wide" format
        index_columns = [
            "asofdate",
            "date",
            "target",
            "unit",
            "forecast_parameters",
            "forecast_date",
        ]
        df = (
            df[index_columns + ["measure", "value"]]
            .pivot(
                index=index_columns,
                columns="measure",
                values="value",
            )
            .reset_index()
        )
        df.columns.name = None

        # When there's an overlap in the observed and forecasted period -- for
        # example, when a monthly forecast is generated mid-month -- the legacy
        # format only records the forecasted value, not the observed value. To
        # account for this, we'll just find the max of the "mean" (forecasted) and
        # "value" (observed) data. In all non-overlapping observed periods, the
        # forecasted value will be NULL. In all non-overlapping forecasted periods,
        # the observed value will be NULL. In overlapping periods, the forecasted
        # value will always be larger because it is the sum of the observed and forecasted
        # values. Below is a query that demonstrates the legacy behavior:
        #
        # SELECT *
        #   FROM `moz-fx-data-shared-prod.telemetry_derived.kpi_automated_forecast_confidences_v1`
        #  WHERE asofdate = "2023-12-31"
        #    AND target = "mobile"
        #    AND unit = "month"
        #    AND forecast_date = "2022-06-04"
        #    AND date BETWEEN "2022-05-01" AND "2022-06-01"
        #  ORDER BY date
        df["value"] = df[["mean", "value"]].max(axis=1)
        df.drop(columns=["mean"], inplace=True)

        return df

    def summarize(
        self,
        periods: List[str] = field(default_factory=list),
        numpy_aggregations: List[str] = field(default_factory=list),
        quantiles: List[str] = field(default_factory=list),
    ) -> None:
        """Aggregate observed and forecasted data."""
        self.summary_df = pd.concat(
            [self._summarize(i, numpy_aggregations, quantiles) for i in periods]
        )

        # TODO: remove this once the forecasting data model is updated:
        # https://docs.google.com/document/d/18esfJraogzUf1gbZv25vgXkHigCLefazyvzly9s-1k0.
        self.summary_df_legacy = self._summarize_legacy()
