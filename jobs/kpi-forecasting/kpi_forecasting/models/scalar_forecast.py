from dataclasses import dataclass
import re
from typing import Dict, List

from google.cloud import bigquery
from google.cloud.bigquery.enums import SqlTypeNames as bq_types
import numpy as np
import pandas as pd

from kpi_forecasting.configs.model_inputs import parse_scalar_adjustments
from kpi_forecasting.models.base_forecast import BaseForecast
from kpi_forecasting import pandas_extras as pdx


@dataclass
class ScalarForecast(BaseForecast):
    """
    ScalarForecast class for generating and managing forecast models where forecasts are
    scalar adjustments of historical data or preceding Prophet-based forecasts. The
    class handles cases where forecasts for a combination of dimensions are required for
    a metric.

    Inherits from BaseForecast and provides methods for initializing forecast
    parameters, building models, generating forecasts, summarizing results,
    and writing results to BigQuery.
    """

    def __post_init__(self) -> None:
        """
        Post-initialization method to set up necessary attributes and configurations.

        This method sets up the dates to predict, constructs segment combinations,
        initializes models for each segment, and prepares attributes for storing results.
        """
        super().__post_init__()

        # Get the list of adjustments for the metric slug being forecasted. That
        ## slug must be a key in scalar_adjustments.yaml; otherwise, this will raise a KeyError
        self.scalar_adjustments = parse_scalar_adjustments(
            self.metric_hub.slug, self.start_date
        )

        # Construct a DataFrame containing all combination of segment values
        ## in the observed_df
        self.combination_df = self.observed_df[
            self.metric_hub.segments.keys()
        ].drop_duplicates()

        # Set up the columns to be used to join the observed_df to the forecast_df in subsequent
        ## methods
        self.join_columns = self.combination_df.columns.to_list() + ["submission_date"]

        # Rename the value column to the metric slug name, to enable supporting a formula with
        ## covariates in the future
        self.observed_df.rename(columns={"value": self.metric_hub.slug}, inplace=True)

        # Cross join to the dates_to_predict DataFrame to create a DataFrame that contains a row
        ## for each forecast date for each segment
        self.forecast_df = self.dates_to_predict.merge(self.combination_df, how="cross")

    @property
    def period_names_map(self) -> Dict[str, pd.DateOffset]:
        """
        Map a period-over-period name to an offset to apply to DataFrame date columns.

        Returns:
            Dict[str, str]: Mapping of column names.
        """
        return {"YOY": pd.DateOffset(years=1), "MOM": pd.DateOffset(months=1)}

    def _parse_formula_for_over_period_changes(self) -> Dict | None:
        """
        Find period-over-period metric specifications in provided formula. If present, create a dict that
        maps a metric name to a period-over-period change.
        """

        # Pattern to match to the words before and after a colon. This will be the standard pattern
        ## in a formula to denote that a period-over-period change will be applied to a metric
        ## for a forecast.
        pattern = r"\b(\w+:\w+)\b"
        match = re.findall(pattern, self.parameters.formula)

        if match:
            # Create dict from list of colon-separated strings (e.g. "metric_name:YOY").
            pop_dict = dict(pair.split(":") for pair in match)
            return pop_dict

        return None

    def _add_scalar_columns(self) -> None:
        """
        Adds the scalars to make metric adjustments to the dates specified in the self.scalar_adjustments
        DataFrames.
        """

        for scalar_adjustment in self.scalar_adjustments:
            adj_df = scalar_adjustment.adjustments_dataframe.rename(
                columns={"value": f"scalar_{scalar_adjustment.name}"}
            )

            # Merge asof to align values based on start dates and dimensions
            self.forecast_df = pd.merge_asof(
                self.forecast_df.sort_values("submission_date"),
                adj_df.sort_values("start_date"),
                by=[self.combination_df.columns],
                left_on="submission_date",
                right_on="start_date",
                direction="backward",
            )

            # Fill values with submission_date before start_date with np.nan, then replace NaN with
            ## 1 to not apply any scalar for dates that don't apply or for segments without that
            ## scalar
            self.forecast_df[f"scalar_{scalar_adjustment.name}"] = np.where(
                self.forecast_df["submission_date"] < self.forecast_df["start_date"],
                np.nan,
                self.forecast_df[f"scalar_{scalar_adjustment.name}"],
            )

            # Fill scalar column with 1. Scalars are always multiplicative, so this removes the scalar effect
            ## for dates/segments where it shouldn't apply
            self.forecast_df[f"scalar_{scalar_adjustment.name}"].fillna(1, inplace=True)

            # Drop the start_date column that isn't needed for forecasting and can be reused for multiple
            ## metrics
            self.forecast_df.drop(columns=["start_date"], inplace=True)

    def _fit(self) -> None:

        # Create period-over-period dict, which defines how observed data is carried forward in cases
        ## where the forecast is a scalar * previously observed data
        pop_dict = self._parse_formula_for_over_period_changes()
        if pop_dict:
            for metric, period in pop_dict:
                metric_pop_name = f"{metric}_{period}"

                # Create date column in the forecast_df with the specified date offset
                ## in order to merge in observed data from that period
                offset = self.period_names_map[period]
                self.forecast_df[f"{metric_pop_name}_date"] = pd.to_datetime(
                    self.forecast_df["submission_date"] - offset
                )

                # Merge observed data to be used in adjustments
                self.forecast_df.merge(
                    self.observed_df[[*self.join_columns, metric]],
                    how="left",
                    left_on=f"{metric_pop_name}_date",
                    right_on="submission_date",
                    inplace=True,
                )

                # Remove unneeded date column
                self.forecast_df.drop(columns=[f"{metric_pop_name}_date"], inplace=True)

        # Update the forecast_df with scalar columns
        self._add_scalar_columns()

    def _predict(self) -> None:

        # Create final scalar as product of individual scalar effects
        self.forecast_df["scalar"] = self.forecast_df[
            [c for c in self.forecast_df.columns if "scalar_" in c]
        ].prod(axis=1)

        # Calculate forecast as product of scalar value and observed value
        self.forecast_df["value"] = (
            self.forecast_df["scalar"] * self.forecast_df[self.metric_hub.slug]
        )

        # Record each scalar value in a dictionary to record in model records
        self.forecast_df["forecast_parameters"] = self.forecast_df[
            [c for c in self.forecast_df.columns if "scalar" in c]
        ].to_dict(orient="records")

    def _summarize(self, period: str) -> pd.DataFrame:
        """
        In cases where no summarization is required, adds the expected columns to a summary DataFrame.

        Args:
            period (str): Aggregation period that should be consistent with the aggregation period of
                the observed data.
        """
        if isinstance(period, list):
            if len(period) > 1:
                raise ValueError(
                    "Can only supply one aggregation period when not summarizing results."
                )
            period = period[0]

        df = self.forecast_df.copy()
        df["source"] = np.where(
            df["submission_date"] < self.start_date,
            "historical",
            "forecast",
        )
        df["measure"] = np.where(
            df["submission_date"] < self.start_date,
            "observed",
            "forecast",
        )

        df["aggregation_period"] = period
        # add Metric Hub metadata columns
        df["metric_alias"] = self.metric_hub.alias.lower()
        df["metric_hub_app_name"] = self.metric_hub.app_name.lower()
        df["metric_hub_slug"] = self.metric_hub.slug.lower()
        df["metric_start_date"] = pd.to_datetime(self.metric_hub.min_date)
        df["metric_end_date"] = pd.to_datetime(self.metric_hub.max_date)
        df["metric_collected_at"] = self.collected_at

        # add forecast model metadata columns
        df["forecast_start_date"] = self.start_date
        df["forecast_end_date"] = self.end_date
        df["forecast_trained_at"] = self.trained_at
        df["forecast_predicted_at"] = self.predicted_at

    def summarize(
        self,
        requires_summarization: bool = True,
        periods: List[str] | str = ["day", "month"],
        numpy_aggregations: List[str] = ["mean"],
        percentiles: List[int] = [10, 50, 90],
    ) -> None:
        """
        There are cases where forecasts created by this class do not require summarization (e.g. the
        scalar adjustment was made to a prior forecast)
        """
        if not requires_summarization:
            self._summarize(periods)

        else:
            # If summarization is required, use the summarization method in the BaseForecast class
            self.summary_df = pd.concat(
                [self._summarize(i, numpy_aggregations, percentiles) for i in periods]
            )

    def write_results(
        self,
        project: str,
        dataset: str,
        table: str,
        write_disposition: str = "WRITE_APPEND",
        components_table: str = "",
        components_dataset: str = "",
    ) -> None:
        """
        Write `self.summary_df` to Big Query.

        Args:
            project (str): The Big Query project that the data should be written to.
            dataset (str): The Big Query dataset that the data should be written to.
            table (str): The Big Query table that the data should be written to.
            write_disposition (str, optional): In the event that the destination table exists,
                should the table be overwritten ("WRITE_TRUNCATE") or appended to ("WRITE_APPEND")? Defaults to "WRITE_APPEND".
            components_table (str, optional): The Big Query table for model components. Defaults to "".
            components_dataset (str, optional): The Big Query dataset for model components. Defaults to "".
        """
        print(
            f"Writing results to `{project}.{dataset}.{table}`.",
            flush=True,
        )
        client = bigquery.Client(project=project)
        schema = [
            bigquery.SchemaField("submission_date", bq_types.DATE),
            *[
                bigquery.SchemaField(k, bq_types.STRING)
                for k in self.metric_hub.segments.keys()
            ],
            bigquery.SchemaField("aggregation_period", bq_types.STRING),
            bigquery.SchemaField("source", bq_types.STRING),
            bigquery.SchemaField("value", bq_types.FLOAT),
            bigquery.SchemaField("metric_alias", bq_types.STRING),
            bigquery.SchemaField("metric_hub_app_name", bq_types.STRING),
            bigquery.SchemaField("metric_hub_slug", bq_types.STRING),
            bigquery.SchemaField("metric_start_date", bq_types.DATE),
            bigquery.SchemaField("metric_end_date", bq_types.DATE),
            bigquery.SchemaField("metric_collected_at", bq_types.TIMESTAMP),
            bigquery.SchemaField("forecast_start_date", bq_types.DATE),
            bigquery.SchemaField("forecast_end_date", bq_types.DATE),
            bigquery.SchemaField("forecast_trained_at", bq_types.TIMESTAMP),
            bigquery.SchemaField("forecast_predicted_at", bq_types.TIMESTAMP),
            bigquery.SchemaField("forecast_parameters", bq_types.STRING),
        ]
        job = client.load_table_from_dataframe(
            dataframe=self.summary_df,
            destination=f"{project}.{dataset}.{table}",
            job_config=bigquery.LoadJobConfig(
                schema=schema,
                autodetect=False,
                write_disposition=write_disposition,
            ),
        )
        # Wait for the job to complete.
        job.result()
