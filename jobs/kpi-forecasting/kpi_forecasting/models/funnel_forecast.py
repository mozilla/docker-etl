import pandas as pd
from pandas.api import types as pd_types
import numpy as np
import prophet

from google.cloud import bigquery
from google.cloud.bigquery.enums import SqlTypeNames as bq_types

from datetime import datetime
from dataclasses import dataclass
from kpi_forecasting.models.base_forecast import BaseForecast
from kpi_forecasting import pandas_extras as pdx
from typing import Dict, List


@dataclass
class FunnelForecast(BaseForecast):
    def __post_init__(self) -> None:
        super().__post_init__()
        combination_df = (
            self.observed_df[self.metric_hub.segments.keys()]
            .value_counts()
            .reset_index(name="count")
            .drop("count", axis=1)
        )
        segment_combinations = []
        for _, row in combination_df:
            segment_combinations.append(row.to_dict())

        self.segment_combinations = segment_combinations

        # initialize a list to hold models for each segment
        self.segment_models = []

    @property
    def column_names_map(self) -> Dict[str, str]:
        return {"submission_date": "ds", "value": "y"}

    def _fit(self) -> None:
        # fit and save a Prophet model for each segment combination
        for recipe in self.segment_combinations:
            model = prophet.Prophet(
                **self.parameters,
                uncertainty_samples=self.number_of_simulations,
                mcmc_samples=0,
            )

            # TODO: Figure out what country_name's holidays to use for ROW. DE?
            if self.use_holidays:
                model.add_country_holidays(country_name="US")

            # Modify observed data to have column names that Prophet expects, and fit
            # the model on rows that match the segment recipe
            model.fit(
                self.observed_df.loc[
                    (self.observed_df[list(recipe)] == pd.Series(recipe)).all(axis=1)
                ].rename(columns=self.column_names_map)
            )
            self.segment_models.append({"segment": {**recipe}, "trained_model": model})

    def _predict(self, model: prophet.Prophet) -> pd.DataFrame:
        # generate the forecast samples
        samples = model.predictive_samples(
            self.dates_to_predict.rename(columns=self.column_names_map)
        )
        df = pd.DataFrame(samples["yhat"])
        df["submission_date"] = self.dates_to_predict

        return df

    def _validate_forecast_df(self, df: pd.DataFrame) -> None:
        """Validate that `self.forecast_df` has been generated correctly."""
        columns = df.columns
        expected_shape = (len(self.dates_to_predict), 1 + self.number_of_simulations)
        numeric_columns = df.drop(columns=["submission_date"]).columns

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
                    "All forecast_df columns except 'submission_date' and segment dims must be numeric,"
                    f" but column {i} has type {df[i].dtypes}."
                )

    def _summarize(
        self,
        observed_df: pd.DataFrame,
        forecast_df: pd.DataFrame,
        period: str,
        numpy_aggregations: List[str],
        percentiles: List[int],
    ) -> pd.DataFrame:
        """
        Calculate summary metrics for `forecast_df` over a given period, and
        add metadata.
        """
        # build a list of all functions that we'll summarize the data by
        aggregations = [getattr(np, i) for i in numpy_aggregations]
        aggregations.extend([pdx.percentile(i) for i in percentiles])

        # aggregate metric to the correct date period (day, month, year)
        observed_summarized = pdx.aggregate_to_period(observed_df, period)
        forecast_agg = pdx.aggregate_to_period(forecast_df, period)

        # find periods of overlap between observed and forecasted data
        overlap = forecast_agg.merge(
            observed_summarized,
            on="submission_date",
            how="left",
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
        forecast_summarized["source"] = "forecast"
        observed_summarized["source"] = "historical"
        observed_summarized["measure"] = "observed"

        # create a single dataframe that contains observed and forecasted data
        df = pd.concat([observed_summarized, forecast_summarized])

        # add summary metadata columns
        df["aggregation_period"] = period.lower()

        # reorder columns to make interpretation easier
        df = df[["submission_date", "aggregation_period", "source", "measure", "value"]]

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
        df["forecast_parameters"] = self.metadata_params

        return df

    def predict(self) -> None:
        """Generate a forecast from `start_date` to `end_date`."""
        print(f"Forecasting from {self.start_date} to {self.end_date}.", flush=True)
        self._set_seed()
        self.predicted_at = datetime.utcnow()

        for segment in self.segment_models:
            forecast_df = self._predict(segment["trained_model"])
            self._validate_forecast_df(forecast_df)

            segment["forecast_df"] = forecast_df.copy(deep=True)
            del forecast_df

    def summarize(
        self,
        periods: List[str] = ["day", "month"],
        numpy_aggregations: List[str] = ["mean"],
        percentiles: List[int] = [10, 50, 90],
    ) -> None:
        summary_df_list = []
        for segment in self.segment_models:
            summary_df = pd.concat(
                [
                    self._summarize(
                        self.observed_df,
                        segment["forecast_df"],
                        i,
                        numpy_aggregations,
                        percentiles,
                    )
                    for i in periods
                ]
            )
            for dim, dim_value in segment["segment"]:
                summary_df[dim] = dim_value

            summary_df_list.append(summary_df.copy(deep=True))
            del summary_df

        self.summary_df = pd.concat(summary_df_list, ignore_index=True)

    def write_results(
        self,
        project: str,
        dataset: str,
        table: str,
        write_disposition: str = "WRITE_APPEND",
    ) -> None:
        """
        Write `self.summary_df` to Big Query.

        Args:
            project (str): The Big Query project that the data should be written to.
            dataset (str): The Big Query dataset that the data should be written to.
            table (str): The Big Query table that the data should be written to.
            write_disposition (str): In the event that the destination table exists,
                should the table be overwritten ("WRITE_TRUNCATE") or appended to
                ("WRITE_APPEND")?
        """
        print(f"Writing results to `{project}.{dataset}.{table}`.", flush=True)
        client = bigquery.Client(project=project)
        schema = [
            bigquery.SchemaField("submission_date", bq_types.DATE),
            *[
                bigquery.SchemaField(k, bq_types.STRING)
                for k in self.metric_hub.segments.keys()
            ],
            bigquery.SchemaField("aggregation_period", bq_types.STRING),
            bigquery.SchemaField("source", bq_types.STRING),
            bigquery.SchemaField("measure", bq_types.STRING),
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
