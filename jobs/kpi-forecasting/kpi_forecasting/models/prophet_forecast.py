import json
import pandas as pd
from pandas.api import types as pd_types
import prophet
import numpy as np
from typing import Dict, List


from datetime import datetime, timezone
from dataclasses import dataclass
from kpi_forecasting.models.base_forecast import BaseForecast
from kpi_forecasting import pandas_extras as pdx
from google.cloud import bigquery
from google.cloud.bigquery.enums import SqlTypeNames as bq_types


@dataclass
class ProphetForecast(BaseForecast):
    @property
    def column_names_map(self) -> Dict[str, str]:
        return {"submission_date": "ds", "value": "y"}

    def _fit(self, observed_df) -> None:
        self.model = prophet.Prophet(
            **self.parameters,
            uncertainty_samples=self.number_of_simulations,
            mcmc_samples=0,
        )

        if self.use_holidays:
            self.model.add_country_holidays(country_name="US")

        # Modify observed data to have column names that Prophet expects, and fit
        # the model
        self.model.fit(observed_df.rename(columns=self.column_names_map))

    def _predict(self, dates_to_predict) -> pd.DataFrame:
        # generate the forecast samples
        samples = self.model.predictive_samples(
            dates_to_predict.rename(columns=self.column_names_map)
        )
        df = pd.DataFrame(samples["yhat"])
        df["submission_date"] = dates_to_predict
        return df

    def _validate_forecast_df(self, df) -> None:
        """Validate that `self.forecast_df` has been generated correctly."""
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

    def _predict_legacy(self) -> pd.DataFrame:
        """
        Recreate the legacy format used in
        `moz-fx-data-shared-prod.telemetry_derived.kpi_automated_forecast_v1`.
        """
        # TODO: This method should be removed once the forecasting data model is updated:
        # https://mozilla-hub.atlassian.net/browse/DS-2676

        df = self.model.predict(
            self.dates_to_predict.rename(columns=self.column_names_map)
        )

        # set legacy column values
        if "dau" in self.metric_hub.alias.lower():
            df["metric"] = "DAU"
        else:
            df["metric"] = self.metric_hub.alias

        df["forecast_date"] = str(
            datetime.now(timezone.utc).replace(tzinfo=None).date()
        )
        df["forecast_parameters"] = str(
            json.dumps({**self.parameters, "holidays": self.use_holidays})
        )

        alias = self.metric_hub.alias.lower()

        if ("desktop" in alias) and ("mobile" in alias):
            raise ValueError(
                "Metric Hub alias must include either 'desktop' or 'mobile', not both."
            )
        elif "desktop" in alias:
            df["target"] = "desktop"
        elif "mobile" in alias:
            df["target"] = "mobile"
        else:
            raise ValueError(
                "Metric Hub alias must include either 'desktop' or 'mobile'."
            )

        columns = [
            "ds",
            "trend",
            "yhat_lower",
            "yhat_upper",
            "trend_lower",
            "trend_upper",
            "additive_terms",
            "additive_terms_lower",
            "additive_terms_upper",
            "extra_regressors_additive",
            "extra_regressors_additive_lower",
            "extra_regressors_additive_upper",
            "holidays",
            "holidays_lower",
            "holidays_upper",
            "regressor_00",
            "regressor_00_lower",
            "regressor_00_upper",
            "weekly",
            "weekly_lower",
            "weekly_upper",
            "yearly",
            "yearly_lower",
            "yearly_upper",
            "multiplicative_terms",
            "multiplicative_terms_lower",
            "multiplicative_terms_upper",
            "yhat",
            "target",
            "forecast_date",
            "forecast_parameters",
            "metric",
        ]

        for column in columns:
            if column not in df.columns:
                df[column] = 0.0

        return df[columns]

    def _combine_forecast_observed(
        self,
        forecast_df,
        observed_df,
        period: str,
        numpy_aggregations: List[str],
        percentiles: List[int],
    ):
        # build a list of all functions that we'll summarize the data by
        aggregations = [getattr(np, i) for i in numpy_aggregations]
        aggregations.extend([pdx.percentile(i) for i in percentiles])

        # aggregate metric to the correct date period (day, month, year)
        observed_summarized = pdx.aggregate_to_period(observed_df, period)
        forecast_agg = pdx.aggregate_to_period(forecast_df, period).sort_values(
            "submission_date"
        )

        # find periods of overlap between observed and forecasted data
        # merge preserves key order so overlap will be sorted by submission_date
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
            .agg(aggregations, axis=1)
            .reset_index()
            # "melt" the df from wide-format to long-format.
            .melt(id_vars="submission_date", var_name="measure")
        )

        # add datasource-specific metadata columns
        forecast_summarized["source"] = "forecast"
        observed_summarized["source"] = "historical"
        observed_summarized["measure"] = "observed"

        # create a single dataframe that contains observed and forecasted data
        df = pd.concat([observed_summarized, forecast_summarized])
        return df

    def _summarize(
        self,
        forecast_df,
        observed_df,
        period: str,
        numpy_aggregations: List[str],
        percentiles: List[int],
    ) -> pd.DataFrame:
        """
        Calculate summary metrics for `self.forecast_df` over a given period, and
        add metadata.
        """

        df = self._combine_forecast_observed(
            forecast_df, observed_df, period, numpy_aggregations, percentiles
        )
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

    def _summarize_legacy(self) -> pd.DataFrame:
        """
        Converts a `self.summary_df` to the legacy format used in
        `moz-fx-data-shared-prod.telemetry_derived.kpi_automated_forecast_confidences_v1`
        """
        # TODO: This method should be removed once the forecasting data model is updated:
        # https://mozilla-hub.atlassian.net/browse/DS-2676

        df = self.summary_df.copy(deep=True)

        # rename columns to legacy values
        df.rename(
            columns={
                "forecast_end_date": "asofdate",
                "submission_date": "date",
                "metric_alias": "target",
                "aggregation_period": "unit",
            },
            inplace=True,
        )
        df["forecast_date"] = df["forecast_predicted_at"].dt.date
        df["type"] = df["source"].replace("historical", "actual")
        df = df.replace(
            {
                "measure": {
                    "observed": "value",
                    "p05": "yhat_p5",
                    "p10": "yhat_p10",
                    "p20": "yhat_p20",
                    "p30": "yhat_p30",
                    "p40": "yhat_p40",
                    "p50": "yhat_p50",
                    "p60": "yhat_p60",
                    "p70": "yhat_p70",
                    "p80": "yhat_p80",
                    "p90": "yhat_p90",
                    "p95": "yhat_p95",
                },
                "target": {
                    "desktop_dau": "desktop",
                    "mobile_dau": "mobile",
                },
            }
        )

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

        # pivot sets the "name" attribute of the columns for some reason. It's
        # None by default, so we just reset that here.
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

        # non-numeric columns are represented in the legacy bq schema as strings
        string_cols = [
            "asofdate",
            "date",
            "target",
            "unit",
            "forecast_parameters",
            "forecast_date",
        ]
        df[string_cols] = df[string_cols].astype(str)

        return df

    def write_results(
        self,
        project: str,
        dataset: str,
        table: str,
        project_legacy: str,
        dataset_legacy: str,
        write_disposition: str = "WRITE_APPEND",
        forecast_table_legacy: str = "kpi_automated_forecast_v1",
        confidences_table_legacy: str = "kpi_automated_forecast_confidences_v1",
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
        # get legacy tables
        # TODO: remove this once the forecasting data model is updated:
        # https://mozilla-hub.atlassian.net/browse/DS-2676
        self.forecast_df_legacy = self._predict_legacy()
        self.summary_df_legacy = self._summarize_legacy()

        print(f"Writing results to `{project}.{dataset}.{table}`.", flush=True)
        client = bigquery.Client(project=project)
        schema = [
            bigquery.SchemaField("submission_date", bq_types.DATE),
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

        # TODO: remove the below jobs once the forecasting data model is updated:
        # https://mozilla-hub.atlassian.net/browse/DS-2676

        job = client.load_table_from_dataframe(
            dataframe=self.forecast_df_legacy,
            destination=f"{project_legacy}.{dataset_legacy}.{forecast_table_legacy}",
            job_config=bigquery.LoadJobConfig(
                write_disposition=write_disposition,
                schema=[
                    bigquery.SchemaField("ds", bq_types.TIMESTAMP),
                    bigquery.SchemaField("forecast_date", bq_types.STRING),
                    bigquery.SchemaField("forecast_parameters", bq_types.STRING),
                ],
            ),
        )
        job.result()

        job = client.load_table_from_dataframe(
            dataframe=self.summary_df_legacy,
            destination=f"{project_legacy}.{dataset_legacy}.{confidences_table_legacy}",
            job_config=bigquery.LoadJobConfig(
                write_disposition=write_disposition,
                schema=[
                    bigquery.SchemaField("asofdate", bq_types.STRING),
                    bigquery.SchemaField("date", bq_types.STRING),
                ],
            ),
        )
        job.result()
