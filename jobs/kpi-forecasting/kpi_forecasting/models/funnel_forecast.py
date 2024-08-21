from dataclasses import dataclass
from datetime import datetime
import itertools
import json
from typing import Dict, List, Union

from google.cloud import bigquery
from google.cloud.bigquery.enums import SqlTypeNames as bq_types
import numpy as np
import pandas as pd
from pandas.api import types as pd_types
import prophet
from prophet.diagnostics import cross_validation

from kpi_forecasting.configs.model_inputs import ProphetHoliday, ProphetRegressor
from kpi_forecasting.models.prophet_forecast import (
    ProphetForecast,
)
from kpi_forecasting.models.base_forecast import BaseEnsembleForecast


class ProphetAutotunerForecast(ProphetForecast):
    def _get_crossvalidation_metric(
        self, m: prophet.Prophet, cv_settings: dict
    ) -> float:
        """function for calculated the metric used for crossvalidation

        Args:
            m (prophet.Prophet): Prophet model for crossvalidation
            cv_settings (dict): settings set by segment in the config file

        Returns:
            float: Metric where closer to zero means a better model
        """
        df_cv = cross_validation(m, **cv_settings)

        df_bias = df_cv.groupby("cutoff")[["yhat", "y"]].sum().reset_index()
        df_bias["pcnt_bias"] = df_bias["yhat"] / df_bias["y"] - 1
        # Prophet splits the historical data when doing cross validation using
        # cutoffs. The `.tail(3)` limits the periods we consider for the best
        # parameters to the 3 most recent cutoff periods.
        return df_bias.tail(3)["pcnt_bias"].mean()

    def _predict(
        self,
        dates_to_predict_raw: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Generate forecast samples for a segment.

        Args:
            dates_to_predict (pd.DataFrame): dataframe of dates to predict
            segment_settings (FunnelSegmentModelSettings): The settings for the segment.

        Returns:
            pd.DataFrame: The forecasted values.
        """
        # add regressors, logistic growth limits (if applicable) to predict dataframe
        dates_to_predict = self._build_predict_dataframe(dates_to_predict_raw)

        # draws samples from Prophet posterior distribution, to provide percentile predictions
        samples = self.model.predictive_samples(dates_to_predict)
        df = pd.DataFrame(samples["yhat"])
        df["submission_date"] = dates_to_predict_raw

        component_cols = [
            "ds",
            "yhat",
            "trend",
            "trend_upper",
            "trend_lower",
            "weekly",
            "weekly_upper",
            "weekly_lower",
            "yearly",
            "yearly_upper",
            "yearly_lower",
        ]

        # use 'predict' method to return components from the Prophet model
        components_df = self.model.predict(dates_to_predict)[component_cols]

        # join observed data to components df, which allows for calc of intra-sample
        # error rates and how components resulted in those predictions. The `fillna`
        # call will fill the missing y values for forecasted dates, where only yhat
        # is available.
        components_df = components_df.merge(
            self.history[["ds", "y"]],
            on="ds",
            how="left",
        ).fillna(0)
        components_df.rename(columns={"ds": "submission_date"}, inplace=True)

        self.components_df = components_df.copy()

        return df

    def _validate_forecast_df(self, df: pd.DataFrame) -> None:
        """
        Validate that the forecast dataframe has been generated correctly for each segment.

        Args:
            df (pd.DataFrame): The forecast dataframe.

        Raises:
            ValueError: If the dataframe does not meet the required conditions.
        """
        columns = df.columns
        numeric_columns = df.drop(columns=["submission_date"]).columns

        if "submission_date" not in columns:
            raise ValueError("forecast_df must contain a 'submission_date' column.")

        for i in numeric_columns:
            if not pd_types.is_numeric_dtype(df[i]):
                raise ValueError(
                    "All forecast_df columns except 'submission_date' and segment dims must be numeric,"
                    f" but column {i} has type {df[i].dtypes}."
                )

    def _auto_tuning(
        self, observed_df, segment_settings: FunnelSegmentModelSettings
    ) -> Dict[str, float]:
        """
        Perform automatic tuning of model parameters.

        Args:
            observed_df (pd.DataFrame): dataframe of observed data
                Expected to have columns:
                specified in the segments section of the config,
                submission_date column with unique dates corresponding to each observation and
                y column containing values of observations
            segment_settings (FunnelSegmentModelSettings): The settings for the segment.

        Returns:
            Dict[str, float]: The tuned parameters.
        """
        add_log_growth_cols = (
            "growth" in segment_settings.grid_parameters.keys()
            and segment_settings.grid_parameters["growth"] == "logistic"
        )

        for k, v in segment_settings.grid_parameters.items():
            if not isinstance(v, list):
                segment_settings.grid_parameters[k] = [v]

        param_grid = [
            dict(zip(segment_settings.grid_parameters.keys(), v))
            for v in itertools.product(*segment_settings.grid_parameters.values())
        ]

        test_dat = self._build_train_dataframe(
            observed_df, segment_settings, add_log_growth_cols
        )
        bias = []

        for params in param_grid:
            m = self._build_model(segment_settings, params)
            m.fit(test_dat)

            crossval_metric = self._get_crossvalidation_metric(
                m, segment_settings.cv_settings
            )
            bias.append(crossval_metric)

        min_abs_bias_index = np.argmin(np.abs(bias))

        return param_grid[min_abs_bias_index]


@dataclass
class FunnelForecast(BaseEnsembleForecast):
    """
    Holds the configuration and results for each segment
    in a funnel forecasting model.
    """

    grid_parameters: Dict[str, Union[List[float], float]] = None
    cv_settings: Dict[str, str] = None
    model_class: object = ProphetAutotunerForecast

    def __post_init__(self):
        super(FunnelForecast, self).__post_init__(self)
        if not isinstance(self.model_class, ProphetAutotunerForecast):
            raise ValueError("model_class set when ProphetForecast is expected")

    def _fit(self, observed_df: pd.DataFrame) -> None:
        """
        Fit and save a Prophet model for each segment combination.

        Args:
            observed_df (pd.DataFrame): dataframe of observations.  Expected to have columns
                specified in the segments section of the config,
                submission_date column with unique dates corresponding to each observation and
                y column containing values of observations
        """
        # Initialize model; build model dataframe
        test_dat = self._build_train_dataframe(observed_df)

        self.model.fit(test_dat)
        self.history = test_dat


def _percentile_name_map(self, percentiles: List[int]) -> Dict[str, str]:
    """
    Map percentiles to their corresponding names for the BQ table.

    Args:
        percentiles (List[int]): The list of percentiles.

    Returns:
        Dict[str, str]: The mapping of percentile names.
    """

    percentiles.sort()
    return {
        f"p{percentiles[0]}": "value_low",
        f"p{percentiles[1]}": "value_mid",
        f"p{percentiles[2]}": "value_high",
        "mean": "value",
    }


def _combine_forecast_observed(
    self,
    forecast_df: pd.DataFrame,
    observed_df: pd.DataFrame,
    period: str,
    numpy_aggregations: List,
    percentiles,
    segment: dict,
) -> pd.DataFrame:
    """Calculate aggregates over the forecast and observed data
        and concatenate the two dataframes
    Args:
        forecast_df (pd.DataFrame): forecast dataframe
        observed_df (pd.DataFrame): observed dataframe
        period (str): period to aggregate up to, must be in (day, month, year)
        numpy_aggregations (List): List of aggregation functions to apply across samples from the
                                posterior-predictive distribution.  Must take
                                in a numpy array and return a single value
        percentiles: 3-element list of percentiles to calculate across samples from the posterior-predictive distribution
        segment (dict): dictionary that lists columns and values corresponding to the segment
                            keys are the column name used to segment and values are the values
                            of that column corresponding to the current segment

    Returns:
        pd.DataFrame: combined dataframe containing aggregated values from observed and forecast
    """
    # filter the forecast data to just the data in the future
    last_historic_date = observed_df["submission_date"].max()
    forecast_df = forecast_df.loc[forecast_df["submission_date"] > last_historic_date]

    forecast_summarized, observed_summarized = self._aggregate_forecast_observed(
        forecast_df, observed_df, period, numpy_aggregations, percentiles
    )

    # add datasource-specific metadata columns
    forecast_summarized["source"] = "forecast"
    observed_summarized["source"] = "historical"

    # add segment columns to forecast  table
    for dim, value in segment.items():
        forecast_summarized[dim] = value

    # rename forecast percentile to low, middle, high
    # rename mean to value
    forecast_summarized = forecast_summarized.rename(
        columns=self._percentile_name_map(percentiles)
    )

    # create a single dataframe that contains observed and forecasted data
    df = pd.concat([observed_summarized, forecast_summarized])
    return df


def _summarize(
    self,
    segment_settings: FunnelSegmentModelSettings,
    period: str,
    numpy_aggregations: List[str],
    percentiles: List[int] = [10, 50, 90],
) -> pd.DataFrame:
    """
    Calculate summary metrics on a specific segment
    for `forecast_df` over a given period, and add metadata.

    Args:
        segment_settings (FunnelSegmentModelSettings): The settings for the segment.
        period (str): The period for aggregation.
        numpy_aggregations (List[str]): List of numpy aggregation functions.
        percentiles (List[int]): List of percentiles.

    Returns:
        pd.DataFrame: The summarized dataframe.
    """
    if len(percentiles) != 3:
        raise ValueError(
            """
            Can only pass a list of length 3 as percentiles, for lower, mid, and upper values.
            """
        )

    # the start date for this segment's historical data, in cases where the full time series
    ## of historical data is not used for model training
    segment_observed_start_date = datetime.strptime(
        segment_settings.start_date, "%Y-%m-%d"
    ).date()

    # find indices in observed_df for rows that exactly match segment dict
    segment_historical_indices = (
        self.observed_df[list(segment_settings.segment)]
        == pd.Series(segment_settings.segment)
    ).all(axis=1)

    segment_observed_df = self.observed_df.loc[
        (segment_historical_indices)
        & (self.observed_df["submission_date"] >= segment_observed_start_date)
    ].copy()

    df = self._combine_forecast_observed(
        segment_settings.forecast_df,
        segment_observed_df,
        period,
        numpy_aggregations,
        percentiles,
        segment_settings.segment,
    )

    df["forecast_parameters"] = json.dumps(segment_settings.trained_parameters)

    # add summary metadata columns
    df["aggregation_period"] = period.lower()
    return df


def summarize(
    self,
    periods: List[str] = ["day", "month"],
    numpy_aggregations: List[str] = ["mean"],
    percentiles: List[int] = [10, 50, 90],
) -> None:
    """
    Summarize the forecast results over specified periods.

    Args:
        periods (List[str], optional): The periods for summarization. Defaults to ["day", "month"].
        numpy_aggregations (List[str], optional): The numpy aggregation functions. Defaults to ["mean"].
        percentiles (List[int], optional): The percentiles for summarization. Defaults to [10, 50, 90].
    """
    summary_df_list = []
    components_df_list = []
    for segment in self.segment_models:
        summary_df = pd.concat(
            [
                self._summarize(
                    segment,
                    i,
                    numpy_aggregations,
                    percentiles,
                )
                for i in periods
            ]
        )
        for dim, dim_value in segment.segment.items():
            segment.components_df[dim] = dim_value
        summary_df_list.append(summary_df.copy(deep=True))
        components_df_list.append(segment.components_df)
        del summary_df

    df = pd.concat(summary_df_list, ignore_index=True)

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

    self.summary_df = df

    self.components_df = pd.concat(components_df_list, ignore_index=True)


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
        bigquery.SchemaField("value_low", bq_types.FLOAT),
        bigquery.SchemaField("value_mid", bq_types.FLOAT),
        bigquery.SchemaField("value_high", bq_types.FLOAT),
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

    if components_table:
        numeric_cols = list(self.components_df.select_dtypes(include=float).columns)
        string_cols = list(self.components_df.select_dtypes(include=object).columns)
        self.components_df["metric_slug"] = self.metric_hub.slug
        self.components_df["forecast_trained_at"] = self.trained_at

        schema = [
            bigquery.SchemaField("submission_date", bq_types.DATE),
            bigquery.SchemaField("metric_slug", bq_types.STRING),
            bigquery.SchemaField("forecast_trained_at", bq_types.TIMESTAMP),
        ]
        schema += [bigquery.SchemaField(col, bq_types.STRING) for col in string_cols]
        schema += [bigquery.SchemaField(col, bq_types.FLOAT) for col in numeric_cols]

        if not components_dataset:
            components_dataset = dataset
        print(
            f"Writing model components to `{project}.{components_dataset}.{components_table}`.",
            flush=True,
        )

        job = client.load_table_from_dataframe(
            dataframe=self.components_df,
            destination=f"{project}.{components_dataset}.{components_table}",
            job_config=bigquery.LoadJobConfig(
                schema=schema,
                autodetect=False,
                write_disposition=write_disposition,
                schema_update_options=[
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
                ],
            ),
        )

        job.result()
