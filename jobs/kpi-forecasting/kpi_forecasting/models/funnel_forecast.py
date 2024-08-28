from dataclasses import dataclass, field
import itertools
from typing import List
import json

from google.cloud import bigquery
from google.cloud.bigquery.enums import SqlTypeNames as bq_types
import numpy as np
import pandas as pd
from pandas.api import types as pd_types
from prophet.diagnostics import cross_validation

from kpi_forecasting.models.prophet_forecast import (
    ProphetForecast,
    aggregate_forecast_observed,
)
from kpi_forecasting.models.base_forecast import BaseEnsembleForecast


@dataclass
class ProphetAutotunerForecast(ProphetForecast):
    grid_parameters: dict = field(default_factory=dict)
    cv_settings: dict = field(default_factory=dict)

    def _get_crossvalidation_metric(self, m: ProphetForecast) -> float:
        """function for calculated the metric used for crossvalidation

        Args:
            m (ProphetForecast): Prophet model for crossvalidation
            cv_settings (dict): settings set by segment in the config file

        Returns:
            float: Metric which should always be positive and where smaller values
                indicate better models
        """
        df_cv = cross_validation(m.model, **self.cv_settings)

        df_bias = df_cv.groupby("cutoff")[["yhat", "y"]].sum().reset_index()
        df_bias["pcnt_bias"] = df_bias["yhat"] / df_bias["y"] - 1
        # Prophet splits the historical data when doing cross validation using
        # cutoffs. The `.tail(3)` limits the periods we consider for the best
        # parameters to the 3 most recent cutoff periods.
        return np.abs(df_bias.tail(3)["pcnt_bias"].mean())

    def _auto_tuning(self, observed_df) -> ProphetForecast:
        """
        Perform automatic tuning of model parameters.

        Args:
            observed_df (pd.DataFrame): dataframe of observed data
                Expected to have columns:
                specified in the segments section of the config,
                submission_date column with unique dates corresponding to each observation and
                y column containing values of observations
        Returns:
            ProphetForecast: ProphetForecast that produced the best crossvalidation metric.
        """

        for k, v in self.grid_parameters.items():
            if not isinstance(v, list):
                self.grid_parameters[k] = [v]

        auto_param_grid = [
            dict(zip(self.grid_parameters.keys(), v))
            for v in itertools.product(*self.grid_parameters.values())
        ]

        set_params = self._get_parameters()
        for param in self.grid_parameters:
            set_params.pop(param)

        auto_param_grid = [dict(**el, **set_params) for el in auto_param_grid]

        bias = np.inf
        best_model = None
        best_params = None
        for params in auto_param_grid:
            m = ProphetForecast(**params)
            m.fit(observed_df)
            crossval_metric = self._get_crossvalidation_metric(m)
            if crossval_metric < bias:
                best_model = m
                bias = crossval_metric
                best_params = params

        # set the parameters of the current object
        # to those of the optimized ProphetForecast object
        for attr_name, best_value in best_params.items():
            setattr(self, attr_name, best_value)
        if best_model.growth == "logistic":
            # case where logistic growth is being used
            # need to set some parameters used to make training and
            # predict dfs
            self.logistic_growth_cap = best_model.logistic_growth_cap
            self.logistic_growth_floor = best_model.logistic_growth_floor

        return best_model.model

    def fit(self, observed_df: pd.DataFrame) -> object:
        """Select the best fit model and set it to the model attribute

        Args:
            observed_df (pd.DataFrame): observed data used to fit
        """
        train_dataframe = self._build_train_dataframe(observed_df)
        # model returned by _auto_tuning is already fit
        self.model = self._auto_tuning(train_dataframe)
        self.history = train_dataframe
        return self

    def predict(
        self,
        dates_to_predict_raw: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Generate forecast samples for a segment.

        Args:
            dates_to_predict (pd.DataFrame): dataframe with a single column,
            submission_date that is a string in `%Y-%m-%d` format
        Returns:
            pd.DataFrame: The forecasted values.
        """
        # add regressors, logistic growth limits (if applicable) to predict dataframe
        dates_to_predict = self._build_predict_dataframe(dates_to_predict_raw)

        # draws samples from Prophet posterior distribution, to provide percentile predictions
        samples = self.model.predictive_samples(dates_to_predict)
        df = pd.DataFrame(samples["yhat"])
        df["submission_date"] = dates_to_predict_raw
        self._validate_forecast_df(df)

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
        history_df = self.history[["ds", "y"]].copy()
        history_df["ds"] = pd.to_datetime(history_df["ds"])
        components_df = components_df.merge(
            history_df,
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


@dataclass
class FunnelForecast(BaseEnsembleForecast):
    """
    Holds the configuration and results for each segment
    in a funnel forecasting model.
    """

    model_class: object = ProphetAutotunerForecast

    def __post_init__(self, *args, **kwargs):
        super(FunnelForecast, self).__post_init__()
        if not self.model_class == ProphetAutotunerForecast:
            raise ValueError("model_class set when ProphetForecast is expected")

    def _get_parameters(self):
        parameter_dict = {}
        for el in self.parameters:
            parameter_dict[str(el["segment"])] = json.dumps(el)
        return parameter_dict


def combine_forecast_observed(
    forecast_summarized: pd.DataFrame,
    observed_summarized: pd.DataFrame,
) -> pd.DataFrame:
    """Combines the observed and forecast data as part of summarization
    Args:
        forecast_summarized (pd.DataFrame): forecast dataframe.  This dataframe should include the segments as columns
            as well as a forecast_parameters column with the forecast parameters
        observed_summarized (pd.DataFrame): observed dataframe

    Returns:
        pd.DataFrame: combined dataframe containing aggregated values from observed and forecast
    """
    # add datasource-specific metadata columns
    forecast_summarized["source"] = "forecast"
    observed_summarized["source"] = "historical"

    # create a single dataframe that contains observed and forecasted data
    df = pd.concat([observed_summarized, forecast_summarized])
    return df


def summarize_with_parameters(
    forecast_df: pd.DataFrame,
    observed_df: pd.DataFrame,
    period: str,
    numpy_aggregations: List,
    percentiles,
    segment_cols: List[str],
) -> pd.DataFrame:
    """Calculate aggregates over the forecast and observed data
        and concatenate the two dataframes for a single set of parameters
    Args:
        forecast_df (pd.DataFrame): forecast dataframe.  This dataframe should include the segments as columns
            as well as a forecast_parameters column with the forecast parameters
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
    # note that if start_date is set, it is the applied to the start of observed_df
    # and that it therefore doesn't need to be applied here
    last_historic_date = observed_df["submission_date"].max()
    forecast_df = forecast_df.loc[forecast_df["submission_date"] > last_historic_date]

    forecast_summarized, observed_summarized = aggregate_forecast_observed(
        forecast_df,
        observed_df,
        period,
        numpy_aggregations,
        percentiles,
        additional_aggregation_columns=segment_cols,
    )

    percentile_name_map = {
        f"p{percentiles[0]}": "value_low",
        f"p{percentiles[1]}": "value_mid",
        f"p{percentiles[2]}": "value_high",
        "mean": "value",
    }

    # rename forecast percentile to low, middle, high
    # rename mean to value
    forecast_summarized = forecast_summarized.rename(columns=percentile_name_map)

    df = combine_forecast_observed(forecast_summarized, observed_summarized)

    df["aggregation_period"] = period.lower()

    return df


def summarize(
    forecast_df: pd.DataFrame,
    observed_df: pd.DataFrame,
    periods: List[str] = ["day", "month"],
    numpy_aggregations: List[str] = ["mean"],
    percentiles: List[int] = [10, 50, 90],
    segment_cols: List[str] = [],
) -> None:
    """
    Summarize the forecast results over specified periods.

    Args:
        forecast_df (pd.DataFrame): forecast dataframe
        observed_df (pd.DataFrame): observed data
        periods (List[str], optional): The periods for summarization. Defaults to ["day", "month"].
        segment_cols (List of str): list of columns used for segmentation
        numpy_aggregations (List[str], optional): The numpy aggregation functions. Defaults to ["mean"].
        percentiles (List[int], optional): The percentiles for summarization. Defaults to [10, 50, 90].
    """
    if len(percentiles) != 3:
        raise ValueError(
            """
            Can only pass a list of length 3 as percentiles, for lower, mid, and upper values.
            """
        )

    summary_df = pd.concat(
        [
            summarize_with_parameters(
                forecast_df,
                observed_df,
                i,
                numpy_aggregations,
                percentiles,
                segment_cols,
            )
            for i in periods
        ]
    )

    return summary_df


def write_results(
    summary_df,
    components_df,
    segment_cols,
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
        *[bigquery.SchemaField(k, bq_types.STRING) for k in segment_cols],
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
        dataframe=summary_df,
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
        numeric_cols = list(components_df.select_dtypes(include=float).columns)
        string_cols = list(components_df.select_dtypes(include=object).columns)

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
            dataframe=components_df,
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
