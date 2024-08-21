import json
import pandas as pd
from pandas.api import types as pd_types
import prophet
import numpy as np
from dataclasses import dataclass, field
from typing import Dict, List, Union


from datetime import datetime, timezone, timedelta
from kpi_forecasting.models.base_forecast import BaseForecast
from kpi_forecasting import pandas_extras as pdx
from google.cloud import bigquery
from google.cloud.bigquery.enums import SqlTypeNames as bq_types

from kpi_forecasting.configs.model_inputs import ProphetHoliday, ProphetRegressor


@dataclass
class ProphetDataManager:
    """Class for managing data to send to prophet

    Args:
        end_date (str): A 'YYYY-MM-DD' formatted-string that specifies the last
            date the metric should be queried.
        predict_historical_dates (bool):  If True, forecast starts at the first
            date in the observed data.  If False, it uses the value of start_date it set
            and the first day after the observed data ends otherwise
    """

    end_date: str
    predict_historical_dates: bool = False


@dataclass
class ProphetForecast(BaseForecast):
    """
    Holds the configuration and results for each segment
    in a funnel forecasting model.

    Args:
        holidays (list): list of ProphetHoliday objects used
            to specify holidays in the Propohet model. Used to create
            the dataframe passed to prophet under the holidays key
        regressors (list): list of ProphetRegressor objects,
            used to set regressors in the Prophet object and
            create them in the data
        use_all_us_holidays (bool): When True, `model.add_country_holidays(country_name="US")`
            is called on the prophet model
        growth (str): Used in Prophet object initialization
            'linear', 'logistic' or 'flat' to specify a linear, logistic or
            flat trend.
        changepoints (list): Used in Prophet object initialization
            List of dates at which to include potential changepoints. If
            not specified, potential changepoints are selected automatically.
        n_changepoints (int): Used in Prophet object initialization
            Number of potential changepoints to include. Not used
            if input `changepoints` is supplied. If `changepoints` is not supplied,
            then n_changepoints potential changepoints are selected uniformly from
            the first `changepoint_range` proportion of the history.
        changepoint_range (float): Used in Prophet object initialization
            Proportion of history in which trend changepoints will
            be estimated. Defaults to 0.8 for the first 80%. Not used if
            `changepoints` is specified.
        yearly_seasonality: Used in Prophet object initialization
            Fit yearly seasonality.
            Can be 'auto', True, False, or a number of Fourier terms to generate.
        weekly_seasonality : Used in Prophet object initialization
            Fit weekly seasonality.
            Can be 'auto', True, False, or a number of Fourier terms to generate.
        daily_seasonality: Used in Prophet object initialization
            Fit daily seasonality.
            Can be 'auto', True, False, or a number of Fourier terms to generate.
        seasonality_mode: Used in Prophet object initialization
            'additive' (default) or 'multiplicative'.
        seasonality_prior_scale: Used in Prophet object initialization
            Parameter modulating the strength of the
            seasonality model. Larger values allow the model to fit larger seasonal
            fluctuations, smaller values dampen the seasonality. Can be specified
            for individual seasonalities using add_seasonality.
        holidays_prior_scale: Used in Prophet object initialization
            Parameter modulating the strength of the holiday
            components model, unless overridden in the holidays input.
        changepoint_prior_scale: Used in Prophet object initialization
            Parameter modulating the flexibility of the
            automatic changepoint selection. Large values will allow many
            changepoints, small values will allow few changepoints.
        mcmc_samples (int): Used in Prophet object initialization
            If greater than 0, will do full Bayesian inference
            with the specified number of MCMC samples. If 0, will do MAP
            estimation.
        interval_width (float): Used in Prophet object initialization
            width of the uncertainty intervals provided
            for the forecast. If mcmc_samples=0, this will be only the uncertainty
            in the trend using the MAP estimate of the extrapolated generative
            model. If mcmc.samples>0, this will be integrated over all model
            parameters, which will include uncertainty in seasonality.
        uncertainty_samples: Used in Prophet object initialization
            Number of simulated draws used to estimate
            uncertainty intervals. Settings this value to 0 or False will disable
            uncertainty estimation and speed up the calculation.
        stan_backend (str): Used in Prophet object initialization
             str as defined in StanBackendEnum default: None - will try to
            iterate over all available backends and find the working one
        holidays_mode (str): Used in Prophet object initialization
            'additive' or 'multiplicative'. Defaults to seasonality_mode.
    """

    holidays: list = field(default_factory=list[ProphetHoliday])
    regressors: list = field(default_factory=list[ProphetRegressor])
    use_all_us_holidays: bool = False

    # these are the arguments used to initialize the Prophet object
    growth = "linear"
    changepoints = None
    n_changepoints = 25
    changepoint_range = 0.8
    yearly_seasonality = "auto"
    weekly_seasonality = "auto"
    daily_seasonality = "auto"
    holidays = None
    seasonality_mode = "additive"
    seasonality_prior_scale = 10.0
    holidays_prior_scale = 10.0
    changepoint_prior_scale = 0.05
    mcmc_samples = 0
    interval_width = 0.80
    uncertainty_samples = 1000
    stan_backend = None
    scaling: str = "absmax"
    holidays_mode = None
    number_of_simulations: int = 1000

    def __post_init__(self):
        holiday_list = []
        regressor_list = []

        if self.holidays == []:
            self.holidays = None
        elif self.holidays:
            holiday_list = [ProphetHoliday(**h) for h in self.holidays]
            holiday_df = pd.concat(
                [
                    pd.DataFrame(
                        {
                            "holiday": h.name,
                            "ds": pd.to_datetime(h.ds),
                            "lower_window": h.lower_window,
                            "upper_window": h.upper_window,
                        }
                    )
                    for h in holiday_list
                ],
                ignore_index=True,
            )
            self.holidays = holiday_df
        if self.regressors:
            regressor_list = [ProphetRegressor(**r) for r in self.regressors]
            self.regressors = regressor_list

        self.model = self._build_model()

    def _build_model(self) -> prophet.Prophet:
        """
        Build a Prophet model from parameters.

        Args:
            segment_settings (FunnelSegmentModelSettings): The settings for the segment.
            parameters (Dict[str, Union[float, str, bool]]): The parameters for the model.

        Returns:
            prophet.Prophet: The Prophet model.
        """

        model = prophet.Prophet(
            growth=self.growth,
            changepoints=self.changepoints,
            n_changepoints=self.n_changepoints,
            changepoint_range=self.changepoint_range,
            yearly_seasonality=self.yearly_seasonality,
            weekly_seasonality=self.weekly_seasonality,
            daily_seasonality=self.daily_seasonality,
            holidays=self.holidays,
            seasonality_mode=self.seasonality_mode,
            seasonality_prior_scale=self.seasonality_prior_scale,
            holidays_prior_scale=self.holidays_prior_scale,
            changepoint_prior_scale=self.changepoint_prior_scale,
            mcmc_samples=self.mcmc_samples,
            interval_width=self.interval_width,
            uncertainty_samples=self.uncertainty_samples,
            stan_backend=self.stan_backend,
            scaling=self.scaling,
            holidays_mode=self.holidays_mode,
        )

        for regressor in self.regressors:
            model.add_regressor(
                regressor.name,
                prior_scale=regressor.prior_scale,
                mode=regressor.mode,
            )

        if self.use_all_us_holidays:
            model.add_country_holidays(country_name="US")

        return model

    @property
    def column_names_map(self) -> Dict[str, str]:
        return {"submission_date": "ds", "value": "y"}

    def _add_regressors(self, df: pd.DataFrame, regressors: List[ProphetRegressor]):
        """
        Add regressor columns to the dataframe for training or prediction.

        Args:
            df (pd.DataFrame): The input dataframe.
            regressors (List[ProphetRegressor]): The list of regressors to add.

        Returns:
            pd.DataFrame: The dataframe with regressors added.
        """
        for regressor in regressors:
            regressor_time_filter = [True] * len(df)
            if regressor.start_date:
                regressor_time_filter &= (
                    df["ds"] >= pd.to_datetime(regressor.start_date).date()
                )
            if regressor.end_date:
                regressor_time_filter &= (
                    df["ds"] <= pd.to_datetime(regressor.end_date).date()
                )
            # finds rows where date is in regressor date ranges and sets that regressor
            ## value to 0, else 1
            df[regressor.name] = (~(regressor_time_filter)).astype(int)
        return df

    def _set_seed(self) -> None:
        """Set random seed to ensure that fits and predictions are reproducible."""
        np.random.seed(42)

    def _build_train_dataframe(self, observed_df) -> pd.DataFrame:
        """
        Build the model dataframe for training

        Args:
            observed_df: dataframe of observed data
            segment_settings (FunnelSegmentModelSettings): The settings for the segment.
            add_logistic_growth_cols (bool, optional): Whether to add logistic growth columns. Defaults to False.

        Returns:
            pd.DataFrame: The dataframe for the model.
        """

        # define limits for logistic growth
        observed_df = observed_df.rename(columns=self.column_names_map)

        if self.growth == "logistic":
            self.logistic_growth_floor = observed_df["y"].min() * 0.5
            observed_df["floor"] = self.logistic_growth_floor
            self.logistic_growth_cap = observed_df["y"].max() * 1.5
            observed_df["cap"] = self.logistic_growth_cap

        if self.regressors:
            observed_df = self._add_regressors(observed_df, self.regressors)

        return observed_df

    def _build_predict_dataframe(self, dates_to_predict: pd.DataFrame) -> pd.DataFrame:
        """creates dataframe used for prediction

        Args:
            dates_to_predict (pd.DataFrame): dataframe of dates to predict
            segment_settings (FunnelSegmentModelSettings): settings related to the segment
            add_logistic_growth_cols (bool):  Whether to add logistic growth columns. Defaults to False.


        Returns:
            pd.DataFrame: dataframe to use used in prediction
        """
        # predict dataframe only needs dates to predict, logistic growth limits, and regressors
        df = dates_to_predict.rename(columns=self.column_names_map).copy()
        if self.growth == "logistic":
            df["floor"] = self.logistic_growth_floor
            df["cap"] = self.logistic_growth_cap

        if self.regressors:
            df = self._add_regressors(df, self.regressors)

        return df

    def fit(self, observed_df) -> None:
        # Modify observed data to have column names that Prophet expects, and fit
        # the model
        train_dataframe = self._build_train_dataframe(observed_df)
        self.model.fit(train_dataframe)

    def predict(self, dates_to_predict) -> pd.DataFrame:
        # generate the forecast samples
        samples = self.model.predictive_samples(
            dates_to_predict.rename(columns=self.column_names_map)
        )
        df = pd.DataFrame(samples["yhat"])
        df["submission_date"] = dates_to_predict
        self._validate_forecast_df(df, dates_to_predict)

        return df

    def _validate_forecast_df(self, df, dates_to_predict) -> None:
        """Validate that `self.forecast_df` has been generated correctly."""
        columns = df.columns
        expected_shape = (len(dates_to_predict), 1 + self.number_of_simulations)
        numeric_columns = df.drop(columns="submission_date").columns

        if "submission_date" not in columns:
            raise ValueError("forecast_df must contain a 'submission_date' column.")

        if df.shape != expected_shape:
            raise ValueError(
                f"Expected forecast_df to have shape {expected_shape}, but it has shape {df.shape}."
            )

        if not df["submission_date"].equals(dates_to_predict["submission_date"]):
            raise ValueError(
                "forecast_df['submission_date'] does not match dates_to_predict['submission_date']."
            )

        for i in numeric_columns:
            if not pd_types.is_numeric_dtype(df[i]):
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
            json.dumps({**self.parameters, "holidays": self.use_all_us_holidays})
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


def aggregate_forecast_observed(
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
    )

    return forecast_summarized, observed_summarized


def combine_forecast_observed(forecast_summarized, observed_summarized):
    # remaining column of metric values get the column name 'value'
    forecast_summarized = forecast_summarized.melt(
        id_vars="submission_date", var_name="measure"
    )
    observed_summarized["measure"] = "observed"

    # add datasource-specific metadata columns
    forecast_summarized["source"] = "forecast"
    observed_summarized["source"] = "historical"

    df = pd.concat([forecast_summarized, observed_summarized])

    return df


def summarize(
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

    forecast_summarized, observed_summarized = aggregate_forecast_observed(
        forecast_df, observed_df, period, numpy_aggregations, percentiles
    )

    df = combine_forecast_observed(forecast_summarized, observed_summarized)
    # add summary metadata columns
    df["aggregation_period"] = period.lower()

    return df


def summarize_legacy(summary_df) -> pd.DataFrame:
    """
    Converts a `self.summary_df` to the legacy format used in
    `moz-fx-data-shared-prod.telemetry_derived.kpi_automated_forecast_confidences_v1`
    """
    # TODO: This method should be removed once the forecasting data model is updated:
    # https://mozilla-hub.atlassian.net/browse/DS-2676

    # rename columns to legacy values
    df = summary_df.rename(
        columns={
            "forecast_end_date": "asofdate",
            "submission_date": "date",
            "metric_alias": "target",
            "aggregation_period": "unit",
        }
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
    summary_df: pd.DataFrame,
    summary_df_legacy: pd.DataFrame,
    forecast_df_legacy: pd.DataFrame,
    project: str,
    dataset: str,
    table: str,
    project_legacy: str,
    dataset_legacy: str,
    write_disposition: str = "WRITE_APPEND",
    forecast_table_legacy: str = "kpi_automated_forecast_v1_branch",
    confidences_table_legacy: str = "kpi_automated_forecast_confidences_v1_branch",
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

    # TODO: remove the below jobs once the forecasting data model is updated:
    # https://mozilla-hub.atlassian.net/browse/DS-2676

    job = client.load_table_from_dataframe(
        dataframe=forecast_df_legacy,
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
        dataframe=summary_df_legacy,
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
