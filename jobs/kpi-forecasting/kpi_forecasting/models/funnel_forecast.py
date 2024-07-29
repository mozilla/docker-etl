from dataclasses import dataclass, field
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

from kpi_forecasting.configs.model_inputs import (
    ProphetHoliday,
    ProphetRegressor,
    holiday_collection,
    regressor_collection,
)
from kpi_forecasting.models.prophet_forecast import ProphetForecast


@dataclass
class SegmentModelSettings:
    """
    Holds the configuration and results for each segment
    in a funnel forecasting model.
    """

    segment: Dict[str, str]
    start_date: str
    end_date: str
    grid_parameters: Dict[str, Union[List[float], float]]
    cv_settings: Dict[str, str]
    holidays: list = field(default_factory=list[ProphetHoliday])
    regressors: list = field(default_factory=list[ProphetRegressor])

    # Hold results as models are trained and forecasts made
    segment_model: prophet.Prophet = None
    trained_parameters: dict = field(default_factory=dict[str, str])
    forecast_df: pd.DataFrame = None
    components_df: pd.DataFrame = None


@dataclass
class FunnelForecast(ProphetForecast):
    """
    FunnelForecast class for generating and managing forecast models. The class handles
    cases where forecasts for a combination of dimensions are required for a metric.

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

        if self.metric_hub is None:
            # this is used to avoid the code below for testing purposes
            return

        # Overwrite dates_to_predict to provide historical date forecasts
        self.dates_to_predict = pd.DataFrame(
            {
                "submission_date": pd.date_range(
                    self.metric_hub.start_date, self.end_date
                ).date
            }
        )

        self._set_segment_models(self.observed_df, self.metric_hub.segments.keys())

        # initialize unset attributes
        self.components_df = None

    def _set_segment_models(
        self, observed_df: pd.DataFrame, segment_column_list: list
    ) -> None:
        """Creates a SegmentSettings object for each segment specified in the
            metric_hub.segments section of the config.  These objects are stored in a list
            in the segment_models attribute
            Parameters can be specified independently for at most one dimension column
            set using model_setting_split_dim in self.parameters

        Args:
            observed_df (pd.DataFrame): dataframe containing observed data used to model
                must contain columns specified in the keys of the segments section of the config
            segment_column_list (list): list of columns of observed_df to use to determine segments
        """
        # Construct a DataFrame containing all combination of segment values
        ## in the observed_df
        combination_df = observed_df[segment_column_list].drop_duplicates()

        # Construct dictionaries from those combinations
        segment_combinations = combination_df.to_dict("records")

        # initialize a list to hold models for each segment
        ## populate the list with segments and parameters for the segment
        split_dim = self.parameters["model_setting_split_dim"]

        # check to make sure split_dim is one of the columns set in segment_column_list
        if split_dim not in segment_column_list:
            columns_str = ",".join(segment_column_list)
            raise ValueError(
                f"model_setting_split_dim set to {split_dim} which is not among segment columns: {columns_str}"
            )

        # For each segment combinination, get the model parameters from the config
        ## file. Parse the holidays and regressors specified in the config file.
        segment_models = []
        for segment in segment_combinations:
            model_params = getattr(
                self.parameters["segment_settings"], segment[split_dim]
            )

            holiday_list = []
            regressor_list = []

            if model_params["holidays"]:
                holiday_list = [
                    getattr(holiday_collection.data, h)
                    for h in model_params["holidays"]
                ]
            if model_params["regressors"]:
                regressor_list = [
                    getattr(regressor_collection.data, r)
                    for r in model_params["regressors"]
                ]

            # Create a SegmentModelSettings object for each segment combination
            segment_models.append(
                SegmentModelSettings(
                    segment=segment,
                    start_date=model_params["start_date"],
                    end_date=self.end_date,
                    holidays=[ProphetHoliday(**h) for h in holiday_list],
                    regressors=[ProphetRegressor(**r) for r in regressor_list],
                    grid_parameters=dict(model_params["grid_parameters"]),
                    cv_settings=dict(model_params["cv_settings"]),
                )
            )
        self.segment_models = segment_models

    @property
    def column_names_map(self) -> Dict[str, str]:
        """
        Map column names from the dataset to the names required by Prophet.

        Returns:
            Dict[str, str]: Mapping of column names.
        """
        return {"submission_date": "ds", "value": "y"}

    def _fill_regressor_dates(self, regressor: ProphetRegressor) -> ProphetRegressor:
        """
        Fill missing start and end dates for a regressor. A ProphetRegressor can be created
        without a 'start_date' or 'end_date' being supplied, so this checks for either date attr
        being missing and fills in with the appropriate date: if 'start_date' is missing, it assumes
        that the regressor starts at the beginning of the observed data; if 'end_date' is missing,
        it assumes that the regressor should be filled until the end of the forecast period.

        Args:
            regressor (ProphetRegressor): The regressor to fill dates for.

        Returns:
            ProphetRegressor: The regressor with filled dates.
        """

        for date in ["start_date", "end_date"]:
            if getattr(regressor, date) is None:
                setattr(regressor, date, getattr(self, date))
            elif isinstance(getattr(regressor, date), str):
                setattr(regressor, date, pd.to_datetime(getattr(regressor, date)))

        if regressor.end_date < regressor.start_date:
            raise Exception(
                f"Regressor {regressor.name} start date comes after end date"
            )
        return regressor

    def _build_model(
        self,
        segment_settings: SegmentModelSettings,
        parameters: Dict[str, Union[float, str, bool]],
    ) -> prophet.Prophet:
        """
        Build a Prophet model from parameters.

        Args:
            segment_settings (SegmentModelSettings): The settings for the segment.
            parameters (Dict[str, Union[float, str, bool]]): The parameters for the model.

        Returns:
            prophet.Prophet: The Prophet model.
        """
        if segment_settings.holidays:
            parameters["holidays"] = pd.concat(
                [
                    pd.DataFrame(
                        {
                            "holiday": h.name,
                            "ds": pd.to_datetime(h.ds),
                            "lower_window": h.lower_window,
                            "upper_window": h.upper_window,
                        }
                    )
                    for h in segment_settings.holidays
                ],
                ignore_index=True,
            )

        m = prophet.Prophet(
            **parameters,
            uncertainty_samples=self.number_of_simulations,
            mcmc_samples=0,
        )
        for regressor in segment_settings.regressors:
            m.add_regressor(
                regressor.name,
                prior_scale=regressor.prior_scale,
                mode=regressor.mode,
            )

        return m

    def _build_train_dataframe(
        self,
        observed_df,
        segment_settings: SegmentModelSettings,
        add_logistic_growth_cols: bool = False,
    ) -> pd.DataFrame:
        """
        Build the model dataframe for training

        Args:
            observed_df: dataframe of observed data
            segment_settings (SegmentModelSettings): The settings for the segment.
            add_logistic_growth_cols (bool, optional): Whether to add logistic growth columns. Defaults to False.

        Returns:
            pd.DataFrame: The dataframe for the model.
        """

        # find indices in observed_df for rows that exactly match segment dict
        segment_historical_indices = (
            observed_df[list(segment_settings.segment)]
            == pd.Series(segment_settings.segment)
        ).all(axis=1)
        df = (
            observed_df.loc[
                (segment_historical_indices)
                & (  # filter observed_df if segment start date > metric_hub start date
                    observed_df["submission_date"]
                    >= datetime.strptime(segment_settings.start_date, "%Y-%m-%d").date()
                )
            ]
            .rename(columns=self.column_names_map)
            .copy()
        )
        # define limits for logistic growth
        if add_logistic_growth_cols:
            df["floor"] = df["y"].min() * 0.5
            df["cap"] = df["y"].max() * 1.5

        if segment_settings.regressors:
            df = self._add_regressors(df, segment_settings.regressors)
        return df

    def _build_predict_dataframe(
        self,
        dates_to_predict: pd.DataFrame,
        segment_settings: SegmentModelSettings,
        add_logistic_growth_cols: bool = False,
    ) -> pd.DataFrame:
        """creates dataframe used for prediction

        Args:
            dates_to_predict (pd.DataFrame): dataframe of dates to predict
            segment_settings (SegmentModelSettings): settings related to the segment
            add_logistic_growth_cols (bool):  Whether to add logistic growth columns. Defaults to False.


        Returns:
            pd.DataFrame: dataframe to use used in prediction
        """
        # predict dataframe only needs dates to predict, logistic growth limits, and regressors
        df = dates_to_predict.rename(columns=self.column_names_map).copy()
        if add_logistic_growth_cols:
            df["floor"] = segment_settings.trained_parameters["floor"]
            df["cap"] = segment_settings.trained_parameters["cap"]

        if segment_settings.regressors:
            df = self._add_regressors(df, segment_settings.regressors)

        return df

    def _fit(self, observed_df: pd.DataFrame) -> None:
        """
        Fit and save a Prophet model for each segment combination.

        Args:
            observed_df (pd.DataFrame): dataframe of observations.  Expected to have columns
                specified in the segments section of the config,
                submission_date column with unique dates corresponding to each observation and
                y column containing values of observations
        """
        for segment_settings in self.segment_models:
            parameters = self._auto_tuning(observed_df, segment_settings)

            # Initialize model; build model dataframe
            add_log_growth_cols = (
                "growth" in parameters.keys() and parameters["growth"] == "logistic"
            )
            test_dat = self._build_train_dataframe(
                observed_df, segment_settings, add_log_growth_cols
            )
            model = self._build_model(segment_settings, parameters)

            model.fit(test_dat)
            if add_log_growth_cols:
                # all values in these colunns are the same
                parameters["floor"] = test_dat["floor"].values[0]
                parameters["cap"] = test_dat["cap"].values[0]

            if "holidays" in parameters.keys():
                parameters["holidays"] = (
                    parameters["holidays"]["holiday"].unique().tolist()
                )
            segment_settings.trained_parameters = parameters
            segment_settings.segment_model = model

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

    def _auto_tuning(
        self, observed_df, segment_settings: SegmentModelSettings
    ) -> Dict[str, float]:
        """
        Perform automatic tuning of model parameters.

        Args:
            observed_df (pd.DataFrame): dataframe of observed data
                Expected to have columns:
                specified in the segments section of the config,
                submission_date column with unique dates corresponding to each observation and
                y column containing values of observations
            segment_settings (SegmentModelSettings): The settings for the segment.

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
                m, **segment_settings.cv_settings
            )
            bias.append(crossval_metric)

        min_abs_bias_index = np.argmin(np.abs(bias))

        return param_grid[min_abs_bias_index]

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
            regressor = self._fill_regressor_dates(regressor)
            # finds rows where date is in regressor date ranges and sets that regressor
            ## value to 1, else 0
            df[regressor.name] = (
                (df["ds"] >= pd.to_datetime(regressor.start_date).date())
                & (df["ds"] <= pd.to_datetime(regressor.end_date).date())
            ).astype(int)
        return df

    def _predict(
        self, dates_to_predict_raw: pd.DataFrame, segment_settings: SegmentModelSettings
    ) -> pd.DataFrame:
        """
        Generate forecast samples for a segment.

        Args:
            dates_to_predict (pd.DataFrame): dataframe of dates to predict
            segment_settings (SegmentModelSettings): The settings for the segment.

        Returns:
            pd.DataFrame: The forecasted values.
        """
        add_log_growth_cols = (
            "growth" in segment_settings.trained_parameters.keys()
            and segment_settings.trained_parameters["growth"] == "logistic"
        )
        # add regressors, logistic growth limits (if applicable) to predict dataframe
        dates_to_predict = self._build_predict_dataframe(
            dates_to_predict_raw, segment_settings, add_log_growth_cols
        )

        # draws samples from Prophet posterior distribution, to provide percentile predictions
        samples = segment_settings.segment_model.predictive_samples(dates_to_predict)
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
        components_df = segment_settings.segment_model.predict(dates_to_predict)[
            component_cols
        ]

        # join observed data to components df, which allows for calc of intra-sample
        # error rates and how components resulted in those predictions. The `fillna`
        # call will fill the missing y values for forecasted dates, where only yhat
        # is available.
        components_df = components_df.merge(
            segment_settings.segment_model.history[["ds", "y"]],
            on="ds",
            how="left",
        ).fillna(0)
        components_df.rename(columns={"ds": "submission_date"}, inplace=True)

        segment_settings.components_df = components_df.copy()

        return df.loc[
            pd.to_datetime(df["submission_date"]) >= pd.to_datetime(self.start_date)
        ]

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
        """Calculate aggregates over the forecase and observed data
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
        segment_settings: SegmentModelSettings,
        period: str,
        numpy_aggregations: List[str],
        percentiles: List[int] = [10, 50, 90],
    ) -> pd.DataFrame:
        """
        Calculate summary metrics on a specific segment
        for `forecast_df` over a given period, and add metadata.

        Args:
            segment_settings (SegmentModelSettings): The settings for the segment.
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

    def predict(self) -> None:
        """Generate a forecast from `start_date` to `end_date`."""
        print(f"Forecasting from {self.start_date} to {self.end_date}.", flush=True)
        self._set_seed()
        self.predicted_at = datetime.utcnow()

        for segment_settings in self.segment_models:
            forecast_df = self._predict(self.dates_to_predict, segment_settings)
            self._validate_forecast_df(forecast_df)

            segment_settings.forecast_df = forecast_df

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
            numeric_cols = self.components_df.dtypes[
                self.components_df.dtypes is float
            ].index.tolist()
            string_cols = self.components_df.dtypes[
                self.components_df.dtypes is object
            ].index.tolist()
            self.components_df["metric_slug"] = self.metric_hub.slug
            self.components_df["forecast_trained_at"] = self.trained_at

            schema = [
                bigquery.SchemaField("submission_date", bq_types.DATE),
                bigquery.SchemaField("metric_slug", bq_types.STRING),
                bigquery.SchemaField("forecast_trained_at", bq_types.TIMESTAMP),
            ]
            schema += [
                bigquery.SchemaField(col, bq_types.STRING) for col in string_cols
            ]
            schema += [
                bigquery.SchemaField(col, bq_types.FLOAT) for col in numeric_cols
            ]

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
