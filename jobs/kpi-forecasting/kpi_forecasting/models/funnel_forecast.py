import itertools
import json
from pathlib import Path

import pandas as pd
from pandas.api import types as pd_types
import numpy as np
import prophet
from prophet.diagnostics import cross_validation

from google.cloud import bigquery
from google.cloud.bigquery.enums import SqlTypeNames as bq_types

from datetime import datetime
from dataclasses import dataclass
from kpi_forecasting.models.base_forecast import BaseForecast
from kpi_forecasting.configs.model_configs import (
    FunnelConfigs,
    Config,
    ProphetRegressor,
)
from kpi_forecasting import pandas_extras as pdx
from typing import Dict, List, Union, Tuple

MODEL_CONFIG_PATH = Path("configs/model_configs")
FUNNEL_CONFIG_FILE_NAME = "funnel_configs.toml"


@dataclass
class FunnelForecast(BaseForecast):
    funnel_config_path: str = ""

    def __post_init__(self) -> None:
        super().__post_init__()
        combination_df = (
            self.observed_df[self.metric_hub.segments.keys()]
            .value_counts()
            .reset_index(name="count")
            .drop("count", axis=1)
        )
        segment_combinations = []
        for _, row in combination_df.iterrows():
            segment_combinations.append(row.to_dict())

        funnel_configs = FunnelConfigs.collect_funnel_configs(
            self.funnel_config_path
            or Path(__file__).parent.parent
            / MODEL_CONFIG_PATH
            / FUNNEL_CONFIG_FILE_NAME
        )

        # initialize a list to hold models for each segment
        ## populate the list with segments and parameters for the segment
        segment_models = []
        for segment in segment_combinations:
            recipe_dict = {"segment": segment}
            for config in funnel_configs.configs:
                if config.metric == self.metric_hub.slug and config.segment == segment:
                    recipe_dict["config"] = config
                    break
            if "config" not in recipe_dict.keys():
                recipe_dict["config"] = Config(
                    metric=self.metric_hub.slug,
                    slug=json.dumps(segment),
                    segment=segment,
                    start_date=self.start_date,
                    holidays=None,
                    regressors=[],
                    parameters=self.parameters,
                    cv_settings=self.parameters["cv_settings"]
                    if "cv_settings" in self.parameters.keys()
                    else None,
                    use_country_holidays=self.use_holidays,
                )

            segment_models.append(recipe_dict)
        self.segment_models = segment_models

    @property
    def column_names_map(self) -> Dict[str, str]:
        return {"submission_date": "ds", "value": "y"}

    def _fill_regressor_dates(self, regressor: ProphetRegressor) -> ProphetRegressor:
        for date in ["start_date", "end_date"]:
            if getattr(regressor, date) is None:
                setattr(regressor, date, getattr(self, date))
            elif isinstance(getattr(regressor, date), str):
                setattr(regressor, date, pd.to_datetime(getattr(regressor, date)))
        return regressor

    def _build_model(
        self,
        recipe: Dict[str, Union[str, FunnelConfigs]],
        parameters: Dict[str, Union[float, str, bool]],
    ) -> prophet.Prophet:
        # Builds a Prophet class from parameters. Adds regressors and holidays
        ## from config file
        if isinstance(recipe["config"].holidays, pd.DataFrame):
            parameters["holidays"] = recipe["config"].holidays
        m = prophet.Prophet(
            **parameters,
            uncertainty_samples=self.number_of_simulations,
            mcmc_samples=0,
        )
        if recipe["config"].use_country_holidays:
            m.add_country_holidays(country_name=recipe["config"].use_country_holidays)
        for regressor in recipe["config"].regressors:
            m.add_regressor(
                regressor.name,
                prior_scale=regressor.prior_scale,
                mode=regressor.mode,
            )

        return m

    def _build_model_dataframe(
        self,
        recipe: Dict[str, Union[str, FunnelConfigs]],
        task: str,
        add_logistic_growth_cols: bool = False,
    ) -> pd.DataFrame:
        if task == "train":
            df = (
                self.observed_df.loc[
                    (
                        self.observed_df[list(recipe["segment"])]
                        == pd.Series(recipe["segment"])
                    ).all(axis=1)
                ]
                .rename(columns=self.column_names_map)
                .copy()
            )
            if add_logistic_growth_cols:
                df["floor"] = df["y"].min() * 0.5
                df["cap"] = df["y"].max() * 1.5
                recipe["logistic_growth_limits"] = {
                    "floor": df["y"].min() * 0.5,
                    "cap": df["y"].max() * 1.5,
                }
        elif task == "predict":
            df = self.dates_to_predict.rename(columns=self.column_names_map).copy()
            if add_logistic_growth_cols:
                df["floor"] = recipe["logistic_growth_limits"]["floor"]
                df["cap"] = recipe["logistic_growth_limits"]["cap"]
        else:
            raise ValueError("task not in ['train','predict']")

        if recipe["config"].regressors:
            df = self._add_regressors(df, recipe["config"].regressors)

        return df

    def _fit(self) -> None:
        # fit and save a Prophet model for each segment combination
        for recipe in self.segment_models:
            if any(
                isinstance(param, list)
                for param in recipe["config"].parameters.values()
            ):
                parameters = self._auto_tuning(recipe)
            else:
                parameters = recipe["config"].parameters

            # Initialize model; build model dataframe
            model = self._build_model(recipe, parameters)
            add_log_growth_cols = (
                "growth" in parameters.keys() and parameters["growth"] == "logistic"
            )
            test_dat = self._build_model_dataframe(recipe, "train", add_log_growth_cols)

            model.fit(test_dat)

            recipe["parameters"] = parameters
            recipe["trained_model"] = model

    def _auto_tuning(
        self, recipe: Dict[str, Union[Dict[str, str], Config]]
    ) -> Dict[str, float]:
        add_log_growth_cols = (
            "growth" in recipe["config"].parameters.keys()
            and recipe["config"].parameters["growth"] == "logistic"
        )

        for k, v in recipe["config"].parameters.items():
            if not isinstance(v, list):
                recipe["config"].parameters[k] = [v]

        param_grid = [
            dict(zip(recipe["config"].parameters.keys(), v))
            for v in itertools.product(*recipe["config"].parameters.values())
        ]

        test_dat = self._build_model_dataframe(recipe, "train", add_log_growth_cols)
        bias = []

        for params in param_grid:
            m = self._build_model(recipe, params)
            m.fit(test_dat)

            df_cv = cross_validation(m, **recipe["config"].cv_settings)

            df_bias = df_cv.groupby("cutoff")[["yhat", "y"]].sum().reset_index()
            df_bias["pcnt_bias"] = df_bias["yhat"] / df_bias["y"] - 1
            bias.append(df_bias.tail(3)["pcnt_bias"].mean())

        min_abs_bias_index = [
            x
            for x in range(len(bias))
            if bias[x] == min(np.min(bias), np.max(bias), key=np.abs)
        ][0]

        return param_grid[min_abs_bias_index]

    def _add_regressors(
        self, test_dat: pd.DataFrame, regressors: List[ProphetRegressor]
    ):
        df = test_dat.copy().rename(columns=self.column_names_map)
        df["ds"] = pd.to_datetime(df["ds"])
        for regressor in regressors:
            regressor = self._fill_regressor_dates(regressor)
            df[regressor.name] = np.where(
                (df["ds"] >= pd.to_datetime(regressor.start_date))
                & (df["ds"] <= pd.to_datetime(regressor.start_date)),
                0,
                1,
            )
        return df

    def _predict(
        self, recipe: Dict[str, Union[Dict[str, str], Config, prophet.Prophet]]
    ) -> pd.DataFrame:
        # generate the forecast samples
        add_log_growth_cols = (
            "growth" in recipe["parameters"].keys()
            and recipe["parameters"]["growth"] == "logistic"
        )
        dates_to_predict = self._build_model_dataframe(
            recipe, "predict", add_log_growth_cols
        )

        samples = recipe["trained_model"].predictive_samples(dates_to_predict)
        df = pd.DataFrame(samples["yhat"])
        df["submission_date"] = self.dates_to_predict

        component_cols = [
            "trend",
            "trend_upper",
            "trend_lower",
            "weekly",
            "weekly_upper",
            "weekly_lower",
            "yearly",
            "yearly_upper",
            "yearly_lower",
            "multiplicative_terms",
            "multiplicative_terms_upper",
            "multiplicative_terms_lower",
            "additive_terms",
            "additive_terms_upper",
            "additive_terms_lower",
        ]

        recipe["component_df"] = recipe["trained_model"].predict(dates_to_predict)

        return df

    def _validate_forecast_df(self, df: pd.DataFrame) -> None:
        """Validate that `forecast_df` has been generated correctly for each segment."""
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
            if not pd_types.is_numeric_dtype(df[i]):
                raise ValueError(
                    "All forecast_df columns except 'submission_date' and segment dims must be numeric,"
                    f" but column {i} has type {df[i].dtypes}."
                )

    def _summarize(
        self,
        segment_results: Dict[str, Union[str, Dict, pd.DataFrame]],
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
        observed_summarized = pdx.aggregate_to_period(
            self.observed_df.loc[
                (
                    self.observed_df[list(segment_results["segment"])]
                    == pd.Series(segment_results["segment"])
                ).all(axis=1)
            ],
            period,
        )
        forecast_agg = pdx.aggregate_to_period(segment_results["forecast_df"], period)

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
        if "holidays" in segment_results["parameters"].keys() and isinstance(
            segment_results["parameters"]["holidays"], pd.DataFrame
        ):
            segment_results["parameters"]["holidays"] = (
                segment_results["parameters"]["holidays"]["holiday"].unique().tolist()
            )
        df["forecast_parameters"] = json.dumps(segment_results["parameters"])

        return df

    def predict(self) -> None:
        """Generate a forecast from `start_date` to `end_date`."""
        print(f"Forecasting from {self.start_date} to {self.end_date}.", flush=True)
        self._set_seed()
        self.predicted_at = datetime.utcnow()

        for recipe in self.segment_models:
            forecast_df = self._predict(recipe)
            self._validate_forecast_df(forecast_df)

            recipe["forecast_df"] = forecast_df.copy(deep=True)
            del forecast_df

    def summarize(
        self,
        periods: List[str] = ["day", "month"],
        numpy_aggregations: List[str] = ["mean"],
        percentiles: List[int] = [10, 50, 90],
    ) -> None:
        summary_df_list = []
        component_df_list = []
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
            for dim, dim_value in segment["segment"].items():
                summary_df[dim] = dim_value
                segment["component_df"][dim] = dim_value
            summary_df_list.append(summary_df.copy(deep=True))
            component_df_list.append(segment["component_df"])
            del summary_df

        self.summary_df = pd.concat(summary_df_list, ignore_index=True)
        self.component_df_list = component_df_list

    def write_results(
        self,
        project: str,
        forecast_dataset: str,
        forecast_table: str,
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
            write_disposition (str): In the event that the destination table exists,
                should the table be overwritten ("WRITE_TRUNCATE") or appended to
                ("WRITE_APPEND")?
        """
        print(
            f"Writing results to `{project}.{forecast_dataset}.{forecast_table}`.",
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
            destination=f"{project}.{forecast_dataset}.{forecast_table}",
            job_config=bigquery.LoadJobConfig(
                schema=schema,
                autodetect=False,
                write_disposition=write_disposition,
            ),
        )
        # Wait for the job to complete.
        job.result()

        if components_table:
            components_df = pd.concat(self.component_df_list, ignore_index=True)
            numeric_cols = components_df.dtypes[
                components_df.dtypes == float
            ].index.tolist()
            string_cols = components_df.dtypes[
                components_df.dtypes == object
            ].index.tolist()
            components_df["metric_slug"] = self.metric_hub.slug
            components_df["trained_at"] = self.trained_at

            schema = [
                bigquery.SchemaField("metric_slug", bq_types.STRING),
                bigquery.SchemaField("trained_at", bq_types.TIMESTAMP),
            ]
            schema += [
                bigquery.SchemaField(col, bq_types.STRING) for col in string_cols
            ]
            schema += [
                bigquery.SchemaField(col, bq_types.FLOAT) for col in numeric_cols
            ]

            if not components_dataset:
                components_dataset = forecast_dataset

            job = client.load_table_from_dataframe(
                dataframe=components_df,
                destination=f"{project}.{components_dataset}.{components_table}",
                job_config=bigquery.LoadJobConfig(
                    schema=schema,
                    autodetect=False,
                    write_disposition=write_disposition,
                ),
            )

            job.result()
