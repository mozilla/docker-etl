from kpi_forecasting.models.funnel_forecast import FunnelForecast, SegmentModelSettings
from dataclasses import dataclass
import pandas as pd
from typing import List, Dict, Union
from google.cloud import bigquery
import prophet
from datetime import datetime

client = bigquery.Client("moz-fx-data-bq-data-science")


@dataclass
class ExtraRegressorProphet(FunnelForecast):

    def __post_init__(self) -> None:
        super().__post_init__()

        self.regressor_data = self._pull_regressor_data()

    def _pull_regressor_data(self) -> pd.DataFrame:

        metric_list = [
            "search_forecasting_daily_active_users",
            "search_forecasting_search_count",
        ]
        query_list = [self._build_regressor_query(metric) for metric in metric_list]

        query = f"""
        SELECT
            *
        FROM
            ({query_list[0]})
        LEFT JOIN
            ({query_list[1]})
        USING (ds, device, country, channel, partner)
        """

        return client.query(query).to_dataframe()

    def _build_regressor_query(self, metric_name: str) -> str:
        return f"""
        SELECT
            value AS {metric_name},
            device,
            country,
            channel,
            partner,
            submission_date AS ds
        FROM
            `moz-fx-data-bq-data-science.mbowerman.joined_final_temp`
        WHERE
            forecast_start_date = '{self.start_date.strftime("%Y-%m-%d")}'
            AND metric_alias = '{metric_name}'
            AND aggregation_period = 'day'
            AND forecast_trained_at = (
                SELECT 
                    MAX(forecast_trained_at)
                FROM
                    `moz-fx-data-bq-data-science.mbowerman.joined_final_temp`
                WHERE
                    forecast_start_date = '{self.start_date.strftime("%Y-%m-%d")}'
                    AND metric_alias = '{metric_name}'
                )
        """

    def _build_model(
        self,
        segment_settings: SegmentModelSettings,
        parameters: Dict[str, Union[float, str, bool]],
    ) -> prophet.Prophet:
        # Builds a Prophet class from parameters. Adds regressors and holidays
        ## from config file
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
        for metric in [
            "search_forecasting_daily_active_users",
            "search_forecasting_search_count",
        ]:
            m.add_regressor(
                metric,
            )

        return m

    def _build_model_dataframe(
        self,
        segment_settings: SegmentModelSettings,
        task: str,
        add_logistic_growth_cols: bool = False,
    ) -> pd.DataFrame:
        # build training dataframe
        if task == "train":
            df = (
                self.observed_df.loc[
                    (  # filter observed_df to rows that exactly match segment dict
                        (
                            self.observed_df[list(segment_settings.segment)]
                            == pd.Series(segment_settings.segment)
                        ).all(axis=1)
                    )
                    & (  # filter observed_df if segment start date > metric_hub start date
                        self.observed_df["submission_date"]
                        >= datetime.strptime(
                            segment_settings.start_date, "%Y-%m-%d"
                        ).date()
                    )
                ]
                .rename(columns=self.column_names_map)
                .copy()
            ).merge(
                self.regressor_data,
                how="left",
                on=["ds", "device", "country", "partner", "channel"],
            )
            # define limits for logistic growth
            if add_logistic_growth_cols:
                df["floor"] = df["y"].min() * 0.5
                df["cap"] = df["y"].max() * 1.5

        # predict dataframe only needs dates to predict, logistic growth limits, and regressors
        elif task == "predict":
            df = (
                self.dates_to_predict.rename(columns=self.column_names_map)
                .copy()
                .merge(
                    self.regressor_data.loc[
                        (
                            (
                                self.regressor_data[list(segment_settings.segment)]
                                == pd.Series(segment_settings.segment)
                            ).all(axis=1)
                        )
                    ]
                )
            )
            if add_logistic_growth_cols:
                df["floor"] = segment_settings.trained_parameters["floor"]
                df["cap"] = segment_settings.trained_parameters["cap"]
        else:
            raise ValueError("task not in ['train','predict']")

        if segment_settings.regressors:
            df = self._add_regressors(df, segment_settings.regressors)

        return df
