import json
import pandas as pd
import prophet

from datetime import datetime
from dataclasses import dataclass
from models.base_forecast import BaseForecast
from typing import Dict


@dataclass
class ProphetForecast(BaseForecast):
    @property
    def _column_mapping(self) -> Dict[str, str]:
        """Map Metric Hub column names to the names that Prophet expects."""
        return {
            "submission_date": "ds",
            self.metric_hub.alias: "y",
        }

    def _fit(self) -> None:
        """
        Fit a Prophet model using the `observed_df` that was generated using
        Metric Hub. This method updates `self.model`.
        """
        self.model = prophet.Prophet(**self.parameters, mcmc_samples=0)

        if self.use_holidays:
            self.model.add_country_holidays(country_name="US")

        # Rename training data to have column names that Prophet expects. We
        # create a copy here so that we don't modify the original dataframe.
        train = self.observed_df.copy(deep=True)
        train.rename(columns=self._column_mapping, inplace=True)

        # fit the model
        self.model.fit(train)

    def _predict(self) -> None:
        """
        Forecast using `self.model`. This method updates `self.forecast_df`.
        """
        # generate the range of dates to forecast
        future_dates = pd.DataFrame(
            {"ds": pd.date_range(self.start_date, self.end_date).date}
        )
        self.forecast_df = self.model.predictive_samples(future_dates)

    def _predict_legacy(self) -> None:
        """
        Recreate the legacy Big Query data model.
        """
        future_dates = pd.DataFrame(
            {"ds": pd.date_range(self.start_date, self.end_date).date}
        )
        df = self.model.predict(future_dates)

        # set legacy column values
        df["metric"] = self.metric_hub.alias
        df["forecast_date"] = str(datetime.utcnow().date())
        df["forecast_parameters"] = str(
            json.dumps({**self.parameters, "holidays": self.use_holidays})
        )

        if "desktop" in self.metric_hub.app_name:
            df["target"] = "desktop"
        elif "mobile" in self.metric_hub.app_name:
            df["target"] = "mobile"
        else:
            df["target"] = None

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

        self.legacy_forecast_df = df[columns]
