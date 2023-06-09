import json
import pandas as pd
import prophet

from datetime import datetime
from dataclasses import dataclass
from models.base_forecast import BaseForecast


@dataclass
class ProphetForecast(BaseForecast):
    def _fit(self) -> None:
        """
        Fit a Prophet model using the `observed_df` that was generated using
        Metric Hub. This method updates `self.model`.
        """
        self.model = prophet.Prophet(
            **self.parameters,
            uncertainty_samples=self.number_of_simulations,
            mcmc_samples=0,
        )

        if self.use_holidays:
            self.model.add_country_holidays(country_name="US")

        # Modify observed data to have column names that Prophet expects, and fit
        # the model
        self.model.fit(
            self.observed_df.rename(
                columns={"submission_date": "ds", self.metric_hub.alias: "y"},
            )
        )

    def _predict(self) -> pd.DataFrame:
        """
        Forecast using `self.model`. This method updates `self.forecast_df`.
        """
        # generate the forecast samples
        samples = self.model.predictive_samples(self.dates_to_predict)
        df = pd.DataFrame(samples["yhat"])

        # add dates_to_predict to the samples
        df["submission_date"] = self.dates_to_predict

        return df

    def _predict_legacy(self) -> pd.DataFrame:
        """
        Recreate the legacy Big Query data model.
        """
        df = self.model.predict(self.dates_to_predict)

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

        return df[columns]
