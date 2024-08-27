import pandas as pd
from datetime import datetime, timezone, timedelta

from kpi_forecasting.inputs import CLI, load_yaml
from kpi_forecasting.models.prophet_forecast import (
    ProphetForecast,
    summarize as prophet_summarize,
    write_results as prophet_write_results,
    summarize_legacy as prophet_summarize_legacy,
)
from kpi_forecasting.models.funnel_forecast import (
    FunnelForecast,
    summarize as funnel_summarize,
    write_results as funnel_write_results,
)
from kpi_forecasting.metric_hub import MetricHub


# A dictionary of available models in the `models` directory.
MODELS = {
    "prophet": ProphetForecast,
    "funnel": FunnelForecast,
}


class KPIPipeline:
    def __init__(self, config_path):
        self.config_data = load_yaml(filepath=config_path)
        if isinstance(self.config_data["forecast_model"]["parameters"], list):
            self.model_type = "funnel"
            self.model_class = FunnelForecast
            self.segments = list(self.config_data["metric_hub"]["segments"].keys())
        else:
            self.model_type = "prophet"
            self.model_class = ProphetForecast
            self.segments = None

    def add_metadata(self, summary_df):
        # add Metric Hub metadata columns
        summary_df["metric_alias"] = self.metric_hub.alias.lower()
        summary_df["metric_hub_app_name"] = self.metric_hub.app_name.lower()
        summary_df["metric_hub_slug"] = self.metric_hub.slug.lower()
        summary_df["metric_start_date"] = pd.to_datetime(self.metric_hub.min_date)
        summary_df["metric_end_date"] = pd.to_datetime(self.metric_hub.max_date)
        summary_df["metric_collected_at"] = self.collected_at

        # add forecast model metadata columns
        summary_df["forecast_start_date"] = self.start_date
        summary_df["forecast_end_date"] = self.end_date
        summary_df["forecast_trained_at"] = self.trained_at
        summary_df["forecast_predicted_at"] = self.predicted_at
        summary_df["forecast_parameters"] = self.metadata_parameters

        return summary_df

    def get_raw_data(self):
        metric_hub = MetricHub(**self.config_data["metric_hub"])
        self.collected_at = datetime.now(timezone.utc).replace(tzinfo=None)
        observed_df = metric_hub.fetch()
        # set attribute to generate metadata later
        self.metric_hub = metric_hub
        return observed_df

    def get_predict_dates(self, observed_df):
        start_date = pd.to_datetime(
            self.config_data["forecast_model"]["forecast_start"]
            or self._default_start_date(observed_df)
        )
        self.start_date = start_date
        end_date = pd.to_datetime(
            self.config_data["forecast_model"]["forecast_end"]
            or self._default_end_date()
        )
        self.end_date = end_date
        return pd.DataFrame(
            {"submission_date": pd.date_range(start_date, end_date).date}
        )

    def fit(self, observed_df):
        # the model parameters are mixed in a bit with paraters to configure
        # the predict dates so we have to do this

        if self.model_type == "funnel":
            model_parameters = {
                "parameters": self.config_data["forecast_model"]["parameters"]
            }
            model_parameters["segments"] = self.segments
        elif self.model_type == "prophet":
            model_parameters = self.config_data["forecast_model"]["parameters"]
        model = self.model_class(**model_parameters)
        self.trained_at = datetime.now(timezone.utc).replace(tzinfo=None)
        self.metadata_parameters = model._get_parameters()
        return model.fit(observed_df)

    def predict_and_summarize(self, model, predict_dates, observed_df):
        raw_predictions = model.predict(predict_dates)
        self.predicted_at = datetime.now(timezone.utc).replace(tzinfo=None)
        if self.model_type == "funnel":
            return funnel_summarize(
                raw_predictions,
                observed_df,
                segment_cols=self.segments,
                **self.config_data["summarize"],
            )
        elif self.model_type == "prophet":
            return prophet_summarize(
                raw_predictions, observed_df, **self.config_data["summarize"]
            )

    def write_results(self, model, summarized, predict_dates):
        summarized = self.add_metadata(summarized)
        if self.model_type == "funnel":
            components_df = pd.concat(
                [el["model"].components_df for el in model.segment_models]
            )
            funnel_write_results(
                summarized,
                components_df,
                segment_cols=self.segments,
                **self.config_data["write_results"],
            )
        elif self.model_type == "prophet":
            forecast_df_legacy = model._predict_legacy(
                predict_dates, self.metric_hub.alias, self.metadata_parameters
            )
            summary_df_legacy = prophet_summarize_legacy(summarized)
            prophet_write_results(
                summarized,
                summary_df_legacy,
                forecast_df_legacy,
                **self.config_data["write_results"],
            )

    def _default_start_date(self, observed_df) -> str:
        """The first day after the last date in the observed dataset."""
        if self.config_data["forecast_model"]["predict_historical_dates"]:
            return observed_df["submission_date"].min()
        else:
            return observed_df["submission_date"].max() + timedelta(days=1)

    def _default_end_date(self) -> str:
        """78 weeks (18 months) ahead of the current UTC date."""
        return (
            datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(weeks=78)
        ).date()


def main() -> None:
    # Load the config
    config_path = CLI().args.config

    pipeline = KPIPipeline(config_path)

    observed_df = pipeline.get_raw_data()
    fit_model = pipeline.fit(observed_df=observed_df)
    predict_dates = pipeline.get_predict_dates(observed_df)
    summarized = pipeline.predict_and_summarize(fit_model, predict_dates, observed_df)
    pipeline.write_results(fit_model, summarized, predict_dates)


if __name__ == "__main__":
    main()
