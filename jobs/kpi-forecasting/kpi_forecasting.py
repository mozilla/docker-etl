import pandas as pd
from datetime import datetime, timezone, timedelta

from kpi_forecasting.inputs import CLI, load_yaml
from kpi_forecasting.models.prophet_forecast import ProphetForecast
from kpi_forecasting.models.funnel_forecast import FunnelForecast
from kpi_forecasting.metric_hub import MetricHub


# A dictionary of available models in the `models` directory.
MODELS = {
    "prophet": ProphetForecast,
    "funnel": FunnelForecast,
}


def get_start_date(observed_df, predict_historical_dates=False) -> datetime:
    """The first day after the last date in the observed dataset."""
    if predict_historical_dates:
        return pd.to_datetime(observed_df["submission_date"].min())
    else:
        return pd.to_datetime(observed_df["submission_date"].max() + timedelta(days=1))


def get_end_date() -> datetime:
    """78 weeks (18 months) ahead of the current UTC date."""
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(weeks=78)


def add_metadata(
    summary_df,
    metric_hub,
    collected_at,
    start_date,
    end_date,
    trained_at,
    predicted_at,
    metadata_params,
):
    # add Metric Hub metadata columns
    summary_df["metric_alias"] = metric_hub.alias.lower()
    summary_df["metric_hub_app_name"] = metric_hub.app_name.lower()
    summary_df["metric_hub_slug"] = metric_hub.slug.lower()
    summary_df["metric_start_date"] = pd.to_datetime(metric_hub.min_date)
    summary_df["metric_end_date"] = pd.to_datetime(metric_hub.max_date)
    summary_df["metric_collected_at"] = collected_at

    # add forecast model metadata columns
    summary_df["forecast_start_date"] = start_date
    summary_df["forecast_end_date"] = end_date
    summary_df["forecast_trained_at"] = trained_at
    summary_df["forecast_predicted_at"] = predicted_at
    summary_df["forecast_parameters"] = metadata_params

    return summary_df


def main() -> None:
    # Load the config
    config = load_yaml(filepath=CLI().args.config)
    model_type = config["forecast_model"]["model_type"]

    if model_type in MODELS:
        metric_hub = MetricHub(**config["metric_hub"])
        collected_at = datetime.now(timezone.utc).replace(tzinfo=None)
        observed_df = metric_hub.fetch()

        start_date = config["forecast_start"] or get_start_date(observed_df)
        end_date = config["forecast_end"] or get_end_date()
        dates_to_predict = pd.DataFrame(
            {"submission_date": pd.date_range(start_date, end_date).date}
        )

        model = MODELS[model_type](metric_hub=metric_hub, **config["forecast_model"])

        trained_at = datetime.now(timezone.utc).replace(tzinfo=None)
        model.fit(observed_df)

        predicted_at = datetime.now(timezone.utc).replace(tzinfo=None)
        prediction = model.predict(dates_to_predict)
        prediction_with_metadata = add_metadata(
            prediction,
            metric_hub=metric_hub,
            collected_at=collected_at,
            start_date=start_date,
            end_date=end_date,
            trained_at=trained_at,
            predicted_at=predicted_at,
            metadata_params=model.metadata_params,
        )
        model.write_results(prediction_with_metadata, **config["write_results"])

    else:
        raise ValueError(f"Don't know how to forecast using {model_type}.")


if __name__ == "__main__":
    main()
