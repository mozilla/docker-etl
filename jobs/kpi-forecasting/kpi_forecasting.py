from kpi_forecasting.inputs import CLI, YAML
from kpi_forecasting.models.prophet_forecast import ProphetForecast
from kpi_forecasting.models.funnel_forecast import FunnelForecast
from kpi_forecasting.models.scalar_forecast import ScalarForecast
from kpi_forecasting.metric_hub import MetricHub
from kpi_forecasting.metric_hub import ForecastDataPull

# A dictionary of available models in the `models` directory.
MODELS = {
    "prophet": ProphetForecast,
    "funnel": FunnelForecast,
    "scalar": ScalarForecast
}


def main() -> None:
    # Load the config
    config = YAML(filepath=CLI().args.config).data
    model_type = config.forecast_model.model_type

    if hasattr(config, "metric_hub"):
        data_puller = MetricHub(**config.metric_hub)
    elif hasattr(config, "forecast_data_pull"):
        data_puller = ForecastDataPull(**config.forecast_data_pull)
    else:
        raise KeyError("No metric_hub or forecast_data_pull key in config to pull data.")

    if model_type in MODELS:
        model = MODELS[model_type](metric_hub=data_puller, **config.forecast_model)
        model.fit()
        model.predict()
        model.summarize(**config.summarize)
        model.write_results(**config.write_results)

    else:
        raise ValueError(f"Don't know how to forecast using {model_type}.")


if __name__ == "__main__":
    main()
