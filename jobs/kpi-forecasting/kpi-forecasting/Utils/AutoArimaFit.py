from datetime import datetime
from datetime import date
import typing

import pandas as pd

from statsforecast.adapters.prophet import AutoARIMAProphet

import holidays


def run_forecast_arima(
    dataset: pd.DataFrame, config: dict
) -> typing.Tuple[pd.DataFrame, pd.DataFrame]:

    fit_parameters = config[
        "forecast_parameters"
    ].copy()  # you must force a copy here or it assigns a reference to
    # the dictionary

    if config["holidays"]:
        holiday_df = pd.DataFrame.from_dict(
            holidays.US(years=[2017, 2018, 2019, 2020, 2021]).items()
        )  # type: pd.DataFrame
        holiday_df.rename({0: "ds", 1: "holiday"}, inplace=True, axis=1)
        fit_parameters["holidays"] = holiday_df
    fit_parameters["growth"] = "flat"

    model = AutoARIMAProphet(**fit_parameters, mcmc_samples=0)

    fit_model = model.fit(dataset)

    periods = len(
        pd.date_range(start=date.today(), end=config["stop_date"], freq="d").to_list()
    )

    future = fit_model.make_future_dataframe(periods=periods)

    future_values = fit_model.predict(future)

    future_values = future_values[future_values["ds"] > datetime.today()]

    uncertainty_samples_raw = fit_model.predictive_samples(future)

    uncertainty_samples = pd.DataFrame.from_records(uncertainty_samples_raw["yhat"])

    uncertainty_samples["ds"] = future["ds"]

    return future_values, uncertainty_samples


def remaining_days(max_day, end_date) -> int:
    if type(max_day) == str:
        parts = [int(part) for part in max_day.split("-")]
        max_day = datetime(year=parts[0], month=parts[1], day=parts[2]).date()

    if type(end_date) == str:
        parts = [int(part) for part in end_date.split("-")]
        end_date = datetime(year=parts[0], month=parts[1], day=parts[2]).date()

    return (end_date - max_day).days


if __name__ == "__main__":
    import yaml

    with open("../yaml/desktop_non_cumulative_arima.yaml", "r") as config_stream:
        config = yaml.safe_load(config_stream)
    test_data_dict = [
        {"ds": "2022-09-24", "y": 0},
        {"ds": "2022-09-25", "y": 1},
        {"ds": "2022-09-26", "y": 2},
        {"ds": "2022-09-24", "y": 3},
        {"ds": "2022-09-25", "y": 4},
        {"ds": "2022-09-26", "y": 5},
        {"ds": "2022-09-24", "y": 6},
        {"ds": "2022-09-25", "y": 7},
        {"ds": "2022-09-26", "y": 8},
        {"ds": "2022-09-24", "y": 9},
        {"ds": "2022-09-25", "y": 10},
        {"ds": "2022-09-26", "y": 11},
    ]
    test_dataset = pd.DataFrame.from_records(test_data_dict)
    print(test_dataset.head())
    run_forecast_arima(test_dataset, config=config)
