import pandas as pd
from dataclasses import dataclass


# Holder for holidays used in Prophet
@dataclass
class HolidayCollection:
    easter = pd.DataFrame(
        {
            "holiday": "easter",
            "ds": pd.to_datetime(
                [
                    "2016-03-27",
                    "2017-04-16",
                    "2018-04-01",
                    "2019-04-21",
                    "2020-04-12",
                    "2021-4-4",
                    "2022-4-17",
                ]
            ),
            "lower_window": -2,
            "upper_window": 1,
        }
    )

    covid_sip1 = pd.DataFrame(
        {
            "holiday": "covid_sip1",
            "ds": pd.to_datetime(["2020-03-14"]),
            "lower_window": 0,
            "upper_window": 45,
        }
    )

    covid_sip11 = pd.DataFrame(
        {
            "holiday": "covid_sip1",
            "ds": pd.to_datetime(["2020-03-14"]),
            "lower_window": -14,
            "upper_window": 30,
        }
    )

    covid_sip12 = pd.DataFrame(
        {
            "holiday": "covid_sip2",
            "ds": pd.to_datetime(["2020-09-15"]),
            "lower_window": -28,
            "upper_window": 30,
        }
    )

    covid_esr = pd.DataFrame(
        {
            "holiday": "covid_esr",
            "ds": pd.to_datetime(["2020-03-14"]),
            "lower_window": 0,
            "upper_window": 120,
        }
    )

    # US
    covid_esr_rev = pd.DataFrame(
        {
            "holiday": "covid_esr_rev",
            "ds": pd.to_datetime(["2020-03-14"]),
            "lower_window": -14,
            "upper_window": 105,
        }
    )

    # esr RoW REV
    covid_esr_row_rev = pd.DataFrame(
        {
            "holiday": "covid_esr_row_rev",
            "ds": pd.to_datetime(["2020-04-15"]),
            "lower_window": -45,
            "upper_window": 75,
        }
    )

    # Mobile RoW REV
    covid_mobile_row_rev = pd.DataFrame(
        {
            "holiday": "covid_mobile_row_rev",
            "ds": pd.to_datetime(["2020-04-01"]),
            "lower_window": -31,
            "upper_window": 60,
        }
    )

    fenix_migration = pd.DataFrame(
        {
            "holiday": "fenix_migration",
            "ds": pd.to_datetime(["2020-09-15"]),
            "lower_window": -28,
            "upper_window": 30,
        }
    )
