from pathlib import Path

import toml
import attr
from typing import List, Dict, Optional, Union
import pandas as pd

from kpi_forecasting.models.holidays import HolidayCollection

holiday_collection = HolidayCollection()


@attr.s(auto_attribs=True, frozen=False)
class ProphetRegressor:
    name: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    prior_scale: Union[int, float] = 1
    mode: str = "multiplicative"


@attr.s(auto_attribs=True, frozen=False)
class Config:
    metric: str
    slug: str
    segment: Dict[str, str]
    start_date: Optional[str] = None
    holidays: Optional[pd.DataFrame] = None
    regressors: Optional[List[ProphetRegressor]] = []
    parameters: Optional[Dict[str, Union[List[float], float]]] = None
    cv_settings: Optional[Dict[str, str]] = {
        "initial": "365 days",
        "period": "30 days",
        "horizon": "30 days",
        "parallel": "processes",
    }
    use_country_holidays: Optional[str] = ""


@attr.s(auto_attribs=True)
class FunnelConfigs:
    config_toml_path: Path
    configs: List[Config] = attr.Factory(list)

    @classmethod
    def collect_funnel_configs(cls, config_toml_path: str):
        config_toml_path = Path(config_toml_path)
        configs = toml.load(config_toml_path)

        parsed_configs = []
        for metric, segments in configs.items():
            metric_name = metric
            for segment_slug, segment_config in segments.items():
                config = {
                    "metric": metric_name,
                    "slug": segment_slug,
                    "segment": segment_config["segment"],
                    "start_date": pd.to_datetime(segment_config["start_date"]),
                }
                if "holidays" in segment_config.keys():
                    if "use_country_holidays" in segment_config["holidays"].keys():
                        config["use_country_holidays"] = segment_config["holidays"][
                            "use_country_holidays"
                        ]
                    config["holidays"] = pd.concat(
                        [
                            getattr(holiday_collection, h)
                            for h in segment_config["holidays"]["holidays"]
                        ]
                    )

                if "regressors" in segment_config.keys():
                    regressor_list = []
                    for name, params in segment_config["regressors"].items():
                        regressor_list.append(
                            ProphetRegressor(
                                name=name,
                                start_date=params["start_date"]
                                if "start_date" in params.keys()
                                else None,
                                end_date=params["end_date"]
                                if "end_date" in params.keys()
                                else None,
                                prior_scale=params["prior_scale"],
                                mode=params["mode"],
                            )
                        )
                    config["regressors"] = regressor_list

                if "parameters" in segment_config.keys():
                    config["parameters"] = segment_config["parameters"]

                if "cv_settings" in segment_config.keys():
                    config["cv_settings"] = segment_config["cv_settings"]

                parsed_configs.append(Config(**config))

        return cls(config_toml_path, parsed_configs)
