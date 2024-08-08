import attr
from dataclasses import dataclass
from datetime import datetime
from dotmap import DotMap
from pathlib import Path
from typing import List, Optional, Union

import pandas as pd

from kpi_forecasting.inputs import YAML


PARENT_PATH = Path(__file__).parent
HOLIDAY_PATH = PARENT_PATH / "holidays.yaml"
REGRESSOR_PATH = PARENT_PATH / "regressors.yaml"
SCALAR_PATH = PARENT_PATH / "scalar_adjustments.yaml"

HOLIDAY_COLLECTION = YAML(HOLIDAY_PATH)
REGRESSOR_COLLECTION = YAML(REGRESSOR_PATH)
SCALAR_ADJUSTMENTS = YAML(SCALAR_PATH)


@attr.s(auto_attribs=True, frozen=False)
class ProphetRegressor:
    """
    Holds necessary data to define a regressor for a Prophet model.
    """

    name: str
    description: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    prior_scale: Union[int, float] = 1
    mode: str = "multiplicative"


@attr.s(auto_attribs=True, frozen=False)
class ProphetHoliday:
    """
    Holds necessary data to define a custom holiday for a Prophet model.
    """

    name: str
    ds: List
    lower_window: int
    upper_window: int


@dataclass
class ScalarAdjustments:
    """
    Holds the names and dates where a scalar adjustment should be applied.

    Args:
        name (str): The name of the adjustment from the scalar_adjustments.yaml file.
        forecast_start_date (datetime): The first forecast_start_date where this iteration of the
            adjustment should be applied. This adjustment will apply to any subsequent forecast
            until another update of this adjustment is made.
        adjustments_dataframe (DataFrame): A DataFrame that contains the dimensions of the segments
            being forecasted as columns, as well as the start dates and values for each scalar
            adjustment.
    """

    name: str
    adjustment_dotmap: DotMap

    def __post_init__(self):
        adj_list = []
        self.forecast_start_date = datetime.strptime(
            self.adjustment_dotmap.forecast_start_date, "%Y-%m-%d"
        )
        for segment_dat in self.adjustment_dotmap.segments:
            segment = {**segment_dat.segment}
            segment_adjustment_dat = [
                {**segment, **adj} for adj in segment_dat.adjustments
            ]
            adj_list.append(pd.DataFrame(segment_adjustment_dat))

        # Create a DataFrame with each dimension in the segments, the start date of
        ## each scalar adjustment, and the value of that adjustment
        self.adjustments_dataframe = pd.concat(adj_list, ignore_index=True)


def parse_scalar_adjustments(
    metric_hub_slug: str, forecast_start_date: datetime
) -> List[ScalarAdjustments]:
    """
    Parses the SCALAR_ADJUSTMENTS to find the applicable scalar adjustments for a given metric hub slug
    and forecast start date.

    Args:
        metric_hub_slug (str): The metric hub slug being forecasted. It must be present by name in the
            scalar_adjustments.yaml.
        forecast_start_date (str): The first date being forecasted. Used here to map to the correct scalar
            adjustments as the adjustments will be updated over time.

    Returns:
        List[ScalarAdjustments]: A list of ScalarAdjustments, where each ScalarAdjustments is a named scalar adjustment with the
            dates that the adjustment should be applied for each segment being modeled.
    """
    metric_adjustments = getattr(SCALAR_ADJUSTMENTS.data, metric_hub_slug)
    if not metric_adjustments:
        raise KeyError(f"No adjustments found for {metric_hub_slug} in {SCALAR_PATH}.")

    # Creates a list of ScalarAdjustments objects that apply for this metric and forecast_start_date
    applicable_adjustments = []
    for named_adjustment in metric_adjustments:
        parsed_named_adjustments = [
            ScalarAdjustments(named_adjustment.name, adj_dotmap)
            for adj_dotmap in named_adjustment.adjustments
        ]

        # Sort list of parsed adjustments by forecast_start_date
        sorted_parsed_named_adjustments = sorted(
            parsed_named_adjustments, key=lambda d: d.forecast_start_date
        )

        # Iterate over the sorted list to find any adjustments that apply after the supplied forecast_start_date.
        ## Returns `None` if no applicable value is found
        matched_adjustment = None
        for parsed_adjustment in sorted_parsed_named_adjustments:
            if forecast_start_date >= parsed_adjustment.forecast_start_date:
                matched_adjustment = parsed_adjustment

        if matched_adjustment:
            applicable_adjustments.append(matched_adjustment)

    return applicable_adjustments
