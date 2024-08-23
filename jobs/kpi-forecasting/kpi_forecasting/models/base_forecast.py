import json
import numpy as np
import pandas as pd
import abc
from dataclasses import dataclass, field
from typing import Dict, List

from datetime import datetime, timedelta, timezone
from kpi_forecasting.metric_hub import MetricHub


@dataclass
class BaseForecast(abc.ABC):
    """
    Holds the configuration and results for each segment
    in a funnel forecasting model.
    """

    @abc.abstractmethod
    def _set_seed(self) -> None:
        """Set random seed to ensure that fits and predictions are reproducible."""
        return NotImplementedError

    @abc.abstractmethod
    def fit(self, observed_df: pd.DataFrame) -> object:
        """Fit a forecasting model using `observed_df.` This will typically
        be the data that was generated using
        Metric Hub in `__post_init__`.
        This method should update (and potentially set) `self.model`.

        Args:
            observed_df (pd.DataFrame): observed data used to fit the model

        Returns: self
        """
        raise NotImplementedError

    @abc.abstractmethod
    def predict(self, dates_to_predict: pd.DataFrame) -> pd.DataFrame:
        """Forecast using `self.model` on dates in `dates_to_predict`.
        This method should return a dataframe that will
        be validated by `_validate_forecast_df`.

        Args:
            dates_to_predict (pd.DataFrame): dataframe of dates to forecast for

        Returns:
            pd.DataFrame: dataframe of predictions
        """
        raise NotImplementedError

    @abc.abstractmethod
    def _validate_forecast_df(self, forecast_df: pd.DataFrame) -> None:
        """Method to validate reults produced by _predict

        Args:
            forecast_df (pd.DataFrame): dataframe produced by `_predict`"""
        raise NotImplementedError


@dataclass
class BaseEnsembleForecast:
    """
    A base class for fitting, forecasting, and summarizing forecasts. This class
    should not be invoked directly; it should be inherited by a child class. The
    child class needs to implement `_fit` and `_forecast` methods in order to work.

    Args:
        parameters (Dict): Parameters that should be passed to the forecasting model.
        model_class: Class to use to construct an ensemble
        segments: segments from the metric hub data pull
    """

    parameters: Dict | List
    model_class: object = BaseForecast
    segments: dict = None

    def __post_init__(self) -> None:
        # metadata
        self.model_type = self.model_class.__class__.__name__.lower().replace(
            "Forecast", ""
        )
        self.metadata_params = json.dumps(
            {
                "model_type": self.model_type,
                "model_params": self.parameters,
            }
        )

    def _set_segment_models(self, observed_df: pd.DataFrame) -> None:
        """Creates an element in the segment_models attribute for each segment specified in the
            metric_hub.segments section of the config.  It is populated from the list of
            parameters in the forecast_model.parameters section of the configuration file.
            The segements section of each element of the list specifies which values within which
            segments the parameters are associated with.

        Args:
            observed_df (pd.DataFrame): dataframe containing observed data used to model
                must contain columns specified in the keys of the segments section of the config
        """

        # Construct a DataFrame containing all combination of segment x
        ## in the observed_df
        combination_df = observed_df[self.segments].drop_duplicates()

        # Construct dictionaries from those combinations
        # this will be used to check that the config actually partitions the data
        segment_combinations = combination_df.to_dict("records")

        # get subset of segment that is used in partitioning
        split_dims = None
        for partition in self.parameters:
            partition_dim = set(partition["segment"].keys())
            if split_dims and partition_dim != split_dims:
                raise ValueError(
                    "Segment keys are not the same across different elements of parameters in the config file"
                )
            elif split_dims is None:
                split_dims = partition_dim
            else:
                # this is case where split_dim is set and matches paritition_dim
                continue
        if not split_dims <= set(combination_df.keys()):
            missing_dims = split_dims - set(combination_df.keys())
            missing_dims_str = ",".join(missing_dims)
            raise ValueError(
                f"Segment keys missing from metric hub segments: {missing_dims_str}"
            )

        # For each segment combinination, get the model parameters from the config
        ## file. Parse the holidays and regressors specified in the config file.
        segment_models = []
        for segment in segment_combinations:
            # find the correct configuration
            for partition in self.parameters:
                partition_segment = partition["segment"]
                selected_partition = None
                # get subset of segment that is used to partition
                subset_segment = {
                    key: val for key, val in segment.items() if key in split_dims
                }
                if partition_segment == subset_segment:
                    selected_partition = partition.copy()
                    break
            if selected_partition is None:
                raise ValueError("Partition not Found")
            selected_partition["segment"] = segment

            if "start_date" in selected_partition:
                start_date = pd.to_datetime(selected_partition["start_date"]).date()
            else:
                start_date = None

            # Create a FunnelSegmentModelSettings object for each segment combination
            segment_models.append(
                {
                    "model": self.model_class(**selected_partition["parameters"]),
                    "segment": segment,
                    "start_date": start_date,
                }
            )
        self.segment_models = segment_models

    def filter_data_to_segment(
        self, df: pd.DataFrame, segment: dict, start_date: str
    ) -> pd.DataFrame:
        column_matches_segment = df[list(segment)] == pd.Series(segment)
        row_in_segment = column_matches_segment.all(axis=1)
        filter_array = row_in_segment
        if start_date:
            row_after_start = df["submission_date"] >= start_date
            filter_array &= row_after_start
        return df.loc[filter_array]

    def fit(self, observed_df) -> None:
        """Fit a model using historic metric data provided by `metric_hub`."""
        print(f"Fitting {self.model_type} model.", flush=True)
        # create list of models depending on whether there are segments or not
        self._set_segment_models(observed_df)
        for segment_model in self.segment_models:
            model = segment_model["model"]
            model._set_seed()
            observed_subset = self.filter_data_to_segment(
                observed_df, segment_model["segment"], segment_model["start_date"]
            )
            model.fit(observed_subset)
        return self

    def get_filtered_observed_df(self, observed_df: pd.DataFrame) -> pd.DataFrame:
        """returns the observed data filtered using the start_date for each segmen
        can only be called after model is fit"""
        observed_data_list = []
        for segment_model in self.segment_models:
            observed_subset = self.filter_data_to_segment(
                observed_df, segment_model["segment"], segment_model["start_date"]
            )
            observed_data_list.append(observed_subset)
        return pd.concat(observed_data_list)

    def predict(self, dates_to_predict) -> None:
        """Generate a forecast from `start_date` to `end_date`.
        Result is set to `self.forecast_df`"""
        start_date = dates_to_predict["submission_date"].iloc[0]
        end_date = dates_to_predict["submission_date"].iloc[-1]

        print(f"Forecasting from {start_date} to {end_date}.", flush=True)
        for segment_model in self.segment_models:
            config_start_date = segment_model["start_date"]

            if config_start_date and config_start_date > start_date:
                dates_to_predict_segment = dates_to_predict[
                    dates_to_predict["submission_date"] >= config_start_date
                ].copy()
            else:
                dates_to_predict_segment = dates_to_predict.copy()

            model = segment_model["model"]
            model._set_seed()
            predict_df = model.predict(dates_to_predict_segment)

            # add segments on as columns
            for column, value in segment_model["segment"].items():
                predict_df[column] = value
            predict_df["forecast_parameters"] = json.dumps(model._get_parameters())

            segment_model["forecast"] = predict_df
        self.forecast_list = [el["forecast"] for el in self.segment_models]
        return pd.concat(self.forecast_list)
