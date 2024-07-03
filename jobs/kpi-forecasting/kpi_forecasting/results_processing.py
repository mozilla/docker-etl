from dataclasses import dataclass
from functools import partial

from google.cloud import bigquery
from google.cloud.bigquery.enums import SqlTypeNames as bq_types

from kpi_forecasting.inputs import YAML
import pandas as pd
import numpy as np


@dataclass
class ModelPerformanceAnalysis:
    input_config_list: list[str]
    output_project: str
    output_dataset: str
    output_table: str
    input_config_path: str = "kpi_forecasting/configs"
    intra_forecast_agg_names: tuple = (
        "max",
        "min",
        "median",
        "mean",
        "percentile_25",
        "percentile_75",
    )
    identifier_columns: tuple = (
        "submission_date",
        "metric_alias",
        "aggregation_period",
    )

    intra_forecast_lookback_months: int = 12 * 100  # will revisit in the year 2123

    def __post_init__(self) -> None:
        """After initalization, set the following outputs after initialization
        output_table_id: id for output table
        job_config: used to set the output table when writing with bigquery
        config_data: a dict of all the data from the list of configs provided
            set inside of _load_config_data
        input_table_full: id of the input data table extracted from the configs
        dimension_list: indicates which columns of the input table represent
            dimensions, where different combinations of values specify separate models
            If a model has no such columns, set to an empty list
        """

        # table is so, so every time this runs it processes all the data
        # and overwrites the old table

        self._load_config_data()
        self._extract_config_data()
        self._set_intra_forecast_agg_functions()
        self.output_table_id = (
            f"{self.output_project}.{self.output_dataset}.{self.output_table}"
        )

        if self.output_project:
            # this case makes it possible to create
            # an object without any bigquery setup
            # for testing
            self.client = bigquery.Client(project=self.output_project)

    def _set_intra_forecast_agg_functions(self):
        """parses function names from the config into functions where
        applicable and sets the result to intra_forecast_agg_names.
        Currently only applies to percentile, where the value following
        the underscore is the percentile to apply"""
        self.intra_forecast_agg_functions = [
            partial(np.percentile, q=int(el.split("_")[1]))
            if isinstance(el, str) and "percentile" in el
            else el
            for el in self.intra_forecast_agg_names
        ]

    def _load_config_data(self):
        """Extracts data from the list of config files passed to the class and stores it in the
        config_data attribute. The filename is the key, and the contents (represnted as a DotMap)
        are the values"""
        self.config_data = {}
        for config_file in self.input_config_list:
            full_path = f"{self.input_config_path}/{config_file}"
            config_data = YAML(full_path).data
            self.config_data[config_file] = config_data

    def _extract_config_data(self):
        """Extracts data from the dictionary created by _load_config_data and uses it to set
            the attributes below:
                input_table_full: id of the input data table extracted from the configs
                dimension_list: indicates which columns of the input table represent
                    dimensions, where different combinations of values specify separate models
                    If a model has no such columns, set to an empty list

        Raises:
            Exception: Raised if list of config files have different values for the dimension list
            Exception: Raised if list of config files have different values for the input table
        """
        segment_data_list = []
        input_table_list = []
        config_file_list = list(self.config_data.keys())
        for config_data in self.config_data.values():
            # get segment data
            metric_hub_data = config_data.metric_hub.toDict()
            if "segments" in metric_hub_data:
                segment_data = metric_hub_data["segments"]
                segment_data_list.append(segment_data)
            else:
                segment_data_list.append(None)

            # get input table info
            input_table_list.append(config_data.write_results.toDict())

        input_table_data = input_table_list.pop(0)
        input_table_matches_first = [input_table_data == el for el in input_table_list]
        if not all(input_table_matches_first):
            config_file_list_string = " ".join(config_file_list)
            raise Exception(
                f"Input Table Data Does not all match for config list: {config_file_list_string}"
            )

        input_project = input_table_data["project"]
        input_dataset = input_table_data["dataset"]
        input_table = input_table_data["table"]

        input_table_full = f"{input_project}.{input_dataset}.{input_table}"

        segment_data = segment_data_list.pop(0)
        segment_data_matches_first = [segment_data == el for el in segment_data_list]
        if not all(segment_data_matches_first):
            config_file_list_string = " ".join(config_file_list)
            raise Exception(
                f"Dimension Data Does not all match for config list: {config_file_list_string}"
            )

        if segment_data:
            # this is the case where dimensions are present
            # we only need the column names for the query
            dimension_list = list(segment_data.keys())
        else:
            dimension_list = []

        self.input_table_full = input_table_full
        self.dimension_list = dimension_list

        if len(self.dimension_list) > 0:
            self.identifier_columns = (*self.identifier_columns, *self.dimension_list)

        # need identifier columns to be a list to make it easy to do pandas operations later
        self.identifier_columns = list(self.identifier_columns)

    def _get_most_recent_forecasts(self, month_level_df: pd.DataFrame) -> pd.DataFrame:
        """Adds the following columns to month_level_df:
                - previous_model_month (timestamp):
                    Timestamp of the first day of the month corresponding to the current forecast
                - forecast_value_previous_month (float): forecast value for the previous montb
        Args:
            month_level_df (pd.DataFrame): Dataframe to process. Must have the following columns
                in addition to those listed in self.identifier_columns:
                - forecast_trained_at_month
                - forecast_value

        Returns:
            pd.DataFrame: DataFrame with new columns added.  Has the same number of rows as input
        """
        current_model_month_df = (
            month_level_df[self.identifier_columns + ["forecast_trained_at_month"]]
            .groupby(self.identifier_columns)
            .agg(current_model_month=("forecast_trained_at_month", "max"))
            .reset_index()
        )
        month_level_df = month_level_df.merge(
            current_model_month_df, on=self.identifier_columns
        )

        exclude_current_model_month = month_level_df[
            month_level_df["forecast_trained_at_month"]
            != month_level_df["current_model_month"]
        ]
        previous_model_month_df = (
            exclude_current_model_month[
                self.identifier_columns + ["forecast_trained_at_month"]
            ]
            .groupby(self.identifier_columns)
            .agg(previous_model_month=("forecast_trained_at_month", "max"))
            .reset_index()
        )
        month_level_df = month_level_df.merge(
            previous_model_month_df, on=self.identifier_columns
        )

        month_level_df = month_level_df.merge(
            month_level_df[
                self.identifier_columns
                + ["forecast_trained_at_month", "forecast_value"]
            ],
            left_on=self.identifier_columns + ["previous_model_month"],
            right_on=self.identifier_columns + ["forecast_trained_at_month"],
            suffixes=(None, "_previous_month"),
        ).drop(columns="forecast_trained_at_month_previous_month")
        return month_level_df

    def query_ctes(self) -> str:
        """Creates ctes that can be used in a queries to generate the validation table.
        The

        Returns:
        (str): Query to generate validation table"""
        identifiers_comma_separated = ",".join(self.identifier_columns)
        # in actual_deduped, the value for historical data won't change so we can use any_value without checking forecast_trained_at
        query_ctes = f"""WITH forecast_with_train_month as (SELECT {identifiers_comma_separated}, forecast_trained_at, value,
                                      DATE_TRUNC(forecast_trained_at, MONTH) as forecast_trained_at_month
                                      FROM {self.input_table_full} 
                                      WHERE source='forecast'),
                                forecast_month_level as (SELECT {identifiers_comma_separated}, forecast_trained_at_month,
                                            MAX(forecast_trained_at) as forecast_trained_at,
                                            ANY_VALUE(value HAVING MAX forecast_trained_at) as forecast_value,
                                        FROM forecast_with_train_month
                                        GROUP BY {identifiers_comma_separated}, forecast_trained_at_month),
                                forecast_deduped as (SELECT {identifiers_comma_separated}, 
                                                    MAX(forecast_trained_at) as forecast_trained_at,
                                                    ANY_VALUE(value HAVING MAX forecast_trained_at) as forecast_value,
                                                    FROM {self.input_table_full} 
                                                    WHERE source='forecast'
                                                    GROUP BY {identifiers_comma_separated}),
                                actual_deduped as (SELECT {identifiers_comma_separated},
                                                    ANY_VALUE(value) as actual_value 
                                                    FROM {self.input_table_full}
                                                    WHERE source='historical'
                                                    GROUP BY {identifiers_comma_separated}),
                            compare_data as (SELECT forecast_deduped.*, actual_deduped.actual_value,
                            (actual_deduped.actual_value-forecast_deduped.forecast_value) as difference,
                            (actual_deduped.actual_value-forecast_deduped.forecast_value)/actual_deduped.actual_value*100 as percent_difference,
                            ABS(actual_deduped.actual_value-forecast_deduped.forecast_value) as absolute_difference,
                            ABS(actual_deduped.actual_value-forecast_deduped.forecast_value)/actual_deduped.actual_value*100 as absolute_percent_difference
                                                FROM forecast_deduped 
                                                INNER JOIN actual_deduped USING ({identifiers_comma_separated}))"""
        return query_ctes

    def _apply_lookback(self, data_df: pd.DataFrame) -> pd.DataFrame:
        """Filters out data that occurs self.intra_forecast_lookback_months months
            before current_model_month

        Args:
            data_df (pd.DataFrame): input data frame.  Must have the following columns:
                - current_model_month (timestamp)
                - forecast_trained_at_month (timestamp)

        Returns:
            pd.DataFrame: Filtered dataframe.  Will have less than or equal to
                the number of rows of the input dataframe
        """
        lookback_mindate = data_df["current_model_month"] - pd.DateOffset(
            months=self.intra_forecast_lookback_months
        )
        lookback_indicator = data_df["forecast_trained_at_month"] >= lookback_mindate
        month_level_lookback_applied = data_df[lookback_indicator]
        return month_level_lookback_applied

    def fetch(self) -> pd.DataFrame:
        """Uses the query produced by the query method to retrieve data
            from bigquery and return as a pandas dataframe

        Returns:
            pd.DataFrame: Validation table as a pandas dataframe
        """
        cte = self.query_ctes()
        month_level_df = self.client.query(
            f"{cte} SELECT * FROM forecast_month_level"
        ).to_dataframe()
        compare_df = self.client.query(
            f"{cte} SELECT * FROM compare_data"
        ).to_dataframe()
        month_level_df_with_most_recent = self._get_most_recent_forecasts(
            month_level_df
        )

        # create dictionary used for aggregation
        zip_intra_forecast_info = zip(
            self.intra_forecast_agg_names, self.intra_forecast_agg_functions
        )
        agg_dict = {
            f"intra_forecast_{name}": ("forecast_value", function)
            for name, function in zip_intra_forecast_info
        }

        # these have the same value for all months so just use max
        agg_dict["forecast_value_previous_month"] = (
            "forecast_value_previous_month",
            "max",
        )
        agg_dict["previous_model_month"] = ("previous_model_month", "max")

        month_level_lookback_applied = self._apply_lookback(
            month_level_df_with_most_recent
        )
        intra_forecast_metrics = (
            month_level_lookback_applied.groupby(self.identifier_columns)
            .agg(**agg_dict)
            .reset_index()
        )

        return compare_df.merge(intra_forecast_metrics, on=self.identifier_columns)

    def _generate_output_bq_schema(
        self, output_df: pd.DataFrame
    ) -> list[bigquery.SchemaField]:
        """Generates a schema from output dataframe used to write it to bigquery

        Args:
            output_df (pd.DataFrame): Output Dataframe

        Raises:
            Exception: If there are columns that don't match the exact
                types in the function, an exception will be raised

        Returns:
            list[bigquery.SchemaField]: schema useable by BigQuery
        """
        schema = []
        for colname, coltype in output_df.dtypes.to_dict().items():
            if coltype == "datetime64[ns, UTC]":
                schema.append(bigquery.SchemaField(colname, bq_types.TIMESTAMP))
            elif coltype == "dbdate":
                schema.append(bigquery.SchemaField(colname, bq_types.DATE))
            elif coltype == "object":
                schema.append(bigquery.SchemaField(colname, bq_types.STRING))
            elif coltype == "float":
                schema.append(bigquery.SchemaField(colname, bq_types.FLOAT))

        columns_in_schema = {el.name for el in schema}
        if columns_in_schema != set(output_df.columns):
            missing_columns = ",".join(list(set(output_df.columns) - columns_in_schema))
            raise Exception(
                f"Schema is missing the following columns due to unexpected type: {missing_columns}"
            )

    def write(self):
        """Write output of query output by query method to the location specified
        by the job_config attribute"""
        output_df = self.fetch()
        schema = self._generate_output_bq_schema(output_df)
        job = self.client.load_table_from_dataframe(
            dataframe=output_df,
            destination=self.output_table_id,
            job_config=bigquery.LoadJobConfig(
                schema=schema,
                autodetect=False,
                write_disposition="WRITE_TRUNCATE",
            ),
        )
        # Wait for the job to complete.
        job.result()
