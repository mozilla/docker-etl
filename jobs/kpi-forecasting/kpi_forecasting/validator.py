from dataclasses import dataclass
from google.cloud import bigquery

from kpi_forecasting.inputs import YAML
import pandas as pd


@dataclass
class Validator:
    input_config_list: list[str]
    output_project: str
    output_dataset: str
    output_table: str
    input_config_path: str = "kpi_forecasting/configs"

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
        self.output_table_id = (
            f"{self.output_project}.{self.output_dataset}.{self.output_table}"
        )

        # table is so, so every time this runs it processes all the data
        # and overwrites the old table
        self.job_config = bigquery.QueryJobConfig(
            destination=self.output_table_id, write_disposition="WRITE_TRUNCATE"
        )
        self._load_config_data()
        self._extract_config_data()

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

    def query(self) -> str:
        """Creates a query used to generate the validation table.

        Returns:
        (str): Query to generate validation table"""
        if len(self.dimension_list) > 0:
            segment_comma_separated = "," + ",".join(self.dimension_list)
        else:
            segment_comma_separated = ""
        # in actual_deduped, the value for historical data won't change so we can use any_value without checking forecast_trained_at
        compare_ctes = f"""WITH forecast_deduped as (SELECT submission_date, metric_alias, aggregation_period {segment_comma_separated}, 
                                                    MAX(forecast_trained_at) as forecast_trained_at,
                                                    ANY_VALUE(value HAVING MAX forecast_trained_at) as forecast_value 
                                                    FROM {self.input_table_full} 
                                                    WHERE source='forecast'
                                                    GROUP BY submission_date, metric_alias, aggregation_period {segment_comma_separated}),
                                actual_deduped as (SELECT submission_date, metric_alias, aggregation_period {segment_comma_separated},
                                                    ANY_VALUE(value) as actual_value 
                                                    FROM {self.input_table_full}
                                                    WHERE source='historical'
                                                    GROUP BY submission_date, metric_alias, aggregation_period {segment_comma_separated}),
                            compare_data as (SELECT forecast_deduped.*, actual_deduped.actual_value,
                            (actual_deduped.actual_value-forecast_deduped.forecast_value) as difference,
                            (actual_deduped.actual_value-forecast_deduped.forecast_value)/actual_deduped.actual_value*100 as percent_difference,
                            ABS(actual_deduped.actual_value-forecast_deduped.forecast_value) as absolute_difference,
                            ABS(actual_deduped.actual_value-forecast_deduped.forecast_value)/actual_deduped.actual_value*100 as absolute_percent_difference
                                                FROM forecast_deduped 
                                                INNER JOIN actual_deduped USING (submission_date, metric_alias, aggregation_period {segment_comma_separated}))"""
        return f"{compare_ctes} SELECT * FROM compare_data"

    def fetch(self) -> pd.DataFrame:
        """Uses the query produced by the query method to retrieve data
            from bigquery and return as a pandas dataframe

        Returns:
            pd.DataFrame: Validation table as a pandas dataframe
        """
        query_job = bigquery.Client(project=self.output_project).query(self.query())
        return query_job.to_dataframe()

    def write(self):
        """Write output of query output by query method to the location specified
        by the job_config attribute"""
        query_job = bigquery.Client(project=self.output_project).query(
            self.query(), job_config=self.job_config
        )
        query_job.result()  # waits for job to complete
