import pandas as pd

from dataclasses import dataclass
from datetime import date, timedelta
from dotmap import DotMap
from google.cloud import bigquery
from mozanalysis.config import ConfigLoader
from textwrap import dedent

from kpi_forecasting.utils import parse_end_date


@dataclass
class BaseDataPull:
    """
    A base class to pull data from BigQuery. For use with the forecast classes in this
    module, a `fetch` method must be implemented.

    Args:
        app_name (str): The app name that applies to the metric being retrieved.
        slug (str): A slug for the metric, intended to mimic the nomenclature used for
            metrics on Metric Hub.
        start_date (str): A 'YYYY-MM-DD' formatted-string that specifies the first
            date the metric should be queried.
        segments (Dict): A dictionary of segments to use to group metric values.
            The keys of the dictionary are aliases for the segment, and the
            value is a SQL snippet that defines the segment.
        where (str): A string specifying a condition to inject into a SQL WHERE clause,
            to filter the data source.
        end_date (str): A 'YYYY-MM-DD' formatted-string that specifies the last
            date the metric should be queried.
        alias (str): An alias for the metric. For example, 'DAU' instead of
            'daily_active_users'.
        project (str): The Big Query project to use when establishing a connection
            to the Big Query client.
        forecast_start_date (str): The forecast_start_date to use as the key to pull
            forecast data.
        forecast_project (str): BigQuery project where forecast table to be accessed is
            located.
        forecast_dataset (str): For pulling forecast data, the dataset where the forecast
            data is stored in BigQuery.
        forecast_table (str): The table name where data is stored in BigQuery for pulling
            past forecast data.
    """

    app_name: str
    slug: str
    start_date: str
    segments: DotMap = None
    where: str = None
    end_date: str = None
    alias: str = None
    project: str = "mozdata"
    forecast_start_date: str = None
    forecast_project: str = None
    forecast_dataset: str = None
    forecast_table: str = None

    def fetch(self) -> pd.DataFrame:
        raise NotImplementedError


@dataclass
class MetricHub(BaseDataPull):
    """
    Programatically get Metric Hub metrics from Big Query.
    See https://mozilla.github.io/metric-hub/metrics/fenix/ for a list of metrics.
    """

    def __post_init__(self) -> None:
        self.start_date = pd.to_datetime(self.start_date).date()
        self.end_date = pd.to_datetime(parse_end_date(self.end_date)).date()

        # Set useful attributes based on the Metric Hub definition
        metric = ConfigLoader.get_metric(
            metric_slug=self.slug,
            app_name=self.app_name,
        )
        self.metric = metric
        self.alias = self.alias or metric.name
        self.submission_date_column = metric.data_source.submission_date_column

        # Modify the metric source table string so that it formats nicely in the query.
        self.from_expression = self.metric.data_source._from_expr.replace(
            "\n", "\n" + " " * 19
        )

        # Add query snippets for segments
        self.segment_select_query = ""
        self.segment_groupby_query = ""

        if self.segments:
            segment_select_query = []
            segments = dict(self.segments)
            for alias, sql in segments.items():
                segment_select_query.append(f"  {sql} AS {alias},")
            self.segment_select_query = "," + "\n              ".join(
                segment_select_query
            )
            self.segment_groupby_query = "," + "\n             ,".join(
                self.segments.keys()
            )

        self.where = f"AND {self.where}" if self.where else ""

    def query(self) -> str:
        """Build a string to query the relevant metric values from Big Query."""
        return dedent(
            f"""
            SELECT {self.submission_date_column} AS submission_date,
                {self.metric.select_expr} AS value
                    {self.segment_select_query}
            FROM {self.from_expression}
            WHERE {self.submission_date_column} BETWEEN '{self.start_date}' AND '{self.end_date}'
                {self.where}
            GROUP BY {self.submission_date_column}
                    {self.segment_groupby_query}
        """
        )

    def fetch(self) -> pd.DataFrame:
        """Fetch the relevant metric values from Big Query."""
        print(
            f"\nQuerying for '{self.app_name}.{self.slug}' aliased as '{self.alias}':"
            f"\n{self.query()}"
        )
        df = bigquery.Client(project=self.project).query(self.query()).to_dataframe()

        # ensure submission_date has type 'date'
        df["submission_date"] = pd.to_datetime(df["submission_date"]).dt.date

        # Track the min and max dates in the data, which may differ from the
        # start/end dates
        self.min_date = str(df["submission_date"].min())
        self.max_date = str(df["submission_date"].max())

        return df


@dataclass
class ForecastDataPull(BaseDataPull):
    """
    Programatically get metrics from Big Query forecast data tables. The tables
    must follow the schema patterns found in the forecast tables produced by the
    `write_results` methods of the model classes in this module.
    """

    def __post_init__(self) -> None:
        self.start_date = pd.to_datetime(self.start_date).date()

        if self.end_date:
            self.end_date = pd.to_datetime(parse_end_date(self.end_date)).date()
        else:
            # Default forecast horizon is 18 months. End date here is extended to 36 months,
            ## to cover all current usecases
            self.end_date = pd.to_datetime(
                date.today() + timedelta(days=365 * 3)
            ).date()

        self.alias = self.alias or (self.slug + "_adjusted")

        # Default submission_date column name is "submission_date". This could be altered to accept
        ## an input, but there is no current need
        self.submission_date_column = "submission_date"

        self.from_expression = (
            f"{self.project}.{self.forecast_dataset}.{self.forecast_table}"
        )

        # Add query snippets for segments
        self.segment_select_query = ""
        self.segment_groupby_query = ""

        if self.segments:
            segment_select_query = []
            segments = dict(self.segments)
            for alias, sql in segments.items():
                segment_select_query.append(f"  {sql} AS {alias},")
            self.segment_select_query = "," + "\n              ".join(
                segment_select_query
            )
            self.segment_groupby_query = "," + "\n             ,".join(
                self.segments.keys()
            )

        self.where = f"AND {self.where}" if self.where else ""

        # Check if forecast_start_date was supplied. If not, create strting to grab the most recent forecast.
        if not self.forecast_start_date:
            self.forecast_start_date_snippet = f"""(
            SELECT 
                MAX(forecast_start_date) 
            FROM {self.from_expression} 
            WHERE metric_slug = '{self.slug}')"""
        else:
            self.forecast_start_date_snippet = f"'{self.forecast_start_date}'"

    def query(self) -> str:
        """Build a string to query the relevant metric values from Big Query."""
        return dedent(
            f"""
            WITH cte AS (
            SELECT
                {self.submission_date_column} AS submission_date,
                forecast_start_date,
                ANY_VALUE(value HAVING MAX forecast_trained_at) AS value
                {self.segment_select_query}
            FROM {self.from_expression}
            WHERE {self.submission_date_column} BETWEEN '{self.start_date}' AND '{self.end_date}'
                AND metric_alias = '{self.slug}' AND forecast_start_date = {self.forecast_start_date_snippet}
                {self.where}
            GROUP BY {self.submission_date_column}, forecast_start_date
                    {self.segment_groupby_query}
            )
            SELECT * EXCEPT (forecast_start_date) FROM cte
        """
        )

    def fetch(self) -> pd.DataFrame:
        """Fetch the relevant metric values from Big Query."""
        print(
            f"\nQuerying for the '{self.app_name}.{self.slug}' forecast':"
            f"\n{self.query()}"
        )
        df = bigquery.Client(project=self.project).query(self.query()).to_dataframe()

        # ensure submission_date has type 'date'
        df["submission_date"] = pd.to_datetime(df["submission_date"]).dt.date

        # Track the min and max dates in the data, which may differ from the
        # start/end dates
        self.min_date = str(df["submission_date"].min())
        self.max_date = str(df["submission_date"].max())

        return df
