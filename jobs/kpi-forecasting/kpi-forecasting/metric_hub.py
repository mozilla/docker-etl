import pandas as pd

from dataclasses import dataclass
from google.cloud import bigquery
from mozanalysis.config import ConfigLoader
from textwrap import dedent
from typing import Optional


@dataclass
class MetricHub:
    """
    Programatically get Metric Hub metrics from Big Query.
    See https://mozilla.github.io/metric-hub/metrics/ for a list of metrics.

    Args:
        app_name (str): The Metric Hub app name for the metric.
        slug (str): The Metric Hub slug for the metric.
        start_date (str): A 'YYYY-MM-DD' formatted-string that specifies the first
            date the metric should be queried.
        end_date (str): A 'YYYY-MM-DD' formatted-string that specifies the last
            date the metric should be queried.
        alias (str): An alias for the metric. For example, 'DAU' instead of
            'daily_active_users'.
        project (str): The Big Query project to use when establishing a connection
            to the Big Query client.
    """

    app_name: str
    slug: str
    start_date: str
    end_date: str = None
    alias: str = None
    project: str = "mozdata"

    def __post_init__(self) -> None:
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

    def _enquote(self, x) -> Optional[str]:
        """
        Enclose a string in quotes. This is helpful for query templating.

        Examples:
            self._enquote("1998")
            > "'1998'"
            self._enquote(None)
            > None
        """
        if x is not None:
            x = f"'{x}'"
        return x

    @property
    def query(self) -> str:
        """Build a string to query the relevant metric values from Big Query."""

        # Make sure start_date and end_date strings are formatted correctly
        start_date = self._enquote(self.start_date)
        end_date = self._enquote(self.end_date) or "CURRENT_DATE()"

        return dedent(
            f"""
            SELECT {self.submission_date_column} AS submission_date,
                   {self.metric.select_expr} AS value
              FROM {self.from_expression}
             WHERE {self.submission_date_column} BETWEEN {start_date} AND {end_date}
             GROUP BY {self.submission_date_column}
            """
        )

    def fetch(self) -> pd.DataFrame:
        """Fetch the relevant metric values from Big Query."""
        print(
            f"\nQuerying for '{self.app_name}.{self.slug}' aliased as '{self.alias}':"
            f"\n{self.query}"
        )
        df = bigquery.Client(project=self.project).query(self.query).to_dataframe()

        # ensure submission_date has type 'date'
        df[self.submission_date_column] = pd.to_datetime(
            df[self.submission_date_column]
        ).dt.date

        # Track the min and max dates in the data, which may differ from the
        # start/end dates
        self.min_date = str(df[self.submission_date_column].min())
        self.max_date = str(df[self.submission_date_column].max())

        return df
