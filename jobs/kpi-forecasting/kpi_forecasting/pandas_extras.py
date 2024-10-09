import numpy as np
import pandas as pd


def percentile(p: int = 50, name_format: str = "p{:02.0f}"):
    """A method to calculate percentiles along dataframe axes via the `pandas.agg` method."""

    def f(x):
        return x.quantile(p / 100)

    f.__name__ = name_format.format(p)
    return f


def aggregate_to_period(
    df: pd.DataFrame,
    period: str,
    aggregation: callable = np.sum,
    date_col: str = "submission_date",
) -> pd.DataFrame:
    """Floor dates to the correct period and aggregate."""
    if period.lower() not in ["day", "month", "year"]:
        raise ValueError(
            f"Don't know how to floor dates by {period}. Please use 'day', 'month', or 'year'."
        )

    x = df.copy(deep=True)
    x[date_col] = pd.to_datetime(x[date_col]).dt.to_period(period[0]).dt.to_timestamp()

    # treat numeric and string types separately
    x_string = x.select_dtypes(include=["datetime64", object])
    x_numeric = x.select_dtypes(include=["float", "int", "datetime64"])

    if set(x_string.columns) | set(x_numeric.columns) != set(x.columns):
        missing_columns = set(x.columns) - (
            set(x_string.columns) | set(x_numeric.columns)
        )
        missing_columns_str = ",".join(missing_columns)
        raise ValueError(
            f"Columns do not have string or numeric type: {missing_columns_str}"
        )

    x_numeric_agg = x_numeric.groupby(date_col).agg(aggregation).reset_index()

    # all values of x_string should be the same because it is just the dimensions
    x_string_agg = x_string.drop_duplicates().reset_index(drop=True)

    if len(x_string_agg) != len(x_numeric_agg):
        raise ValueError(
            "String and Numeric dataframes have different length, likely due to strings not being unique up to aggregation"
        )

    # unique preseves order so we should be fine to concat
    output_df = pd.concat(
        [x_numeric_agg, x_string_agg.drop(columns=[date_col])], axis=1
    )
    return output_df
