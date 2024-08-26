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
    additional_aggregation_columns: list = [],
) -> pd.DataFrame:
    """Floor dates to the correct period and aggregate."""
    if period.lower() not in ["day", "month", "year"]:
        raise ValueError(
            f"Don't know how to floor dates by {period}. Please use 'day', 'month', or 'year'."
        )

    x = df.copy(deep=True)
    x[date_col] = pd.to_datetime(x[date_col]).dt.to_period(period[0]).dt.to_timestamp()

    aggregation_cols = [date_col] + additional_aggregation_columns
    # treat numeric and string types separately
    x_no_aggregation_cols = x[[el for el in x.columns if el not in aggregation_cols]]
    x_string = x_no_aggregation_cols.select_dtypes(include=["datetime64", object])
    x_numeric = x_no_aggregation_cols.select_dtypes(include=["float", "int"])

    # put aggergation columns back into x_numeric so groupby works
    x_numeric = x[list(x_numeric.columns) + aggregation_cols]
    x_string = x[list(x_string.columns) + aggregation_cols]

    if set(x_string.columns) | set(x_numeric.columns) != set(x.columns):
        missing_columns = set(x.columns) - (
            set(x_string.columns) | set(x_numeric.columns)
        )
        missing_columns_str = ",".join(missing_columns)
        raise ValueError(
            f"Columns do not have string or numeric type: {missing_columns_str}"
        )

    x_numeric_agg = x_numeric.groupby(aggregation_cols).agg(aggregation).reset_index()

    # all values of x_string should be the same because it is just the dimensions
    x_string_agg = x_string.drop_duplicates().reset_index(drop=True)

    if len(x_string_agg) != len(x_numeric_agg):
        raise ValueError(
            "String and Numeric dataframes have different length, likely due to strings not being unique up to aggregation"
        )

    # unique preseves order so we should be fine to concat
    output_df = x_numeric_agg.merge(x_string_agg, on=aggregation_cols)
    return output_df
