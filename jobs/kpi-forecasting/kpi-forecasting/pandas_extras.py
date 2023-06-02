import numpy as np
import pandas as pd


def quantile(q: int = 50, name_format: str = "q{:02.0f}"):
    """
    TODO
    """

    def f(x):
        return x.quantile(q / 100)

    f.__name__ = name_format.format(q / 100)
    return f


def aggregate_to_period(
    df: pd.DataFrame,
    period: str,
    aggregation: callable = np.sum,
    date_col: str = "submission_date",
) -> pd.DataFrame:
    """Floor dates to the correct period and aggregate."""
    if period.lower() in ["day", "month", "year"]:
        period = period[0]
    else:
        raise ValueError(f"Don't know how to floor dates by {period}.")

    x = df.copy(deep=True)
    x[date_col] = pd.to_datetime(x[date_col]).dt.to_period(period).dt.to_timestamp()
    return x.groupby(date_col).agg(aggregation).reset_index()
