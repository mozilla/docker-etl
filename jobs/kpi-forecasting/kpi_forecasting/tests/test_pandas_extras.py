import pandas as pd
import pytest

from kpi_forecasting.pandas_extras import aggregate_to_period


def test_only_numeric():
    df = pd.DataFrame(
        {
            "submission_date": [
                "2020-01-01",
                "2020-01-01",
                "2020-01-02",
                "2020-02-01",
                "2020-02-02",
            ],
            "ints": [1, 2, 3, 4, 5],
            "floats": [10.0, 20.0, 30.0, 40.0, 50.0],
        }
    )

    day_output = aggregate_to_period(df, "day")

    expected_day = pd.DataFrame(
        {
            "submission_date": [
                pd.to_datetime("2020-01-01"),
                pd.to_datetime("2020-01-02"),
                pd.to_datetime("2020-02-01"),
                pd.to_datetime("2020-02-02"),
            ],
            "ints": [3, 3, 4, 5],
            "floats": [30.0, 30.0, 40.0, 50.0],
        }
    )

    pd.testing.assert_frame_equal(day_output, expected_day)

    month_output = aggregate_to_period(df, "month")

    expected_month = pd.DataFrame(
        {
            "submission_date": [
                pd.to_datetime("2020-01-01"),
                pd.to_datetime("2020-02-01"),
            ],
            "ints": [6, 9],
            "floats": [60.0, 90.0],
        }
    )

    pd.testing.assert_frame_equal(month_output, expected_month)


def test_with_string_and_numeric():
    df = pd.DataFrame(
        {
            "submission_date": [
                "2020-01-01",
                "2020-01-01",
                "2020-01-02",
                "2020-02-01",
                "2020-02-02",
            ],
            "ints": [1, 2, 3, 4, 5],
            "floats": [10.0, 20.0, 30.0, 40.0, 50.0],
            "string": ["jan", "jan", "jan", "feb", "feb"],
        }
    )

    day_output = aggregate_to_period(df, "day")

    expected_day = pd.DataFrame(
        {
            "submission_date": [
                pd.to_datetime("2020-01-01"),
                pd.to_datetime("2020-01-02"),
                pd.to_datetime("2020-02-01"),
                pd.to_datetime("2020-02-02"),
            ],
            "ints": [3, 3, 4, 5],
            "floats": [30.0, 30.0, 40.0, 50.0],
            "string": ["jan", "jan", "feb", "feb"],
        }
    )

    pd.testing.assert_frame_equal(day_output, expected_day)

    month_output = aggregate_to_period(df, "month")

    expected_month = pd.DataFrame(
        {
            "submission_date": [
                pd.to_datetime("2020-01-01"),
                pd.to_datetime("2020-02-01"),
            ],
            "ints": [6, 9],
            "floats": [60.0, 90.0],
            "string": ["jan", "feb"],
        }
    )

    pd.testing.assert_frame_equal(month_output, expected_month)


def test_only_string():
    df = pd.DataFrame(
        {
            "submission_date": [
                "2020-01-01",
                "2020-01-01",
                "2020-01-02",
                "2020-02-01",
                "2020-02-02",
            ],
            "string": ["jan", "jan", "jan", "feb", "feb"],
        }
    )

    day_output = aggregate_to_period(df, "day")

    expected_day = pd.DataFrame(
        {
            "submission_date": [
                pd.to_datetime("2020-01-01"),
                pd.to_datetime("2020-01-02"),
                pd.to_datetime("2020-02-01"),
                pd.to_datetime("2020-02-02"),
            ],
            "string": ["jan", "jan", "feb", "feb"],
        }
    )

    pd.testing.assert_frame_equal(day_output, expected_day)

    month_output = aggregate_to_period(df, "month")

    expected_month = pd.DataFrame(
        {
            "submission_date": [
                pd.to_datetime("2020-01-01"),
                pd.to_datetime("2020-02-01"),
            ],
            "string": ["jan", "feb"],
        }
    )

    pd.testing.assert_frame_equal(month_output, expected_month)


def test_non_unique_string_exception():
    df = pd.DataFrame(
        {
            "submission_date": [
                "2020-01-01",
                "2020-01-01",
                "2020-01-02",
                "2020-02-01",
                "2020-02-02",
            ],
            "ints": [1, 2, 3, 4, 5],
            "floats": [10.0, 20.0, 30.0, 40.0, 50.0],
            "string": ["jan", "jane", "yan", "fev", "feb"],
        }
    )

    with pytest.raises(
        ValueError,
        match="String and Numeric dataframes have different length, likely due to strings not being unique up to aggregation",
    ):
        _ = aggregate_to_period(df, "day")


def test_column_type_exception():
    df = pd.DataFrame(
        {
            "submission_date": [
                "2020-01-01",
                "2020-01-01",
                "2020-01-02",
                "2020-02-01",
                "2020-02-02",
            ],
            "ints": [1, 2, 3, 4, 5],
            "floats": [10.0, 20.0, 30.0, 40.0, 50.0],
            "string": ["jan", "jane", "yan", "fev", "feb"],
            "bool": [True, True, True, False, False],
        }
    )

    with pytest.raises(
        ValueError,
        match="Columns do not have string or numeric type: bool",
    ):
        _ = aggregate_to_period(df, "day")


def test_agg_exception():
    df = pd.DataFrame(
        {
            "submission_date": [
                "2020-01-01",
                "2020-01-01",
                "2020-01-02",
                "2020-02-01",
                "2020-02-02",
            ],
            "ints": [1, 2, 3, 4, 5],
            "floats": [10.0, 20.0, 30.0, 40.0, 50.0],
            "string": ["jan", "jane", "yan", "fev", "feb"],
            "bool": [True, True, True, False, False],
        }
    )

    with pytest.raises(
        ValueError,
        match="Don't know how to floor dates by hamburger. Please use 'day', 'month', or 'year'.",
    ):
        _ = aggregate_to_period(df, "hamburger")
