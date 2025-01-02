import pytest
import yaml

import pandas as pd

from kpi_forecasting.results_processing import PerformanceAnalysis


@pytest.fixture(scope="module")
def directory_of_configs(tmp_path_factory):
    tmpdir = tmp_path_factory.mktemp("configs")

    data_dict_hassegments1 = {
        "write_results": {
            "project": "x",
            "dataset": "y",
            "table": "z",
        },
        "metric_hub": {"segments": {"a": None, "b": None, "c": None}},
    }
    f1 = tmpdir / "config_hassegments1_1.yaml"
    f2 = tmpdir / "config_hassegments1_2.yaml"

    with open(f1, "w") as outfile:
        yaml.dump(data_dict_hassegments1, outfile, default_flow_style=False)
    with open(f2, "w") as outfile:
        yaml.dump(data_dict_hassegments1, outfile, default_flow_style=False)

    data_dict_hassegments2 = {
        "write_results": {
            "project": "x",
            "dataset": "y",
            "table": "z",
        },
        "metric_hub": {"segments": {"a": None, "b": None, "different": None}},
    }
    f3 = tmpdir / "config_hassegments2_1.yaml"

    with open(f3, "w") as outfile:
        yaml.dump(data_dict_hassegments2, outfile, default_flow_style=False)

    data_dict_hassegments3 = {
        "write_results": {
            "project": "q",
            "dataset": "p",
            "table": "z",
        },
        "metric_hub": {"segments": {"a": None, "b": None, "different": None}},
    }
    f3b = tmpdir / "config_hassegments3_1.yaml"

    with open(f3b, "w") as outfile:
        yaml.dump(data_dict_hassegments3, outfile, default_flow_style=False)

    data_dict_nosegments1 = {
        "write_results": {
            "project": "x",
            "dataset": "y",
            "table": "z",
        },
        "metric_hub": {},
    }
    f4 = tmpdir / "config_nosegments1_1.yaml"
    f5 = tmpdir / "config_nosegments1_2.yaml"

    with open(f4, "w") as outfile:
        yaml.dump(data_dict_nosegments1, outfile, default_flow_style=False)
    with open(f5, "w") as outfile:
        yaml.dump(data_dict_nosegments1, outfile, default_flow_style=False)

    data_dict_nosegments2 = {
        "write_results": {
            "project": "a",
            "dataset": "q",
            "table": "z",
        },
        "metric_hub": {},
    }
    f6 = tmpdir / "config_nosegments2_1.yaml"

    with open(f6, "w") as outfile:
        yaml.dump(data_dict_nosegments2, outfile, default_flow_style=False)
    return f1.parent


@pytest.fixture(scope="module")
def get_forecast_performance_config(tmp_path_factory):
    tmpdir = tmp_path_factory.mktemp("configs")

    data_dict = {
        "write_results": {
            "project": "",
            "dataset": "",
            "table": "",
        },
        "metric_hub": {},
    }
    f1 = tmpdir / "config.yaml"
    with open(f1, "w") as outfile:
        yaml.dump(data_dict, outfile, default_flow_style=False)
    return f1.parent


@pytest.fixture()
def get_forecast_performance(get_forecast_performance_config):
    mpa = PerformanceAnalysis(
        ["config.yaml"],
        "",
        "",
        "",
        intra_forecast_agg_names=(sum,),
        identifier_columns=("index",),
        input_config_path=get_forecast_performance_config,
    )
    return mpa


def test_get_most_recent_forecasts(get_forecast_performance):
    index = [1, 1, 1, 2, 2, 2]
    timestamp_increment_by_month = [
        pd.Timestamp(2024, 1, 1) + pd.DateOffset(months=i) for i in range(3)
    ]
    forecast_trained_at_month = 2 * timestamp_increment_by_month
    forecast_value = range(6)
    input_df = pd.DataFrame(
        {
            "index": index,
            "forecast_trained_at_month": forecast_trained_at_month,
            "forecast_value": forecast_value,
        }
    )
    output_df = get_forecast_performance._get_most_recent_forecasts(input_df)
    expected_df = input_df.copy()
    expected_df["forecast_value_previous_month"] = [1, 1, 1, 4, 4, 4]
    expected_df["current_forecast_month"] = [pd.Timestamp(2024, 3, 1)] * 6
    expected_df["previous_forecast_month"] = [pd.Timestamp(2024, 2, 1)] * 6

    assert set(output_df.columns) == set(expected_df.columns)
    assert output_df[output_df.columns].equals(expected_df[output_df.columns])


def test_lookback_default(get_forecast_performance):
    index = [1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2]
    timestamp_increment_by_month = [
        pd.Timestamp(2024, 1, 1) + pd.DateOffset(months=i) for i in range(6)
    ]
    forecast_trained_at_month = 2 * timestamp_increment_by_month
    current_forecast_month = [max(timestamp_increment_by_month)] * 12
    input_df = pd.DataFrame(
        {
            "index": index,
            "forecast_trained_at_month": forecast_trained_at_month,
            "current_forecast_month": current_forecast_month,
        }
    )
    output_df = get_forecast_performance._apply_lookback(input_df)
    assert output_df.equals(input_df)


def test_lookback_three_mo(get_forecast_performance):
    get_forecast_performance.intra_forecast_lookback_months = 3
    index = [1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2]
    timestamp_increment_by_month = [
        pd.Timestamp(2024, 1, 1) + pd.DateOffset(months=i) for i in range(6)
    ]
    forecast_trained_at_month = 2 * timestamp_increment_by_month
    current_forecast_month = [max(timestamp_increment_by_month)] * 12
    input_df = pd.DataFrame(
        {
            "index": index,
            "forecast_trained_at_month": forecast_trained_at_month,
            "current_forecast_month": current_forecast_month,
        }
    )
    output_df = get_forecast_performance._apply_lookback(input_df)

    half_trained_at_month = [
        pd.Timestamp(2024, 3, 1) + pd.DateOffset(months=i) for i in range(4)
    ]
    expected_df = pd.DataFrame(
        {
            "index": [1, 1, 1, 1, 2, 2, 2, 2],
            "forecast_trained_at_month": half_trained_at_month * 2,
            "current_forecast_month": current_forecast_month[:8],
        }
    )
    assert output_df.reset_index(drop=True).equals(expected_df)


def test_generate_schema_exception(get_forecast_performance):
    df = pd.DataFrame(
        {
            "some_floats": [0.0, 1.0, 2.0],
            "complex_numbers": [complex(1, 2), complex(2, 3), complex(3, 4)],
        }
    )
    with pytest.raises(
        Exception,
        match="Schema is missing the following columns due to unexpected type: complex_numbers",
    ):
        _ = get_forecast_performance._generate_output_bq_schema(df)


def test_no_segments_working(directory_of_configs):
    config_list = ["config_nosegments1_1.yaml", "config_nosegments1_2.yaml"]
    test_forecast_performance = PerformanceAnalysis(
        config_list,
        "",
        "",
        "",
        input_config_path=directory_of_configs,
    )
    assert test_forecast_performance.input_table_full == "x.y.z"
    assert test_forecast_performance.dimension_list == []


def test_segments_working(directory_of_configs):
    config_list = ["config_hassegments1_1.yaml", "config_hassegments1_2.yaml"]
    test_forecast_performance = PerformanceAnalysis(
        config_list,
        "",
        "",
        "",
        input_config_path=directory_of_configs,
    )
    assert test_forecast_performance.input_table_full == "x.y.z"
    assert set(test_forecast_performance.dimension_list) == {"a", "b", "c"}


def test_segment_error(directory_of_configs):
    config_list = ["config_hassegments1_1.yaml", "config_hassegments2_1.yaml"]

    with pytest.raises(
        Exception,
        match="Dimension Data Does not all match for config list: config_hassegments1_1.yaml config_hassegments2_1.yaml",
    ):
        _ = PerformanceAnalysis(
            config_list,
            "",
            "",
            "",
            input_config_path=directory_of_configs,
        )


def test_mixed_segment_error(directory_of_configs):
    config_list = ["config_hassegments1_1.yaml", "config_nosegments1_2.yaml"]

    with pytest.raises(
        Exception,
        match="Dimension Data Does not all match for config list: config_hassegments1_1.yaml config_nosegments1_2.yaml",
    ):
        _ = PerformanceAnalysis(
            config_list,
            "",
            "",
            "",
            input_config_path=directory_of_configs,
        )


def test_input_table_with_segment_error(directory_of_configs):
    config_list = ["config_hassegments2_1.yaml", "config_hassegments3_1.yaml"]

    with pytest.raises(
        Exception,
        match="Input Table Data Does not all match for config list: config_hassegments2_1.yaml config_hassegments3_1.yaml",
    ):
        _ = PerformanceAnalysis(
            config_list,
            "",
            "",
            "",
            input_config_path=directory_of_configs,
        )


def test_input_table_no_segment_error(directory_of_configs):
    config_list = ["config_nosegments1_1.yaml", "config_nosegments2_1.yaml"]

    with pytest.raises(
        Exception,
        match="Input Table Data Does not all match for config list: config_nosegments1_1.yaml config_nosegments2_1.yaml",
    ):
        _ = PerformanceAnalysis(
            config_list,
            "",
            "",
            "",
            input_config_path=directory_of_configs,
        )
