import pytest
import yaml

from kpi_forecasting.validator import Validator


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
    }
    f6 = tmpdir / "config_nosegments2_1.yaml"

    with open(f6, "w") as outfile:
        yaml.dump(data_dict_nosegments2, outfile, default_flow_style=False)
    return f1.parent


def test_no_segments_working(directory_of_configs):
    config_list = ["config_nosegments1_1.yaml", "config_nosegments1_2.yaml"]
    test_validator = Validator(
        config_list,
        "dummy",
        "dummy",
        "dummy",
        input_config_path=directory_of_configs,
    )
    assert test_validator.input_table_full == "x.y.z"
    assert test_validator.dimension_list == []


def test_segments_working(directory_of_configs):
    config_list = ["config_hassegments1_1.yaml", "config_hassegments1_2.yaml"]
    test_validator = Validator(
        config_list,
        "dummy",
        "dummy",
        "dummy",
        input_config_path=directory_of_configs,
    )
    assert test_validator.input_table_full == "x.y.z"
    assert set(test_validator.dimension_list) == {"a", "b", "c"}


def test_segment_error(directory_of_configs):
    config_list = ["config_hassegments1_1.yaml", "config_hassegments2_1.yaml"]

    with pytest.raises(
        Exception,
        match="Dimension Data Does not all match for config list: config_hassegments1_1.yaml config_hassegments2_1.yaml",
    ):
        _ = Validator(
            config_list,
            "dummy",
            "dummy",
            "dummy",
            input_config_path=directory_of_configs,
        )


def test_mixed_segment_error(directory_of_configs):
    config_list = ["config_hassegments1_1.yaml", "config_nosegments1_2.yaml"]

    with pytest.raises(
        Exception,
        match="Dimension Data Does not all match for config list: config_hassegments1_1.yaml config_nosegments1_2.yaml",
    ):
        _ = Validator(
            config_list,
            "dummy",
            "dummy",
            "dummy",
            input_config_path=directory_of_configs,
        )


def test_input_table_with_segment_error(directory_of_configs):
    config_list = ["config_hassegments2_1.yaml", "config_hassegments3_1.yaml"]

    with pytest.raises(
        Exception,
        match="Input Table Data Does not all match for config list: config_hassegments2_1.yaml config_hassegments3_1.yaml",
    ):
        _ = Validator(
            config_list,
            "dummy",
            "dummy",
            "dummy",
            input_config_path=directory_of_configs,
        )


def test_input_table_no_segment_error(directory_of_configs):
    config_list = ["config_nosegments1_1.yaml", "config_nosegments2_1.yaml"]

    with pytest.raises(
        Exception,
        match="Input Table Data Does not all match for config list: config_nosegments1_1.yaml config_nosegments2_1.yaml",
    ):
        _ = Validator(
            config_list,
            "dummy",
            "dummy",
            "dummy",
            input_config_path=directory_of_configs,
        )
