import pytest
import yaml

from kpi_forecasting.validator import Validator


@pytest.fixture(scope="module")
def directory_of_configs(tmp_path_factory):
    tmpdir = tmp_path_factory.mktemp("configs")

    data_dict_1 = {
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
        yaml.dump(data_dict_1, outfile, default_flow_style=False)
    with open(f2, "w") as outfile:
        yaml.dump(data_dict_1, outfile, default_flow_style=False)

    data_dict_2 = {
        "write_results": {
            "project": "x",
            "dataset": "y",
            "table": "z",
        },
        "metric_hub": {"segments": {"a": None, "b": None, "different": None}},
    }
    f3 = tmpdir / "config_hassegments2_1.yaml"

    with open(f3, "w") as outfile:
        yaml.dump(data_dict_2, outfile, default_flow_style=False)
    return f1.parent


def test_fixture(directory_of_configs):
    config_list = ["config_hassegments1_1.yaml", "config_hassegments1_2.yaml"]
    test_validator = Validator(
        config_list,
        "dummy",
        "dummy",
        "dummy",
        input_config_path=directory_of_configs,
    )
    test_validator._extract_config_data()
    assert test_validator.input_table_full == "x.y.z"
    assert set(test_validator.dimension_list) == {"a", "b", "c"}
