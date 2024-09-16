from dap_collector_ppa_dev.main import parse_vector


def test_parse_vector():
    ret = parse_vector("54, 49, 340282366920938462946865773367900766208, 340282366920938462946865773367900766206, 1")
    assert ret == [54, 49, -1, -3, 1]
