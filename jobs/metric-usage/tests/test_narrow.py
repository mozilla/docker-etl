"""narrow(): a column fed by a positional pack of probes (clients_daily's
map_sum_aggregates[OFFSET(95)].map AS ..._sum) is cut down to the probe its
name says it holds."""

from metric_usage import lineage as L

LEAVES = {"a": "tab_open", "b": "tab_close", "c": "uri_count", "d": "total_uri_count",
          "e": "search_count", "f": "ad_click"}


def pack(*mids):
    return L.Leaf({m: (1, "t:packed", True, True) for m in mids})


def test_column_named_for_one_probe_is_narrowed():
    cut, did = L.narrow(("tab_open_sum",), pack(*LEAVES), LEAVES)
    assert did
    assert set(cut) == {"a"}


def test_longest_name_wins():
    cut, did = L.narrow(("total_uri_count_sum",), pack(*LEAVES), LEAVES)
    assert set(cut) == {"d"}


def test_no_name_match_is_left_alone():
    cut, did = L.narrow(("something_else",), pack(*LEAVES), LEAVES)
    assert not did
    assert set(cut) == set(LEAVES)


def test_tie_is_left_alone():
    leaves = dict(LEAVES, g="tab_open")
    cut, did = L.narrow(("tab_open_sum",), pack(*leaves), leaves)
    assert not did


def test_small_combination_is_not_a_pack():
    # below NARROW_MIN it is more likely a real combination, such as COALESCE
    # of a scalar and its older histogram
    cut, did = L.narrow(("tab_open",), pack("a", "b", "c", "d"), LEAVES)
    assert not did
    assert set(cut) == {"a", "b", "c", "d"}
