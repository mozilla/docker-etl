"""Unit tests for the per-client categorization pipeline.

These exercise the ported mapper logic on small synthetic rows. They are the
primary guard for output parity with the legacy Spark job: each test pins a
behavior the legacy mappers had, including the bug-for-bug edge cases.

The query reduces each histogram in SQL and returns the column as one JSON
string (count -> [int|None], enumerated -> sparse [{"h": [{"k","v"}]|None}],
keyed -> [{"ext": [{"key", "value"}]}]; see sql/out_of_date_details.sql). These
helpers build those shapes so the tests cover processing.Ping's parsing as well
as the mappers.
"""

import datetime as dt
import json

import pytest

from update_orphaning_dashboard import processing

MIN_SUBSESSION_DATE = dt.date(2026, 3, 1)
MIN_SUBSESSION_SECONDS = 2 * 60 * 60
MIN_UPDATE_PING_COUNT = 4
EARLIEST_UP_TO_DATE = "138"

# Number of aligned pings in the default fixture. has_min_update_ping_count
# advances its index by 2 per qualifying ping (a bug-for-bug quirk of the legacy
# job — see processing._has_min_update_ping_count), so a client needs ~2x
# min_update_ping_count pings of the current version to be "of concern".
DEFAULT_N_PINGS = 8


def _sparse(buckets):
    """The non-zero buckets of a histogram as sorted (k, v) structs.

    Matches the query's enum_nz output: only buckets with value > 0, ordered by
    index.
    """
    return [{"k": idx, "v": val} for idx, val in sorted((buckets or {}).items())]


def enum_col(n_pings, name, buckets=None):
    """An enumerated histogram column.

    The query keeps each ping sparse and emits the column as
    TO_JSON_STRING(ARRAY_AGG(STRUCT(nz AS h))), i.e. a JSON string of
    ``[{"h": [{"k","v"}]|None}, ...]``; processing.Ping does one json.loads and
    builds a per-ping ``{bucket: count}`` dict.
    """
    return json.dumps([{"h": _sparse(buckets)} for _ in range(n_pings)])


def count_col(n_pings, name, value=0):
    """A count histogram column: JSON string of ``[int|None, ...]`` (bucket-0)."""
    return json.dumps([value for _ in range(n_pings)])


def keyed_col(n_pings, name, entries=None):
    """A keyed count histogram column.

    JSON string of ``[{"ext": [{"key", "value": int}]}, ...]``. ``entries`` maps
    key -> per-ping bucket-0 value (same for every ping).
    """
    per_ping = [{"key": key, "value": v} for key, v in (entries or {}).items()]
    return json.dumps([{"ext": list(per_ping)} for _ in range(n_pings)])


def make_row(n_pings=DEFAULT_N_PINGS, version="120.0", **overrides):
    """An out_of_date_details row with ``n_pings`` aligned pings.

    Defaults describe an out-of-date, of-concern client whose update check
    reported a general error (code 22), so it flows through every stage to the
    of-concern categorization. Any column can be overridden with a full value.
    """

    def col(value):
        return [value] * n_pings

    row = {
        "client_id": "c1",
        "version": col(version),
        "session_length": col(3600),
        "enabled": col(True),
        "subsession_start_date": col("2026-05-26T00:00:00.0+00:00"),
        "subsession_length": col(7200),
        # update check code 22 -> general error
        "update_check_code_notify": enum_col(
            n_pings, "update_check_code_notify", {22: 1}
        ),
        "update_check_extended_error_notify": keyed_col(
            n_pings, "update_check_extended_error_notify"
        ),
        "update_check_no_update_notify": count_col(
            n_pings, "update_check_no_update_notify", 0
        ),
        "update_not_pref_update_auto_notify": count_col(
            n_pings, "update_not_pref_update_auto_notify", 0
        ),
        "update_ping_count_notify": count_col(n_pings, "update_ping_count_notify", 1),
        "update_unable_to_apply_notify": count_col(
            n_pings, "update_unable_to_apply_notify", 0
        ),
        "update_download_code_partial": enum_col(
            n_pings, "update_download_code_partial"
        ),
        "update_download_code_complete": enum_col(
            n_pings, "update_download_code_complete"
        ),
        "update_state_code_partial_stage": enum_col(
            n_pings, "update_state_code_partial_stage"
        ),
        "update_state_code_complete_stage": enum_col(
            n_pings, "update_state_code_complete_stage"
        ),
        "update_state_code_unknown_stage": enum_col(
            n_pings, "update_state_code_unknown_stage"
        ),
        "update_state_code_partial_startup": enum_col(
            n_pings, "update_state_code_partial_startup"
        ),
        "update_state_code_complete_startup": enum_col(
            n_pings, "update_state_code_complete_startup"
        ),
        "update_state_code_unknown_startup": enum_col(
            n_pings, "update_state_code_unknown_startup"
        ),
        "update_status_error_code_complete_startup": enum_col(
            n_pings, "update_status_error_code_complete_startup"
        ),
        "update_status_error_code_partial_startup": enum_col(
            n_pings, "update_status_error_code_partial_startup"
        ),
        "update_status_error_code_unknown_startup": enum_col(
            n_pings, "update_status_error_code_unknown_startup"
        ),
        "update_status_error_code_complete_stage": enum_col(
            n_pings, "update_status_error_code_complete_stage"
        ),
        "update_status_error_code_partial_stage": enum_col(
            n_pings, "update_status_error_code_partial_stage"
        ),
        "update_status_error_code_unknown_stage": enum_col(
            n_pings, "update_status_error_code_unknown_stage"
        ),
    }
    row.update(overrides)
    return row


def categorize(rows):
    return processing.categorize(
        rows,
        min_subsession_date=MIN_SUBSESSION_DATE,
        min_subsession_seconds=MIN_SUBSESSION_SECONDS,
        min_update_ping_count=MIN_UPDATE_PING_COUNT,
        earliest_up_to_date_version=EARLIEST_UP_TO_DATE,
    )


# ---------------------------------------------------------------------------
# Histogram parsing (processing.Ping): enumerated columns become sparse dicts
# ---------------------------------------------------------------------------
def test_enumerated_histogram_parsed_to_sparse_dict():
    ping = processing.Ping(make_row(n_pings=1))
    # update_check_code_notify has only bucket 22 set.
    hist = ping.update_check_code_notify[0]
    assert hist == {22: 1}


def test_enumerated_histogram_all_null_is_none():
    row = make_row(
        n_pings=2,
        update_download_code_partial=json.dumps([{"h": None}, {"h": None}]),
    )
    ping = processing.Ping(row)
    assert ping.update_download_code_partial is None


def test_enumerated_histogram_null_ping_becomes_empty():
    # A column with one present and one null ping -> null ping becomes {}.
    row = make_row(
        n_pings=2,
        update_download_code_partial=json.dumps([{"h": _sparse({5: 1})}, {"h": None}]),
    )
    ping = processing.Ping(row)
    assert ping.update_download_code_partial[0] == {5: 1}
    assert ping.update_download_code_partial[1] == {}


def test_count_histogram_reduced_to_bucket_zero():
    ping = processing.Ping(make_row(n_pings=3))
    # default update_ping_count_notify bucket 0 == 1 per ping
    assert ping.update_ping_count_notify == [1, 1, 1]


def test_count_histogram_all_null_is_none():
    row = make_row(
        n_pings=2,
        update_unable_to_apply_notify=json.dumps([None, None]),
    )
    ping = processing.Ping(row)
    assert ping.update_unable_to_apply_notify is None


def test_count_histogram_null_ping_zero_filled():
    row = make_row(n_pings=3, update_ping_count_notify=json.dumps([2, None, 3]))
    ping = processing.Ping(row)
    assert ping.update_ping_count_notify == [2, 0, 3]


def test_keyed_histogram_merged_to_per_ping_lists():
    row = make_row(
        n_pings=2,
        update_check_extended_error_notify=keyed_col(
            2, "update_check_extended_error_notify", {"K": 3}
        ),
    )
    ping = processing.Ping(row)
    assert ping.update_check_extended_error_notify == {"K": [3, 3]}


def test_keyed_histogram_empty_is_none():
    ping = processing.Ping(make_row())  # default keyed col is empty
    assert ping.update_check_extended_error_notify is None


# ---------------------------------------------------------------------------
# has_out_of_date_max_version: any up-to-date version anywhere -> False
# ---------------------------------------------------------------------------
def test_has_out_of_date_max_version_true_when_all_old():
    row = make_row()
    row["version"] = ["120.0"]
    counts = categorize([row])
    assert counts["hasOutOfDateMaxVersion"][True] == 1


def test_has_out_of_date_max_version_false_when_recent_present():
    # 139 > earliest_up_to_date (138) -> not out of date
    row = make_row()
    row["version"] = ["120.0", "139.0"]
    counts = categorize([row])
    assert counts["hasOutOfDateMaxVersion"].get(True, 0) == 0
    assert counts["hasOutOfDateMaxVersion"][False] == 1


# ---------------------------------------------------------------------------
# has_min_subsession_length: needs >= 2h within the current version
# ---------------------------------------------------------------------------
def test_has_min_subsession_length_true():
    counts = categorize([make_row()])  # default 7200s/ping >= 2h
    assert counts["hasMinSubsessionLength"][True] == 1


def test_has_min_subsession_length_false_when_short():
    counts = categorize([make_row(n_pings=1, subsession_length=[60])])
    assert counts["hasMinSubsessionLength"].get(True, 0) == 0


# ---------------------------------------------------------------------------
# is_supported: update_check_code_notify[index][28] > 0 -> unsupported
# ---------------------------------------------------------------------------
def test_is_supported_false_when_unsupported_code():
    row = make_row(
        update_check_code_notify=enum_col(
            DEFAULT_N_PINGS, "update_check_code_notify", {28: 1}
        ),
    )
    counts = categorize([row])
    assert counts["isSupported"][False] == 1


# ---------------------------------------------------------------------------
# check_code_notify: first nonzero bucket index is the key
# ---------------------------------------------------------------------------
def test_check_code_notify_returns_bucket_index():
    counts = categorize([make_row()])  # bucket 22 set
    assert counts["checkCodeNotifyOfConcern"][22] == 1


# ---------------------------------------------------------------------------
# check_ex_error_notify: key parsing (strip 17 chars, trim 4->3) on general err
# ---------------------------------------------------------------------------
# The legacy mapper parses the error code with key[17:] (a 17-char prefix), then
# trims a leading char when the remainder is 4 chars.
_EX_PREFIX = "UPDATE_STATUS_ERR"  # 17 chars
assert len(_EX_PREFIX) == 17


def test_check_ex_error_notify_key_parsing():
    # general error path requires check code 22 or 23 (default row uses 22)
    keyed = keyed_col(
        DEFAULT_N_PINGS,
        "update_check_extended_error_notify",
        {_EX_PREFIX + "2152398878": 1},
    )
    counts = categorize([make_row(update_check_extended_error_notify=keyed)])
    assert counts["checkExErrorNotifyOfConcern"][2152398878] == 1


def test_check_ex_error_notify_four_char_key_trimmed():
    keyed = keyed_col(
        DEFAULT_N_PINGS,
        "update_check_extended_error_notify",
        {_EX_PREFIX + "2000": 1},
    )
    counts = categorize([make_row(update_check_extended_error_notify=keyed)])
    # key[17:] = "2000" (len 4) -> trimmed to "000" -> int 0
    assert counts["checkExErrorNotifyOfConcern"][0] == 1


# ---------------------------------------------------------------------------
# has_update_apply_failure: state_code_*_startup bucket 12 > 0 -> True
# ---------------------------------------------------------------------------
def test_has_update_apply_failure_true():
    # To reach has_update_apply_failure a ping must survive the no-update-found
    # and no-download-code splits (both False), so give it a download code, and
    # a startup state-code failure (bucket 12) for the apply failure itself.
    row = make_row(
        update_download_code_partial=enum_col(
            DEFAULT_N_PINGS, "update_download_code_partial", {5: 1}
        ),
        update_state_code_partial_startup=enum_col(
            DEFAULT_N_PINGS, "update_state_code_partial_startup", {12: 1}
        ),
    )
    counts = categorize([row])
    assert counts["hasUpdateApplyFailure"][True] == 1
    # ofConcernCategorized is the same dict
    assert counts["ofConcernCategorized"] is counts["hasUpdateApplyFailure"]


# ---------------------------------------------------------------------------
# of_concern_by_version keys on the most recent version
# ---------------------------------------------------------------------------
def test_of_concern_by_version():
    counts = categorize([make_row(version="115.0"), make_row(version="120.0")])
    assert counts["ofConcernByVersion"]["115.0"] == 1
    assert counts["ofConcernByVersion"]["120.0"] == 1


# ---------------------------------------------------------------------------
# is_able_to_apply raises if there is no update ping at all (legacy behavior)
# ---------------------------------------------------------------------------
def test_is_able_to_apply_raises_without_update_ping():
    ping = processing.Ping(
        make_row(
            n_pings=1,
            update_ping_count_notify=count_col(1, "update_ping_count_notify", 0),
        )
    )
    with pytest.raises(ValueError):
        processing._is_able_to_apply(ping)


def test_empty_input_produces_empty_counts():
    counts = categorize([])
    assert counts["ofConcernByVersion"] == {}
    assert dict(counts["hasOutOfDateMaxVersion"]) == {}
