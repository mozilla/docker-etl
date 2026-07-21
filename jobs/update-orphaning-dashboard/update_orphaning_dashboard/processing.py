"""Per-client categorization for the Out Of Date dashboard.

This is a direct port of the RDD pipeline in the legacy Spark job
(mozilla/telemetry-airflow:jobs/update_orphaning_dashboard_etl.py). Each
``*_mapper`` function there operated on a single client record and returned a
``(key, ping)`` pair; the driver collected counts with ``countByKey()`` and
filtered to the ``True``/matching records before the next stage.

Because the input is a 1% sample already reduced to the ~10k-15k candidate
clients by the SQL ``out_of_date_details`` filter, the whole pipeline runs in
plain python over an in-memory list. ``countByKey`` becomes ``collections.Counter``
and ``.filter(...).values()`` becomes a list comprehension. The mapper bodies
are kept as close to the originals as possible so the output matches exactly.

A "ping" here is a :class:`Ping` whose attributes mirror the columns the legacy
Spark Row exposed (``version``, ``update_ping_count_notify``, etc.), so the
mapper bodies read identically to the Spark version.
"""

import datetime as dt
import json
import re
from collections import Counter

# RegEx for a valid release version, except for 50.1.0 which is handled
# separately. Matches the legacy `p` pattern.
VERSION_RE = re.compile(r"^[0-9]{2,3}\.0[\.0-9]*$")

# Columns carried through unchanged from the query row (scalars / scalar arrays).
_PASSTHROUGH = (
    "client_id",
    "version",
    "session_length",
    "enabled",
    "subsession_start_date",
    "subsession_length",
)

# Enumerated histogram columns. The query already densified each to its fixed
# length (the legacy n_values + 1); python only finalizes the all-null rule.
_ENUMERATED = (
    "update_check_code_notify",
    "update_download_code_partial",
    "update_download_code_complete",
    "update_state_code_partial_stage",
    "update_state_code_complete_stage",
    "update_state_code_unknown_stage",
    "update_state_code_partial_startup",
    "update_state_code_complete_startup",
    "update_state_code_unknown_startup",
    "update_status_error_code_complete_startup",
    "update_status_error_code_partial_startup",
    "update_status_error_code_unknown_startup",
    "update_status_error_code_complete_stage",
    "update_status_error_code_partial_stage",
    "update_status_error_code_unknown_stage",
)

# Count histogram columns: per-ping reduced to the bucket-0 scalar.
_COUNT = (
    "update_check_no_update_notify",
    "update_not_pref_update_auto_notify",
    "update_ping_count_notify",
    "update_unable_to_apply_notify",
)


def merge_enumerated_histogram(per_ping):
    """Finalize an enumerated histogram column kept sparse in SQL.

    ``per_ping`` is the parsed JSON column: ``[{"h": [{"k", "v"}] | None}, ...]``
    (one struct per ping), where ``enum_nz`` in the query emitted only the
    non-zero buckets of each present histogram (a null ping stays None, a present
    all-zero histogram is an empty list). Returns the list of per-ping
    ``{bucket: count}`` dicts (null pings -> ``{}``), or None if every ping was
    null -- matching the legacy ``merge_enumerated_histograms`` whole-column
    None rule. The legacy shim densified each present histogram to ``[0..n]``;
    a sparse dict where an absent index reads as 0 is exactly equivalent for the
    mappers (which only test ``value > 0`` and use the bucket index), at a
    fraction of the memory (median 1 non-zero bucket vs 51-101 dense slots).
    """
    arrays = [p["h"] for p in per_ping]
    if all(a is None for a in arrays):
        return None
    return [{} if a is None else {b["k"]: b["v"] for b in a} for a in arrays]


def merge_count_histogram(per_ping):
    """Finalize a count histogram column already reduced in SQL.

    ``per_ping`` is the parsed JSON column: ``[int | None, ...]`` (bucket-0 count
    per ping, None for a null ping). Mirrors the legacy
    ``merge_count_histograms``: a null ping contributes 0, but the whole column
    is None if every ping was null.
    """
    if all(v is None for v in per_ping):
        return None
    return [0 if v is None else v for v in per_ping]


def merge_keyed_count_histogram(per_ping):
    """Pivot a keyed count histogram column to ``{key: [per-ping bucket-0]}``.

    ``per_ping`` is the parsed JSON column: ``[{"ext": [{"key", "value": int}]},
    ...]`` (one struct per ping). Mirrors the legacy
    ``merge_keyed_count_histograms``: keys are added lazily and backfilled with
    zeros so every list has one entry per ping.
    """
    res = {}
    n_hist = len(per_ping)
    for i, struct in enumerate(per_ping):
        for entry in struct["ext"]:
            key = entry["key"]
            if key not in res:
                res[key] = [0] * n_hist
            res[key][i] = entry["value"]
    return res


_KEYED = "update_check_extended_error_notify"


class Ping:
    """A single client's longitudinal record.

    Built from a BigQuery result row. Each histogram column arrives as a single
    JSON *string* of the already-reduced per-ping values (the query did the
    legacy Spark UDFs' densification). So one client row is ~24 scalar strings,
    each parsed with exactly one ``json.loads`` -- not ~24 arrays of up to 1000
    nested structs (which the BigQuery client deserializes cell-by-cell, ~30x
    slower) and not per-ping histogram strings (~140 ``json.loads`` per client).

    The parsed shapes are: count -> ``[int|None]``, enumerated (sparse) ->
    ``[{"h": [{"k", "v"}]|None}]``, keyed -> ``[{"ext": [{"key", "value": int}]}]``.
    The ``merge_*`` helpers finalize them into the per-ping lists / dicts the
    mappers consume: enumerated columns become a list of ``{bucket: count}`` dicts
    (an absent bucket reads as 0), exactly equivalent to the legacy dense arrays.
    """

    def __init__(self, row):
        for name in _PASSTHROUGH:
            setattr(self, name, row[name])
        # One json.loads per column (the query already reduced the histograms).
        for name in _ENUMERATED:
            setattr(self, name, merge_enumerated_histogram(json.loads(row[name])))
        for name in _COUNT:
            setattr(self, name, merge_count_histogram(json.loads(row[name])))
        # keyed histogram column -> {key: [per-ping bucket-0]}, empty -> None to
        # match the legacy mappers' `is not None` guards.
        keyed = merge_keyed_count_histogram(json.loads(row[_KEYED]))
        self.update_check_extended_error_notify = keyed or None


def categorize(
    rows,
    *,
    min_subsession_date,
    min_subsession_seconds,
    min_update_ping_count,
    earliest_up_to_date_version,
):
    """Run the full pipeline over the query rows.

    ``rows`` may be any iterable of result-row dicts (e.g. the streaming Arrow
    iterator from ``main.iter_query_rows``); each client is parsed, classified,
    and discarded one at a time, so peak memory is one ``Ping`` rather than the
    whole ~100k-client result set.

    Returns a dict of the count-by-key dictionaries the report JSON needs, keyed
    by the same names the legacy ``results_dict`` used (minus ``reportDetails``
    and ``summary``, which are assembled by the caller). The counts are identical
    to the list-based pipeline: the funnel is a per-client sequence of stages, so
    walking one client through it and tallying each stage it reaches produces the
    same per-stage Counters as filtering the whole population stage by stage.
    """
    # Funnel stage counters (each tallies True/False over the clients that reach
    # it). hasUpdateEnabled doubles as `ofConcern`, as in the legacy results_dict.
    has_out_of_date_max_version = Counter()
    has_update_ping = Counter()
    has_min_subsession_length = Counter()
    has_min_update_ping_count = Counter()
    is_supported = Counter()
    is_able_to_apply = Counter()
    has_update_enabled = Counter()

    # Of-concern categorization counters.
    of_concern_by_version = Counter()
    check_code_notify_of_concern = Counter()
    check_ex_error_notify_of_concern = Counter()
    download_code_of_concern = Counter()
    state_code_stage_of_concern = Counter()
    state_failure_code_stage_of_concern = Counter()
    state_code_startup_of_concern = Counter()
    state_failure_code_startup_of_concern = Counter()
    has_only_no_update_found = Counter()
    has_no_download_code = Counter()
    has_update_apply_failure = Counter()

    print("  Streaming clients through the funnel")
    n_clients = 0
    n_of_concern = 0
    for row in rows:
        n_clients += 1
        if n_clients % 25000 == 0:
            print(f"    processed {n_clients} clients ({n_of_concern} of concern)")
        ping = Ping(row)

        # --- "out of date, potentially of concern" funnel, short-circuiting on
        # the first False exactly as the staged filter did. ---
        key, _ = _has_out_of_date_max_version(ping, earliest_up_to_date_version)
        has_out_of_date_max_version[key] += 1
        if key is not True:
            continue

        key, _ = _has_update_ping(ping)
        has_update_ping[key] += 1
        if key is not True:
            continue

        key, _ = _has_min_subsession_length(
            ping, min_subsession_date, min_subsession_seconds
        )
        has_min_subsession_length[key] += 1
        if key is not True:
            continue

        key, _ = _has_min_update_ping_count(
            ping, min_subsession_date, min_update_ping_count
        )
        has_min_update_ping_count[key] += 1
        if key is not True:
            continue

        key, _ = _is_supported(ping, min_update_ping_count)
        is_supported[key] += 1
        if key is not True:
            continue

        key, _ = _is_able_to_apply(ping)
        is_able_to_apply[key] += 1
        if key is not True:
            continue

        key, _ = _has_update_enabled(ping)
        has_update_enabled[key] += 1
        if key is not True:
            continue

        # --- out of date, of concern: categorize this client. ---
        n_of_concern += 1
        of_concern_by_version[ping.version[0]] += 1

        code, _ = _check_code_notify(ping)
        check_code_notify_of_concern[code] += 1
        if code in (22, 23):
            ex_code, _ = _check_ex_error_notify(ping)
            check_ex_error_notify_of_concern[ex_code] += 1

        dl_code, _ = _download_code(ping)
        download_code_of_concern[dl_code] += 1

        stage_code, _ = _state_code_stage(ping)
        state_code_stage_of_concern[stage_code] += 1
        if stage_code == 12:
            fail, _ = _state_failure_code_stage(ping)
            state_failure_code_stage_of_concern[fail] += 1

        startup_code, _ = _state_code_startup(ping)
        state_code_startup_of_concern[startup_code] += 1
        if startup_code == 12:
            fail, _ = _state_failure_code_startup(ping)
            state_failure_code_startup_of_concern[fail] += 1

        # --- of-concern sub-categorizations (sequential False-survivor chain) ---
        only_no_update, _ = _has_only_no_update_found(ping)
        has_only_no_update_found[only_no_update] += 1
        if only_no_update is False:
            no_download, _ = _has_no_download_code(ping)
            has_no_download_code[no_download] += 1
            if no_download is False:
                apply_failure, _ = _has_update_apply_failure(ping)
                has_update_apply_failure[apply_failure] += 1

    print(f"  Done: {n_clients} clients, {n_of_concern} of concern")

    return {
        "hasOutOfDateMaxVersion": has_out_of_date_max_version,
        "hasUpdatePing": has_update_ping,
        "hasMinSubsessionLength": has_min_subsession_length,
        "hasMinUpdatePingCount": has_min_update_ping_count,
        "isSupported": is_supported,
        "isAbleToApply": is_able_to_apply,
        "hasUpdateEnabled": has_update_enabled,
        "ofConcern": has_update_enabled,
        "hasOnlyNoUpdateFound": has_only_no_update_found,
        "hasNoDownloadCode": has_no_download_code,
        "hasUpdateApplyFailure": has_update_apply_failure,
        "ofConcernCategorized": has_update_apply_failure,
        "ofConcernByVersion": of_concern_by_version,
        "checkCodeNotifyOfConcern": check_code_notify_of_concern,
        "checkExErrorNotifyOfConcern": check_ex_error_notify_of_concern,
        "downloadCodeOfConcern": download_code_of_concern,
        "stateCodeStageOfConcern": state_code_stage_of_concern,
        "stateFailureCodeStageOfConcern": state_failure_code_stage_of_concern,
        "stateCodeStartupOfConcern": state_code_startup_of_concern,
        "stateFailureCodeStartupOfConcern": state_failure_code_startup_of_concern,
    }


# ----------------------------------------------------------------------------
# Mappers — ported verbatim from the legacy Spark job. Each takes a Ping and
# returns (key, ping). The ping half of the pair is vestigial now that
# categorize() streams (it threaded the record through the legacy RDD stages);
# callers ignore it, but it is kept so the bodies stay identical to the original.
# ----------------------------------------------------------------------------
def _has_out_of_date_max_version(ping, earliest_up_to_date_version):
    index = 0
    while index < len(ping.version):
        if (
            ping.version[index] == "50.1.0" or VERSION_RE.match(ping.version[index])
        ) and ping.version[index] > earliest_up_to_date_version:
            return False, ping
        index += 1
    return True, ping


def _has_update_ping(ping):
    if ping.update_ping_count_notify is not None and (
        ping.update_check_code_notify is not None
        or ping.update_check_no_update_notify is not None
    ):
        return True, ping
    return False, ping


def _has_min_subsession_length(ping, min_subsession_date, min_subsession_seconds):
    seconds = 0
    index = 0
    current_version = ping.version[0]
    while (
        seconds < min_subsession_seconds
        and index < len(ping.subsession_start_date)
        and index < len(ping.version)
        and ping.version[index] == current_version
    ):
        try:
            date = dt.datetime.strptime(
                ping.subsession_start_date[index][:10], "%Y-%m-%d"
            ).date()
            if date < min_subsession_date:
                return False, ping
            seconds += ping.subsession_length[index]
            index += 1
        except Exception:  # catch *all* exceptions
            index += 1

    if seconds >= min_subsession_seconds:
        return True, ping
    return False, ping


def _has_min_update_ping_count(ping, min_subsession_date, min_update_ping_count):
    index = 0
    update_ping_count_total = 0
    current_version = ping.version[0]
    while (
        update_ping_count_total < min_update_ping_count
        and index < len(ping.update_ping_count_notify)
        and index < len(ping.version)
        and ping.version[index] == current_version
    ):
        ping_count = ping.update_ping_count_notify[index]
        # Is this an update ping or just a placeholder for the telemetry ping?
        if ping_count > 0:
            try:
                date = dt.datetime.strptime(
                    ping.subsession_start_date[index][:10], "%Y-%m-%d"
                ).date()
                if date < min_subsession_date:
                    return False, ping
            except Exception:  # catch *all* exceptions
                index += 1
                continue

            # Is there also a valid update check code or no update telemetry ping?
            if (
                ping.update_check_code_notify is not None
                and len(ping.update_check_code_notify) > index
            ):
                # Sparse: the dict holds exactly the non-zero buckets, so one
                # iteration per non-zero bucket -- matching the legacy dense loop
                # that acted once per `code_value > 0` (the index-by-2 quirk: a
                # qualifying ping with one non-zero bucket advances `index` twice).
                for _code_value in ping.update_check_code_notify[index].values():
                    update_ping_count_total += ping_count
                    index += 1

            if (
                ping.update_check_no_update_notify is not None
                and len(ping.update_check_no_update_notify) > index
                and ping.update_check_no_update_notify[index] > 0
            ):
                update_ping_count_total += ping_count

        index += 1

    if update_ping_count_total < min_update_ping_count:
        return False, ping
    return True, ping


def _is_supported(ping, min_update_ping_count):
    index = 0
    update_ping_count_total = 0
    current_version = ping.version[0]
    while (
        update_ping_count_total < min_update_ping_count
        and index < len(ping.update_ping_count_notify)
        and index < len(ping.version)
        and ping.version[index] == current_version
    ):
        ping_count = ping.update_ping_count_notify[index]
        # Is this an update ping or just a placeholder for the telemetry ping?
        if ping_count > 0:
            # Is there also a valid update check code or no update telemetry ping?
            if (
                ping.update_check_code_notify is not None
                and len(ping.update_check_code_notify) > index
                and ping.update_check_code_notify[index].get(28, 0) > 0
            ):
                return False, ping
        index += 1
    return True, ping


def _is_able_to_apply(ping):
    index = 0
    current_version = ping.version[0]
    while (
        index < len(ping.update_ping_count_notify)
        and index < len(ping.version)
        and ping.version[index] == current_version
    ):
        if ping.update_ping_count_notify[index] > 0:
            # Only check the last value for update_unable_to_apply_notify
            # to determine if the client is unable to apply.
            if (
                ping.update_unable_to_apply_notify is not None
                and ping.update_unable_to_apply_notify[index] > 0
            ):
                return False, ping
            return True, ping
        index += 1
    raise ValueError("Missing update unable to apply value!")


def _has_update_enabled(ping):
    index = 0
    current_version = ping.version[0]
    while (
        index < len(ping.update_ping_count_notify)
        and index < len(ping.version)
        and ping.version[index] == current_version
    ):
        if ping.update_ping_count_notify[index] > 0:
            # If there is an update ping and settings.update.enabled has a value
            # for the same telemetry submission then use the value of
            # settings.update.enabled to determine whether app update is enabled.
            if ping.enabled is not None and ping.enabled[index] is False:
                return False, ping
            return True, ping
        index += 1
    raise ValueError("Missing update enabled value!")


def _check_code_notify(ping):
    index = 0
    current_version = ping.version[0]
    while (
        index < len(ping.update_ping_count_notify)
        and index < len(ping.version)
        and ping.version[index] == current_version
    ):
        if (
            ping.update_ping_count_notify[index] > 0
            and ping.update_check_code_notify is not None
        ):
            # First non-zero bucket index (sparse: the smallest key present).
            hist = ping.update_check_code_notify[index]
            if hist:
                return min(hist), ping

            if (
                ping.update_check_no_update_notify is not None
                and ping.update_check_no_update_notify[index] > 0
            ):
                return 0, ping
        index += 1
    return -1, ping


def _check_ex_error_notify(ping):
    current_version = ping.version[0]
    for index, version in enumerate(ping.version):
        if (
            ping.update_ping_count_notify[index] > 0
            and ping.update_check_extended_error_notify is not None
        ):
            for key_name in ping.update_check_extended_error_notify:
                if ping.update_check_extended_error_notify[key_name][index] > 0:
                    if version == current_version:
                        key_name = key_name[17:]
                        if len(key_name) == 4:
                            key_name = key_name[1:]
                        return int(key_name), ping
                    return -1, ping
    return -2, ping


def _download_code(ping):
    current_version = ping.version[0]
    for index, version in enumerate(ping.version):
        if ping.update_download_code_partial is not None:
            hist = ping.update_download_code_partial[index]
            if hist:
                if version == current_version:
                    return min(hist), ping
                return -1, ping

        if ping.update_download_code_complete is not None:
            hist = ping.update_download_code_complete[index]
            if hist:
                if version == current_version:
                    return min(hist), ping
                return -1, ping
    return -2, ping


def _first_nonzero_state(ping, cols, current_version):
    """Shared body of the state-code mappers.

    Walk pings most-recent-first; within each ping check the given sparse
    histogram columns in order and return the first non-zero bucket index (or -1
    if the hit is on a non-current version), matching the legacy dense loops.
    """
    for index, version in enumerate(ping.version):
        for col in cols:
            if col is not None:
                hist = col[index]
                if hist:
                    if version == current_version:
                        return min(hist), ping
                    return -1, ping
    return -2, ping


def _state_code_stage(ping):
    return _first_nonzero_state(
        ping,
        (
            ping.update_state_code_partial_stage,
            ping.update_state_code_complete_stage,
            ping.update_state_code_unknown_stage,
        ),
        ping.version[0],
    )


def _state_failure_code_stage(ping):
    return _first_nonzero_state(
        ping,
        (
            ping.update_status_error_code_partial_stage,
            ping.update_status_error_code_complete_stage,
            ping.update_status_error_code_unknown_stage,
        ),
        ping.version[0],
    )


def _state_code_startup(ping):
    return _first_nonzero_state(
        ping,
        (
            ping.update_state_code_partial_startup,
            ping.update_state_code_complete_startup,
            ping.update_state_code_unknown_startup,
        ),
        ping.version[0],
    )


def _state_failure_code_startup(ping):
    return _first_nonzero_state(
        ping,
        (
            ping.update_status_error_code_partial_startup,
            ping.update_status_error_code_complete_startup,
            ping.update_status_error_code_unknown_startup,
        ),
        ping.version[0],
    )


def _has_only_no_update_found(ping):
    if ping.update_check_no_update_notify is None:
        return False, ping

    current_version = ping.version[0]
    for index, version in enumerate(ping.version):
        if current_version != version:
            return True, ping

        if ping.update_ping_count_notify[index] > 0:
            # If there is an update ping and update_check_no_update_notify has a
            # value equal to 0 then the update check returned a value other than
            # no update found.
            if ping.update_check_no_update_notify[index] == 0:
                return False, ping
    return True, ping


def _has_no_download_code(ping):
    current_version = ping.version[0]
    for index, version in enumerate(ping.version):
        if current_version != version:
            return True, ping

        if ping.update_download_code_partial is not None:
            if ping.update_download_code_partial[index]:
                return False, ping

        if ping.update_download_code_complete is not None:
            if ping.update_download_code_complete[index]:
                return False, ping
    return True, ping


def _has_update_apply_failure(ping):
    current_version = ping.version[0]
    for index, version in enumerate(ping.version):
        if current_version != version:
            return False, ping

        if (
            ping.update_state_code_partial_startup is not None
            and ping.update_state_code_partial_startup[index].get(12, 0) > 0
        ):
            return True, ping

        if (
            ping.update_state_code_complete_startup is not None
            and ping.update_state_code_complete_startup[index].get(12, 0) > 0
        ):
            return True, ping
    return False, ping
