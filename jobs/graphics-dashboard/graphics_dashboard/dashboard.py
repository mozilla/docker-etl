"""Build the per-snapshot Firefox Graphics Telemetry dashboard JSON files.

Replacement for the snapshot portion of the legacy Spark job
`mozetl.graphics.graphics_telemetry_dashboard`. Runs the single consolidated
query in `graphics_dashboard/sql/graphics_dashboard.sql` once, then pivots the
(output, dimension, key, subkey, value) rows into each `*-statistics.json` blob
the public dashboard consumes, and uploads them to GCS.

Each output file is built by a small builder function registered in BUILDERS.
A builder receives an `OutputData` view of just its rows plus the shared
`sessions` block, and returns the file's JSON payload. Add a new file by adding
its CTEs to the query (tagged with a new `output` name) and a builder here.
"""

import datetime
import time

import click

from graphics_dashboard import common

DEFAULT_TIME_WINDOW = 14

# Fixed lengths of the histogram `results` arrays, matching the legacy output
# (the Glean custom_distribution only carries populated buckets, so the reshape
# densifies sparse buckets to 0 up to these lengths).
TDR_RESULTS_LEN = 11
STARTUP_RESULTS_LEN = 21


class OutputData:
    """A view over one output's query rows, grouped by dimension.

    flat(dim)   -> {key: value}
    nested(dim) -> {key: {subkey: value}}
    results(dim, length) -> [v0, v1, ...] densified by integer bucket index
    count(dim)  -> scalar value of a single-row dimension (default 0)
    """

    def __init__(self):
        self._flat = {}
        self._nested = {}

    def add(self, dimension, key, subkey, value):
        self._flat.setdefault(dimension, {})[key] = value
        if subkey is not None:
            self._nested.setdefault(dimension, {}).setdefault(key, {})[subkey] = value

    def flat(self, dimension):
        return dict(self._flat.get(dimension, {}))

    def nested(self, dimension):
        return {k: dict(v) for k, v in self._nested.get(dimension, {}).items()}

    def results(self, dimension, length):
        buckets = self._flat.get(dimension, {})
        arr = [0] * length
        for key, value in buckets.items():
            idx = int(key)
            if 0 <= idx < length:
                arr[idx] = value
        return arr

    def count(self, dimension, key="count", default=0):
        return self._flat.get(dimension, {}).get(key, default)


# ----------------------------------------------------------------------------
# Per-output builders. Each takes (data: OutputData, sessions: dict) -> payload.
# ----------------------------------------------------------------------------
def build_mac(data, sessions):
    return {
        "versions": data.flat("versions"),
        "retina": data.flat("retina"),
        "arch": data.flat("arch"),
        "sessions": sessions,
    }


def build_linux(data, sessions):
    return {
        "driverVendors": data.flat("driverVendors"),
        "compositors": data.flat("compositors"),
        "sessions": sessions,
    }


def build_general(data, sessions):
    # byFx is nested fx_version -> {os, windows, vendors}. The query emits
    # dimension='byFx_<category>', key=<fxVer|'all'>, subkey=<breakdown value>.
    by_fx = {}
    for category in ("os", "windows", "vendors"):
        for fx_ver, breakdown in data.nested(f"byFx_{category}").items():
            by_fx.setdefault(fx_ver, {})[category] = breakdown
    return {
        "devices": data.flat("devices"),
        "drivers": data.flat("drivers"),
        "byFx": by_fx,
        "sessions": sessions,
    }


def build_device(data, sessions):
    return {
        "deviceAndDriver": data.flat("deviceAndDriver"),
        "sessions": sessions,
    }


def build_system(data, sessions):
    return {
        "logical_cores": data.flat("logical_cores"),
        "x86": {
            "total": data.count("x86_total", key="total"),
            "features": data.flat("x86_features"),
        },
        "memory": data.flat("memory"),
        "wow": data.flat("wow"),
        "sessions": sessions,
    }


def build_monitor(data, sessions):
    return {
        "counts": data.flat("counts"),
        "refreshRates": data.flat("refreshRates"),
        "resolutions": data.flat("resolutions"),
        "sessions": sessions,
    }


# Windows versions broken out individually in windows-features byVersion, and the
# per-version fields the query emits (dimension 'byver_<field>').
_WF_BYVER_FIELDS = {
    "compositors": "byver_compositors",
    "content_backends": "byver_content_backends",
    "gpu_process": "byver_gpu_process",
    "advanced_layers": "byver_advanced_layers",
    "d3d11": "byver_d3d11",
    "d2d": "byver_d2d",
    "warp": "byver_warp",
}


def build_windows_features(data, sessions):
    all_block = {
        "compositors": data.flat("all_compositors"),
        "content_backends": data.flat("all_content_backends"),
        "d3d11": data.flat("all_d3d11"),
        "d2d": data.flat("all_d2d"),
        "textureSharing": data.flat("all_textureSharing"),
        "warp": data.flat("all_warp"),
        # plugin_models is deprecated (empty in prod); advanced_layers has no
        # Glean source so it is all-'none'.
        "plugin_models": [],
        "media_decoders": _densify_media(data.flat("all_media_decoders")),
        "gpu_process": data.flat("all_gpu_process"),
        "advanced_layers": data.flat("all_advanced_layers"),
    }

    # byVersion: version -> {count, compositors, ..., media_decoders, warp}.
    counts = data.flat("byver_count")
    by_version = {}
    for version, count in counts.items():
        entry = {"count": count, "plugin_models": []}
        for json_key, dim in _WF_BYVER_FIELDS.items():
            entry[json_key] = data.nested(dim).get(version, {})
        # media_decoders is an ordered array per version.
        media = data.nested("byver_media_decoders").get(version, {})
        entry["media_decoders"] = _densify_media(media)
        by_version[version] = entry

    return {
        "all": all_block,
        "byVersion": by_version,
        "d3d11_blacklist": {
            "devices": data.flat("blacklist_devices"),
            "drivers": data.flat("blacklist_drivers"),
            "os": data.flat("blacklist_os"),
        },
        "d3d11_blocked": {"vendors": data.flat("blocked_vendors")},
        "sessions": sessions,
    }


def _densify_media(breakdown):
    if not breakdown:
        return []
    length = max(int(k) for k in breakdown) + 1
    arr = [0] * length
    for k, v in breakdown.items():
        arr[int(k)] = v
    return arr


def build_tdr(data, sessions):
    reason_to_vendor = data.nested("reasonToVendor")
    # vendorToReason is the same data transposed: vendor -> {reason: count}.
    vendor_to_reason = {}
    for reason, vendors in reason_to_vendor.items():
        for vendor, count in vendors.items():
            vendor_to_reason.setdefault(vendor, {})[reason] = count
    # Legacy emitted these as lists of [key, breakdown] pairs.
    return {
        "tdrPings": data.count("tdr_count"),
        "results": data.results("results", TDR_RESULTS_LEN),
        "reasonToVendor": [[int(r), v] for r, v in reason_to_vendor.items()],
        "vendorToReason": [[v, r] for v, r in vendor_to_reason.items()],
        "sessions": sessions,
    }


def build_startup(data, sessions):
    return {
        "startupTestPings": data.count("startup_count"),
        "results": data.results("results", STARTUP_RESULTS_LEN),
        "windows": data.flat("windows"),
        "sessions": sessions,
    }


def _coalesce_to_n(breakdown, max_items):
    """Collapse all but the top `max_items` entries of {key: count} into 'Other'.

    Mirrors the legacy coalesce_to_n_items so byDevice/byDriver stay small.
    """
    if len(breakdown) <= max_items:
        return dict(breakdown)
    items = sorted(breakdown.items(), key=lambda kv: kv[1], reverse=True)
    out = dict(items[:max_items])
    other = sum(v for _, v in items[max_items:])
    if other:
        out["Other"] = out.get("Other", 0) + other
    return out


def build_sanity(data, sessions):
    def as_pairs(dimension, coalesce=None):
        # nested() is {outcome_str: {breakdown_key: value}}; legacy emits a list
        # of [outcome_int, breakdown] pairs.
        nested = data.nested(dimension)
        pairs = []
        for outcome, breakdown in nested.items():
            if coalesce is not None:
                breakdown = _coalesce_to_n(breakdown, coalesce)
            pairs.append([int(outcome), breakdown])
        return pairs

    windows = {
        "sanityTestPings": data.count("sanity_count"),
        "totalPings": data.count("total_count"),
        "results": data.flat("results"),
        "byVendor": as_pairs("byVendor"),
        "byOS": as_pairs("byOS"),
        "byDevice": as_pairs("byDevice", coalesce=10),
        "byDriver": as_pairs("byDriver", coalesce=10),
        "windows": data.flat("windows"),
    }
    return {"windows": windows, "sessions": sessions}


def build_webgl(data, sessions):
    def block(version):
        # Dimensions are named '<version>_<successes|failures>_<field>'.
        successes = {
            "count": data.count(f"{version}_successes_count"),
            "os": data.flat(f"{version}_successes_os"),
            "compositors": data.flat(f"{version}_successes_compositors"),
        }
        failures = {
            "count": data.count(f"{version}_failures_count"),
            "os": data.flat(f"{version}_failures_os"),
            "vendors": data.flat(f"{version}_failures_vendors"),
            "devices": data.flat(f"{version}_failures_devices"),
            "drivers": data.flat(f"{version}_failures_drivers"),
        }
        return {"successes": successes, "failures": failures}

    return {
        "webgl1": block("webgl1"),
        "webgl2": block("webgl2"),
        "general": {
            "webgl": {
                "acceleration_status": data.flat("general_acceleration_status"),
                "status": data.flat("general_status"),
            }
        },
        "sessions": sessions,
    }


# filename -> (output tag in query, builder)
BUILDERS = {
    "mac-statistics.json": ("mac", build_mac),
    "linux-statistics.json": ("linux", build_linux),
    "general-statistics.json": ("general", build_general),
    "device-statistics.json": ("device", build_device),
    "system-statistics.json": ("system", build_system),
    "monitor-statistics.json": ("monitor", build_monitor),
    "tdr-statistics.json": ("tdr", build_tdr),
    "startup-test-statistics.json": ("startup", build_startup),
    "sanity-test-statistics.json": ("sanity", build_sanity),
    "webgl-statistics.json": ("webgl", build_webgl),
    "windows-features.json": ("windows-features", build_windows_features),
}


def _sessions_block(count, share, fraction, time_window, run_timestamp):
    return {
        "count": count,
        "timestamp": time.mktime(run_timestamp.timetuple()),
        "shortdate": run_timestamp.strftime("%Y%m%d"),
        "metadata": [
            {
                "info": {
                    "channel": "*",
                    "fraction": fraction,
                    "day_range": time_window,
                }
            }
        ],
        "share": share,
    }


# The session_count dimension feeds the shared `sessions.count` block rather
# than a builder. Histogram-subset counts (tdr_count, startup_count) flow into
# OutputData so their builders can read them via data.count(...).
_SESSION_COUNT_DIMENSION = "session_count"


def reshape(rows, fraction, time_window, run_timestamp, only=None):
    """Pivot (output, dimension, key, subkey, value) rows into the JSON files.

    Returns a dict of {filename: payload}. A single pass groups rows by output;
    each registered builder then assembles its file. `only` optionally restricts
    which files are built.
    """
    # The Firefox-version share is shared metadata across all files.
    share = {r["key"]: int(r["value"]) for r in rows if r["output"] == "share"}

    # Group rows into an OutputData per output, and track each output's count.
    grouped = {}
    counts = {}
    for r in rows:
        out = r["output"]
        if out == "share":
            continue
        if r["dimension"] == _SESSION_COUNT_DIMENSION:
            counts[out] = int(r["value"])
            continue
        grouped.setdefault(out, OutputData()).add(
            r["dimension"], r["key"], r["subkey"], int(r["value"])
        )

    payloads = {}
    for filename, (out, builder) in BUILDERS.items():
        if only and filename not in only:
            continue
        sessions = _sessions_block(
            counts.get(out, 0), share, fraction, time_window, run_timestamp
        )
        payloads[filename] = builder(grouped.get(out, OutputData()), sessions)
    return payloads


@click.command()
@click.option(
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="Inclusive end of the window (YYYY-MM-DD). Defaults to yesterday UTC.",
)
@click.option("--time-window", type=int, default=DEFAULT_TIME_WINDOW, show_default=True)
@common.sample_id_count_option
@common.billing_project_option
@common.output_location_options
@click.option(
    "--only",
    multiple=True,
    help="Limit to specific output files (repeatable). Defaults to all.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Write the JSON files to --test-output-dir instead of uploading to GCS.",
)
def main(
    end_date,
    time_window,
    sample_id_count,
    billing_project,
    output_bucket,
    output_prefix,
    test_output_dir,
    only,
    dry_run,
):
    # click.DateTime yields a datetime; we want a date for the day window.
    end_date = end_date.date() if end_date else common.default_end_date()

    start = datetime.datetime.utcnow()
    rows = common.run_query(
        billing_project,
        common.load_sql("graphics_dashboard.sql"),
        end_date=end_date,
        time_window=time_window,
        sample_id_count=sample_id_count,
    )
    payloads = reshape(
        rows,
        common.sample_fraction(sample_id_count),
        time_window,
        datetime.datetime.utcnow(),
        only=list(only),
    )
    phase_time = (datetime.datetime.utcnow() - start).total_seconds()
    for payload in payloads.values():
        payload["phaseTime"] = phase_time

    for filename, payload in payloads.items():
        if dry_run:
            print(
                f"Wrote {common.write_local_json(test_output_dir, filename, payload)}"
            )
        else:
            common.upload_json(output_bucket, output_prefix, filename, payload)


if __name__ == "__main__":
    main()
