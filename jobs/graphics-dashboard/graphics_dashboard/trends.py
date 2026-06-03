"""Build the weekly trend-*-v2.json files of the Firefox Graphics dashboard.

Replacement for the Spark job `mozetl.graphics.graphics_telemetry_trends`. Runs
the single weekly query in `graphics_dashboard/sql/graphics_trends.sql`, turns each week's
(week_start, output, key, value) rows into a trend data point, and merges those
points into the cached history for each file:

    {"created": <ts>, "trend": [{"start", "end", "total", "data": {k: v}}, ...]}

stored at gs://<bucket>/<prefix>/trend-<name>-v2.json. Weeks already present in
the cache are replaced by freshly computed ones; new weeks are appended; the
trend list stays sorted ascending by start. Because the query only emits whole
weeks, a partial trailing week is never written (the next run picks it up once
complete) — matching the legacy backfill behavior.
"""

import datetime
import json
import time

import click

from graphics_dashboard import common

# Maps the query's `output` tag to the trend-<name>-v2.json filename stem.
TREND_FILES = {
    "firefox": "trend-firefox",
    "windows-versions": "trend-windows-versions",
    "windows-compositors": "trend-windows-compositors",
    "windows-arch": "trend-windows-arch",
    "windows-vendors": "trend-windows-vendors",
    "windows-d2d": "trend-windows-d2d",
    "windows-d3d11": "trend-windows-d3d11",
    "device-gen-intel": "trend-windows-device-gen-intel",
    "device-gen-nvidia": "trend-windows-device-gen-nvidia",
    "device-gen-amd": "trend-windows-device-gen-amd",
}

# device-gen outputs carry raw device ids; these are mapped to GPU generations
# via gfxdevices.json. output tag -> vendor id key in that file.
DEVICE_GEN_VENDORS = {
    "device-gen-intel": "0x8086",
    "device-gen-nvidia": "0x10de",
    "device-gen-amd": "0x1002",
}

GFXDEVICES_URL = (
    "https://raw.githubusercontent.com/FirefoxGraphics/moz-gfx-telemetry/"
    "master/www/gfxdevices.json"
)

# How far back to (re)compute weeks by default. The merge replaces these weeks
# in the cache, so a modest window keeps the daily run cheap while still
# self-healing recent data.
DEFAULT_BACKFILL_DAYS = 28


def fetch_device_map():
    """Fetch gfxdevices.json: {vendor: {device_id: [generation, codename]}}."""
    import urllib.request

    with urllib.request.urlopen(GFXDEVICES_URL) as resp:
        return json.loads(resp.read())


def _week_bounds(week_start):
    """(start_ts, end_ts) for a Sunday week_start date, end = start + 7 days.

    Matches the legacy data points where end - start == 7 days (the Sunday that
    begins the following week).
    """
    start_dt = datetime.datetime(week_start.year, week_start.month, week_start.day)
    end_dt = start_dt + datetime.timedelta(days=7)
    return time.mktime(start_dt.timetuple()), time.mktime(end_dt.timetuple())


def build_points(rows, output, device_map=None):
    """Turn this output's rows into {week_start_date: {start,end,total,data}}.

    For device-gen outputs, device ids are mapped to GPU generations and
    re-aggregated; `device_map` is the vendor's {device_id: [gen, ...]} block.
    """
    weeks = {}
    for r in rows:
        if r["output"] != output:
            continue
        wk = r["week_start"]  # datetime.date
        key = r["key"]
        value = int(r["value"])
        if device_map is not None:
            entry = device_map.get(key)
            key = entry[0] if entry else "unknown"
        bucket = weeks.setdefault(wk, {})
        bucket[key] = bucket.get(key, 0) + value

    points = {}
    for wk, data in weeks.items():
        start_ts, end_ts = _week_bounds(wk)
        points[wk] = {
            "start": start_ts,
            "end": end_ts,
            "total": sum(data.values()),
            "data": data,
        }
    return points


def merge_trend(cache, new_points):
    """Merge {week_date: point} into a cache dict, replacing overlapping weeks.

    Returns the updated cache. Weeks are identified by their `start` timestamp;
    a freshly computed week replaces any existing entry with the same start.
    """
    if cache is None:
        cache = {
            "created": time.mktime(datetime.datetime.now(datetime.UTC).timetuple()),
            "trend": [],
        }
    by_start = {pt["start"]: pt for pt in cache.get("trend") or []}
    for pt in new_points.values():
        by_start[pt["start"]] = pt
    cache["trend"] = [by_start[s] for s in sorted(by_start)]
    return cache


# Each trend file is stored as <stem>-v2.json.
def _filename(stem):
    return f"{stem}-v2.json"


@click.command()
@click.option(
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help=f"Inclusive start of the backfill window. Defaults to "
    f"{DEFAULT_BACKFILL_DAYS} days before --end-date.",
)
@click.option(
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="Inclusive end of the window (YYYY-MM-DD). Defaults to yesterday UTC.",
)
@common.billing_project_option
@common.sample_id_count_option
@common.output_location_options
@click.option(
    "--only",
    multiple=True,
    help="Limit to specific trend output tags (repeatable). Defaults to all.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Write the merged trend files to --test-output-dir instead of "
    "uploading. The existing history is still read from GCS so each file is "
    "the real merged result.",
)
def main(
    start_date,
    end_date,
    billing_project,
    sample_id_count,
    output_bucket,
    output_prefix,
    test_output_dir,
    only,
    dry_run,
):
    end_date = end_date.date() if end_date else common.default_end_date()
    start_date = (
        start_date.date()
        if start_date
        else (end_date - datetime.timedelta(days=DEFAULT_BACKFILL_DAYS))
    )
    outputs = list(only) or list(TREND_FILES)

    rows = common.run_query(
        billing_project,
        common.load_sql("graphics_trends.sql"),
        start_date=start_date,
        end_date=end_date,
        sample_id_count=sample_id_count,
    )

    device_map = None
    if any(o in DEVICE_GEN_VENDORS for o in outputs):
        device_map = fetch_device_map()

    results = {}
    for output in outputs:
        stem = TREND_FILES[output]
        vendor_block = (
            device_map.get(DEVICE_GEN_VENDORS[output], {})
            if (device_map and output in DEVICE_GEN_VENDORS)
            else None
        )
        new_points = build_points(rows, output, device_map=vendor_block)
        # Read the existing history in both modes so the merged file is the real
        # result; --dry-run only changes where the result is written.
        cache = common.read_json(output_bucket, output_prefix, _filename(stem))
        results[stem] = merge_trend(cache, new_points)

    for stem, cache in results.items():
        name = _filename(stem)
        if dry_run:
            path = common.write_local_json(test_output_dir, name, cache)
            print(f"Wrote {path} ({len(cache['trend'])} points)")
        else:
            common.upload_json(output_bucket, output_prefix, name, cache)


if __name__ == "__main__":
    main()
