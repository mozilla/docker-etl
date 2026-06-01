# Firefox Graphics Telemetry dashboard jobs

These jobs produce the data behind the Firefox Graphics Telemetry dashboard
(<https://firefoxgraphics.github.io/telemetry/>), a public dashboard the
Graphics team uses to see the GPU/driver/OS/feature landscape of the Firefox
desktop population: which graphics adapters and drivers are in use, what WebGL,
Direct3D, Direct2D, and compositor states clients report, monitor resolutions and
refresh rates, CPU/memory characteristics, and how all of this trends over time.

The dashboard is a static site that fetches a set of JSON files from
`gs://moz-fx-data-static-websit-8565-analysis-output/gfx/telemetry-data/`
(served at `https://analysis-output.telemetry.mozilla.org/gfx/telemetry-data/`).
These jobs compute those JSON files from Firefox telemetry and upload them.

## What the jobs produce

There are two jobs, differing in cadence and shape of output.

Dashboard (snapshot) job: a point-in-time view over a rolling 14-day
window, rebuilt daily. 11 files:

| File | What it shows |
| ---- | ------------- |
| `general-statistics.json` | device & driver counts; per-Firefox-version OS / Windows-version / vendor breakdowns |
| `device-statistics.json` | vendor/device/driver triples |
| `system-statistics.json` | CPU logical cores, CPU feature flags, RAM buckets, OS bitness |
| `monitor-statistics.json` | monitor counts, refresh rates, resolutions (Windows) |
| `mac-statistics.json` | macOS versions, retina scale, architecture |
| `linux-statistics.json` | driver vendors, compositors |
| `windows-features.json` | compositor, D3D11, D2D, GPU-process, texture-sharing, and media-decoder states, overall and per Windows version; D3D11 blocklist/blocklisted breakdowns |
| `tdr-statistics.json` | GPU device-reset (TDR) reasons, broken down by vendor (Windows) |
| `sanity-test-statistics.json` | graphics sanity-test outcomes by vendor/OS/device/driver (Windows) |
| `startup-test-statistics.json` | graphics driver startup-test results (Windows) |
| `webgl-statistics.json` | WebGL 1 and 2 success/failure by OS/vendor/device/driver/compositor, plus failure-id breakdowns |

Trends job: a weekly time series, one data point per complete week,
appended to a growing per-file history. 10 files (all `-v2.json`):
`trend-firefox`, `trend-windows-versions`, `trend-windows-compositors`,
`trend-windows-arch`, `trend-windows-vendors`, `trend-windows-d2d`,
`trend-windows-d3d11`, and `trend-windows-device-gen-{intel,nvidia,amd}` (GPU
generation mix per vendor).

Each trend file is `{"created": <ts>, "trend": [{start, end, total, data}, ...]}`.
A run computes the latest complete week(s) and merges them into the existing
history, keeping the long back-history intact.

## How they work

Both jobs are the same two-stage shape: aggregate in BigQuery, reshape in
Python.

1. SQL (`graphics_dashboard/sql/`) aggregates the Firefox desktop Glean
   `metrics` ping (`moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1`)
   into a long-format `(output, dimension, key, ...)` row set. One query per
   cadence:
   - `graphics_dashboard.sql`: one scan of the 14-day window feeds all 11
     snapshot files.
   - `graphics_trends.sql`: one scan feeds all 10 trend files, grouped into
     Sunday-aligned weeks.

   Within each query, every output file's rows are a set of CTEs and `UNION ALL`
   branches tagged with an `output` name. All the counting and bucketing is
   done here as `GROUP BY`s, so the source ping is scanned once per query (two
   scans total for all 21 files) rather than once per file.

2. Python (`graphics_dashboard/`) runs the query and pivots the
   rows into each file's JSON shape, then uploads to GCS:
   - `dashboard.py`: each file is built by a small function in the `BUILDERS`
     registry from an `OutputData` view of its rows.
   - `trends.py`: turns each week's rows into a `{start, end, total, data}`
     point and merges it into the cached `{created, trend:[...]}` history,
     replacing recomputed weeks and appending new ones.

### Sampling

Both queries sample on Glean `sample_id` (0 to 99, each bucket is 1% of clients),
selecting buckets `[0, N)` where `N = --sample-id-count` (default 1, i.e. ~1%).
For the dashboard, the `fraction` reported in each file's `sessions.metadata`
is derived from the same `N`, so the label always matches the actual filter.
Widen with `--sample-id-count 10` for ~10%, and so on. Aggregation cost scales with
the sample but stays a `GROUP BY`, not a per-ping shuffle.

### Running locally

```bash
pip install -r requirements.txt
# Snapshot files (all, or one with --only):
python -m graphics_dashboard.dashboard --dry-run --billing-project=mozdata
python -m graphics_dashboard.dashboard --dry-run --billing-project=mozdata --only mac-statistics.json
# Weekly trend files:
python -m graphics_dashboard.trends --dry-run --billing-project=mozdata
python -m graphics_dashboard.trends --dry-run --billing-project=mozdata --only firefox
```

Authenticate with `gcloud auth application-default login`. `--dry-run` writes
the JSON files to a local `test_output/` directory (override with
`--test-output-dir`) instead of uploading to GCS. Each file is byte-for-byte
what would be uploaded, so you can inspect or diff it. `--billing-project`
selects the GCP project the BigQuery query runs and bills in (the query reads
`moz-fx-data-shared-prod` tables by fully-qualified name regardless); under
on-demand billing this is how query cost is attributed. The CLIs use
[click](https://click.palletsprojects.com/); run with `--help` for all options.

---

# Migration notes

This directory is a prototype that replaces the legacy Spark/Dataproc jobs
in `mozetl/graphics/` (`graphics_telemetry_dashboard.py`,
`graphics_telemetry_trends.py`). Those jobs run on an old Dataproc image and
depend on `python_moztelemetry` plus the FirefoxGraphics `bigquery_shim`, none of
which are supported anymore. The output JSON files, the dashboard
frontend, and the GCS location are all unchanged.

## Key decisions

- Source ping is Glean `metrics_v1`, not legacy `main_v5`. Everything reads
  `firefox_desktop_stable.metrics_v1`. The field mapping and the semantic
  differences this forces are tabulated below.
- Aggregate in BigQuery, thin client. All counting/bucketing is SQL
  `GROUP BY`s; the Python only pivots tidy aggregate rows into JSON. This is
  what makes the client cheap and Spark-free.
- Two queries, not 22 and not 1. They are grouped by the one boundary that
  differs: snapshot (14-day point-in-time) vs trend (weekly series). Files in a
  group share one scan via the long-format `(output, ...)` shape, so the ping is
  scanned twice total.
- docker-etl only, no bigquery-etl. This is a low-importance job not
  expected to see further development, so it optimizes for the lowest
  operational surface: the SQL is embedded in the docker image and run ad-hoc
  rather than promoted to a scheduled bigquery-etl derived table. One repo, one
  PR, one deploy, and the SQL-to-reshape contract stays atomic. The cost concern
  is handled by BigQuery on-demand billing (`--billing-project`) rather than a
  materialized cache. We give up catalog/lineage visibility, which only matters
  under active development.
- The CLIs use `click` (the repo standard), not `argparse`.

## Directory layout

```
graphics-dashboard/                 # the whole job: reshape + upload + SQL
├── Dockerfile                     #   one image, two entry points (dashboard, trends)
├── README.md
├── requirements.txt               #   click, google-cloud-bigquery, -storage
├── setup.py
└── graphics_dashboard/
    ├── common.py                  #   shared SQL loading, query exec, GCS/local IO, click options
    ├── dashboard.py               #   snapshot reshape (BUILDERS registry)
    ├── trends.py                  #   weekly reshape + GCS history merge
    └── sql/
        ├── README.md              #     describes the two queries
        ├── graphics_dashboard.sql #     one scan, 11 *-statistics / windows-features
        └── graphics_trends.sql    #     one scan, 10 trend-*-v2 (per complete week)
```

The SQL lives inside the `graphics_dashboard` package, so it ships with the
code (no separate copy step); `common.load_sql` reads it relative to the
package. The SQL files are named for what they produce and documented in
`graphics_dashboard/sql/README.md`. The image exposes both modules via
`ENTRYPOINT ["python", "-m"]`.

## Row shape

Rows are `(output, dimension, key, subkey, value)`. `subkey` is NULL for flat
`{key: value}` dimensions and used for two-level breakdowns (e.g. TDR
`reasonToVendor` = key=reason, subkey=vendor). Histogram `results` arrays are
emitted as flat rows keyed by integer bucket index; the reshape orders and
densifies them. (The trends query omits `subkey`; its rows are
`(week_start, output, key, value)`.)

## Field mapping (legacy `main_v5` to Glean `metrics_v1`)

| Output | Legacy (`main_v5`)                       | Glean (`metrics_v1`)                          |
| ------ | ---------------------------------------- | --------------------------------------------- |
| shared | `environment.build.version`              | `client_info.app_display_version`             |
| shared | `environment.system.os.name`             | `client_info.os` (`Darwin`, `Linux`, `Windows`) |
| mac    | `environment.build.architecture`         | `client_info.architecture`                    |
| mac    | `environment.system.os.version`          | `client_info.os_version`                      |
| mac    | `gfx.monitors[0].scale`                  | `gfx_monitors[0].contentsScaleFactor` (JSON)  |
| linux  | `gfx.adapters[0].driverVendor`           | `gfx_adapter_primary_driver_vendor`           |
| linux  | `gfx.features.compositor`                | `gfx_features_compositor`                     |
| general/device | adapter deviceID/driverVersion   | `gfx_adapter_primary_{device_id,driver_version}` |
| system | `environment.system.cpu.count`           | `system_cpu_logical_cores`                    |
| system | `environment.system.cpu.extensions`      | `system_cpu_extensions` (string_list)         |
| system | `environment.system.memoryMB`            | `system_memory` (MB)                          |
| monitor| `gfx.monitors[*]` (screenW/H, refreshRate) | `gfx_monitors` (JSON array)                 |
| windows-features | `gfx.features.{d3d11,d2d,gpuProcess}` | `gfx_features_{d3d11,d2d,gpu_process}` (JSON) |
| windows-features | `gfx.ContentBackend`           | `gfx_content_backend`                         |
| windows-features | `.../MEDIA_DECODER_BACKEND_USED`  | `custom_distribution.media_decoder_backend_used` |
| tdr    | `payload/histograms/DEVICE_RESET_REASON` | `custom_distribution.gfx_device_reset_reason` |
| startup| `.../GRAPHICS_DRIVER_STARTUP_TEST`         | `custom_distribution.gfx_graphics_driver_startup_test` |
| sanity | `.../GRAPHICS_SANITY_TEST`                 | `custom_distribution.gfx_sanity_test`         |
| sanity/webgl | adapter vendorID/deviceID/driverVersion | `gfx_adapter_primary_{vendor_id,device_id,driver_version}` |
| webgl  | `.../CANVAS_WEBGL_SUCCESS` (bucket 0=fail, 1=ok) | `labeled_counter.canvas_webgl_success` (keys `false`/`true`) |
| webgl  | `.../CANVAS_WEBGL2_SUCCESS`                | `labeled_counter.canvas_webgl2_success`       |
| webgl  | `.../CANVAS_WEBGL_FAILURE_ID`              | `labeled_counter.canvas_webgl_failure_id`     |
| webgl  | `.../CANVAS_WEBGL_ACCL_FAILURE_ID`         | `labeled_counter.canvas_webgl_accl_failure_id`|
| trends device-gen | `gfxdevices.json` device-to-generation map | same JSON, fetched at run time and applied in `trends.py` |

Glean-specific semantic notes (also documented inline in the queries):

- Architecture: Glean reports `aarch64` (Apple Silicon) and `x86_64`, not
  `x86-64` or `x86`. All 64-bit forms fold into the `64` bucket. Apple Silicon
  was counted as `unknown` by the legacy job, so expect `64` up and `unknown`
  down on the mac dashboard.
- OS version strings: Glean `os_version` is `major.minor` (`10.0`, `6.1`)
  with no service-pack component, so legacy keys like `Windows-6.1.1.0` collapse
  to `Windows-<os_version>.0` (e.g. `Windows-10.0.0`). Service-pack granularity
  is not recoverable.
- Histogram custom_distributions expose a `.values` array of
  `{key: bucket_index, value: count}`; `UNNEST` plus `SUM(value)` reproduces the
  legacy element-wise histogram sum.
- WebGL success/failure is a per-session classification: failure if the
  `false` counter > 0, success if `false == 0 AND true > 0` (successes are not
  double-counted with failures), matching the legacy 2-bucket logic.
- windows-features feature status: the d3d11/d2d/gpu_process status strings
  are derived from the Glean `gfx_features_*` JSON objects (status maps to
  version|warp for available d3d11, etc.). Two fields have no Glean source:
  `advancedLayers` (WebRender-era deprecation; reported all-`none`) and
  `plugin_models` (deprecated; empty, as in current prod). The legacy
  `blacklisted`/`blocked` d3d11 statuses are now `blocklisted:FEATURE_FAILURE_...`,
  matched by prefix.
- system `wow` (OS bitness): Glean has no `isWow64`, so the legacy
  `32_on_64` bucket cannot be distinguished, and 32-bit builds map to `32`.

## Trends specifics

- The query only emits whole Sunday-aligned weeks, so a partial trailing
  week is never published (the next daily run picks it up once complete).
- The reshape keys cached weeks by their `start` timestamp and replaces any
  recomputed week, so re-running over an overlapping window is idempotent.
- One ping per client per week is chosen deterministically
  (`FARM_FINGERPRINT(client_id, document_id)`), replacing the legacy
  `get_one_ping_per_client`.
- Device-gen trends map device ids to GPU generation via `gfxdevices.json`,
  fetched from the FirefoxGraphics repo at run time and applied in `trends.py`.

## `layers-failureid-statistics.json` is dropped

The `LAYERS_D3D11_FAILURE_ID` / `LAYERS_OPENGL_FAILURE_ID` histograms that fed
it were never migrated to Glean (no corresponding metric in `metrics_v1`), and
the prod file has been frozen since 2024-08-21. It is not produced by this
pipeline.

## Validating against current prod output

```bash
fn=mac-statistics.json   # any covered file
python -m graphics_dashboard.dashboard --dry-run --only "$fn"   # writes test_output/$fn
curl -s "https://analysis-output.telemetry.mozilla.org/gfx/telemetry-data/$fn" > /tmp/prod.json
diff <(jq -S 'del(.phaseTime, .sessions.timestamp, .sessions.shortdate)' /tmp/prod.json) \
     <(jq -S 'del(.phaseTime, .sessions.timestamp, .sessions.shortdate)' "test_output/$fn")
```

Counts won't match exactly. The legacy job sampled ~3% of clients (`main_v5`
`sample_id = 42` at 10% buckets, further reduced by a `RAND()` filter), whereas
this query reads the first `--sample-id-count` Glean buckets (default 1 ≈ 1%).
The set of keys should be very similar; large differences in the *relative*
distribution of any bucket signal a regression. Expected deliberate
differences:

- `mac` `arch`: `64` now includes Apple Silicon (`aarch64`); `unknown` shrinks.
- Windows OS-version keys collapse to `Windows-<major.minor>.0` (no service
  pack), so e.g. legacy `6.1.0` and `6.1.1.0` merge.
- `tdr`/`sanity`/`monitor`/`windows-features` `sessions.count` is the count of
  the legacy ping set passed to each (Windows pings), while
  `general`/`device`/`system`/`startup`/`webgl` use all general pings;
  histogram-bearing subset counts (`tdrPings`, `startupTestPings`,
  `sanityTestPings`, webgl success/failure) are reported separately.
- `system` `wow` has no `32_on_64` bucket (Glean lacks `isWow64`).
- `windows-features` `advanced_layers` is all-`none` and `plugin_models` is
  empty (no Glean source). `byVersion` covers `10.0.0`/`6.1.0`/`6.2.0`/`6.3.0`
  (the `6.1.1` service-pack variant is gone).
- Trends use the first Glean `sample_id` bucket (~1%) vs the legacy weekly
  ~0.3% fraction, so trend `total`s shift but distributions are comparable. The
  legacy back-history in the existing `trend-*-v2.json` files is preserved, and
  new Glean weeks simply append.
