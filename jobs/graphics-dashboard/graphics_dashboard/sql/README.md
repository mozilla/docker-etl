# Graphics telemetry queries

Two BigQuery queries aggregate the Firefox desktop Glean `metrics` ping
(`moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1`) into long-format
`(output, dimension, key, …)` rows. Each is run ad-hoc by the docker job, which
filters by the `output` column and reshapes the rows into the dashboard JSON
files. See the job's top-level `README.md` for the full pipeline and field
mappings.

## `graphics_dashboard.sql`

Per-snapshot aggregates. A single scan over a rolling 14-day window
(`@end_date`, `@time_window`) feeds every `*-statistics.json` file plus
`windows-features.json` — i.e. a point-in-time view rebuilt daily, with no
history. Replaces the snapshot portion of the legacy Spark job
`mozetl.graphics.graphics_telemetry_dashboard`.

Rows are `(output, dimension, key, subkey, value)`. Outputs: `mac`, `linux`,
`general`, `device`, `system`, `monitor`, `tdr`, `startup`, `sanity`, `webgl`,
`windows-features` (every snapshot file the legacy job produced except
`layers-failureid`, which has no Glean source). The reshape lives in
`graphics_dashboard.dashboard`.

Parameters: `@end_date` (default yesterday), `@time_window` (default 14),
`@sample_id_count` (default 1; number of Glean `sample_id` buckets to scan).

## `graphics_trends.sql`

Weekly trend aggregates. One scan over `[@start_date, @end_date]` produces one
data point per complete Sunday-aligned week for every `trend-*-v2.json` file.
Replaces the legacy Spark job `mozetl.graphics.graphics_telemetry_trends` (and
its `python_moztelemetry` `get_one_ping_per_client` dependency, via a
deterministic per-week dedup).

Rows are `(week_start, output, key, value)`. Outputs: `firefox`,
`windows-versions`, `windows-compositors`, `windows-arch`, `windows-vendors`,
`windows-d2d`, `windows-d3d11`, `device-gen-{intel,nvidia,amd}`. The reshape +
GCS history merge lives in `graphics_dashboard.trends`, which also maps device
ids to GPU generations via `gfxdevices.json` for the device-gen outputs. The
history is stored as JSON in GCS, not a materialized table.

Parameters: `@start_date`, `@end_date` (default yesterday), `@sample_id_count`
(default 1).
