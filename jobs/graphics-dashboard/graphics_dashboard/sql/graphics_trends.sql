-- Weekly aggregates feeding the trend-*-v2.json files of the Firefox Graphics
-- Telemetry dashboard (https://firefoxgraphics.github.io/telemetry/).
--
-- Unlike the per-snapshot dashboard query, trends are a weekly time series. One
-- scan over the requested window produces (week_start, output, key, value)
-- rows for every complete week in range; the docker job merges these into the
-- cached {created, trend:[{start,end,total,data}]} history for each file.
--
-- Source: firefox_desktop_stable.metrics_v1 (replacing telemetry_stable.main_v5
-- and python_moztelemetry's get_one_ping_per_client). Field mapping mirrors the
-- dashboard query; see that file's header for the legacy->Glean details.
--
-- Outputs (one per trend-*-v2.json):
--   firefox              -> Firefox major version counts
--   windows-versions     -> Windows OSVersion counts
--   windows-compositors  -> features.compositor
--   windows-arch         -> OS bitness (64/32/unknown)
--   windows-vendors      -> primary adapter vendorID
--   windows-d2d          -> d2d feature status
--   windows-d3d11        -> d3d11 feature status
--   windows-device-gen-{intel,nvidia,amd}
--                        -> primary adapter deviceID, grouped by GPU generation
--                           in the docker job via gfxdevices.json. SQL emits raw
--                           (vendor, device_id) counts; the reshape maps to gen.
--
-- Sampling/filtering (matching the legacy weekly job):
--   * sample_id in [0, @sample_id_count)  (each Glean bucket = 1% of clients;
--       the script sets the count)
--   * Firefox major version >= 53
--   * One ping per client per week (random within the week)
--   * Whole Sunday-aligned weeks fully inside [@start_date, @end_date]
--
-- Parameters:
--   @start_date       DATE   inclusive start of the backfill window
--   @end_date         DATE   inclusive end (default: yesterday)
--   @sample_id_count  INT64  number of sample_id buckets, [0, N) (default 1)
WITH params AS (
  SELECT
    @start_date AS start_date,
    COALESCE(@end_date, DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AS end_date,
    COALESCE(@sample_id_count, 1) AS sample_id_count
),
sampled AS (
  SELECT
    m.client_info.client_id AS client_id,
    DATE_TRUNC(DATE(m.submission_timestamp), WEEK(SUNDAY)) AS week_start,
    -- Stable per-(client, week) ordering so the retained ping is deterministic.
    FARM_FINGERPRINT(CONCAT(m.client_info.client_id, m.document_id)) AS dedup_order,
    SPLIT(m.client_info.app_display_version, '.')[SAFE_OFFSET(0)] AS fx_major,
    m.client_info.os AS os_name,
    CONCAT(m.client_info.os_version, '.0') AS os_version_key,
    m.client_info.architecture AS architecture,
    m.metrics.string.gfx_features_compositor AS compositor,
    CASE
      WHEN m.metrics.string.gfx_adapter_primary_vendor_id = 'Intel Open Source Technology Center'
        THEN '0x8086'
      ELSE m.metrics.string.gfx_adapter_primary_vendor_id
    END AS vendor_id,
    m.metrics.string.gfx_adapter_primary_device_id AS device_id,
    m.metrics.object.gfx_features_d3d11 AS d3d11_obj,
    m.metrics.object.gfx_features_d2d AS d2d_obj
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1` AS m,
    params
  WHERE
    DATE(m.submission_timestamp) BETWEEN params.start_date AND params.end_date
    AND m.sample_id < params.sample_id_count
    AND SAFE_CAST(SPLIT(m.client_info.app_display_version, '.')[SAFE_OFFSET(0)] AS INT64) >= 53
),
-- Only keep whole Sunday-to-Saturday weeks fully inside the window, so a
-- partial trailing week is never published (the legacy job re-queries the last
-- incomplete week on the next run).
complete_weeks AS (
  SELECT s.*
  FROM sampled s, params
  WHERE s.week_start >= DATE_TRUNC(params.start_date, WEEK(SUNDAY))
    AND DATE_ADD(s.week_start, INTERVAL 6 DAY) <= params.end_date
    AND s.week_start >= params.start_date
),
one_per_client_week AS (
  SELECT *
  FROM complete_weeks
  QUALIFY ROW_NUMBER() OVER (PARTITION BY client_id, week_start ORDER BY dedup_order) = 1
),
windows_week AS (
  SELECT *,
    CASE
      WHEN d3d11_obj IS NULL THEN 'unknown'
      WHEN JSON_VALUE(d3d11_obj, '$.status') != 'available'
        THEN COALESCE(JSON_VALUE(d3d11_obj, '$.status'), 'unknown')
      WHEN COALESCE(SAFE_CAST(JSON_VALUE(d3d11_obj, '$.warp') AS BOOL), FALSE) THEN 'warp'
      ELSE COALESCE(CAST(SAFE_CAST(JSON_VALUE(d3d11_obj, '$.version') AS FLOAT64) AS STRING), 'unknown')
    END AS d3d11_status,
    CASE
      WHEN d2d_obj IS NULL THEN 'unknown'
      WHEN JSON_VALUE(d2d_obj, '$.status') != 'available'
        THEN COALESCE(JSON_VALUE(d2d_obj, '$.status'), 'unknown')
      ELSE COALESCE(JSON_VALUE(d2d_obj, '$.version'), 'unknown')
    END AS d2d_status,
    COALESCE(compositor, 'none') AS compositor_key,
    CASE
      WHEN architecture IN ('x86-64', 'x86_64', 'aarch64') THEN '64'
      WHEN architecture = 'x86' THEN '32' ELSE 'unknown'
    END AS arch_bucket
  FROM one_per_client_week
  WHERE os_name = 'Windows'
)

-- firefox: Firefox major version, all platforms.
SELECT week_start, 'firefox' AS output, fx_major AS key, COUNT(*) AS value
FROM one_per_client_week WHERE fx_major IS NOT NULL GROUP BY week_start, fx_major
UNION ALL
SELECT week_start, 'windows-versions', os_version_key, COUNT(*)
FROM windows_week GROUP BY week_start, os_version_key
UNION ALL
SELECT week_start, 'windows-compositors', compositor_key, COUNT(*)
FROM windows_week GROUP BY week_start, compositor_key
UNION ALL
SELECT week_start, 'windows-arch', arch_bucket, COUNT(*)
FROM windows_week GROUP BY week_start, arch_bucket
UNION ALL
SELECT week_start, 'windows-vendors', COALESCE(vendor_id, 'unknown'), COUNT(*)
FROM windows_week GROUP BY week_start, 3
UNION ALL
SELECT week_start, 'windows-d2d', d2d_status, COUNT(*)
FROM windows_week GROUP BY week_start, d2d_status
UNION ALL
SELECT week_start, 'windows-d3d11', d3d11_status, COUNT(*)
FROM windows_week GROUP BY week_start, d3d11_status
UNION ALL
-- device-gen: raw (vendor, device_id) counts for the three tracked vendors. The
-- docker job maps device_id -> GPU generation via gfxdevices.json and
-- re-aggregates. output carries the vendor, key the device id.
SELECT week_start,
  CASE vendor_id WHEN '0x8086' THEN 'device-gen-intel'
                 WHEN '0x10de' THEN 'device-gen-nvidia'
                 WHEN '0x1002' THEN 'device-gen-amd' END AS output,
  COALESCE(device_id, 'unknown') AS key, COUNT(*)
FROM windows_week
WHERE vendor_id IN ('0x8086', '0x10de', '0x1002')
GROUP BY week_start, 2, key
