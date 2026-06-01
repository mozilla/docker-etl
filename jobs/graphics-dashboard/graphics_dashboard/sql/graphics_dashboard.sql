-- Aggregates feeding the per-snapshot files of the Firefox Graphics Telemetry
-- dashboard (https://firefoxgraphics.github.io/telemetry/).
--
-- One scan of the Glean `metrics` ping over a rolling 14-day window feeds
-- every per-platform "*-statistics.json" file plus windows-features.json. Each
-- output row is one (output, dimension, key, subkey, value) tuple; the
-- downstream reshape script filters by `output` and pivots the rest into the
-- JSON shape each file requires.
--
-- `subkey` is NULL for flat {key: value} dimensions. It is used for two-level
-- breakdowns: e.g. TDR's reasonToVendor is (dimension='reasonToVendor',
-- key=<reason>, subkey=<vendor>, value=<count>), which the reshape nests as
-- {reason: {vendor: count}}. Ordered "results" arrays are emitted as a flat
-- dimension keyed by the integer bucket index; the reshape sorts and densifies.
--
-- Outputs: mac, linux, general, device, system, monitor, tdr, startup-test,
-- sanity-test, webgl, windows-features. This covers every snapshot file the
-- legacy job produced except layers-failureid, which has no Glean source (the
-- LAYERS_*_FAILURE_ID histograms were never migrated). To add another output,
-- add a CTE that emits rows tagged with its `output` name and UNION ALL it at
-- the bottom.
--
-- Source: firefox_desktop_stable.metrics_v1, replacing the legacy
-- telemetry_stable.main_v5 ping the Spark job used. Field mapping (legacy ->
-- Glean) per output is documented inline in each output's CTEs.
--
-- Sampling/filtering preserved from the legacy Spark job
-- (`mozetl.graphics.graphics_telemetry_dashboard`):
--   * sample_id in [0, @sample_id_count)  (each Glean bucket = 1% of clients;
--       the script sets the count and derives the sessions.metadata `fraction`
--       from the same value, so they always agree)
--   * Firefox major version >= 53
--   * One ping per client_id (latest by submission_timestamp)
--   * Rolling @time_window-day window ending @end_date
--
-- Parameters:
--   @end_date         DATE   (inclusive end of the window; default: yesterday)
--   @time_window      INT64  (default 14)
--   @sample_id_count  INT64  (number of sample_id buckets, [0, N); default 1)
WITH params AS (
  SELECT
    COALESCE(@end_date, DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) AS end_date,
    COALESCE(@time_window, 14) AS time_window,
    COALESCE(@sample_id_count, 1) AS sample_id_count
),
sampled AS (
  SELECT
    m.client_info.client_id AS client_id,
    m.submission_timestamp,
    m.client_info.app_display_version AS fx_version_raw,
    m.client_info.architecture AS architecture,
    m.client_info.os AS os_name,
    m.client_info.os_version AS os_version,
    -- Linux: gfx.adapters[0].driverVendor; in Glean this is exploded into the
    -- primary-adapter scalar, already in the legacy 'mesa/iris' form.
    m.metrics.string.gfx_adapter_primary_driver_vendor AS driver_vendor,
    -- features.compositor; in Glean exploded to a top-level string scalar.
    m.metrics.string.gfx_features_compositor AS compositor,
    -- Primary adapter ids. Legacy folded the "Intel Open Source Technology
    -- Center" vendor string into 0x8086; reproduce that here.
    CASE
      WHEN m.metrics.string.gfx_adapter_primary_vendor_id = 'Intel Open Source Technology Center'
        THEN '0x8086'
      ELSE m.metrics.string.gfx_adapter_primary_vendor_id
    END AS vendor_id,
    m.metrics.string.gfx_adapter_primary_device_id AS device_id,
    m.metrics.string.gfx_adapter_primary_driver_version AS driver_version,
    -- Windows OS string. Glean os_version is major.minor (e.g. '10.0', '6.1')
    -- with no service-pack component, so the legacy 'Windows-10.0.0' /
    -- 'Windows-6.1.1.0' keys collapse to 'Windows-<os_version>.0'. (Service
    -- pack granularity is not available in Glean.)
    CONCAT('Windows-', m.client_info.os_version, '.0') AS windows_os,
    -- Histogram-shaped metrics (custom_distribution .values = [{key, value}]).
    m.metrics.custom_distribution.gfx_device_reset_reason.values AS tdr_values,
    m.metrics.custom_distribution.gfx_graphics_driver_startup_test.values AS startup_values,
    m.metrics.custom_distribution.gfx_sanity_test.values AS sanity_values,
    -- deviceID/driverVersion are vendor-prefixed in the legacy output.
    CONCAT(
      CASE WHEN m.metrics.string.gfx_adapter_primary_vendor_id = 'Intel Open Source Technology Center'
        THEN '0x8086' ELSE COALESCE(m.metrics.string.gfx_adapter_primary_vendor_id, 'Unknown') END,
      '/', COALESCE(m.metrics.string.gfx_adapter_primary_device_id, 'Unknown')
    ) AS device_key,
    CONCAT(
      CASE WHEN m.metrics.string.gfx_adapter_primary_vendor_id = 'Intel Open Source Technology Center'
        THEN '0x8086' ELSE COALESCE(m.metrics.string.gfx_adapter_primary_vendor_id, 'Unknown') END,
      '/', COALESCE(m.metrics.string.gfx_adapter_primary_driver_version, 'Unknown')
    ) AS driver_key,
    -- sanity 'windows' share uses bare OSVersion (e.g. '10.0.0'), not the
    -- 'Windows-' prefixed OS string that startup uses.
    CONCAT(m.client_info.os_version, '.0') AS os_version_key,
    -- General OS string (legacy `OS`): 'Windows-<ver>.0', 'Darwin-<ver>',
    -- 'Linux', else '<os>-<ver>'. Used by the webgl os breakdowns.
    CASE m.client_info.os
      WHEN 'Windows' THEN CONCAT('Windows-', m.client_info.os_version, '.0')
      WHEN 'Darwin' THEN CONCAT('Darwin-', m.client_info.os_version)
      WHEN 'Linux' THEN 'Linux'
      ELSE CONCAT(m.client_info.os, '-', m.client_info.os_version)
    END AS os_key,
    -- WebGL session success/failure counters (Glean labeled_counter keyed
    -- 'true'/'false', replacing the legacy 2-bucket SUCCESS histograms where
    -- bucket[0]=failure, bucket[1]=success).
    (SELECT COALESCE(SUM(IF(v.key = 'false', v.value, 0)), 0)
       FROM UNNEST(m.metrics.labeled_counter.canvas_webgl_success) v) AS webgl1_fail,
    (SELECT COALESCE(SUM(IF(v.key = 'true', v.value, 0)), 0)
       FROM UNNEST(m.metrics.labeled_counter.canvas_webgl_success) v) AS webgl1_ok,
    ARRAY_LENGTH(m.metrics.labeled_counter.canvas_webgl_success) AS webgl1_len,
    (SELECT COALESCE(SUM(IF(v.key = 'false', v.value, 0)), 0)
       FROM UNNEST(m.metrics.labeled_counter.canvas_webgl2_success) v) AS webgl2_fail,
    (SELECT COALESCE(SUM(IF(v.key = 'true', v.value, 0)), 0)
       FROM UNNEST(m.metrics.labeled_counter.canvas_webgl2_success) v) AS webgl2_ok,
    ARRAY_LENGTH(m.metrics.labeled_counter.canvas_webgl2_success) AS webgl2_len,
    -- Keyed failure-id counters for the 'general' webgl block.
    m.metrics.labeled_counter.canvas_webgl_failure_id AS webgl_failure_id,
    m.metrics.labeled_counter.canvas_webgl_accl_failure_id AS webgl_accl_failure_id,
    -- general/device: deviceID and driverVersion (already vendor-prefixed via
    -- device_key/driver_key). deviceAndDriver appends driverVersion to deviceID.
    CONCAT(
      CASE WHEN m.metrics.string.gfx_adapter_primary_vendor_id = 'Intel Open Source Technology Center'
        THEN '0x8086' ELSE COALESCE(m.metrics.string.gfx_adapter_primary_vendor_id, 'Unknown') END,
      '/', COALESCE(m.metrics.string.gfx_adapter_primary_device_id, 'Unknown'),
      '/', COALESCE(m.metrics.string.gfx_adapter_primary_driver_version, 'Unknown')
    ) AS device_and_driver_key,
    -- general byFx 'windows' breakdown uses bare OSVersion (e.g. '10.0.0').
    SPLIT(m.client_info.app_display_version, '.')[SAFE_OFFSET(0)] AS fx_major,
    -- system: cpu logical cores, extensions, total memory (MB).
    m.metrics.quantity.system_cpu_logical_cores AS logical_cores,
    m.metrics.string_list.system_cpu_extensions AS cpu_extensions,
    m.metrics.quantity.system_memory AS memory_mb,
    -- windows-features: content backend + feature-status objects (JSON).
    m.metrics.string.gfx_content_backend AS content_backend,
    m.metrics.object.gfx_features_d3d11 AS d3d11_obj,
    m.metrics.object.gfx_features_d2d AS d2d_obj,
    m.metrics.object.gfx_features_gpu_process AS gpu_process_obj,
    m.metrics.custom_distribution.media_decoder_backend_used.values AS media_values,
    -- monitor-statistics: full monitors JSON array.
    JSON_QUERY_ARRAY(m.metrics.object.gfx_monitors) AS monitors_arr,
    -- mac: monitors[0].scale -> primary monitor contentsScaleFactor (JSON string).
    JSON_VALUE(
      JSON_QUERY_ARRAY(m.metrics.object.gfx_monitors)[SAFE_OFFSET(0)],
      '$.contentsScaleFactor'
    ) AS monitor_scale
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1` AS m,
    params
  WHERE
    DATE(m.submission_timestamp)
      BETWEEN DATE_SUB(params.end_date, INTERVAL params.time_window - 1 DAY) AND params.end_date
    AND m.sample_id < params.sample_id_count
    AND SAFE_CAST(SPLIT(m.client_info.app_display_version, '.')[SAFE_OFFSET(0)] AS INT64) >= 53
),
one_per_client AS (
  SELECT
    *
  FROM sampled
  QUALIFY ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY submission_timestamp DESC) = 1
),
-- The shared 'sessions.share' block is the Firefox-major-version breakdown over
-- all general pings (not platform-filtered). Emitted once per output that needs
-- it; the reshape attaches it to that output's `sessions` block.
fx_version_share AS (
  SELECT
    SPLIT(fx_version_raw, '.')[SAFE_OFFSET(0)] AS key,
    COUNT(*) AS value
  FROM one_per_client
  WHERE fx_version_raw IS NOT NULL
  GROUP BY key
),

-- Reusable per-ping derivations shared by general/device. os_name_simple maps
-- the Glean os to the legacy OSName ('Windows'/'Darwin'/'Linux'/other), and
-- os_string is the legacy OS string used by byFx 'os'/'windows' breakdowns.
general_base AS (
  SELECT *,
    CASE os_name WHEN 'Windows' THEN 'Windows' WHEN 'Darwin' THEN 'Darwin' WHEN 'Linux' THEN 'Linux' ELSE os_name END AS os_name_simple
  FROM one_per_client
),
-- Windows-only ping set, shared by system/monitor/tdr/sanity/windows-features.
windows_pings AS (
  SELECT * FROM one_per_client WHERE os_name = 'Windows'
),

-- ========================== general-statistics.json =========================
-- devices/drivers: vendor-prefixed counts over all general pings. byFx: per
-- Firefox major version (plus an 'all' rollup) the os / windows-version /
-- vendor breakdowns. Emitted as dimension='byFx_<category>', key=<fxVer|'all'>,
-- subkey=<breakdown value>; the reshape nests byFx[fxVer][category][subkey].
general_rows AS (
  SELECT 'devices' AS dimension, device_key AS key, CAST(NULL AS STRING) AS subkey, COUNT(*) AS value
  FROM general_base GROUP BY device_key
  UNION ALL
  SELECT 'drivers', driver_key, CAST(NULL AS STRING), COUNT(*) FROM general_base GROUP BY driver_key
  -- byFx 'os': OSName counts, per fx version and 'all'.
  UNION ALL SELECT 'byFx_os', fx_major, os_name_simple, COUNT(*) FROM general_base GROUP BY fx_major, os_name_simple
  UNION ALL SELECT 'byFx_os', 'all', os_name_simple, COUNT(*) FROM general_base GROUP BY os_name_simple
  -- byFx 'windows': Windows OSVersion counts (Windows pings only).
  UNION ALL SELECT 'byFx_windows', fx_major, os_version_key, COUNT(*) FROM general_base WHERE os_name = 'Windows' GROUP BY fx_major, os_version_key
  UNION ALL SELECT 'byFx_windows', 'all', os_version_key, COUNT(*) FROM general_base WHERE os_name = 'Windows' GROUP BY os_version_key
  -- byFx 'vendors': vendorID counts.
  UNION ALL SELECT 'byFx_vendors', fx_major, COALESCE(vendor_id, 'unknown'), COUNT(*) FROM general_base GROUP BY fx_major, 3
  UNION ALL SELECT 'byFx_vendors', 'all', COALESCE(vendor_id, 'unknown'), COUNT(*) FROM general_base GROUP BY 3
  UNION ALL SELECT 'session_count', 'count', CAST(NULL AS STRING), COUNT(*) FROM general_base
),

-- =========================== device-statistics.json =========================
-- deviceAndDriver: vendor/device/driver triple counts over all general pings.
device_rows AS (
  SELECT 'deviceAndDriver' AS dimension, device_and_driver_key AS key, CAST(NULL AS STRING) AS subkey, COUNT(*) AS value
  FROM general_base GROUP BY device_and_driver_key
  UNION ALL
  SELECT 'session_count', 'count', CAST(NULL AS STRING), COUNT(*) FROM general_base
),

-- ============================ mac-statistics.json ============================
mac_pings AS (
  SELECT * FROM one_per_client WHERE os_name = 'Darwin'
),
mac_rows AS (
  SELECT 'versions' AS dimension, os_version AS key, CAST(NULL AS STRING) AS subkey, COUNT(*) AS value
  FROM mac_pings WHERE os_version IS NOT NULL GROUP BY key
  UNION ALL
  SELECT
    'retina' AS dimension,
    -- Legacy keys were float-formatted ('2.0','1.0') with 'null' for missing.
    -- Glean stores scale as an int-ish JSON string ('2'); BigQuery's
    -- FLOAT64->STRING drops the '.0', so format whole numbers explicitly.
    CASE
      WHEN monitor_scale IS NULL THEN 'null'
      WHEN SAFE_CAST(monitor_scale AS FLOAT64) IS NULL THEN monitor_scale
      WHEN SAFE_CAST(monitor_scale AS FLOAT64) = TRUNC(SAFE_CAST(monitor_scale AS FLOAT64))
        THEN CONCAT(CAST(SAFE_CAST(monitor_scale AS INT64) AS STRING), '.0')
      ELSE CAST(SAFE_CAST(monitor_scale AS FLOAT64) AS STRING)
    END AS key,
    CAST(NULL AS STRING) AS subkey,
    COUNT(*) AS value
  FROM mac_pings GROUP BY key
  UNION ALL
  SELECT
    'arch' AS dimension,
    -- Glean reports x86_64 (Intel) and aarch64 (Apple Silicon); legacy main_v5
    -- reported x86-64 / x86. All 64-bit forms fold into the '64' bucket.
    CASE
      WHEN architecture IN ('x86-64', 'x86_64', 'aarch64') THEN '64'
      WHEN architecture = 'x86' THEN '32'
      ELSE 'unknown'
    END AS key,
    CAST(NULL AS STRING) AS subkey,
    COUNT(*) AS value
  FROM mac_pings GROUP BY key
  UNION ALL
  SELECT 'session_count', 'count', CAST(NULL AS STRING), COUNT(*) FROM mac_pings
),

-- =========================== linux-statistics.json ==========================
linux_pings AS (
  SELECT * FROM one_per_client WHERE os_name = 'Linux'
),
linux_rows AS (
  -- driverVendors: legacy counts pings where adapter.driverVendor is set.
  SELECT 'driverVendors' AS dimension, driver_vendor AS key, CAST(NULL AS STRING) AS subkey, COUNT(*) AS value
  FROM linux_pings WHERE driver_vendor IS NOT NULL GROUP BY key
  UNION ALL
  -- compositors: legacy counts pings that have a gfx features block. A reported
  -- compositor implies a features block, so filter to non-null compositor.
  SELECT 'compositors', compositor, CAST(NULL AS STRING), COUNT(*)
  FROM linux_pings WHERE compositor IS NOT NULL GROUP BY compositor
  UNION ALL
  SELECT 'session_count', 'count', CAST(NULL AS STRING), COUNT(*) FROM linux_pings
),

-- =========================== system-statistics.json ========================
-- logical_cores (cpu logical core count), x86 (cpu extension features + total),
-- memory (bucketed total RAM), wow (Windows OS bitness). All over general pings
-- except wow which is Windows-only.
system_rows AS (
  SELECT 'logical_cores' AS dimension,
         COALESCE(CAST(logical_cores AS STRING), 'unknown') AS key,
         CAST(NULL AS STRING) AS subkey, COUNT(*) AS value
  FROM general_base GROUP BY key
  UNION ALL
  -- x86 total: pings with extensions reported, excluding the Firefox 39 ARMv6
  -- false-positive bug (a real x86 box never has hasARMv6).
  SELECT 'x86_total', 'total', CAST(NULL AS STRING), COUNT(*)
  FROM general_base
  WHERE ARRAY_LENGTH(cpu_extensions) > 0 AND 'hasARMv6' NOT IN UNNEST(cpu_extensions)
  UNION ALL
  -- x86 features: count of each extension across those same pings.
  SELECT 'x86_features', ext, CAST(NULL AS STRING), COUNT(*)
  FROM general_base, UNNEST(cpu_extensions) AS ext
  WHERE 'hasARMv6' NOT IN UNNEST(cpu_extensions)
  GROUP BY ext
  UNION ALL
  -- memory buckets (RAM in MB; legacy divides by 1000).
  SELECT 'memory',
    CASE
      WHEN CAST(memory_mb / 1000 AS INT64) < 1 THEN 'less_1gb'
      WHEN CAST(memory_mb / 1000 AS INT64) <= 4 THEN CAST(CAST(memory_mb / 1000 AS INT64) AS STRING)
      WHEN CAST(memory_mb / 1000 AS INT64) <= 8 THEN '4_to_8'
      WHEN CAST(memory_mb / 1000 AS INT64) <= 16 THEN '8_to_16'
      WHEN CAST(memory_mb / 1000 AS INT64) <= 32 THEN '16_to_32'
      ELSE 'more_32'
    END,
    CAST(NULL AS STRING), COUNT(*)
  FROM general_base WHERE memory_mb > 0 GROUP BY 2
  UNION ALL
  -- wow (OS bitness), Windows only. Glean lacks isWow64, so the legacy
  -- '32_on_64' bucket cannot be distinguished; x86 maps to '32'.
  SELECT 'wow',
    CASE WHEN architecture IN ('x86-64', 'x86_64', 'aarch64') THEN '64'
         WHEN architecture = 'x86' THEN '32' ELSE 'unknown' END,
    CAST(NULL AS STRING), COUNT(*)
  FROM general_base WHERE os_name = 'Windows' GROUP BY 2
  UNION ALL
  SELECT 'session_count', 'count', CAST(NULL AS STRING), COUNT(*) FROM general_base
),

-- =========================== monitor-statistics.json ========================
-- Windows-only (legacy used windows_pings). counts = number of monitors per
-- ping; refreshRates / resolutions are taken across all monitors. The monitors
-- JSON array carries screenWidth/screenHeight/refreshRate per element.
monitor_pings AS (
  SELECT * FROM one_per_client WHERE os_name = 'Windows' AND ARRAY_LENGTH(monitors_arr) > 0
),
monitor_rows AS (
  SELECT 'counts' AS dimension, CAST(ARRAY_LENGTH(monitors_arr) AS STRING) AS key,
         CAST(NULL AS STRING) AS subkey, COUNT(*) AS value
  FROM monitor_pings GROUP BY key
  UNION ALL
  -- refreshRates: refresh rate (>1) per monitor, else 'Unknown'. Float-keyed.
  SELECT 'refreshRates',
    CASE
      WHEN SAFE_CAST(JSON_VALUE(mon, '$.refreshRate') AS FLOAT64) > 1
        THEN CAST(SAFE_CAST(JSON_VALUE(mon, '$.refreshRate') AS FLOAT64) AS STRING)
      ELSE 'Unknown'
    END,
    CAST(NULL AS STRING), COUNT(*)
  FROM monitor_pings, UNNEST(monitors_arr) AS mon GROUP BY 2
  UNION ALL
  -- resolutions: '<w>x<h>' per monitor, 'Unknown' when width/height missing.
  SELECT 'resolutions',
    CASE
      WHEN SAFE_CAST(JSON_VALUE(mon, '$.screenWidth') AS INT64) > 0
       AND SAFE_CAST(JSON_VALUE(mon, '$.screenHeight') AS INT64) > 0
        THEN CONCAT(JSON_VALUE(mon, '$.screenWidth'), 'x', JSON_VALUE(mon, '$.screenHeight'))
      ELSE 'Unknown'
    END,
    CAST(NULL AS STRING), COUNT(*)
  FROM monitor_pings, UNNEST(monitors_arr) AS mon GROUP BY 2
  UNION ALL
  SELECT 'session_count', 'count', CAST(NULL AS STRING), COUNT(*) FROM windows_pings
),

-- ============================ tdr-statistics.json ===========================
-- TDR (device reset) data is Windows-only. Each Windows ping may carry an
-- 11-bucket histogram (gfx_device_reset_reason); legacy summed it element-wise
-- into a `results` array and broke reasons 1-7 down by vendor.
--
-- NOTE: like the legacy job, sessions.count for this file is the count of ALL
-- Windows pings (`windows_pings`, defined above), while `tdrPings` is the count
-- of Windows pings that actually carry TDR data.
tdr_pings AS (
  SELECT * FROM windows_pings WHERE tdr_values IS NOT NULL
),
tdr_rows AS (
  -- results: element-wise sum per histogram bucket (reshape orders by bucket).
  SELECT 'results' AS dimension, v.key AS key, CAST(NULL AS STRING) AS subkey, SUM(v.value) AS value
  FROM tdr_pings, UNNEST(tdr_values) AS v
  GROUP BY key
  UNION ALL
  -- reasonToVendor: reason (1-7) -> vendor -> summed reset count.
  SELECT 'reasonToVendor', CAST(SAFE_CAST(v.key AS INT64) AS STRING),
         COALESCE(vendor_id, 'unknown'), SUM(v.value)
  FROM tdr_pings, UNNEST(tdr_values) AS v
  WHERE SAFE_CAST(v.key AS INT64) BETWEEN 1 AND 7 AND v.value > 0
  GROUP BY 2, 3
  UNION ALL
  -- tdrPings = pings carrying TDR data; session_count = all Windows pings.
  SELECT 'tdr_count', 'count', CAST(NULL AS STRING), COUNT(*) FROM tdr_pings
  UNION ALL
  SELECT 'session_count', 'count', CAST(NULL AS STRING), COUNT(*) FROM windows_pings
),

-- ======================= startup-test-statistics.json =======================
-- gfx_graphics_driver_startup_test histogram, summed element-wise, plus a
-- Windows OS-version breakdown. sessions.count is ALL general pings (the legacy
-- job passed `general_pings`); startupTestPings is the histogram-bearing subset.
startup_pings AS (
  SELECT * FROM one_per_client WHERE startup_values IS NOT NULL
),
startup_rows AS (
  SELECT 'results' AS dimension, v.key AS key, CAST(NULL AS STRING) AS subkey, SUM(v.value) AS value
  FROM startup_pings, UNNEST(startup_values) AS v
  GROUP BY key
  UNION ALL
  SELECT 'windows', windows_os, CAST(NULL AS STRING), COUNT(*)
  FROM startup_pings WHERE os_name = 'Windows' GROUP BY windows_os
  UNION ALL
  SELECT 'startup_count', 'count', CAST(NULL AS STRING), COUNT(*) FROM startup_pings
  UNION ALL
  SELECT 'session_count', 'count', CAST(NULL AS STRING), COUNT(*) FROM one_per_client
),

-- ========================= sanity-test-statistics.json ======================
-- Windows-only. Each ping's gfx_sanity_test histogram is classified to a single
-- outcome by priority (passed > crashed > render > video > timedout), matching
-- the legacy get_sanity_test_result(). Breakdowns (byVendor/byOS/byDevice/
-- byDriver) sum the chosen outcome's bucket value for failure outcomes 1-4.
sanity_pings AS (
  SELECT * FROM windows_pings WHERE sanity_values IS NOT NULL
),
sanity_buckets AS (
  SELECT *,
    (SELECT COALESCE(SUM(IF(v.key = '0', v.value, 0)), 0) FROM UNNEST(sanity_values) v) AS b0,
    (SELECT COALESCE(SUM(IF(v.key = '1', v.value, 0)), 0) FROM UNNEST(sanity_values) v) AS b1,
    (SELECT COALESCE(SUM(IF(v.key = '2', v.value, 0)), 0) FROM UNNEST(sanity_values) v) AS b2,
    (SELECT COALESCE(SUM(IF(v.key = '3', v.value, 0)), 0) FROM UNNEST(sanity_values) v) AS b3,
    (SELECT COALESCE(SUM(IF(v.key = '4', v.value, 0)), 0) FROM UNNEST(sanity_values) v) AS b4
  FROM sanity_pings
),
sanity_classified AS (
  SELECT *,
    -- Priority order from the legacy get_sanity_test_result().
    CASE
      WHEN b0 > 0 THEN 0  -- PASSED
      WHEN b3 > 0 THEN 3  -- CRASHED
      WHEN b1 > 0 THEN 1  -- FAILED_RENDER
      WHEN b2 > 0 THEN 2  -- FAILED_VIDEO
      WHEN b4 > 0 THEN 4  -- TIMEDOUT
      ELSE NULL
    END AS outcome,
    -- The histogram bucket value for the chosen outcome (used by breakdowns).
    CASE
      WHEN b0 > 0 THEN b0 WHEN b3 > 0 THEN b3 WHEN b1 > 0 THEN b1
      WHEN b2 > 0 THEN b2 WHEN b4 > 0 THEN b4 ELSE 0
    END AS outcome_value
  FROM sanity_buckets
),
sanity_data AS (
  SELECT * FROM sanity_classified WHERE outcome IS NOT NULL
),
sanity_rows AS (
  -- results: ping count per outcome.
  SELECT 'results' AS dimension, CAST(outcome AS STRING) AS key, CAST(NULL AS STRING) AS subkey, COUNT(*) AS value
  FROM sanity_data GROUP BY outcome
  UNION ALL
  -- byVendor/byOS/byDevice/byDriver: per failure outcome (1-4), sum the chosen
  -- outcome's bucket value grouped by the breakdown dimension. key = outcome,
  -- subkey = breakdown value.
  SELECT 'byVendor', CAST(outcome AS STRING), COALESCE(vendor_id, 'Unknown'), SUM(outcome_value)
  FROM sanity_data WHERE outcome BETWEEN 1 AND 4 GROUP BY outcome, 3
  UNION ALL
  SELECT 'byOS', CAST(outcome AS STRING), CONCAT('Windows-', os_version_key), SUM(outcome_value)
  FROM sanity_data WHERE outcome BETWEEN 1 AND 4 GROUP BY outcome, 3
  UNION ALL
  SELECT 'byDevice', CAST(outcome AS STRING), device_key, SUM(outcome_value)
  FROM sanity_data WHERE outcome BETWEEN 1 AND 4 GROUP BY outcome, 3
  UNION ALL
  SELECT 'byDriver', CAST(outcome AS STRING), driver_key, SUM(outcome_value)
  FROM sanity_data WHERE outcome BETWEEN 1 AND 4 GROUP BY outcome, 3
  UNION ALL
  -- windows: OSVersion share over the classified data.
  SELECT 'windows', os_version_key, CAST(NULL AS STRING), COUNT(*)
  FROM sanity_data GROUP BY os_version_key
  UNION ALL
  SELECT 'sanity_count', 'count', CAST(NULL AS STRING), COUNT(*) FROM sanity_data
  UNION ALL
  SELECT 'total_count', 'count', CAST(NULL AS STRING), COUNT(*) FROM sanity_pings
  UNION ALL
  SELECT 'session_count', 'count', CAST(NULL AS STRING), COUNT(*) FROM windows_pings
),

-- ========================== webgl-statistics.json ===========================
-- WebGL session success/failure, per the legacy web_gl_statistics_for_key():
-- a session is a failure if its 'false' counter > 0; a success if 'false' == 0
-- and 'true' > 0 (successes are not double-counted with failures). Computed for
-- webgl1 (canvas_webgl_success) and webgl2 (canvas_webgl2_success). The
-- `general` block sums the keyed failure-id counters across all pings.
--
-- `dimension` encodes the path, e.g. 'webgl1_failures_os', 'webgl2_successes_compositors',
-- 'general_status'. `key` holds the breakdown value (or a sentinel for counts).
webgl1_pings AS (
  SELECT *,
    CASE WHEN webgl1_fail > 0 THEN 'failure'
         WHEN webgl1_fail = 0 AND webgl1_ok > 0 THEN 'success' END AS webgl_class,
    -- compositor reported only on Windows (see legacy bug 1247148 note).
    CASE WHEN os_name = 'Windows' THEN COALESCE(compositor, 'none') ELSE 'unknown' END AS cc
  FROM one_per_client WHERE webgl1_len > 0
),
webgl2_pings AS (
  SELECT *,
    CASE WHEN webgl2_fail > 0 THEN 'failure'
         WHEN webgl2_fail = 0 AND webgl2_ok > 0 THEN 'success' END AS webgl_class,
    CASE WHEN os_name = 'Windows' THEN COALESCE(compositor, 'none') ELSE 'unknown' END AS cc
  FROM one_per_client WHERE webgl2_len > 0
),
webgl_rows AS (
  -- ---- webgl1 successes ----
  SELECT 'webgl1_successes_count' AS dimension, 'count' AS key, CAST(NULL AS STRING) AS subkey, COUNT(*) AS value
  FROM webgl1_pings WHERE webgl_class = 'success'
  UNION ALL SELECT 'webgl1_successes_os', os_key, CAST(NULL AS STRING), COUNT(*) FROM webgl1_pings WHERE webgl_class = 'success' GROUP BY os_key
  UNION ALL SELECT 'webgl1_successes_compositors', cc, CAST(NULL AS STRING), COUNT(*) FROM webgl1_pings WHERE webgl_class = 'success' GROUP BY cc
  -- ---- webgl1 failures ----
  UNION ALL SELECT 'webgl1_failures_count', 'count', CAST(NULL AS STRING), COUNT(*) FROM webgl1_pings WHERE webgl_class = 'failure'
  UNION ALL SELECT 'webgl1_failures_os', os_key, CAST(NULL AS STRING), COUNT(*) FROM webgl1_pings WHERE webgl_class = 'failure' GROUP BY os_key
  UNION ALL SELECT 'webgl1_failures_vendors', COALESCE(vendor_id, 'Unknown'), CAST(NULL AS STRING), COUNT(*) FROM webgl1_pings WHERE webgl_class = 'failure' GROUP BY 2
  UNION ALL SELECT 'webgl1_failures_devices', device_key, CAST(NULL AS STRING), COUNT(*) FROM webgl1_pings WHERE webgl_class = 'failure' GROUP BY device_key
  UNION ALL SELECT 'webgl1_failures_drivers', driver_key, CAST(NULL AS STRING), COUNT(*) FROM webgl1_pings WHERE webgl_class = 'failure' GROUP BY driver_key
  -- ---- webgl2 successes ----
  UNION ALL SELECT 'webgl2_successes_count', 'count', CAST(NULL AS STRING), COUNT(*) FROM webgl2_pings WHERE webgl_class = 'success'
  UNION ALL SELECT 'webgl2_successes_os', os_key, CAST(NULL AS STRING), COUNT(*) FROM webgl2_pings WHERE webgl_class = 'success' GROUP BY os_key
  UNION ALL SELECT 'webgl2_successes_compositors', cc, CAST(NULL AS STRING), COUNT(*) FROM webgl2_pings WHERE webgl_class = 'success' GROUP BY cc
  -- ---- webgl2 failures ----
  UNION ALL SELECT 'webgl2_failures_count', 'count', CAST(NULL AS STRING), COUNT(*) FROM webgl2_pings WHERE webgl_class = 'failure'
  UNION ALL SELECT 'webgl2_failures_os', os_key, CAST(NULL AS STRING), COUNT(*) FROM webgl2_pings WHERE webgl_class = 'failure' GROUP BY os_key
  UNION ALL SELECT 'webgl2_failures_vendors', COALESCE(vendor_id, 'Unknown'), CAST(NULL AS STRING), COUNT(*) FROM webgl2_pings WHERE webgl_class = 'failure' GROUP BY 2
  UNION ALL SELECT 'webgl2_failures_devices', device_key, CAST(NULL AS STRING), COUNT(*) FROM webgl2_pings WHERE webgl_class = 'failure' GROUP BY device_key
  UNION ALL SELECT 'webgl2_failures_drivers', driver_key, CAST(NULL AS STRING), COUNT(*) FROM webgl2_pings WHERE webgl_class = 'failure' GROUP BY driver_key
  -- ---- general: summed keyed failure-id counters across all pings ----
  UNION ALL SELECT 'general_status', f.key, CAST(NULL AS STRING), SUM(f.value)
    FROM one_per_client, UNNEST(webgl_failure_id) AS f GROUP BY f.key
  UNION ALL SELECT 'general_acceleration_status', f.key, CAST(NULL AS STRING), SUM(f.value)
    FROM one_per_client, UNNEST(webgl_accl_failure_id) AS f GROUP BY f.key
  UNION ALL SELECT 'session_count', 'count', CAST(NULL AS STRING), COUNT(*) FROM one_per_client
),

-- ============================ windows-features.json ==========================
-- Windows-only. Derives the legacy feature-status strings from the Glean
-- gfx_features_* objects, then emits both an 'all' rollup and a per-version
-- breakdown for the important Windows versions. Field derivations mirror the
-- legacy helpers (get_d3d11_status, get_d2d_status, get_warp_status,
-- gpu_process_status, get_texture_sharing_status, get_compositor).
--
-- Glean gaps (documented): advancedLayers has no Glean metric (WebRender-era
-- deprecation), so 'advanced_layers' is reported as all-'none'; plugin_models
-- is empty (deprecated, already empty in prod).
wf_pings AS (
  SELECT *,
    os_version_key AS wf_version,
    -- compositor: legacy get_compositor; advancedLayers unavailable so the
    -- 'd3d11' -> 'advanced_layers' promotion never fires.
    COALESCE(compositor, 'none') AS wf_compositor,
    content_backend AS wf_content_backend,
    -- d3d11 status: available -> warp|version(float); else the status string.
    CASE
      WHEN d3d11_obj IS NULL THEN 'unknown'
      WHEN JSON_VALUE(d3d11_obj, '$.status') != 'available'
        THEN COALESCE(JSON_VALUE(d3d11_obj, '$.status'), 'unknown')
      WHEN COALESCE(SAFE_CAST(JSON_VALUE(d3d11_obj, '$.warp') AS BOOL), FALSE) THEN 'warp'
      ELSE COALESCE(
        CAST(SAFE_CAST(JSON_VALUE(d3d11_obj, '$.version') AS FLOAT64) AS STRING), 'unknown')
    END AS wf_d3d11,
    -- d2d status: available -> version; else status.
    CASE
      WHEN d2d_obj IS NULL THEN 'unknown'
      WHEN JSON_VALUE(d2d_obj, '$.status') != 'available'
        THEN COALESCE(JSON_VALUE(d2d_obj, '$.status'), 'unknown')
      ELSE COALESCE(JSON_VALUE(d2d_obj, '$.version'), 'unknown')
    END AS wf_d2d,
    -- textureSharing (only counted for working d3d11).
    JSON_VALUE(d3d11_obj, '$.status') = 'available' AS wf_d3d11_working,
    COALESCE(JSON_VALUE(d3d11_obj, '$.textureSharing'), 'unknown') AS wf_texture_sharing,
    -- gpuProcess status; 'none' when absent.
    CASE
      WHEN gpu_process_obj IS NULL OR JSON_VALUE(gpu_process_obj, '$.status') IS NULL THEN 'none'
      ELSE JSON_VALUE(gpu_process_obj, '$.status')
    END AS wf_gpu_process,
    -- d3d11 == 'warp' detection for the warp-status breakdown.
    CASE
      WHEN d3d11_obj IS NOT NULL AND JSON_VALUE(d3d11_obj, '$.status') = 'available'
       AND COALESCE(SAFE_CAST(JSON_VALUE(d3d11_obj, '$.warp') AS BOOL), FALSE)
        -- get_warp_status: blacklisted flag present? (Glean lacks it -> unknown)
        THEN 'unknown'
    END AS wf_warp,
    -- blacklist/blocked detection (status strings).
    JSON_VALUE(d3d11_obj, '$.status') AS wf_d3d11_status_raw
  FROM windows_pings
),
wf_important AS (
  SELECT * FROM wf_pings
  WHERE wf_version IN ('6.1.0', '6.2.0', '6.3.0', '10.0.0')
),
windows_features_rows AS (
  -- ----- all: top-level feature breakdowns -----
  SELECT 'all_compositors' AS dimension, wf_compositor AS key, CAST(NULL AS STRING) AS subkey, COUNT(*) AS value
  FROM wf_pings GROUP BY wf_compositor
  UNION ALL SELECT 'all_content_backends', wf_content_backend, CAST(NULL AS STRING), COUNT(*)
    FROM wf_pings WHERE wf_content_backend IS NOT NULL GROUP BY wf_content_backend
  UNION ALL SELECT 'all_d3d11', wf_d3d11, CAST(NULL AS STRING), COUNT(*) FROM wf_pings GROUP BY wf_d3d11
  UNION ALL SELECT 'all_d2d', wf_d2d, CAST(NULL AS STRING), COUNT(*) FROM wf_pings GROUP BY wf_d2d
  UNION ALL SELECT 'all_textureSharing', wf_texture_sharing, CAST(NULL AS STRING), COUNT(*)
    FROM wf_pings WHERE wf_d3d11_working GROUP BY wf_texture_sharing
  UNION ALL SELECT 'all_warp', wf_warp, CAST(NULL AS STRING), COUNT(*)
    FROM wf_pings WHERE wf_d3d11 = 'warp' GROUP BY wf_warp
  UNION ALL SELECT 'all_gpu_process', wf_gpu_process, CAST(NULL AS STRING), COUNT(*) FROM wf_pings GROUP BY wf_gpu_process
  UNION ALL SELECT 'all_advanced_layers', 'none', CAST(NULL AS STRING), COUNT(*) FROM wf_pings
  UNION ALL SELECT 'all_media_decoders', v.key, CAST(NULL AS STRING), SUM(v.value)
    FROM wf_pings, UNNEST(media_values) AS v GROUP BY v.key
  -- ----- d3d11 blacklist / blocked breakdowns -----
  -- Legacy matched the bare 'blacklisted'/'blocked' statuses; modern Glean uses
  -- 'blocklisted:FEATURE_FAILURE_...' (with reason suffix) and 'blocked', so
  -- match by prefix to preserve intent.
  UNION ALL SELECT 'blacklist_devices', device_key, CAST(NULL AS STRING), COUNT(*)
    FROM wf_pings WHERE wf_d3d11_status_raw LIKE 'bl%cklisted%' GROUP BY device_key
  UNION ALL SELECT 'blacklist_drivers', driver_key, CAST(NULL AS STRING), COUNT(*)
    FROM wf_pings WHERE wf_d3d11_status_raw LIKE 'bl%cklisted%' GROUP BY driver_key
  UNION ALL SELECT 'blacklist_os', os_version_key, CAST(NULL AS STRING), COUNT(*)
    FROM wf_pings WHERE wf_d3d11_status_raw LIKE 'bl%cklisted%' GROUP BY os_version_key
  UNION ALL SELECT 'blocked_vendors', COALESCE(vendor_id, 'unknown'), CAST(NULL AS STRING), COUNT(*)
    FROM wf_pings WHERE wf_d3d11_status_raw LIKE 'blocked%' GROUP BY 2
  -- ----- per-version breakdowns (key=version, subkey=value) -----
  UNION ALL SELECT 'byver_count', wf_version, CAST(NULL AS STRING), COUNT(*) FROM wf_important GROUP BY wf_version
  UNION ALL SELECT 'byver_compositors', wf_version, wf_compositor, COUNT(*) FROM wf_important GROUP BY wf_version, wf_compositor
  UNION ALL SELECT 'byver_content_backends', wf_version, wf_content_backend, COUNT(*)
    FROM wf_important WHERE wf_content_backend IS NOT NULL GROUP BY wf_version, wf_content_backend
  UNION ALL SELECT 'byver_gpu_process', wf_version, wf_gpu_process, COUNT(*) FROM wf_important GROUP BY wf_version, wf_gpu_process
  UNION ALL SELECT 'byver_advanced_layers', wf_version, 'none', COUNT(*) FROM wf_important GROUP BY wf_version
  UNION ALL SELECT 'byver_d3d11', wf_version, wf_d3d11, COUNT(*) FROM wf_important GROUP BY wf_version, wf_d3d11
  UNION ALL SELECT 'byver_d2d', wf_version, wf_d2d, COUNT(*) FROM wf_important GROUP BY wf_version, wf_d2d
  UNION ALL SELECT 'byver_warp', wf_version, wf_warp, COUNT(*) FROM wf_important WHERE wf_d3d11 = 'warp' GROUP BY wf_version, wf_warp
  UNION ALL SELECT 'byver_media_decoders', wf_version, v.key, SUM(v.value)
    FROM wf_important, UNNEST(media_values) AS v GROUP BY wf_version, v.key
  UNION ALL SELECT 'session_count', 'count', CAST(NULL AS STRING), COUNT(*) FROM windows_pings
)

SELECT 'mac' AS output, dimension, key, subkey, value FROM mac_rows
UNION ALL SELECT 'linux', dimension, key, subkey, value FROM linux_rows
UNION ALL SELECT 'general', dimension, key, subkey, value FROM general_rows
UNION ALL SELECT 'device', dimension, key, subkey, value FROM device_rows
UNION ALL SELECT 'system', dimension, key, subkey, value FROM system_rows
UNION ALL SELECT 'monitor', dimension, key, subkey, value FROM monitor_rows
UNION ALL SELECT 'tdr', dimension, key, subkey, value FROM tdr_rows
UNION ALL SELECT 'startup', dimension, key, subkey, value FROM startup_rows
UNION ALL SELECT 'sanity', dimension, key, subkey, value FROM sanity_rows
UNION ALL SELECT 'webgl', dimension, key, subkey, value FROM webgl_rows
UNION ALL SELECT 'windows-features', dimension, key, subkey, value FROM windows_features_rows
-- The Firefox-version share is shared metadata; tag it with output 'share' so
-- the reshape can attach it to every file's `sessions` block.
UNION ALL SELECT 'share', 'share', key, CAST(NULL AS STRING), value FROM fx_version_share
