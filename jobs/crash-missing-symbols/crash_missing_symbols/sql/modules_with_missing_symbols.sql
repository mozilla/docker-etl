-- Modules in Firefox crash reports that were missing symbols.
--
-- Replaces the explode/dropDuplicates/reduceByKey chain in the old Spark job.
-- Only the module fields the report needs are projected, so json_dump (~48 GB
-- over a 5 day window) is never pulled across the wire.
--
-- @dedupe_key selects how a repeated module within one crash report is counted:
--
--   module-struct  what Spark did. dropDuplicates(["uuid", "module"]) keyed on
--                  the whole struct, so a file mapped at several addresses
--                  (differing base_addr/end_addr) counts once per mapping. This
--                  is the default because the migration targets parity.
--   crash-report   count each crash report once per module, which is what the
--                  email's "# of crash reports" column claims to mean.
--
-- The two differ more than you'd guess, and it's memory mapped files rather than
-- loaded libraries that drive it. Over the 3 days ending 2026-08-07:
-- 261 rows totalling 70,285 under module-struct against 225 and 46,665 under
-- crash-report, with nvidiactl counted 11,393 times across 207 crash reports.
--
-- Spark deduped before filtering on missing_symbols. Since that field is part of
-- the struct, filtering first is equivalent (verified: 131,175 rows either way),
-- so the filter stays in the WHERE clause where BigQuery can push it down.
WITH modules AS (
  SELECT
    uuid,
    m.element.filename AS name,
    m.element.version AS version,
    m.element.debug_id AS debug_id,
    m.element.debug_file AS debug_file,
    -- Only the address fields distinguish rows that are otherwise identical, so
    -- this is enough to reproduce the whole-struct dedupe. Verified against
    -- TO_JSON_STRING(m.element): loaded_symbols, symbol_url and
    -- symbol_disk_cache_hit never vary within a (uuid, name, version, debug_id,
    -- debug_file) group.
    m.element.base_addr AS base_addr,
    m.element.end_addr AS end_addr,
    m.element.code_id AS code_id
  FROM `moz-fx-data-shared-prod.telemetry_derived.socorro_crash_v2`,
    UNNEST(json_dump.modules.list) AS m
  WHERE
    -- Half open: end_date is excluded, so a run covers @window_days complete days
    -- and never a partial today. The Spark job had no upper bound at all; see the
    -- README.
    crash_date >= DATE_SUB(@end_date, INTERVAL @window_days DAY)
    AND crash_date < @end_date
    AND product = 'Firefox'
    AND m.element.missing_symbols
    -- Unloaded modules whose file was deleted; nothing to look up.
    AND NOT CONTAINS_SUBSTR(m.element.filename, '(deleted)')
),

deduped AS (
  SELECT DISTINCT
    uuid,
    name,
    version,
    debug_id,
    debug_file,
    IF(@dedupe_key = 'module-struct', base_addr, NULL) AS base_addr,
    IF(@dedupe_key = 'module-struct', end_addr, NULL) AS end_addr,
    IF(@dedupe_key = 'module-struct', code_id, NULL) AS code_id
  FROM modules
)

SELECT
  name,
  version,
  debug_id,
  debug_file,
  COUNT(*) AS crash_count
FROM deduped
GROUP BY name, version, debug_id, debug_file
HAVING crash_count > @min_crash_count
ORDER BY crash_count DESC, name, version, debug_id
