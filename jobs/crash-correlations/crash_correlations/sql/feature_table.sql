-- Draft. One row per crash report with the feature columns the miner counts over.
--
-- This replaces augment() and drop_unneeded() from crash_deviations.py. The point is that
-- json_dump, addons, app_notes and graphics_critical_error are read inside BigQuery and only
-- the derived features come back, so the ~48 GB of json_dump over a 5 day window never
-- crosses the wire. Spark pulled it all to compute the same booleans.
--
-- Parts of this are generated rather than static: there is one column per frequent module,
-- app note, graphics critical error and addon, and those lists are data dependent. The
-- caller substitutes them, the same way augment() built columns from all_modules and
-- all_app_notes. Placeholders below are marked {like_this}.
--
-- Types matter here and are easy to get wrong. The values reach Python as-is and
-- ignore_rule() in the miner discriminates on `is False` / `is None` and on the strings '1'
-- and 'Active', so a BOOL column has to stay BOOL and a NULL has to stay NULL. Don't cast
-- anything to STRING for convenience. The Spark job's equivalent bug was a UDF declaring
-- StringType() while returning a bool, which silently disabled one of its dedupe rules.

WITH window_crashes AS (
  -- A denylist, not an allowlist, matching drop_unneeded() at crash_deviations.py:493.
  -- Every column that isn't dropped becomes a feature.
  --
  -- This is deliberate even though naming ~42 wanted columns would be tidier. An
  -- allowlist fails silent: a column added to socorro_crash_v2 is invisible until
  -- someone notices it missing from the tab, and an earlier version of this file was
  -- missing abort_message, addons_checked and graphics_startup_test that way, all three
  -- of which appear in production output. A denylist fails safe, picking up new columns
  -- automatically, which is also what the job being replaced does.
  --
  -- The list below is upstream's, verbatim and in its order. Some entries are useless
  -- as features (flash_version and the android_* columns are 100% NULL on desktop,
  -- minidump_sha256_hash is near unique per row so it can never correlate). They are
  -- kept anyway: production has behaved this way for years, a constant or unique column
  -- produces no correlations and costs only a little memory, and dropping them here
  -- would be a silent divergence from the output people are used to.
  --
  -- Both `date` and `crash_date` are dropped. `date` is in upstream's list. `crash_date`
  -- is the partition column this query filters on and upstream never saw it, but it's a
  -- per-crash timestamp, so leaving it in would add a high cardinality feature that
  -- production doesn't have. The six raw columns the
  -- derived features are built from (app_notes, addons, json_dump, cpu_info,
  -- total_virtual_memory, graphics_critical_error) are NOT dropped here, because
  -- `flattened` and the final SELECT still need them; they're excluded from the result at
  -- the end instead, which is also the order upstream does it in (augment then
  -- drop_unneeded). memory_ghost_windows and memory_top_none_detached are in upstream's
  -- drop list but don't exist in this table, so naming them here would be an error.
  SELECT * EXCEPT (
    total_physical_memory,
    available_virtual_memory,
    available_physical_memory,
    oom_allocation_size,
    date,
    crash_date,
    user_comments,
    uuid,
    additional_minidumps,
    classifications,
    crash_id,
    java_stack_trace,
    last_crash,
    install_age,
    memory_measures,
    memory_report,
    uptime,
    winsock_lsp,
    version,
    topmost_filenames,
    proto_signature,
    processor_notes,
    product,
    productid
  )
  FROM `moz-fx-data-shared-prod.telemetry_derived.socorro_crash_v2`
  WHERE
    -- Half open, so a run covers @window_days complete days and never a partial today.
    -- The Spark job had no upper bound and read `crash_date >= utcnow() - days`, which is
    -- part of why its runs aren't reproducible for a past date.
    crash_date >= DATE_SUB(@end_date, INTERVAL @window_days DAY)
    AND crash_date < @end_date
    AND product = @product
    AND version IN UNNEST(@versions)
),

-- Flatten the repeated fields once per row, so the generated feature columns can test
-- membership against an array instead of each running its own correlated subquery.
--
-- This matters enormously. There are ~2,000 module features, and writing each as
-- EXISTS(SELECT 1 FROM UNNEST(json_dump.modules.list) ...) gives BigQuery 2,000
-- correlated subqueries per row, which did not finish in 10 minutes on a 5,831 row
-- channel. Collecting the filenames once and testing `'x' IN UNNEST(module_files)`
-- is a single flatten per row.
flattened AS (
  SELECT
    *,
    ARRAY(
      SELECT DISTINCT m.element.filename
      FROM UNNEST(json_dump.modules.list) AS m
      WHERE m.element.filename IS NOT NULL
    ) AS module_files,
    ARRAY(
      SELECT DISTINCT SPLIT(a.element, ':')[OFFSET(0)]
      FROM UNNEST(addons.list) AS a
      WHERE a.element IS NOT NULL AND STRPOS(a.element, ':') > 0
    ) AS addon_names,
    ARRAY(
      SELECT a.element
      FROM UNNEST(addons.list) AS a
      WHERE a.element IS NOT NULL AND STRPOS(a.element, ':') > 0
    ) AS addon_entries
  FROM window_crashes
)

SELECT
  -- Everything that survived the denylist, minus the raw columns the derived features
  -- below are built from and the arrays `flattened` added. dom_ipc_enabled is dropped
  -- here because it's renamed to e10s_enabled below, matching withColumnRenamed().
  --
  -- `* EXCEPT` rather than a column list so a new column in socorro_crash_v2 becomes a
  -- feature automatically, the way it does upstream. Note plugin_version stays a feature
  -- in its own right as well as feeding `plugin`, which is what upstream does: augment()
  -- adds the plugin boolean with withColumn and never drops plugin_version, and
  -- production output has plugin_version = null rows.
  * EXCEPT (
    app_notes,
    graphics_critical_error,
    addons,
    json_dump,
    total_virtual_memory,
    cpu_info,
    dom_ipc_enabled,
    module_files,
    addon_names,
    addon_entries
  ),

  -- plugin: presence of a plugin_version, not the version itself.
  plugin_version IS NOT NULL AS plugin,

  -- CPU Info and Is Multicore. augment() used substring_index(cpu_info, ' | ', 1) and
  -- substring_index(cpu_info, ' | ', -1) != '1', i.e. the text before the first ' | ' and
  -- whether the text after the last one isn't '1'.
  --
  -- Measured: over the 5 days ending 2026-08-14, none of the 196,475 rows contain '|' at all
  -- (443 distinct values, 3,590 literally 'unknown', 4,285 NULL). So substring_index returns
  -- the whole string both times, `CPU Info` is just cpu_info, and `Is Multicore` is `true`
  -- for every row including 'unknown'. Reproduced here rather than "fixed", since a constant
  -- column produces no correlations and quietly dropping it would be a behaviour change.
  -- Worth confirming the field didn't change format upstream at some point.
  SPLIT(cpu_info, ' | ')[OFFSET(0)] AS `CPU Info`,
  SPLIT(cpu_info, ' | ')[OFFSET(ARRAY_LENGTH(SPLIT(cpu_info, ' | ')) - 1)] != '1'
    AS `Is Multicore`,

  -- os_arch: the get_arch UDF. SAFE_CAST replaces the bare `except: return 'unknown'`, so a
  -- non-numeric total_virtual_memory lands in the same bucket without hiding other errors.
  CASE
    WHEN total_virtual_memory IS NOT NULL AND total_virtual_memory != ''
      THEN CASE
        WHEN SAFE_CAST(total_virtual_memory AS INT64) IS NULL THEN 'unknown'
        WHEN SAFE_CAST(total_virtual_memory AS INT64) < 2684354560 THEN 'x86'
        ELSE 'amd64'
      END
    WHEN platform = 'Mac OS X' THEN 'amd64'
    WHEN STRPOS(platform_version, 'i686') != 0 THEN 'x86'
    WHEN STRPOS(platform_version, 'x86_64') != 0 THEN 'amd64'
    -- get_arch falls off the end and returns None here. Kept, so the NULL itemset that
    -- ignore_rule() looks for still appears.
    ELSE NULL
  END AS os_arch,

  -- adapter_driver_version_clean: get_driver_version, which is vendor specific string
  -- slicing rather than a regex. Intel (0x8086) keeps only the build after the last dot.
  -- Nvidia (0x10de) rebuilds the marketing version out of the last six characters, e.g.
  -- '32.0.15.8107' -> '581.07'. Everything else, AMD included, passes through unchanged.
  --
  -- Checked against the Python original on real driver strings: identical for every 4 part
  -- version, which is all of them in practice. The two implementations only diverge on inputs
  -- shorter than 6 characters, because Python clamps negative slice bounds and BigQuery's
  -- SUBSTR clamps differently, so upstream produces things like '9..8' from '9.8'. The
  -- LENGTH >= 6 guard below keeps those out; they're malformed values either way and upstream's
  -- output for them is meaningless.
  CASE
    WHEN adapter_driver_version IS NULL THEN NULL
    WHEN adapter_vendor_id IN ('0x8086', '8086')
      -- rfind('.') + 1 onwards, i.e. the last dot separated segment.
      THEN ARRAY_REVERSE(SPLIT(adapter_driver_version, '.'))[OFFSET(0)]
    WHEN adapter_vendor_id IN ('0x10de', '10de')
      AND LENGTH(adapter_driver_version) >= 6
      THEN CONCAT(
        SUBSTR(adapter_driver_version, -6, 1),
        SUBSTR(adapter_driver_version, -4, 2),
        '.',
        SUBSTR(adapter_driver_version, -2, 2)
      )
    ELSE adapter_driver_version
  END AS adapter_driver_version_clean,

  -- "Has dual GPUs" is a fixed app note that augment() special-cased.
  STRPOS(app_notes, 'Has dual GPUs') != 0 AS `has dual GPUs`,

  -- dom_ipc_enabled is renamed to e10s_enabled by augment(), and the priors graph refers to
  -- it by the new name.
  dom_ipc_enabled AS e10s_enabled,

  -- No `ghost_windows > 0` or `top(none)/detached > 0`. augment() builds those from
  -- memory_ghost_windows and memory_top_none_detached, but neither column exists in
  -- socorro_crash_v2, so its `if ... in df.columns` guards are always false and the features
  -- have never been produced. Left out rather than invented.

  -- Generated: one BOOL per frequent app note, graphics critical error and module, and one
  -- BOOL plus one STRING per frequent addon. The caller substitutes these from
  -- frequent_values.sql (modules, addons) and the searchfox scrapes (app notes, gfx errors).
  --
  -- These MUST be given synthetic positional names, not the feature text. BigQuery column names
  -- are restricted to letters, digits and underscores, and backtick quoting does not lift that.
  -- Tested: `D3D11 Layers+` and `CPU Info` happen to be accepted, but `GFX_ERROR "("`,
  -- `webcompat@mozilla.org` and `top(none)/detached > 0` are all rejected, and real feature
  -- values include quotes, parentheses, slashes, at signs and non-ASCII text.
  --
  -- So use APPNOTE0, GFX0, MOD0, ADDON0, ADDON0_VERSION and so on, keep the
  -- {name: feature text} mapping on the Python side, and translate back when the itemsets are
  -- turned into the output JSON's "item" keys. Upstream already does this for modules, which is
  -- what module_ids and the 'Module "..."' relabelling in clean_candidate() are for; this extends
  -- the same treatment to the other three kinds. It also replaces upstream's __DOT__ escaping,
  -- which existed only to get dots through Spark column names.
  --
  -- app notes:   STRPOS(app_notes, '<note>') != 0 AS APPNOTE<n>
  -- gfx errors:  STRPOS(graphics_critical_error, '<error>') != 0 AS GFX<n>
  -- modules:     '<filename>' IN UNNEST(module_files) AS MOD<n>
  -- addons:      '<guid>' IN UNNEST(addon_names) AS ADDON<n>
  --
  -- Note addons.list is a repeated STRUCT<element STRING>, so it's a.element rather than a. That
  -- distinction is exactly what broke the Spark job's addon counting: it got a Row where it
  -- expected a string and dropped every addon. Since that pass always produced zero, there are
  -- no addon columns in the Spark job's feature table at all, and adding them here is new
  -- behaviour rather than a port.
  --
  -- The addon version column reproduces get_addon_version_udf, which distinguishes three cases
  -- that ignore_rule() treats differently, so don't collapse them:
  --   addons is NULL            -> NULL
  --   addon not in the list     -> 'Not installed'
  --   addon present             -> the text after the first colon
  --
  --   CASE
  --     WHEN addons IS NULL THEN NULL
  --     ELSE COALESCE(
  --       (SELECT SPLIT(a.element, ':')[SAFE_OFFSET(1)]
  --        FROM UNNEST(addons.list) a
  --        WHERE SPLIT(a.element, ':')[OFFSET(0)] = '<guid>' LIMIT 1),
  --       'Not installed')
  --   END AS ADDON<n>_VERSION
  {generated_feature_columns}

FROM flattened
