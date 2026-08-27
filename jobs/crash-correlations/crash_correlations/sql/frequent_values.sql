-- Values frequent enough to be worth a feature, and their counts.
--
-- Replaces the "Counting addons" and "Counting modules" passes in crash_deviations.py, which
-- exploded those columns into RDDs and reduceByKey'd.
--
-- This query does two jobs, and the second one is easy to miss. Upstream, each of these passes
-- ends in save_results(), which writes the counts straight into saved_counts. The level 1
-- counting pass then deliberately skips them: line 669 is
--
--     columns = [c for c in dfReference.columns
--                if c not in get_columns('reference', dfReference.columns)]
--
-- so anything already counted is excluded. That's why the release channel finds 2,415 modules
-- but prints "1 CANDIDATES: 52". The modules and addons are features, and they do get columns
-- in the augmented dataframe for the level 2 pass, but they are never re-counted at level 1.
--
-- So the output here is not just a column list, it's the level 1 counts for those features. Feed
-- it into the same {group: {itemset: count}} structure mining.count_level_1 produces, and have
-- the level 1 pass cover only the scalar columns. Getting this wrong would double count.
--
-- Not everything upstream counted is here. The app_notes and graphics_critical_error candidate
-- lists don't come from the data at all: app_notes.py and gfx_critical_errors.py scrape
-- mozilla-central through searchfox for the string literals passed to ScopedGfxFeatureReporter
-- and gfxCriticalError, then look for those as substrings. Those stay in Python (30 and 481
-- candidates respectively at time of writing, of which 13 and 13 clear the threshold on
-- release). Their counting is the same shape as this and can reuse the same code path.
--
-- Upstream kept a value when its support exceeded @min_support_diff in the reference OR in any
-- single signature's crashes, so a module that's rare overall but present in most crashes for
-- one signature survives. That's the whole point of the job, so the group side matters at least
-- as much as the reference side.
--
-- MIN_COUNT (5, for the chi-squared test) applies as well; a value under that can't produce a
-- usable statistic either way.

WITH window_crashes AS (
  SELECT
    signature,
    uuid,
    addons,
    json_dump
  FROM `moz-fx-data-shared-prod.telemetry_derived.socorro_crash_v2`
  WHERE
    crash_date >= DATE_SUB(@end_date, INTERVAL @window_days DAY)
    AND crash_date < @end_date
    AND product = @product
    AND version IN UNNEST(@versions)
),

signature_totals AS (
  SELECT signature, COUNT(*) AS total_group
  FROM window_crashes
  WHERE signature IN UNNEST(@signatures)
  GROUP BY signature
),

reference_total AS (
  SELECT COUNT(*) AS total_reference FROM window_crashes
),

-- Modules. Upstream deduplicated on (uuid, module filename) before counting, so a module
-- mapped at several addresses in one crash report counts once. DISTINCT here is that dedupe.
module_rows AS (
  SELECT DISTINCT
    signature,
    uuid,
    m.element.filename AS value
  FROM window_crashes, UNNEST(json_dump.modules.list) AS m
  WHERE m.element.filename IS NOT NULL
),

-- Addons arrive as "guid:version" strings; the name half is the feature. Note addons.list is a
-- repeated STRUCT<element STRING> rather than a repeated STRING, so it's a.element, and the
-- get_addon_name equivalent returns NULL when there's no colon, which upstream filters out.
addon_rows AS (
  SELECT DISTINCT
    signature,
    uuid,
    SPLIT(a.element, ':')[OFFSET(0)] AS value
  FROM window_crashes, UNNEST(addons.list) AS a
  WHERE a.element IS NOT NULL AND STRPOS(a.element, ':') > 0
),

combined AS (
  SELECT 'module' AS kind, signature, value FROM module_rows
  UNION ALL
  SELECT 'addon' AS kind, signature, value FROM addon_rows
),

reference_counts AS (
  SELECT kind, value, COUNT(*) AS count_reference
  FROM combined
  GROUP BY kind, value
),

group_counts AS (
  SELECT
    c.kind,
    c.value,
    c.signature,
    COUNT(*) AS count_group,
    ANY_VALUE(s.total_group) AS total_group
  FROM combined AS c
  JOIN signature_totals AS s USING (signature)
  GROUP BY c.kind, c.value, c.signature
),

-- Best group support for each value, i.e. the signature where it stands out most. Only the
-- maximum matters, since the value is kept if any single signature clears the threshold.
best_group_support AS (
  SELECT
    kind,
    value,
    MAX(SAFE_DIVIDE(count_group, total_group)) AS max_support_group
  FROM group_counts
  WHERE count_group >= @min_count
  GROUP BY kind, value
),

-- The two kinds use different rules, which is not obvious from reading the code and matters a
-- lot for the column count.
--
--   modules  group support only. crash_deviations.py:327 is
--            `all_modules = set.union(set(), *all_modules_groups.values())`, with no
--            all_modules_ref term at all, so a module is a feature only if it clears the
--            threshold within some signature. A module present on most crashes channel wide is
--            not interesting and is dropped.
--   addons   reference OR group, per crash_deviations.py:269
--            `all_addons = all_addons_ref.union(*all_addons_groups.values())`.
--
-- Applying the addon rule to modules pulls in every common DLL: measured on beta over the 5 days
-- ending 2026-08-14, the OR rule keeps 1,354 modules where the group-only rule keeps far fewer.
-- Since level 2 counting is O(rows x columns^2), that difference is the whole feasibility of the
-- job, so it is not a cosmetic parity detail.
kept AS (
  SELECT r.kind, r.value, r.count_reference
  FROM reference_counts AS r
  CROSS JOIN reference_total AS t
  LEFT JOIN best_group_support AS g USING (kind, value)
  WHERE
    r.count_reference >= @min_count
    AND (
      CASE r.kind
        WHEN 'module' THEN COALESCE(g.max_support_group, 0) > @min_support_diff
        ELSE
          SAFE_DIVIDE(r.count_reference, t.total_reference) > @min_support_diff
          OR COALESCE(g.max_support_group, 0) > @min_support_diff
      END
    )
)

-- One row per (kind, value, signature) plus one with a NULL signature for the reference count,
-- which is the shape mining.count_level_1 returns and what save_results() stored. Only kept
-- values appear, so this is both the feature list and its level 1 counts.
SELECT
  k.kind,
  k.value,
  CAST(NULL AS STRING) AS signature,
  k.count_reference AS count
FROM kept AS k

UNION ALL

SELECT
  g.kind,
  g.value,
  g.signature,
  g.count_group AS count
FROM group_counts AS g
JOIN kept AS k USING (kind, value)
WHERE g.count_group >= @min_count

ORDER BY kind, value, signature
