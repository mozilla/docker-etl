-- Which of the searchfox-scraped substrings actually appear often enough to be features.
--
-- Replaces count_substrings() in crash_deviations.py, which is called twice: once for the
-- app_notes candidates and once for graphics_critical_error. It returns all_substrings, and
-- augment() then builds a column only for those, not for every candidate.
--
-- Skipping this filter is not an optimisation you can defer. The candidate lists are large
-- (30 app notes, 481 gfx errors at time of writing) while the surviving sets are small (13 and 13
-- on release). Level 2 counting is O(rows x columns^2), so carrying 511 columns instead of 26 is
-- a ~390x cost increase on the dominant stage.
--
-- Same keep rule as addons, reference OR any single group clearing @min_support_diff, per
-- crash_deviations.py:161 `all_substrings_ref.union(*all_substrings_groups.values())`.
--
-- @substrings is the candidate list; @kinds is the matching field per candidate, either
-- 'app_note' or 'gfx_error', so both passes run as one query.

WITH window_crashes AS (
  SELECT
    signature,
    app_notes,
    graphics_critical_error
  FROM `moz-fx-data-shared-prod.telemetry_derived.socorro_crash_v2`
  WHERE
    crash_date >= DATE_SUB(@end_date, INTERVAL @window_days DAY)
    AND crash_date < @end_date
    AND product = @product
    AND version IN UNNEST(@versions)
),

reference_total AS (
  SELECT COUNT(*) AS total_reference FROM window_crashes
),

signature_totals AS (
  SELECT signature, COUNT(*) AS total_group
  FROM window_crashes
  WHERE signature IN UNNEST(@signatures)
  GROUP BY signature
),

candidates AS (
  SELECT kind, value
  FROM UNNEST(@substrings) AS value WITH OFFSET AS pos
  JOIN UNNEST(@kinds) AS kind WITH OFFSET AS kind_pos
    ON pos = kind_pos
),

-- One row per (crash, matching candidate).
--
-- STRPOS rather than CONTAINS_SUBSTR: the latter requires a constant second argument, so it
-- can't take a value joined in from the candidate list. STRPOS is also the closer match to
-- upstream, which uses Spark's instr(...) != 0. Note the two differ in case sensitivity,
-- CONTAINS_SUBSTR normalises case while STRPOS does not, and upstream's instr does not either,
-- so STRPOS is correct here on both counts.
matches AS (
  SELECT
    w.signature,
    c.kind,
    c.value
  FROM window_crashes AS w
  CROSS JOIN candidates AS c
  WHERE
    CASE c.kind
      WHEN 'app_note' THEN STRPOS(w.app_notes, c.value) != 0
      WHEN 'gfx_error' THEN STRPOS(w.graphics_critical_error, c.value) != 0
      ELSE FALSE
    END
),

reference_counts AS (
  SELECT kind, value, COUNT(*) AS count_reference
  FROM matches
  GROUP BY kind, value
),

group_counts AS (
  SELECT
    m.kind,
    m.value,
    m.signature,
    COUNT(*) AS count_group,
    ANY_VALUE(s.total_group) AS total_group
  FROM matches AS m
  JOIN signature_totals AS s USING (signature)
  GROUP BY m.kind, m.value, m.signature
),

best_group_support AS (
  SELECT kind, value, MAX(SAFE_DIVIDE(count_group, total_group)) AS max_support_group
  FROM group_counts
  WHERE count_group >= @min_count
  GROUP BY kind, value
),

kept AS (
  SELECT r.kind, r.value, r.count_reference
  FROM reference_counts AS r
  CROSS JOIN reference_total AS t
  LEFT JOIN best_group_support AS g USING (kind, value)
  WHERE
    r.count_reference >= @min_count
    AND (
      SAFE_DIVIDE(r.count_reference, t.total_reference) > @min_support_diff
      OR COALESCE(g.max_support_group, 0) > @min_support_diff
    )
)

-- Same shape as frequent_values.sql: NULL signature marks the reference count. These are the
-- level 1 counts for these features, so the level 1 pass must skip their columns.
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
