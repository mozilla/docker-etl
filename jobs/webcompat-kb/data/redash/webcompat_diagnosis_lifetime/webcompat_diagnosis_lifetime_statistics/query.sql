WITH
diagnosis_enter AS (
SELECT 
    bugs_history.number, 
    MIN(bugs_history.change_time) as change_time
FROM moz-fx-dev-dschubert-wckb.webcompat_knowledge_base.bugs_history bugs_history,
    UNNEST (bugs_history.changes) as changes
WHERE changes.field_name LIKE '%keywords%'
    AND changes.added LIKE'%webcompat:needs_diagnosis%'
GROUP BY bugs_history.number
),
diagnosis_exit AS (
SELECT 
    bugs_history.number, 
    MAX(bugs_history.change_time) as change_time
FROM moz-fx-dev-dschubert-wckb.webcompat_knowledge_base.bugs_history bugs_history,
    UNNEST (bugs_history.changes) as changes
WHERE changes.field_name LIKE '%keywords%'
    AND changes.removed LIKE'%webcompat:needs_diagnosis%'
GROUP BY bugs_history.number
),
lifetimes AS (
SELECT
  site_reports.webcompat_priority as priority,
  COUNT(1) as bug_count,
  APPROX_QUANTILES(DATETIME_DIFF(diagnosis_exit.change_time, diagnosis_enter.change_time, HOUR)/24, 100)[OFFSET(50)] as median_lifetime,
  AVG(DATETIME_DIFF(diagnosis_exit.change_time, diagnosis_enter.change_time, HOUR)/24) as avg_lifetime,
  MIN(DATETIME_DIFF(diagnosis_exit.change_time, diagnosis_enter.change_time, HOUR)/24) as min_lifetime,
  MAX(DATETIME_DIFF(diagnosis_exit.change_time, diagnosis_enter.change_time, HOUR)/24) as max_lifetime,
FROM diagnosis_enter
    JOIN diagnosis_exit ON diagnosis_enter.number = diagnosis_exit.number
    JOIN moz-fx-dev-dschubert-wckb.webcompat_knowledge_base.scored_site_reports as site_reports ON site_reports.number = diagnosis_enter.number
WHERE site_reports.webcompat_priority IN ('P1', 'P2', 'P3')
    AND DATE(site_reports.creation_time) BETWEEN DATE('{{from}}') AND DATE('{{to}}')
    AND
      CASE "{{ param("metric") }}" {% for metric in metrics.values() %}
        WHEN "{{ metric.pretty_name }}" THEN {{ metric.condition("site_reports") }}
    {%- endfor %}
GROUP BY site_reports.webcompat_priority
),
totals AS (
    SELECT 
        site_reports.webcompat_priority as priority,
        COUNT(1) as total
    FROM `moz-fx-dev-dschubert-wckb.webcompat_knowledge_base.scored_site_reports` site_reports
    WHERE site_reports.webcompat_priority IN ('P1', 'P2', 'P3')
      AND DATE(site_reports.creation_time) BETWEEN DATE('{{from}}') AND DATE('{{to}}')
      AND
        CASE "{{ param("metric") }}" {% for metric in metrics.values() %}
          WHEN "{{ metric.pretty_name }}" THEN {{ metric.condition("site_reports") }}
      {%- endfor %}
    GROUP BY priority
)

SELECT 
    lifetimes.priority as Priority,
    totals.total as `Total created`,
    lifetimes.bug_count as `Total diagnosed`,
    lifetimes.bug_count/total * 100 as Ratio,
    lifetimes.median_lifetime as `Median lifetime`,
    lifetimes.avg_lifetime as `Average`,
    lifetimes.min_lifetime as `Min`,
    lifetimes.max_lifetime as `Max`
FROM lifetimes lifetimes
    JOIN totals ON lifetimes.priority = totals.priority
ORDER BY priority
;