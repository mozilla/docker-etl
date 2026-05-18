WITH
diagnosis_enter AS (
SELECT
  history.number,
  history.change_time, 
  changes.added
FROM moz-fx-dev-dschubert-wckb.webcompat_knowledge_base.bugs_history history,
  UNNEST (history.changes) as changes
WHERE changes.field_name LIKE '%keywords%'
  AND changes.added LIKE'%webcompat:needs_diagnosis%'
ORDER BY history.number
),
diagnosis_exit AS (
SELECT
  history.number,
  history.change_time,
  changes.removed
FROM moz-fx-dev-dschubert-wckb.webcompat_knowledge_base.bugs_history history,
  UNNEST (history.changes) as changes
WHERE changes.field_name LIKE '%keywords%'
  AND changes.removed LIKE'%webcompat:needs_diagnosis%'
ORDER BY history.number
),
diagnosis_lifetimes AS (
SELECT
  site_reports.webcompat_priority,
  DATE_DIFF(diagnosis_exit.change_time, diagnosis_enter.change_time, DAY) as lifetime_days
FROM diagnosis_enter diagnosis_enter
  JOIN diagnosis_exit diagnosis_exit on diagnosis_enter.number = diagnosis_exit.number
  JOIN moz-fx-dev-dschubert-wckb.webcompat_knowledge_base.scored_site_reports site_reports ON site_reports.number = diagnosis_enter.number
WHERE site_reports.webcompat_priority IN ('P1', 'P2', 'P3')
  AND DATE(site_reports.creation_time) BETWEEN DATE('{{from}}') AND DATE('{{to}}')
  AND
    CASE "{{ param("metric") }}" {% for metric in metrics.values() %}
      WHEN "{{ metric.pretty_name }}" THEN {{ metric.condition("site_reports") }}
  {%- endfor %}
),
stats AS (
SELECT
  webcompat_priority,
  COUNT(*) as total_bugs,
  AVG(lifetime_days) as avg_lifetime,
  APPROX_QUANTILES(lifetime_days, 100)[OFFSET(10)] as p10_days,
  APPROX_QUANTILES(lifetime_days, 100)[OFFSET(25)] as p25_days,
  APPROX_QUANTILES(lifetime_days, 100)[OFFSET(50)] as p50_days,
  APPROX_QUANTILES(lifetime_days, 100)[OFFSET(75)] as p75_days,
  APPROX_QUANTILES(lifetime_days, 100)[OFFSET(90)] as p90_days,
  APPROX_QUANTILES(lifetime_days, 100)[OFFSET(100)] as p100_days
FROM diagnosis_lifetimes
GROUP BY webcompat_priority
),
unpivoted AS (
SELECT
  webcompat_priority,
  percentile,
  resolved_in_days
FROM stats
CROSS JOIN UNNEST([
  STRUCT('p10' as percentile, p10_days as resolved_in_days),
  STRUCT('p25', p25_days),
  STRUCT('p50', p50_days),
  STRUCT('p75', p75_days),
  STRUCT('p90', p90_days),
  STRUCT('p100', p100_days)
])
)

SELECT
  percentile,
  MAX(CASE WHEN webcompat_priority = 'P1' THEN resolved_in_days END) as P1,
  MAX(CASE WHEN webcompat_priority = 'P2' THEN resolved_in_days END) as P2,
  MAX(CASE WHEN webcompat_priority = 'P3' THEN resolved_in_days END) as P3
FROM unpivoted
GROUP BY percentile
;
