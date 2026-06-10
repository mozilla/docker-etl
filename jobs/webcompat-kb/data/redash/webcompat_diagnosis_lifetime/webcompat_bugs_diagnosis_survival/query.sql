WITH
diagnosis_enter AS (
  SELECT
    history.number,
    MIN(history.change_time) as enter_time
  FROM {{ ref ("webcompat_knowledge_base.bugs_history") }} AS history,
    UNNEST (history.changes) as changes
  WHERE changes.field_name LIKE '%keywords%'
    AND changes.added LIKE '%webcompat:needs_diagnosis%'
  GROUP BY history.number
),
diagnosis_exit AS (
  SELECT
    history.number,
    MIN(history.change_time) as exit_time
  FROM {{ ref ("webcompat_knowledge_base.bugs_history") }} AS history,
    UNNEST (history.changes) as changes
  WHERE changes.field_name LIKE '%keywords%'
    AND changes.removed LIKE '%webcompat:needs_diagnosis%'
  GROUP BY history.number
),
diagnosis_durations AS (
  SELECT
    site_reports.webcompat_priority,
    diagnosis_enter.number,
    diagnosis_enter.enter_time,
    diagnosis_exit.exit_time,
    TIMESTAMP_DIFF(diagnosis_exit.exit_time, diagnosis_enter.enter_time, SECOND)/(3600*24) as days_in_status
  FROM diagnosis_enter
  INNER JOIN diagnosis_exit USING(number)
  JOIN {{ ref ("webcompat_knowledge_base.scored_site_reports") }} AS site_reports USING(number)
  WHERE site_reports.webcompat_priority IN ('P1', 'P2', 'P3')
    AND DATE(site_reports.creation_time) BETWEEN DATE('{{ param("from") }}') AND DATE('{{ param("to") }}')
    AND
      CASE "{{ param("metric") }}" {% for metric in metrics.values() %}
        WHEN "{{ metric.pretty_name }}" THEN {{ metric.condition("site_reports") }}
  {%- endfor %}
),
totals AS (
  SELECT
    webcompat_priority, COUNT(*) as total
  FROM diagnosis_durations
  GROUP BY webcompat_priority
),
counts AS (
  SELECT 
    day,
    COUNTIF(d.webcompat_priority = 'P1' AND d.days_in_status >= day) as open_p1,
    COUNTIF(d.webcompat_priority = 'P2' AND d.days_in_status >= day) as open_p2,
    COUNTIF(d.webcompat_priority = 'P3' AND d.days_in_status >= day) as open_p3
  FROM UNNEST(GENERATE_ARRAY(0, 90)) as day
    LEFT JOIN diagnosis_durations d ON TRUE
  GROUP BY day
)

SELECT
  day,
  SAFE_DIVIDE(open_p1, (SELECT total FROM totals WHERE webcompat_priority = 'P1')) as P1,
  SAFE_DIVIDE(open_p2, (SELECT total FROM totals WHERE webcompat_priority = 'P2')) as P2,
  SAFE_DIVIDE(open_p3, (SELECT total FROM totals WHERE webcompat_priority = 'P3')) as P3
FROM counts
ORDER BY day
;
