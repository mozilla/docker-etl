/* DEPRECATED: Replaced by webcompat_topline_metric_sightline */

WITH
  bugs AS (
  SELECT
    DISTINCT site_reports.number as number,
    site_reports.creation_time,
    site_reports.resolved_time,
    site_reports.type_needs_diagnosis,
    site_reports.type_platform_bug,
    site_reports.firefox_not_supported,
    site_reports.score
  FROM
    `{{ ref('webcompat_topline_metric_site_reports') }}` AS site_reports)
SELECT
  date,
  count(bugs.number) as bug_count,
  SUM(if(bugs.type_needs_diagnosis, bugs.score, 0)) as needs_diagnosis_score,
  SUM(if(bugs.type_platform_bug, bugs.score, 0)) as platform_score,
  SUM(if(bugs.firefox_not_supported, bugs.score, 0)) as not_supported_score,
  SUM(bugs.score) AS total_score
FROM
  UNNEST(GENERATE_DATE_ARRAY(DATE_TRUNC(DATE("2024-01-01"), week), DATE_TRUNC(CURRENT_DATE(), week), INTERVAL 1 week)) AS date
LEFT JOIN
  bugs
ON
  DATE(bugs.creation_time) <= date
  AND
IF
  (bugs.resolved_time IS NOT NULL, DATE(bugs.resolved_time) >= date, TRUE)
GROUP BY
  date
order by date
