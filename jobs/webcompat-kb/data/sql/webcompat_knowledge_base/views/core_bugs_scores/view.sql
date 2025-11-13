WITH core_bug_scores AS (
  SELECT
    core_bug as number,
    {% for metric in metrics.values() -%}
      SUM(IF({{ metric.condition('scored_site_reports') }}, score, 0)) as score_{{ metric.name }}{{ ',' if not loop.last }}
    {% endfor %}
  FROM `{{ ref('breakage_reports_core_bugs') }}` AS breakage_reports_core_bugs
  JOIN `{{ ref('scored_site_reports') }}` AS scored_site_reports ON scored_site_reports.number = breakage_reports_core_bugs.breakage_bug
  WHERE scored_site_reports.resolution = ""
  GROUP BY breakage_reports_core_bugs.core_bug
)

SELECT core_bugs.*, core_bug_scores.* EXCEPT(number)
FROM core_bug_scores
JOIN `{{ ref('bugzilla_bugs') }}` AS core_bugs USING(number)
