{% set metric_name = "all" %}
SELECT
  date,
  {% for metric_type in metric_types -%}
    {{ metric_type.agg_function('bugs', metrics[metric_name], False) }} as {{ metric_type.name }}{{ ',' if not loop.last }}
  {% endfor %}
FROM
  UNNEST(GENERATE_DATE_ARRAY(DATE_TRUNC(DATE("2024-01-01"), week), DATE_TRUNC(CURRENT_DATE(), week), INTERVAL 1 week)) AS date
LEFT JOIN
  `{{ ref('scored_site_reports') }}` AS bugs
ON
  DATE(bugs.creation_time) <= date
  AND
IF
  (bugs.resolved_time IS NOT NULL, DATE(bugs.resolved_time) >= date, TRUE)
WHERE {{ metrics[metric_name].condition('bugs') }}
GROUP BY
  date
order by date
