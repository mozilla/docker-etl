WITH last_import_time AS (
  SELECT DATETIME(MAX(run_at)) FROM `{{ ref('import_runs') }}`
)

SELECT
  (SELECT * FROM last_import_time) AS time,
  {% for metric in metrics.values() -%}
    -- {{ metric.name }}
    {% for metric_type in metric_types -%}
      {{ metric_type.agg_function('bugs', metric) }} as {{ metric_type.name }}_{{ metric.name }}{{ ',' if not loop.last }}
    {% endfor %}
    {{ ',' if not loop.last }}
  {% endfor %}
FROM
  `{{ ref('scored_site_reports') }}` AS bugs

WHERE
  bugs.resolution = ""
