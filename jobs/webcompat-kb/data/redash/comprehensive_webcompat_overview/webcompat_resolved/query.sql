SELECT count(*) AS bug_count_all,
{% for metric_name, metric in metrics.items() -%}
    {% for metric_type in metric_types if metric_type.name in ["bug_count", "total_score"] %}
    cast({{ metric_type.agg_function("site_reports", metric) }} AS int64) AS {{ metric_type.name }}_{{ metric_name }}{{ ',' if not loop.last }}
    {%- endfor %}
{%- endfor %}
FROM `{{ ref("webcompat_knowledge_base.scored_site_reports") }}` AS site_reports
WHERE (site_reports.resolution = "FIXED" OR site_reports.resolution = "WORKSFORME") AND
      DATE(site_reports.resolved_time) >= DATE("{{ param("start_date") }}")
