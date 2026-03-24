SELECT date,
{% for field in ["bug_count", "not_supported_score", "total_score", "needs_diagnosis_score"] %}
  CASE "{{ param("country") }}"
  {% for key, metric in metrics.items() if metric.country_code %}
    WHEN "{{ metric.pretty_name }}" THEN cast({{ field }}_{{ key }} as int)
  {% endfor %}
  END as {{ field }},
  CASE "{{ param("country") }}"
  {% for key, metric in metrics.items() if metric.country_code %}
    WHEN "{{ metric.pretty_name }}" THEN cast({{ field }}_{{ key }}_mobile as int)
  {% endfor %}
  END as {{ field }}_mobile{{ "," if not loop.last }}
{% endfor %}
FROM `{{ ref("webcompat_knowledge_base.webcompat_topline_metric_daily") }}`
ORDER BY date DESC;
