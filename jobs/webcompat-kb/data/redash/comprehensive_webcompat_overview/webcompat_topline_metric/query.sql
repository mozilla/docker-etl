SELECT
  date,
{% for metric in metrics -%}
  cast(needs_diagnosis_score_{{ metric }} as int) as needs_diagnosis_score_{{ metric }},
  cast(not_supported_score_{{ metric }} as int) as not_supported_score_{{ metric }},
  cast(total_score_{{ metric }} as int) as total_score_{{ metric }},
{% endfor %}
FROM `{{ ref("webcompat_knowledge_base.webcompat_topline_metric_daily") }}`
ORDER BY date DESC;
