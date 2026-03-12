SELECT
  cast(site_reports.score as int64) as score,
  site_reports.number,
  site_reports.title,
  site_reports.webcompat_priority,
  next_action
FROM `{{ ref("webcompat_knowledge_base.scored_site_reports") }}` as site_reports
JOIN `{{ ref("webcompat_knowledge_base.site_reports_next_action") }}` as next_actions USING(number)
JOIN UNNEST(next_actions.next_action_teams) AS team
WHERE
  site_reports.resolution = "" AND
  ("ALL" in UNNEST([{{ param("next_action_team") }}]) OR TRIM(team) IN UNNEST([{{ param("next_action_team") }}])) AND
  ("ANY" IN UNNEST([{{ param("next_action") }}]) OR next_action IN UNNEST([{{ param("next_action") }}])) AND
  CASE "{{ param("metric") }}" {% for metric in metrics.values() %}
    WHEN "{{ metric.pretty_name }}" THEN {{ metric.condition("site_reports") }}
{%- endfor %}
    ELSE FALSE
  END
ORDER BY score DESC, number ASC
