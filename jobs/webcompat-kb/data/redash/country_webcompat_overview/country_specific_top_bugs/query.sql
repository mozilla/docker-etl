SELECT
  number,
  title,
  cast(score as int) as `Impact Score`,
  webcompat_priority, net.host(url) as host,
  ARRAY_TO_STRING(next_action_teams, ",") as team
FROM `{{ ref("webcompat_knowledge_base.scored_site_reports") }}` AS bugs
JOIN `{{ ref("webcompat_knowledge_base.site_reports_next_action") }}` USING(number)
WHERE bugs.resolution = "" AND (
  CASE "{{ param("country") }}"
  {% for key, metric in metrics.items() if metric.country_code %}
    WHEN "{{ metric.pretty_name }}" THEN (
      (bugs.is_{{ key }} AND NOT bugs.is_global_1000)
      {% for tld in metric.tlds %}
        OR net.host(url) LIKE "%{{ tld }}"
      {% endfor %}
    )
  {% endfor %}
    ELSE FALSE
  END
) AND CASE "{{ param("platforms") }}"
    WHEN "All" THEN TRUE
    WHEN "Mobile" THEN is_mobile
    WHEN "Desktop" THEN is_desktop
  END
ORDER BY score DESC
