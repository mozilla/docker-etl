WITH bugs_with_flags AS (
  SELECT
    score,
    metric_type_firefox_not_supported,
    webcompat_priority,
    is_mobile,
    is_desktop,
    CASE "{{ param("country") }}"
    {% for metric in dashboard_metrics %}
      WHEN "{{ metric.pretty_name }}" THEN (
        {{ metric.condition("bugs") }}
        {% for tld in metric.tlds %}
          OR net.host(url) LIKE "%{{ tld }}"
        {% endfor %}
      )
    {% endfor %}
      ELSE FALSE
    END AS is_country_all,
    CASE "{{ param("country") }}"
    {% for metric in dashboard_metrics %}
      WHEN "{{ metric.pretty_name }}" THEN (
        ({{ metric.condition("bugs") }} AND NOT bugs.is_global_1000)
        {% for tld in metric.tlds %}
          OR net.host(url) LIKE "%{{ tld }}"
        {% endfor %}
      )
    {% endfor %}
      ELSE FALSE
    END AS is_country_specific
  FROM `{{ ref("webcompat_knowledge_base.scored_site_reports") }}` AS bugs
  WHERE bugs.resolution = ""
)

SELECT "All" AS webcompat_priority,
  COUNTIF(is_country_all) AS bug_count_all,
  COUNTIF(is_country_all AND is_mobile) AS bug_count_all_mobile,
  COUNTIF(is_country_all AND is_desktop) AS bug_count_all_desktop,
  COUNTIF(is_country_specific) AS bug_count_specific,
  COUNTIF(is_country_specific AND is_mobile) AS bug_count_specific_mobile,
  COUNTIF(is_country_specific AND is_desktop) AS bug_count_specific_desktop,
  cast(SUM(IF(is_country_all, score, 0)) as int) AS score_all,
  cast(SUM(IF(is_country_all AND is_mobile, score, 0)) as int) AS score_all_mobile,
  cast(SUM(IF(is_country_all AND is_desktop, score, 0)) as int) AS score_all_desktop,
  cast(SUM(IF(is_country_specific, score, 0)) as int) AS score_specific,
  cast(SUM(IF(is_country_specific AND is_mobile, score, 0)) as int) AS score_specific_mobile,
  cast(SUM(IF(is_country_specific AND is_desktop, score, 0)) as int) AS score_specific_desktop,
  100 * SAFE_DIVIDE(COUNTIF(is_country_specific), COUNTIF(is_country_all)) AS share_bugs,
  100 * SAFE_DIVIDE(COUNTIF(is_country_specific AND is_mobile), COUNTIF(is_country_all AND is_mobile)) AS share_bugs_mobile,
  100 * SAFE_DIVIDE(COUNTIF(is_country_specific AND is_desktop), COUNTIF(is_country_all AND is_desktop)) AS share_bugs_desktop,
  100 * SAFE_DIVIDE(SUM(IF(is_country_specific, score, 0)), SUM(IF(is_country_all, score, 0))) AS share_score,
  100 * SAFE_DIVIDE(SUM(IF(is_country_specific AND is_mobile, score, 0)), SUM(IF(is_country_all AND is_mobile, score, 0))) AS share_score_mobile,
  100 * SAFE_DIVIDE(SUM(IF(is_country_specific AND is_desktop, score, 0)), SUM(IF(is_country_all AND is_desktop, score, 0))) AS share_score_desktop,
  COUNTIF(is_country_specific AND metric_type_firefox_not_supported) AS not_supported_count_specific,
  COUNTIF(is_country_specific AND is_mobile AND metric_type_firefox_not_supported) AS not_supported_count_specific_mobile,
  COUNTIF(is_country_specific AND is_desktop AND metric_type_firefox_not_supported) AS not_supported_count_specific_desktop
FROM bugs_with_flags

UNION ALL

SELECT
  webcompat_priority,
  COUNTIF(is_country_all) AS bug_count_all,
  COUNTIF(is_country_all AND is_mobile) AS bug_count_all_mobile,
  COUNTIF(is_country_all AND is_desktop) AS bug_count_all_desktop,
  COUNTIF(is_country_specific) AS bug_count_specific,
  COUNTIF(is_country_specific AND is_mobile) AS bug_count_specific_mobile,
  COUNTIF(is_country_specific AND is_desktop) AS bug_count_specific_desktop,
  cast(SUM(IF(is_country_all, score, 0)) as int) AS score_all,
  cast(SUM(IF(is_country_all AND is_mobile, score, 0)) as int) AS score_all_mobile,
  cast(SUM(IF(is_country_all AND is_desktop, score, 0)) as int) AS score_all_desktop,
  cast(SUM(IF(is_country_specific, score, 0)) as int) AS score_specific,
  cast(SUM(IF(is_country_specific AND is_mobile, score, 0)) as int) AS score_specific_mobile,
  cast(SUM(IF(is_country_specific AND is_desktop, score, 0)) as int) AS score_specific_desktop,
  100 * SAFE_DIVIDE(COUNTIF(is_country_specific), COUNTIF(is_country_all)) AS share_bugs,
  100 * SAFE_DIVIDE(COUNTIF(is_country_specific AND is_mobile), COUNTIF(is_country_all AND is_mobile)) AS share_bugs_mobile,
  100 * SAFE_DIVIDE(COUNTIF(is_country_specific AND is_desktop), COUNTIF(is_country_all AND is_desktop)) AS share_bugs_desktop,
  100 * SAFE_DIVIDE(SUM(IF(is_country_specific, score, 0)), SUM(IF(is_country_all, score, 0))) AS share_score,
  100 * SAFE_DIVIDE(SUM(IF(is_country_specific AND is_mobile, score, 0)), SUM(IF(is_country_all AND is_mobile, score, 0))) AS share_score_mobile,
  100 * SAFE_DIVIDE(SUM(IF(is_country_specific AND is_desktop, score, 0)), SUM(IF(is_country_all AND is_desktop, score, 0))) AS share_score_desktop,
  COUNTIF(is_country_specific AND metric_type_firefox_not_supported) AS not_supported_count_specific,
  COUNTIF(is_country_specific AND is_mobile AND metric_type_firefox_not_supported) AS not_supported_count_specific_mobile,
  COUNTIF(is_country_specific AND is_desktop AND metric_type_firefox_not_supported) AS not_supported_count_specific_desktop
FROM bugs_with_flags
GROUP BY webcompat_priority
HAVING webcompat_priority IN ("P1", "P2", "P3")

ORDER BY CASE webcompat_priority
  WHEN "All" THEN 0
  WHEN "P1" THEN 1
  WHEN "P2" THEN 2
  WHEN "P3" THEN 3
END
