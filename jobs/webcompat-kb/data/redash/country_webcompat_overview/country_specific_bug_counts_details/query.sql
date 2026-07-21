WITH bugs AS (
  SELECT * FROM `{{ ref("webcompat_knowledge_base.scored_site_reports") }}`
  WHERE resolution = ""
),
country_bugs AS (
  SELECT
    score,
    metric_type_firefox_not_supported,
    webcompat_priority,
    is_mobile,
    is_desktop,
    c.country,
    c.is_all,
    c.is_specific
  FROM bugs
  CROSS JOIN UNNEST([
    {% for metric in dashboard_metrics %}
    STRUCT(
      "{{ metric.pretty_name }}" AS country,
      (
        {{ metric.condition("bugs") }}
        {% for tld in metric.tlds %}
        OR net.host(bugs.url) LIKE "%{{ tld }}"
        {% endfor %}
      ) AS is_all,
      (
        ({{ metric.condition("bugs") }} AND NOT bugs.is_global_1000)
        {% for tld in metric.tlds %}
        OR net.host(bugs.url) LIKE "%{{ tld }}"
        {% endfor %}
      ) AS is_specific
    ){{ "," if not loop.last }}
    {% endfor %}
  ]) AS c
), transformed as (

SELECT country, "All" AS webcompat_priority,
  COUNTIF(is_all) AS bug_count_all,
  COUNTIF(is_all AND is_mobile) AS bug_count_all_mobile,
  COUNTIF(is_all AND is_desktop) AS bug_count_all_desktop,
  COUNTIF(is_specific) AS bug_count_specific,
  COUNTIF(is_specific AND is_mobile) AS bug_count_specific_mobile,
  COUNTIF(is_specific AND is_desktop) AS bug_count_specific_desktop,
  cast(SUM(IF(is_all, score, 0)) as int) AS score_all,
  cast(SUM(IF(is_all AND is_mobile, score, 0)) as int) AS score_all_mobile,
  cast(SUM(IF(is_all AND is_desktop, score, 0)) as int) AS score_all_desktop,
  cast(SUM(IF(is_specific, score, 0)) as int) AS score_specific,
  cast(SUM(IF(is_specific AND is_mobile, score, 0)) as int) AS score_specific_mobile,
  cast(SUM(IF(is_specific AND is_desktop, score, 0)) as int) AS score_specific_desktop,
  100 * SAFE_DIVIDE(COUNTIF(is_specific), COUNTIF(is_all)) AS share_bugs,
  100 * SAFE_DIVIDE(COUNTIF(is_specific AND is_mobile), COUNTIF(is_all AND is_mobile)) AS share_bugs_mobile,
  100 * SAFE_DIVIDE(COUNTIF(is_specific AND is_desktop), COUNTIF(is_all AND is_desktop)) AS share_bugs_desktop,
  100 * SAFE_DIVIDE(SUM(IF(is_specific, score, 0)), SUM(IF(is_all, score, 0))) AS share_score,
  100 * SAFE_DIVIDE(SUM(IF(is_specific AND is_mobile, score, 0)), SUM(IF(is_all AND is_mobile, score, 0))) AS share_score_mobile,
  100 * SAFE_DIVIDE(SUM(IF(is_specific AND is_desktop, score, 0)), SUM(IF(is_all AND is_desktop, score, 0))) AS share_score_desktop,
  COUNTIF(is_specific AND metric_type_firefox_not_supported) AS not_supported_count_specific,
  COUNTIF(is_specific AND is_mobile AND metric_type_firefox_not_supported) AS not_supported_count_specific_mobile,
  COUNTIF(is_specific AND is_desktop AND metric_type_firefox_not_supported) AS not_supported_count_specific_desktop
FROM country_bugs
GROUP BY country

UNION ALL

SELECT country, webcompat_priority,
  COUNTIF(is_all) AS bug_count_all,
  COUNTIF(is_all AND is_mobile) AS bug_count_all_mobile,
  COUNTIF(is_all AND is_desktop) AS bug_count_all_desktop,
  COUNTIF(is_specific) AS bug_count_specific,
  COUNTIF(is_specific AND is_mobile) AS bug_count_specific_mobile,
  COUNTIF(is_specific AND is_desktop) AS bug_count_specific_desktop,
  cast(SUM(IF(is_all, score, 0)) as int) AS score_all,
  cast(SUM(IF(is_all AND is_mobile, score, 0)) as int) AS score_all_mobile,
  cast(SUM(IF(is_all AND is_desktop, score, 0)) as int) AS score_all_desktop,
  cast(SUM(IF(is_specific, score, 0)) as int) AS score_specific,
  cast(SUM(IF(is_specific AND is_mobile, score, 0)) as int) AS score_specific_mobile,
  cast(SUM(IF(is_specific AND is_desktop, score, 0)) as int) AS score_specific_desktop,
  100 * SAFE_DIVIDE(COUNTIF(is_specific), COUNTIF(is_all)) AS share_bugs,
  100 * SAFE_DIVIDE(COUNTIF(is_specific AND is_mobile), COUNTIF(is_all AND is_mobile)) AS share_bugs_mobile,
  100 * SAFE_DIVIDE(COUNTIF(is_specific AND is_desktop), COUNTIF(is_all AND is_desktop)) AS share_bugs_desktop,
  100 * SAFE_DIVIDE(SUM(IF(is_specific, score, 0)), SUM(IF(is_all, score, 0))) AS share_score,
  100 * SAFE_DIVIDE(SUM(IF(is_specific AND is_mobile, score, 0)), SUM(IF(is_all AND is_mobile, score, 0))) AS share_score_mobile,
  100 * SAFE_DIVIDE(SUM(IF(is_specific AND is_desktop, score, 0)), SUM(IF(is_all AND is_desktop, score, 0))) AS share_score_desktop,
  COUNTIF(is_specific AND metric_type_firefox_not_supported) AS not_supported_count_specific,
  COUNTIF(is_specific AND is_mobile AND metric_type_firefox_not_supported) AS not_supported_count_specific_mobile,
  COUNTIF(is_specific AND is_desktop AND metric_type_firefox_not_supported) AS not_supported_count_specific_desktop
FROM country_bugs
GROUP BY country, webcompat_priority
HAVING webcompat_priority IN ("P1", "P2", "P3")

ORDER BY country, CASE webcompat_priority
  WHEN "All" THEN 0
  WHEN "P1" THEN 1
  WHEN "P2" THEN 2
  WHEN "P3" THEN 3
END
)

SELECT
  *,
  webcompat_priority AS priority__filter,
  SAFE_DIVIDE(share_score, share_bugs) as ratio,
  share_bugs_mobile - share_bugs AS uplift_bugs,
  share_score_mobile - share_score AS uplift_score,
  100 * SAFE_DIVIDE(bug_count_specific, bug_count_all) AS pct_specific
FROM transformed
