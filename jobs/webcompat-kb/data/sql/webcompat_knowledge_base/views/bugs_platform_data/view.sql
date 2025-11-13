WITH webcompat_bugs AS (
  SELECT core_bugs.number AS number, ARRAY_AGG(STRUCT(webcompat_bugs.number, webcompat_bugs.webcompat_priority)) as bugs
  FROM `{{ ref('core_bugs_all') }}` as core_bugs_link
  JOIN `{{ ref('bugzilla_bugs') }}` as core_bugs ON core_bugs_link.core_bug = core_bugs.number
  LEFT JOIN `{{ ref('breakage_reports') }}` as breakage_reports ON core_bugs_link.knowledge_base_bug = breakage_reports.knowledge_base_bug
  LEFT JOIN `{{ ref('bugzilla_bugs') }}` as webcompat_bugs ON webcompat_bugs.number = breakage_reports.breakage_bug
  GROUP BY number
),

bugs_features AS (
  SELECT number, feature
  FROM `{{ ref('bugzilla_bugs') }}` as bugs
  LEFT JOIN `{{ ref('web_features.features_latest') }}` AS web_features ON feature IN UNNEST(`{{ ref('EXTRACT_ARRAY') }}`(bugs.user_story, "$.web-feature"))
),

feature_support_dates_browser AS (
  SELECT
    web_features.feature,
    browser,
    browser_versions.date
  FROM
    `{{ ref('web_features.features_latest') }}` AS web_features
  CROSS JOIN UNNEST(["firefox", "chrome", "safari"]) as browser
  LEFT JOIN UNNEST(web_features.support) as support ON support.browser = browser
  LEFT JOIN `{{ ref('web_features.browser_versions') }}` as browser_versions ON browser = browser_versions.browser_id AND support.browser_version = browser_versions.version
),

feature_support_dates AS (
  SELECT * from feature_support_dates_browser
  PIVOT(MIN(date) for browser in ("firefox", "chrome", "safari"))
),

bug_see_alsos AS (SELECT number, `{{ ref('URL_PARSE') }}`(see_also) as see_also_url
  FROM `{{ ref('bugzilla_bugs') }}` as bugs
  LEFT JOIN UNNEST(bugs.see_also) as see_also
),

mozilla_sp_see_also AS (
  SELECT number, CAST(ARRAY_LAST(SPLIT(see_also_url.path, "/")) AS INTEGER) as issue
  FROM bug_see_alsos
  WHERE see_also_url.host = "github.com" and STARTS_WITH(see_also_url.path, "/mozilla/standards-positions/issues/")
),

mozilla_standards_positions AS (
  SELECT DISTINCT number, sp_mozilla.*
  FROM bugs_features
  JOIN `{{ ref('bugzilla_bugs') }}` as bugs USING(number)
  LEFT JOIN mozilla_sp_see_also USING(number)
  LEFT JOIN
  `{{ ref('standards_positions.mozilla_standards_positions') }}` AS sp_mozilla
ON
  `{{ ref('BUG_ID_FROM_BUGZILLA_URL') }}`(sp_mozilla.bug) = bugs_features.number OR sp_mozilla.issue = mozilla_sp_see_also.issue
),

webkit_standards_positions AS (
  SELECT *, CAST(ARRAY_LAST(SPLIT(mozilla_url.path, "/")) AS INTEGER) as mozilla_sp
  FROM (
    SELECT *, `{{ ref('URL_PARSE') }}`(mozilla) as mozilla_url
    FROM `{{ ref('standards_positions.webkit_standards_positions') }}`
  )
  WHERE mozilla_url.host = "github.com" AND STARTS_WITH(mozilla_url.path, "mozilla/standards-position/issues/")
),

use_counters as (SELECT DISTINCT feature, date, day_percentage * 100 as use_count FROM `{{ ref('chrome_use_counters.use_counters') }}`
JOIN (SELECT feature, max(date) as date FROM `{{ ref('chrome_use_counters.use_counters') }}` GROUP BY feature) USING(feature, date))

SELECT DISTINCT
  number,
  component_owners.team,
  size_estimate,
  SAFE.PARSE_DATE("%F", JSON_VALUE(user_story, "$.platform-scheduled")) AS scheduled_date,
  bugs_features.feature,
  web_features.name as web_features_name,
  /*a11y_impact,
  privacy_impact,
  performance_impact,*/
  use_count as chrome_use_counter,
  sp_mozilla.issue AS mozilla_standards_position_issue,
  sp_mozilla.position AS mozilla_standards_position,
  sp_webkit.issue as webkit_standards_position_issue,
  sp_webkit.position AS webkit_standards_position,
  feature_support_dates.chrome IS NOT NULL OR "parity-chrome" IN UNNEST(bugs.keywords) as chrome_implemented,
  feature_support_dates.chrome AS chrome_supported_date,
  feature_support_dates.safari IS NOT NULL OR "parity-safari" IN UNNEST(bugs.keywords) as safari_implemented,
  feature_support_dates.safari AS safari_supported_date,
  webcompat_bugs.bugs as webcompat_bugs,
  CASE REGEXP_EXTRACT(bugs.whiteboard, r"webcompat:risk-(\w+)")
    WHEN "high" THEN "high"
    WHEN "moderate" THEN "moderate"
    WHEN "low" THEN "low"
    ELSE NULL
  END as webcompat_risk
FROM `{{ ref('bugzilla_bugs') }}` AS bugs
LEFT JOIN bugs_features USING(number)
LEFT JOIN `{{ ref('web_features.features_latest') }}` AS web_features USING(feature)
LEFT JOIN use_counters USING(feature)
LEFT JOIN feature_support_dates USING(feature)
LEFT JOIN mozilla_standards_positions AS sp_mozilla USING(number)
LEFT JOIN webkit_standards_positions AS sp_webkit ON sp_mozilla.number = sp_webkit.mozilla_sp
LEFT JOIN webcompat_bugs USING(number)
LEFT JOIN `{{ ref('bugzilla_components_ownership') }}` as component_owners ON component_owners.bugzilla_product = bugs.product AND component_owners.bugzilla_component = bugs.component
ORDER BY number ASC
