WITH webcompat_bugs AS (
  SELECT number, ARRAY_AGG(STRUCT(webcompat_bugs.number, webcompat_bugs.webcompat_priority)) as bugs
  FROM
  `{{ ref('platform_features') }}` AS platform_features
  LEFT JOIN
  `{{ ref('core_bugs_all') }}` as core_bugs_link ON core_bugs_link.core_bug = platform_features.bug

  LEFT JOIN `{{ ref('breakage_reports') }}` as breakage_reports ON core_bugs_link.knowledge_base_bug = breakage_reports.knowledge_base_bug
  LEFT JOIN `{{ ref('bugzilla_bugs') }}` as webcompat_bugs ON webcompat_bugs.number = breakage_reports.breakage_bug
  GROUP BY number
),

platform_planning_bugs as (
  SELECT
    platform_planning.bug,
  FROM
    `{{ ref('platform_features') }}` AS platform_planning
  JOIN
    `{{ ref('bugzilla_bugs') }}` AS bugs
  ON
    bugs.number = platform_planning.bug
),

platform_feature_bugs as (
  SELECT
    bugs.number as bug
  FROM
    `{{ ref('bugzilla_bugs') }}` AS bugs
  WHERE CONTAINS_SUBSTR(bugs.whiteboard, "[platform-feature]")
),

all_bugs AS (
  SELECT * from platform_planning_bugs
  UNION DISTINCT
  SELECT * from platform_feature_bugs
),

all_bugs_features AS (
  SELECT bug, feature
  FROM all_bugs
  LEFT JOIN `{{ ref('bugzilla_bugs') }}` AS bugs ON bugs.number = bug
  LEFT JOIN `{{ ref('web_features.features_latest') }}` AS web_features ON feature IN UNNEST(`{{ ref('EXTRACT_ARRAY') }}`(bugs.user_story, "$.web-feature"))
),

missing_features as (
  SELECT web_features.feature
  FROM
    `{{ ref('web_features.features_latest') }}` AS web_features
  WHERE "chrome" IN UNNEST(web_features.support.browser) AND "safari" IN UNNEST(web_features.support.browser) AND NOT "firefox" in UNNEST(web_features.support.browser)
  EXCEPT DISTINCT
  SELECT feature FROM all_bugs_features
),

missing_feature_bugs as (
  SELECT bugs.number as bug, feature
  FROM missing_features
  LEFT JOIN
    `{{ ref('bugzilla_bugs') }}` AS bugs ON feature IN UNNEST(`{{ ref('EXTRACT_ARRAY') }}`(bugs.user_story, "$.web-feature"))
),

bugs_features as (
  SELECT bug, feature FROM all_bugs_features
  UNION DISTINCT
  SELECT bug, feature FROM missing_feature_bugs
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
  JOIN `{{ ref('bugzilla_bugs') }}` as bugs ON bugs.number = bugs_features.bug
  LEFT JOIN mozilla_sp_see_also USING(number)
  LEFT JOIN
  `{{ ref('standards_positions.mozilla_standards_positions') }}` AS sp_mozilla
ON
  `{{ ref('BUG_ID_FROM_BUGZILLA_URL') }}`(sp_mozilla.bug) = bugs_features.bug OR sp_mozilla.issue = mozilla_sp_see_also.issue
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
  bugs_features.bug,
  bugs_features.feature,
  IFNULL(platform_features.name, IFNULL(web_features.name, IFNULL(bugs.title, web_features.feature))) as title,
  component_owners.team,
  size_estimate,
  SAFE.PARSE_DATE("%F", JSON_VALUE(user_story, "$.platform-scheduled")) as scheduled_date,
  has_polyfill,
  cosmetic_only,
  partner_request,
  a11y_impact,
  privacy_impact,
  performance_impact,
  gecko_priority,
  state_of,
  use_count as chrome_use_counter,
  chrome_use_counter_override,
  other_dev_interest,
  sp_mozilla.issue AS mozilla_standards_position_issue,
  sp_mozilla.position AS mozilla_standards_position,
  sp_webkit.issue as webkit_standards_position_issue,
  sp_webkit.position AS webkit_standards_position,
  IF(bugs.alias IS NOT NULL, STARTS_WITH(bugs.alias, "interop-"), FALSE) OR ARRAY_LENGTH(`{{ ref('EXTRACT_ARRAY') }}`(bugs.user_story, "$.interop")) > 0 AS in_interop,
  feature_support_dates.chrome IS NOT NULL OR "parity-chrome" IN UNNEST(bugs.keywords) as chrome_implemented,
  feature_support_dates.chrome AS chrome_supported_date,
  feature_support_dates.safari IS NOT NULL OR "parity-safari" IN UNNEST(bugs.keywords) as safari_implemented,
  feature_support_dates.safari AS safari_supported_date,
  webcompat_bugs.bugs as webcompat_bugs,
  REGEXP_EXTRACT(bugs.whiteboard, r"webcompat:risk-(\w+)") as webcompat_risk
FROM
  bugs_features
LEFT JOIN
  `{{ ref('platform_features') }}` AS platform_features USING(bug)
LEFT JOIN
  `{{ ref('bugzilla_bugs') }}` AS bugs
ON
  bugs.number = bugs_features.bug
LEFT JOIN `{{ ref('web_features.features_latest') }}` AS web_features USING(feature)
LEFT JOIN use_counters USING(feature)
LEFT JOIN feature_support_dates USING(feature)
LEFT JOIN mozilla_standards_positions AS sp_mozilla ON sp_mozilla.number = bugs_features.bug
LEFT JOIN webkit_standards_positions AS sp_webkit ON sp_mozilla.number = sp_webkit.mozilla_sp
LEFT JOIN webcompat_bugs ON webcompat_bugs.number = bugs_features.bug
LEFT JOIN `{{ ref('bugzilla_components_ownership') }}` as component_owners ON component_owners.bugzilla_product = bugs.product AND component_owners.bugzilla_component = bugs.component
ORDER BY bug ASC
