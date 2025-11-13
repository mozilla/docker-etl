WITH interop_bugs as (
  SELECT number, CAST(ARRAY_LAST(SPLIT(`{{ ref('URL_PARSE') }}`(see_also).path, "/")) as INTEGER) as issue, concat("https://bugzilla.mozilla.org/show_bug.cgi?id=", number) as link
  FROM {{ ref('bugzilla_bugs') }} as bugs
  JOIN UNNEST(bugs.see_also) as see_also
  WHERE starts_with(see_also, "https://github.com/web-platform-tests/interop/issues/")
),

web_features_bugs as (
  SELECT number, web_feature
  FROM (SELECT number, `{{ ref('EXTRACT_ARRAY') }}`(bugs.user_story, "$.web-feature") as web_features
  FROM {{ ref('bugzilla_bugs') }} as bugs)
  JOIN UNNEST(web_features) as web_feature
),

webcompat_scores AS (
  SELECT bugs.number AS number,
  SUM(scored_site_reports.score) AS webcompat_score
  FROM `{{ ref('bugs_platform_data') }}` AS bugs
  JOIN UNNEST(webcompat_bugs) as webcompat_bug
  JOIN `{{ ref('scored_site_reports') }}` AS scored_site_reports ON scored_site_reports.number = webcompat_bug.number
  GROUP BY number
),

user_story_data AS (
  SELECT bugs.number AS NUMBER,
  CASE UPPER(JSON_VALUE(site_reports.user_story, "$.a11y-size"))
    WHEN "XL" THEN "XL"
    WHEN "L" THEN "L"
    WHEN "M" THEN "M"
    WHEN "S" THEN "S"
    WHEN "XS" THEN "XS"
    ELSE NULL
  END AS a11y_size_estimate,
  CASE LOWER(JSON_VALUE(site_reports.user_story, "$.a11y-impact"))
    WHEN "positive" THEN "Positive"
    WHEN "negative" THEN "Negative"
    WHEN "none" THEN "None"
    ELSE NULL
  END AS a11y_impact,
  CASE LOWER(JSON_VALUE(site_reports.user_story, "$.performance-impact"))
    WHEN "high" THEN "High"
    WHEN "medium" THEN "Medium"
    WHEN "none" THEN "None"
    WHEN "negative" THEN "Negative"
    ELSE NULL
  END AS performance_impact,
  CASE LOWER(JSON_VALUE(site_reports.user_story, "$.privacy-impact"))
    WHEN "positive" THEN "Positive"
    WHEN "negative" THEN "Negative"
    WHEN "none" THEN "None"
    ELSE NULL
  END AS privacy_impact,
  FROM {{ ref('bugzilla_bugs') }} as bugs
)

SELECT
  issue,
  interop_proposals.title as title,
  proposal_type,
  state,
  concat("https://github.com/web-platform-tests/interop/issues/", issue) as link,
  ARRAY_TO_STRING(features, "\n") as features,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT interop_bugs.link IGNORE NULLS), "\n") as bug,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT team IGNORE NULLS), "\n") as team,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT resolution IGNORE NULLS), "\n") as resolution,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT CAST(scheduled_date AS STRING) IGNORE NULLS), "\n") as scheduled_date,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT platform_data.size_estimate IGNORE NULLS), "\n") as size_estimate,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT user_story_data.a11y_size_estimate IGNORE NULLS), "\n") as a11y_size_estimate,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT CAST(chrome_implemented AS STRING) IGNORE NULLS), "\n") as chrome_implemented,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT CAST(safari_implemented AS STRING) IGNORE NULLS), "\n") as safari_implemented,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT mozilla_standards_position IGNORE NULLS), "\n") AS standards_position,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT CONCAT("https://github.com/mozilla/standards-positions/issues/", mozilla_standards_position_issue) IGNORE NULLS), "\n") AS standards_position_link,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT user_story_data.a11y_impact IGNORE NULLS), "\n") as a11y_impact,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT user_story_data.performance_impact IGNORE NULLS), "\n") as performance_impact,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT user_story_data.privacy_impact IGNORE NULLS), "\n") as privacy_impact,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT webcompat_risk IGNORE NULLS), "\n") as webcompat_risk,
  ARRAY_TO_STRING(ARRAY_AGG(DISTINCT CAST(webcompat_scores.webcompat_score AS STRING) IGNORE NULLS), "\n") as webcompat_score
FROM `{{ ref('interop.interop_proposals') }}` AS interop_proposals
LEFT JOIN interop_bugs USING(issue)
LEFT JOIN `{{ ref('bugzilla_bugs') }}` USING(number)
LEFT JOIN `{{ ref('bugs_platform_data') }}` AS platform_data USING(number)
LEFT JOIN UNNEST(bugs) as bug
LEFT JOIN webcompat_scores AS webcompat_scores USING(number)
LEFT JOIN user_story_data AS user_story_data USING(number)
WHERE year = 2026
GROUP BY issue, title, proposal_type, state, features
ORDER BY proposal_type, issue
