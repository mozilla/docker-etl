WITH platform as (
  SELECT number, TRIM(platform) as platform
  FROM
    {{ ref('bugzilla_bugs') }} AS bugs
  JOIN UNNEST(SPLIT(LOWER(IFNULL(JSON_VALUE(bugs.user_story, "$.platform"), "")), ",")) as platform
),
platforms as (
  SELECT number, ARRAY_AGG(platform.platform) as platforms
  FROM platform
  GROUP BY number
)

SELECT
  bugs.*,
  manager,
  team,
  TRIM(LOWER(IFNULL(IFNULL(JSON_VALUE(bugs.user_story, "$.diagnosis-team"), component_owners.team), "unknown"))) as assigned_team,
  "webcompat:sitepatch-applied" IN UNNEST(bugs.keywords) AS has_intervention,
  `{{ ref('WEBCOMPAT_BLOCKED_REASON') }}`(bugs.keywords, bugs.user_story) IS NOT NULL AS blocked,
  `{{ ref('WEBCOMPAT_BLOCKED_REASON') }}`(bugs.keywords, bugs.user_story) AS blocked_reason,
  --Affected Platforms
  platforms.platforms as platforms,
  "ios" IN UNNEST(platforms.platforms) OR "android" in UNNEST(platforms.platforms) as is_mobile,
  "windows" IN UNNEST(platforms.platforms) OR "mac" in UNNEST(platforms.platforms) OR "linux" in UNNEST(platforms.platforms) as is_desktop,
  --Categories for the metric
  "webcompat:needs-diagnosis" IN UNNEST(bugs.keywords) AS metric_type_needs_diagnosis,
  "webcompat:needs-diagnosis" NOT IN UNNEST(bugs.keywords) AND ("webcompat:platform-bug" IN UNNEST(bugs.keywords) OR
     EXISTS (SELECT * FROM `{{ ref('breakage_reports_core_bugs') }}` where breakage_bug = bugs.number)) AS metric_type_platform_bug,
  "blocked" = IFNULL(JSON_VALUE(bugs.user_story, "$.impact"), "") AS metric_type_firefox_not_supported,
FROM
  {{ ref('bugzilla_bugs') }} AS bugs
LEFT JOIN
  `{{ ref('bugzilla_components_ownership') }}` as component_owners ON (component_owners.bugzilla_product = product AND component_owners.bugzilla_component = component)
LEFT JOIN
  platforms USING(number)
WHERE
  ((bugs.product = "Web Compatibility" AND bugs.component = "Site Reports")
  OR (bugs.product != "Web Compatibility" AND "webcompat:site-report" IN UNNEST(bugs.keywords)))
