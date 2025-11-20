WITH platform_bugs as (
  SELECT number as core_bug,
  knowledge_base_bug,
  keywords,
  user_story,
  resolution,
  team,
  size_estimate,
  SAFE.PARSE_DATE("%F", JSON_VALUE(user_story, "$.platform-scheduled")) as scheduled_date,
  FROM `{{ ref('bugzilla_bugs') }}` as platform_bugs
  JOIN `{{ ref('core_bugs_all') }}` as core_bugs_all ON platform_bugs.number = core_bugs_all.core_bug
  LEFT JOIN `{{ ref ('bugzilla_components_ownership') }}` AS components ON (components.bugzilla_product = product AND components.bugzilla_component = component)
)

select *,
CASE
  WHEN resolution != "" THEN "resolved"
  WHEN `{{ ref('WEBCOMPAT_BLOCKED_REASON') }}`(keywords, user_story) IS NOT NULL THEN "blocked"
  WHEN (size_estimate = "" or size_estimate is null) AND scheduled_date is null THEN "unsized"
  WHEN scheduled_date IS NULL AND size_estimate IN UNNEST(["M", "L", "XL"]) THEN "unscheduled"
  WHEN scheduled_date > DATE_ADD(CURRENT_DATE(), INTERVAL 6 WEEK) AND size_estimate IN UNNEST(["M", "L", "XL"]) THEN "future"
  ELSE "ready"
END as state
FROM platform_bugs
