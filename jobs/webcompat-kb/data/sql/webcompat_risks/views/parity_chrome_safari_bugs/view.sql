SELECT
  bugs.number,
  bugs.title,
  bugs.status,
  bugs.resolution,
  bugs.product,
  bugs.component,
  bugs.severity,
  bugs.priority,
  bugs.creation_time,
  bugs.assigned_to,
  bugs.keywords,
  bugs.url,
  bugs.user_story,
  risks.Risk_Priority AS risk_priority,
  risks.Standards_Position AS standards_position,
  risks.interop_risk AS interop_risk,
  risks.chrome_counters AS chrome_counters,
  risks.Github_usage_query AS github_query,
  risks.Likely_Dev_Frustration_When_Encountered AS likely_dev_frustration,
  risks.Has_Fallbacks__Polyfills__Workarounds_ AS polyfillable,
  risks.Likely_Greatest_User_Impact likely_user_impact
FROM
  `{{ ref('chrome_safari_parity') }}` AS risks
JOIN
  `{{ ref('webcompat_knowledge_base.bugzilla_bugs') }}` AS bugs
ON
  bugs.number = risks.Bugzilla_Bug_Number
WHERE
  risk_priority IS NOT NULL
ORDER BY
  risk_priority asc
