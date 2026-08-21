WITH
hackbot_repro AS (
  SELECT
    PARSE_NUMERIC(JSON_VALUE(scheduled.extra_data, "$.bug_id")) AS number,
    DATETIME_DIFF(completed.completed_at, completed.created_at, SECOND) AS execution_time
  FROM `{{ ref('hackbot_scheduled') }}` scheduled
  JOIN `{{ ref('hackbot_completed') }}` completed USING (run_id)
  WHERE scheduled.task_name = 'repro'
  AND STARTS_WITH(scheduled.source_key, "bugzilla:")
)
SELECT
reports.number AS number,
DATE(reports.creation_time) AS creation_date,
CASE
  WHEN reports.whiteboard LIKE '%[webcompat-source:product]%' THEN 'product'
  WHEN reports.whiteboard LIKE '%[webcompat-source:web-bugs]%' THEN 'web-bugs'
  ELSE 'other'
END AS origin,
CASE
  WHEN JSON_VALUE(reports.user_story, '$.impact') IS NULL THEN FALSE
  ELSE TRUE
END AS triaged,
scored.score AS impact_score,
scored.webcompat_priority AS webcompat_priority,
CASE
  WHEN reports.whiteboard LIKE '%autowebcompat:processed%' THEN TRUE
  ELSE FALSE
END AS autowebcompat_processed,
JSON_VALUE(reports.user_story, "$.autowebcompat-repro-status") AS reproduced,
JSON_VALUE(reports.user_story, "$.autowebcompat-repro-reason") AS repro_failure_cause,
JSON_VALUE(reports.user_story, "$.autowebcompat-repro-chrome-mask-fixed") AS chrome_mask_fixed,
JSON_VALUE(reports.user_story, "$.autowebcompat-repro-channels") AS repro_channels,
CASE
  WHEN reports.whiteboard LIKE '%autowebcompat:interv-ua-override-proposed%'
    OR JSON_VALUE(reports.user_story, "$.autowebcompat-repro-chrome-mask-fixed") = "true"
  THEN TRUE
  ELSE FALSE
END AS ua_override_proposed,
hackbot_repro.execution_time AS repro_time
FROM `{{ ref('webcompat_knowledge_base.site_reports') }}` reports
LEFT JOIN `{{ ref('webcompat_knowledge_base.scored_site_reports') }}` scored USING (number)
INNER JOIN hackbot_repro USING (number)
ORDER BY reports.creation_time DESC;
