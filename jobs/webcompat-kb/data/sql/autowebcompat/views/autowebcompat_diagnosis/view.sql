WITH
requesters AS (
  SELECT distinct number, LEFT(who, STRPOS(who,"@")-1) as who
  FROM `{{ ref('webcompat_knowledge_base.bugs_history') }}` h,
  UNNEST (h.changes) as ch
  WHERE ch.field_name LIKE '%whiteboard%'
  AND ch.added LIKE'%autowebcompat:diagnose%'
),
requesters_list as (
  SELECT number, ARRAY_TO_STRING(ARRAY_AGG(who ORDER BY who), ", ")  as who
  FROM requesters
  GROUP BY number
),
hackbot_diagnosis AS (
  SELECT
  PARSE_NUMERIC(JSON_VALUE(scheduled.extra_data, "$.bug_id")) as number,
  DATE(completed.completed_at) as processing_completion_date,
  DATE(completed.created_at) as processing_start_date,
  DATETIME_DIFF(completed.completed_at, completed.created_at, SECOND) as execution_time
  FROM `{{ ref('hackbot_scheduled') }}` scheduled
  JOIN `{{ ref('hackbot_completed') }}` completed USING (run_id)
  WHERE scheduled.task_name = 'diagnosis'
  AND STARTS_WITH(scheduled.source_key, "bugzilla:")
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY PARSE_NUMERIC(scheduled.run_key)
    ORDER BY completed.completed_at DESC
  ) = 1
)
SELECT
reports.number AS number,
CASE
  WHEN reports.whiteboard LIKE '%[webcompat-source:product]%' THEN 'product'
  WHEN reports.whiteboard LIKE '%[webcompat-source:web-bugs]%' THEN 'web-bugs'
  ELSE 'other'
END AS origin,
requesters_list.who as requested_by,
DATE(reports.creation_time) AS creation_date,
hackbot_diagnosis.processing_start_date as processing_start_date,
CASE
  WHEN JSON_VALUE(reports.user_story, '$.impact') IS NULL THEN FALSE
  ELSE TRUE
END AS triaged,
scored.score AS impact_score,
scored.webcompat_priority AS webcompat_priority,
JSON_VALUE(reports.user_story, "$.autowebcompat-diagnosis-status") AS diagnosis_status,
JSON_VALUE(reports.user_story, "$.autowebcompat-diagnosis-reason") AS diagnosis_failure_reason,
JSON_VALUE(reports.user_story, '$.diagnosis-team') as diagnosis_team,
COALESCE(next_action.next_action,'unknown') AS next_action,
hackbot_diagnosis.execution_time AS diagnosis_time
FROM `{{ ref('webcompat_knowledge_base.site_reports') }}` reports
LEFT JOIN `{{ ref('webcompat_knowledge_base.scored_site_reports') }}` scored USING (number)
JOIN hackbot_diagnosis USING (number)
JOIN `{{ ref('webcompat_knowledge_base.site_reports_next_action') }}` next_action USING(number)
LEFT JOIN requesters_list USING(number)
ORDER BY impact_score DESC;