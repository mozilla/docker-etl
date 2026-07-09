WITH
    hackbot_repro AS (
        SELECT 
            PARSE_NUMERIC(scheduled.run_key) as number,
            DATETIME_DIFF(completed.completed_at, completed.created_at, SECOND) as execution_time
        FROM `{{ ref('hackbot_scheduled') }}` scheduled
        JOIN `{{ ref('hackbot_completed') }}` completed USING (run_id)
        WHERE scheduled.task_name = 'repro'
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
    scored.impact_score AS impact_score,
    scored.webcompat_priority AS webcompat_priority,
    CASE 
        WHEN reports.whiteboard like '%autowebcompat:processed%' THEN TRUE
        ELSE FALSE
    END AS autowebcompat_processed,
    CASE 
        WHEN reports.whiteboard like '%autowebcompat:repro-success%' THEN TRUE
        WHEN reports.whiteboard like '%autowebcompat:repro-failed%' THEN FALSE
        ELSE NULL
    END AS autowebcompat_reproduced,
    hackbot_repro.execution_time AS repro_time,
    CASE 
        WHEN reports.whiteboard like '%autowebcompat:interv-ua-override-proposed%' THEN TRUE
        ELSE FALSE
    END AS interv_ua_override_proposed
FROM `{{ ref('webcompat_knowledge_base.site_reports') }}` reports
    LEFT JOIN `{{ ref('webcompat_knowledge_base.scored_site_reports') }}` scored USING (number)
    LEFT JOIN hackbot_repro USING (number)
ORDER BY reports.creation_time DESC;

