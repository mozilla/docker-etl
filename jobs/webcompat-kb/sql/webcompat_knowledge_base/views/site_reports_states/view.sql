WITH core_bug_states AS (
  SELECT breakage_reports.breakage_bug as number,
  ARRAY_AGG(DISTINCT core_bugs.state) as states,
  FROM `{{ ref('breakage_reports_core_bugs') }}` as breakage_reports
  JOIN `{{ ref('core_bug_states') }}` as core_bugs on breakage_reports.core_bug = core_bugs.core_bug
  GROUP BY breakage_reports.breakage_bug
),

outreach_keyword_times as (
  SELECT number,
  MAX(CASE
      WHEN EXISTS (
            SELECT 1
            FROM UNNEST(SPLIT(changes.added, ',')) AS keyword
            WHERE TRIM(keyword) = 'webcompat:contact-in-progress'
      )
        THEN change_time
        END) as contact_in_progress_added,
    MAX(CASE
      WHEN EXISTS (
            SELECT 1
            FROM UNNEST(SPLIT(changes.added, ',')) AS keyword
            WHERE TRIM(keyword) = 'webcompat:contact-complete'
      )
        THEN change_time
        END) as contact_complete_added
  FROM `{{ ref('bugs_history') }}`
  JOIN UNNEST(changes) as changes
  GROUP BY number
),

-- TODO: handle multiple outreach dates
outreach_user_story_times as (
  SELECT number,
  PARSE_DATE("%F", JSON_VALUE(user_story, "$.outreach-contact-date")) as contact_date,
  PARSE_DATE("%F", JSON_VALUE(user_story, "$.outreach-response-date")) as response_date,
  IF(bugs.webcompat_priority = "P1", INTERVAL 7 DAY, INTERVAL 14 DAY) as outreach_wait_interval
  FROM `{{ ref('site_reports') }}` as bugs
)

SELECT
bugs.number,
bugs.webcompat_priority,
`{{ ref('WEBCOMPAT_BLOCKED_REASON') }}`(bugs.keywords, bugs.user_story) IS NOT NULL as blocked,
`{{ ref('WEBCOMPAT_BLOCKED_REASON') }}`(bugs.keywords, bugs.user_story) as blocked_reason,
bugs.webcompat_priority is NULL OR bugs.webcompat_priority IN UNNEST(["?", "revisit"]) OR (bugs.product = "Web Compatibility" AND bugs.severity IS NULL) as needs_triage,
"webcompat:needs-diagnosis" IN UNNEST(bugs.keywords) as needs_diagnosis,
CASE
  WHEN "webcompat:needs-sitepatch" IN UNNEST(bugs.keywords) THEN "needed"
  WHEN "webcompat:sitepatch-ready" IN UNNEST(bugs.keywords) THEN "ready"
  WHEN "webcompat:sitepatch-applied" IN UNNEST(bugs.keywords) THEN "complete"
  ELSE NULL
END as intervention_state,
CASE
  WHEN "webcompat:needs-contact" IN UNNEST(bugs.keywords) THEN "needs contact"
  WHEN "webcompat:contact-ready" IN UNNEST(bugs.keywords) THEN "contact ready"
  WHEN "webcompat:contact-complete" IN UNNEST(bugs.keywords) AND CURRENT_DATE() > DATE_ADD(DATE(outreach_keyword_times.contact_complete_added), INTERVAL 14 DAY) THEN "revisit"
  WHEN "webcompat:contact-complete" IN UNNEST(bugs.keywords) THEN "complete"
  WHEN "webcompat:contact-in-progress" IN UNNEST(bugs.keywords) AND CURRENT_DATE() > DATE_ADD(DATE(outreach_keyword_times.contact_in_progress_added), INTERVAL 14 DAY) OR (outreach_user_story_times.contact_date IS NOT NULL AND (outreach_user_story_times.response_date IS NULL OR outreach_user_story_times.contact_date > outreach_user_story_times.response_date)) AND CURRENT_DATE() > outreach_user_story_times.contact_date + outreach_user_story_times.outreach_wait_interval THEN "timed out"
  WHEN "webcompat:contact-in-progress" IN UNNEST(bugs.keywords) THEN "in progress"
  WHEN "webcompat:contact-complete" NOT in UNNEST(bugs.keywords) AND "webcompat:contact-in-progress" NOT in UNNEST(bugs.keywords) AND (outreach_user_story_times.contact_date IS NOT NULL OR outreach_keyword_times.contact_in_progress_added IS NOT NULL) THEN "failed"
  ELSE NULL
END as outreach_state,
CASE
  WHEN core_bug_states.states IS NULL THEN IF("webcompat:platform-bug" IN UNNEST(bugs.keywords), "missing", NULL)
  WHEN EXISTS (SELECT * FROM UNNEST(core_bug_states.states) AS state WHERE state = "blocked") THEN "blocked"
  WHEN EXISTS (SELECT * FROM UNNEST(core_bug_states.states) AS state WHERE state = "unsized") THEN "unsized"
  WHEN EXISTS (SELECT * FROM UNNEST(core_bug_states.states) AS state WHERE state = "unscheduled") THEN "unscheduled"
  WHEN NOT EXISTS (SELECT * FROM UNNEST(core_bug_states.states) AS state WHERE state != "resolved") THEN "resolved"
ELSE "ready"
END as platform_state
FROM `{{ ref('site_reports') }}` as bugs
LEFT JOIN core_bug_states USING(number)
LEFT JOIN outreach_keyword_times USING(number)
LEFT JOIN outreach_user_story_times USING(number)
ORDER BY number desc
