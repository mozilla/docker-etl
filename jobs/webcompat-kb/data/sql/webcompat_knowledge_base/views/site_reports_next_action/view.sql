WITH
core_bug_teams AS (
  SELECT breakage_reports.breakage_bug as number,
  ARRAY_AGG(DISTINCT IF(core_bug_states.state = "ready", owners.team, NULL) IGNORE NULLS) as ready_teams,
  ARRAY_AGG(DISTINCT IF(core_bug_states.state = "unsized", owners.team, NULL) IGNORE NULLS) as unsized_teams,
  ARRAY_AGG(DISTINCT IF(core_bug_states.state = "unscheduled", owners.team, NULL) IGNORE NULLS) as unscheduled_teams
  FROM `{{ ref('breakage_reports_core_bugs') }}` as breakage_reports
  JOIN `{{ ref('core_bug_states') }}` as core_bug_states on breakage_reports.core_bug = core_bug_states.core_bug
  JOIN `{{ ref('bugzilla_bugs') }}` as core_bugs ON core_bug_states.core_bug = core_bugs.number
  JOIN `{{ ref('bugzilla_components_ownership') }}` as owners ON (owners.bugzilla_product = core_bugs.product AND owners.bugzilla_component = core_bugs.component)
  GROUP BY breakage_reports.breakage_bug
),

platform_action as (
SELECT
number,
CASE
  WHEN platform_state IS NULL THEN "blocked (no-action)"
  WHEN platform_state = "missing" THEN "needs diagnosis"
  WHEN platform_state = "ready" THEN "platform fix"
  WHEN platform_state = "resolved" THEN "qa retriage"
  WHEN platform_state = "unsized" THEN "platform triage"
  WHEN platform_state = "unscheduled" THEN "platform schedule"
  WHEN platform_state = "blocked" THEN "blocked (platform)"
  ELSE CONCAT("ERROR: Unknown platform_state: ", platform_state)
END as action
FROM `{{ ref('site_reports_states') }}`
),

next_action as (SELECT number,
CASE
WHEN needs_triage THEN "triage"
WHEN site_reports_states.blocked THEN "blocked (explicit)"
WHEN needs_diagnosis THEN "needs diagnosis"
WHEN intervention_state = "complete" AND (outreach_state IS NULL OR outreach_state = "complete") AND platform_state IS NULL THEN "wait (intervention-patched)"
WHEN outreach_state = "in progress" THEN
  CASE
    WHEN platform_state = "unsized" THEN "platform triage"
    WHEN platform_state = "ready" THEN "platform fix"
    WHEN site_reports_states.webcompat_priority = "P1" AND intervention_state = "needed" THEN "prepare intervention"
    ELSE "wait (outreach-in-progress)"
  END
WHEN site_reports_states.webcompat_priority = "P1" THEN
  CASE
    WHEN outreach_state IS NULL OR outreach_state = "failed" THEN
      CASE
        WHEN intervention_state IS NULL or intervention_state = "complete" THEN
          CASE
            WHEN outreach_state = "failed" AND platform_state IS NULL THEN "blocked (site)"
            ELSE platform_action.action
          END
        WHEN intervention_state = "needed" THEN "prepare intervention"
        WHEN intervention_state = "ready" THEN "ship intervention"
        ELSE CONCAT("ERROR: Unknown intervention_state: ", intervention_state)
      END
    WHEN outreach_state = "contact ready" OR outreach_state = "timed out" OR outreach_state = "needs contact" THEN "outreach"
    WHEN outreach_state = "complete" THEN "wait"
    WHEN outreach_state = "revisit" THEN "qa retriage"
    ELSE CONCAT("ERROR, Unknown outreach_state: ", outreach_state)
  END
WHEN site_reports_states.webcompat_priority = "P2" THEN
  CASE
    WHEN outreach_state = "contact ready" OR outreach_state = "timed out" OR (outreach_state = "needs contact" AND site_reports.is_japan_1000) THEN
      CASE
        WHEN platform_state IS NULL OR platform_state = "future" OR platform_state = "blocked" THEN "outreach"
        ELSE platform_action.action
      END
    WHEN outreach_state IS NULL OR outreach_state = "needs contact" OR outreach_state = "failed" THEN
      CASE
        WHEN intervention_state = "needed" THEN
          CASE
            WHEN platform_state IS NULL OR platform_state = "future" OR platform_state = "blocked" THEN "prepare intervention"
            ELSE platform_action.action
          END
        WHEN intervention_state = "ready" THEN "ship intervention"
        ELSE platform_action.action
      END
  END
WHEN site_reports_states.webcompat_priority = "P3" THEN
  CASE
    WHEN platform_state IS NULL OR platform_state = "blocked" THEN
      CASE
        WHEN intervention_state = "needed" THEN "prepare intervention"
        WHEN intervention_state = "ready" THEN "ship intervention"
        WHEN platform_state = "blocked" THEN platform_action.action
        ELSE "blocked (low-priority)"
      END
    WHEN platform_state = "resolved" THEN platform_action.action
    ELSE "blocked (low-priority)"
  END
ELSE CONCAT("ERROR: Unknown webcompat_priority: ", site_reports_states.webcompat_priority)
END as next_action

FROM `{{ ref('site_reports_states') }}` as site_reports_states
JOIN platform_action USING(number)
JOIN `{{ ref('scored_site_reports') }}` as site_reports USING(number))

SELECT
number,
next_action.next_action,
CASE
  WHEN next_action.next_action = "triage" THEN ["webcompat"]
  WHEN next_action.next_action = "qa retriage" THEN ["qa"]
  WHEN next_action.next_action = "blocked (explicit)" OR next_action.next_action = "blocked (platform)" THEN ["platform-leadership"]
  WHEN next_action.next_action = "wait (intervention-patched)" THEN [""]
  WHEN next_action.next_action = "blocked (site)" THEN [""]
  WHEN next_action.next_action = "wait" THEN [""]
  WHEN next_action.next_action = "outreach" AND site_reports.is_japan_1000 AND not site_reports.is_global_1000 then ["japan"]
  WHEN next_action.next_action = "platform fix" THEN core_bug_teams.ready_teams
  WHEN next_action.next_action = "platform triage" THEN core_bug_teams.unsized_teams
  WHEN next_action.next_action = "platform schedule" THEN core_bug_teams.unscheduled_teams
  ELSE [ifnull(site_reports.assigned_team, "unknown")]
END AS next_action_teams,
FROM next_action
JOIN `{{ ref('scored_site_reports') }}` as site_reports USING(number)
LEFT JOIN core_bug_teams USING(number)
