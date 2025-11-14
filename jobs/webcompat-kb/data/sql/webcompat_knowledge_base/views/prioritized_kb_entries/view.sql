WITH
  kb_reports AS (
  SELECT
    bugs.number AS kb_id,
    site_reports.number AS site_report_id,
    site_reports.title AS site_report_title,
    site_reports.url AS site_report_url,
    site_reports.severity AS site_report_severity,
    site_reports.priority AS site_report_priority,
    site_reports.severity_score,
    site_reports.platform_score,
    site_reports.impact_score,
    site_reports.affects_score,
    site_reports.site_rank_score,
    site_reports.intervention_score,
    site_reports.triage_score,
  FROM
    `{{ ref('bugzilla_bugs') }}` AS bugs
  JOIN
    `{{ ref('kb_bugs') }}` AS kb_bugs
  ON
    bugs.number = kb_bugs.number
  LEFT OUTER JOIN
    `{{ ref('breakage_reports') }}` AS site_reports_link
  ON
    site_reports_link.knowledge_base_bug = bugs.number
  LEFT OUTER JOIN
    `{{ ref('scored_site_reports') }}` AS site_reports
  ON
    site_reports.number = site_reports_link.breakage_bug
  WHERE
    site_reports.resolution = ""
    OR site_reports.resolution IS NULL),

  /*
   * Knowledge base entries, and the summed scores across all of their site reports.
   */
  kb_scores AS (
  SELECT
    kb_reports.kb_id AS bug,
    SUM(kb_reports.severity_score) AS severity_score,
    SUM(kb_reports.triage_score) AS user_story_score,
    SUM(IFNULL(kb_reports.triage_score, kb_reports.severity_score)) AS score,
    MIN(kb_reports.site_report_priority) AS priority,
    ARRAY_AGG(JSON_OBJECT("bug", kb_reports.site_report_id, "url", kb_reports.site_report_url, "priority", kb_reports.site_report_priority, "platform_score", kb_reports.platform_score, "impact_score", kb_reports.impact_score, "affects_score", kb_reports.affects_score, "site_rank_score", kb_reports.site_rank_score, "intervention_score", kb_reports.intervention_score, "triage_score", kb_reports.triage_score)) AS reports
  FROM
    kb_reports
  GROUP BY
    kb_reports.kb_id)

/*
 * Knowledge base entries, sorted by impact score, and the corresponding core bugs (if any).
 */
SELECT
  kb_entries.number AS kb_bug,
  kb_entries.title AS kb_bug_title,
  kb_scores.reports AS site_issue_reports,
  kb_scores.severity_score AS severity_score,
  kb_scores.user_story_score AS user_story_score,
  kb_scores.priority AS higest_priority,
  IFNULL(kb_scores.score, 0) AS score,
  kb_entries.resolution AS resolution,
  kb_entries.creation_time AS creation_time,
  kb_entries.resolved_time AS resolved_time,
  `{{ ref('WEBCOMPAT_BLOCKED_REASON') }}`(kb_entries.keywords, kb_entries.user_story) IS NOT NULL AS blocked,
  `{{ ref('WEBCOMPAT_BLOCKED_REASON') }}`(kb_entries.keywords, kb_entries.user_story) AS blocked_reason,
  core_bugs.number AS core_bug,
  core_bugs.title AS core_bug_title,
  core_bugs.product AS core_bug_product,
  core_bugs.component AS core_bug_component,
  core_bugs.priority AS core_bug_priority,
  core_bugs.severity AS core_bug_severity,
  core_bugs.resolution AS core_bug_resolution,
  core_bugs.creation_time AS core_bug_creation_time,
  core_bugs.resolved_time AS core_bug_resolved_time,
  `{{ ref('WEBCOMPAT_BLOCKED_REASON') }}`(core_bugs.keywords, core_bugs.user_story) IS NOT NULL AS core_bug_blocked,
  `{{ ref('WEBCOMPAT_BLOCKED_REASON') }}`(core_bugs.keywords, core_bugs.user_story) AS core_bug_blocked_reason,
  core_bugs.user_story,
FROM
  kb_scores
JOIN
  `{{ ref('bugzilla_bugs') }}` AS kb_entries
ON
  kb_entries.number = kb_scores.bug
LEFT OUTER JOIN
  `{{ ref('core_bugs_all') }}` AS core_bugs_link
ON
  core_bugs_link.knowledge_base_bug = kb_entries.number
LEFT OUTER JOIN
  `{{ ref('bugzilla_bugs') }}` AS core_bugs
ON
  core_bugs.number = core_bugs_link.core_bug
ORDER BY
  score DESC,
  kb_entries.priority asc
