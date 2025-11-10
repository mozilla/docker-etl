/* DEPRECATED: Prefer using scored_site_reports directly */
  SELECT DISTINCT
    site_reports.number as number,
    site_reports.title,
    site_reports.status,
    site_reports.resolution,
    site_reports.product,
    site_reports.component,
    site_reports.severity,
    site_reports.priority,
    site_reports.creation_time,
    site_reports.resolved_time,
    site_reports.assigned_to,
    site_reports.url,
    site_reports.whiteboard,
    site_reports.team,
    "webcompat:needs-diagnosis" IN UNNEST(site_reports.keywords) as type_needs_diagnosis,
    "webcompat:platform-bug" IN UNNEST(site_reports.keywords) as type_platform_bug,
    "blocked" = JSON_VALUE(site_reports.user_story, "$.impact") as firefox_not_supported,
    site_reports.blocked,
    IFNULL(site_reports.triage_score, site_reports.severity_score) as score
  FROM
    `{{ ref('scored_site_reports') }}` AS site_reports
  WHERE
    site_reports.is_sightline
