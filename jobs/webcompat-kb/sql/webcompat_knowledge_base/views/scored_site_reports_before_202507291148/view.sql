WITH
  host_categories AS (
  SELECT
    `{{ ref('WEBCOMPAT_HOST') }}`(host) as webcompat_host,
    MIN(sightline_rank) <= 1000 OR MIN(global_rank) <= 1000 as is_sightline,
    MIN(global_rank) <= 1000 as is_global_1000,
    MIN(japan_rank) <= 1000 as is_japan_1000
  FROM
    `{{ ref('crux_imported.host_min_ranks') }}`
  WHERE
    yyyymm = `{{ ref('WEBCOMPAT_METRIC_YYYYMM_before_202507291148') }}`()
  GROUP BY
    webcompat_host),
  /* Individual score components for each bug.

  These should always match the logic in `{{ ref('WEBCOMPAT_METRIC_SCORE_NO_SITE_RANK') }}`
  such that multiplying all the columns except severity score is equivalent to running that function.*/

  scores AS (
  SELECT
    number,
    SUM(
    IF
      (weights.lookup_type = "severity"
        AND weights.lookup_value = CAST(site_reports.severity AS string), weights.score, 0)) AS severity_score,
    SUM(
    IF
      (weights.lookup_type = "impact"
        AND weights.lookup_value = JSON_VALUE(site_reports.user_story, "$.impact"), weights.score, 0)) AS impact_score,
    SUM(
    IF
      (weights.lookup_type = "platform"
        AND weights.lookup_value IN UNNEST(SPLIT(JSON_VALUE(site_reports.user_story, "$.platform"))), weights.score, 0)) AS platform_score,
    SUM(
    IF
      (weights.lookup_type = "configuration"
        AND weights.lookup_value = IFNULL(JSON_VALUE(site_reports.user_story, "$.configuration"), "general"), weights.score, 0)) AS configuration_score,
    SUM(
    IF
      (weights.lookup_type = "users_affected"
        AND weights.lookup_value = IFNULL(JSON_VALUE(site_reports.user_story, "$.affects"), "all"), weights.score, 0)) AS affects_score,
    SUM(
    IF
      (weights.lookup_type = "patch_applied"
        AND weights.lookup_value =
      IF
        ("webcompat:sitepatch-applied" IN UNNEST(site_reports.keywords),
        IF
          ("webcompat:platform-bug" IN UNNEST(site_reports.keywords), "platform-bug", "site-bug"), "none"), weights.score, 0)) AS intervention_score,
    SUM(
    IF
      (weights.lookup_type = "branch"
        AND weights.lookup_value = IFNULL(JSON_VALUE(site_reports.user_story, "$.branch"), "release"), weights.score, 0)) AS branch_score,
  FROM
    `{{ ref('site_reports') }}` AS site_reports
  CROSS JOIN
    `{{ ref('dim_bug_score') }}` AS weights
  GROUP BY
    number),
  /* Computed scores for each bug

  These could be inlined, but it's slightly easier to read if they're computed in one place
  */ computed_scores AS (
  SELECT
    number,
    `{{ ref('WEBCOMPAT_METRIC_SCORE_NO_SITE_RANK') }}`(keywords,
      user_story) AS triage_score_no_rank,
    `{{ ref('WEBCOMPAT_METRIC_SCORE_SITE_RANK_MODIFER') }}`(url,
      `{{ ref('WEBCOMPAT_METRIC_YYYYMM_before_202507291148') }}`()) AS site_rank_score
  FROM
    `{{ ref('site_reports') }}` AS site_reports),
  site_report_scores AS (
  SELECT
    site_reports.*,
    severity_score,
    impact_score,
    platform_score,
    configuration_score,
    affects_score,
    intervention_score,
    site_rank_score,
    branch_score,
    triage_score_no_rank * site_rank_score AS triage_score,
    IFNULL(host_categories.is_global_1000, FALSE) AS is_global_1000,
    IFNULL(host_categories.is_sightline, FALSE) AS is_sightline,
    IFNULL(host_categories.is_japan_1000, FALSE) AS is_japan_1000,
  FROM
    `{{ ref('site_reports') }}` AS site_reports
  JOIN
    scores
  USING
    (number)
  JOIN
    computed_scores
  USING
    (number)
  LEFT JOIN
    host_categories
  ON
    host_categories.webcompat_host = `{{ ref('WEBCOMPAT_HOST') }}`(site_reports.url))

SELECT
  site_report_scores.*,
  -- This used to fall back to severity score
  site_report_scores.triage_score AS score
FROM
  site_report_scores
