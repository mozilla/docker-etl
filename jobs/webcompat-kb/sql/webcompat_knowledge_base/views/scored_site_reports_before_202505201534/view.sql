WITH
  /*
   * CrUX month to use in yyyymm format. Set to NULL to use the latest available data.
   */ crux_override AS (
  SELECT
    202409),
  /*
  * Lookup table for per-platform score weighting
  */ platform_scores AS (
  SELECT
    b.number,
    SUM(d.score) AS platform_score
  FROM
    `{{ ref('bugzilla_bugs') }}` b
  JOIN
    UNNEST(SPLIT(JSON_VALUE(b.user_story, "$.platform"))) AS pl
  JOIN
    `{{ ref('dim_bug_score') }}` d
  ON
    d.lookup_value = pl
  WHERE
    d.lookup_type = 'platform'
  GROUP BY
    b.number ),
  /*
  * Lookup table for impact category score weighting
  */ impact_scores AS (
  SELECT
    lookup_value AS impact,
    score
  FROM
    `{{ ref('dim_bug_score') }}`
  WHERE
    lookup_type = 'impact' ),
  /*
  * Lookup table for impact category score weighting
  */ configuration_scores AS (
  SELECT
    lookup_value AS configuration,
    score
  FROM
    `{{ ref('dim_bug_score') }}`
  WHERE
    lookup_type = 'configuration' ),
  /*
  * Lookup table for users affected category score weighting
  */ affects_scores AS (
  SELECT
    lookup_value AS affects,
    score
  FROM
    `{{ ref('dim_bug_score') }}`
  WHERE
    lookup_type = 'users_affected' ),
  /*
  * Lookup table for severity score weighting
  */ severity_scores AS (
  SELECT
    CAST(lookup_value AS int64) AS severity,
    score
  FROM
    `{{ ref('dim_bug_score') }}`
  WHERE
    lookup_type = 'severity' ),
  /*
  * Lookup table for site rank score weighting
  */ site_rank_scores AS (
  SELECT
    CAST(lookup_value AS int64) AS site_rank,
    score
  FROM
    `{{ ref('dim_bug_score') }}`
  WHERE
    lookup_type = 'site_rank' ),
  /*
  * Lookup table for site rank score weighting
  */ intervention_scores AS (
  SELECT
    lookup_value AS patch_applied,
    score
  FROM
    `{{ ref('dim_bug_score') }}`
  WHERE
    lookup_type = 'patch_applied' ),
  crux_latest AS (
  SELECT
    IFNULL((
      SELECT
        *
      FROM
        crux_override
      LIMIT
        1),(
      SELECT
        yyyymm
      FROM
        `{{ ref('crux_imported.import_runs') }}`
      ORDER BY
        yyyymm DESC
      LIMIT
        1))),
  /*
  * Score multipliers for each host.
  *
  * For hosts where the global rank is equal to or greater than the local rank, we increase the score multiplier by 50%.
  *
  * This is also a convenient place to calculate whether a site is in the sightline set (sightline rank is <= 1000)
  */ host_scores AS (
  SELECT
    `{{ ref('WEBCOMPAT_HOST') }}`(host) AS host,
  IF
    (MIN(host_ranks.global_rank) <= MIN(host_ranks.local_rank), MAX(1.5 * global_scores.score), MAX(local_scores.score)) AS score,
  IF
    (MIN(host_ranks.global_rank) <= 1000, TRUE, FALSE) AS is_global_1000,
  IF
    (MIN(sightline_rank) <= 1000, TRUE, FALSE) AS is_sightline
  FROM
    `{{ ref('crux_imported.host_min_ranks') }}` AS host_ranks
  JOIN
    `{{ ref('site_reports') }}` AS site_reports
  ON
    `{{ ref('WEBCOMPAT_HOST') }}`(host_ranks.host) = `{{ ref('WEBCOMPAT_HOST') }}`(site_reports.url)
  LEFT JOIN
    site_rank_scores AS local_scores
  ON
    host_ranks.local_rank <= local_scores.site_rank
  LEFT JOIN
    site_rank_scores AS global_scores
  ON
    host_ranks.global_rank <= global_scores.site_rank
  WHERE
    host_ranks.yyyymm = (
    SELECT
      yyyymm
    FROM
      crux_latest)
  GROUP BY
    `{{ ref('WEBCOMPAT_HOST') }}`(host)),
  /*
  * Individual site reports that are linked to a knowledge base entry,
  * along with the computed impact scores for each site report.
  */ scored_site_reports AS (
  SELECT
    site_reports.*,
    COALESCE(ss.score,0) AS severity_score,
    -- This is an attempt to backfill cases where we don't have triage decisions recorded in the user_story field
    ps.platform_score,
    ims.score AS impact_score,
    IFNULL(cf.score, 1) AS configuration_score,
    af.score AS affects_score,
    IFNULL(host_scores.score, 1) AS site_rank_score,
    ins.score AS intervention_score,
    "webcompat:sitepatch-applied" IN UNNEST(site_reports.keywords) as has_intervention,
    `{{ ref('WEBCOMPAT_BLOCKED_REASON') }}`(site_reports.keywords, site_reports.user_story) is not null as blocked,
    `{{ ref('WEBCOMPAT_BLOCKED_REASON') }}`(site_reports.keywords, site_reports.user_story) as blocked_reason,
    IFNULL(host_scores.is_global_1000, FALSE) AS is_global_1000,
    IFNULL(host_scores.is_sightline, FALSE) AS is_sightline,
    /* Categories for the metric */
    "webcompat:needs-diagnosis" IN UNNEST(site_reports.keywords) as metric_type_needs_diagnosis,
    "webcompat:platform-bug" IN UNNEST(site_reports.keywords) as metric_type_platform_bug,
    "blocked" = ifNULL(JSON_VALUE(site_reports.user_story, "$.impact"), "") as metric_type_firefox_not_supported,
  FROM
    `{{ ref('site_reports') }}` AS site_reports
  LEFT JOIN
    host_scores AS host_scores
  ON
    host_scores.host = `{{ ref('WEBCOMPAT_HOST') }}`(site_reports.url)
  LEFT JOIN
    configuration_scores cf
  ON
    cf.configuration = IFNULL(JSON_VALUE(site_reports.user_story, "$.configuration"), "general")
  LEFT JOIN
    affects_scores af
  ON
    af.affects = IFNULL(JSON_VALUE(site_reports.user_story, "$.affects"), "all")
  LEFT JOIN
    severity_scores ss
  ON
    ss.severity = site_reports.severity
  LEFT JOIN
    intervention_scores ins
  ON
    ins.patch_applied =
    CASE
      WHEN "webcompat:sitepatch-applied" IN UNNEST(site_reports.keywords) THEN "1"
      ELSE "0"
  END
  LEFT JOIN
    impact_scores ims
  ON
    ims.impact = JSON_VALUE(site_reports.user_story, "$.impact")
  LEFT JOIN
    platform_scores ps
  ON
    ps.number = site_reports.number),
  with_triage_score AS (
  SELECT
    scored_site_reports.*,
    scored_site_reports.platform_score * scored_site_reports.impact_score * scored_site_reports.configuration_score * scored_site_reports.affects_score * scored_site_reports.site_rank_score * scored_site_reports.intervention_score AS triage_score,
  FROM
    scored_site_reports)
SELECT
  with_triage_score.*,
  IFNULL(with_triage_score.triage_score, with_triage_score.severity_score) AS score
FROM
  with_triage_score
