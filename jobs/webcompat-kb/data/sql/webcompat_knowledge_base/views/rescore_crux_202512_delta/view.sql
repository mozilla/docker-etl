
with scores as (
  SELECT
    number,
    old_scored_site_reports.score AS old_score,
    new_scored_site_reports.score AS new_score,
    old_scored_site_reports.is_global_1000 AS is_global_1000_old,
    new_scored_site_reports.is_global_1000 AS is_global_1000_new,
    old_scored_site_reports.is_sightline AS is_sightline_old,
    new_scored_site_reports.is_sightline AS is_sightline_new,
    old_scored_site_reports.is_japan_1000 AS is_japan_1000_old,
    new_scored_site_reports.is_japan_1000 AS is_japan_1000_new,
    old_scored_site_reports.is_japan_1000_mobile AS is_japan_1000_mobile_old,
    new_scored_site_reports.is_japan_1000_mobile AS is_japan_1000_mobile_new
  FROM `{{ ref('rescore_crux_202512_scored_site_reports') }}` as new_scored_site_reports
  FULL OUTER JOIN `{{ ref('scored_site_reports') }}` AS old_scored_site_reports USING(number)
  WHERE new_scored_site_reports.resolution = ""
)
SELECT
  number,
  old_score,
  new_score,
  is_global_1000_old,
  is_global_1000_new,
  is_sightline_old,
  is_sightline_new,
  is_japan_1000_old,
  is_japan_1000_new,
  is_japan_1000_mobile_old,
  is_japan_1000_mobile_new,
  new_score - old_score AS delta
FROM scores
