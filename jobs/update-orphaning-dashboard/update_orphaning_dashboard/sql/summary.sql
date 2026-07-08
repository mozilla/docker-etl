-- Top-level version counts for the Out Of Date dashboard report.
--
-- Mirrors the legacy summary_sql: the same population as out_of_date_details
-- (release channel, report-week subsession, valid release version), but
-- WITHOUT the major-version filter, since the whole point is to bucket clients
-- by how their version compares to the latest. Returns a single row.
--
-- Parameters:
--   @date_from           inclusive lower bound on DATE(submission_timestamp)
--   @date_to             inclusive upper bound on DATE(submission_timestamp)
--   @min_report_date     report-week start (subsession_start_date >= this)
--   @max_report_date     report-week end   (subsession_start_date <  this)
--   @channel             update channel to keep (e.g. 'release')
--   @min_version         minimum supported major version
--   @up_to_date_low      latest_version - up_to_date_releases
--   @up_to_date_high     latest_version + 2

WITH main_sample AS (
  SELECT
    client_id,
    CAST(environment.build.version AS STRING) AS version,
    mozfun.norm.truncate_version(environment.build.version, 'major') AS major_version,
    environment.build.application_name AS application_name,
    environment.settings.update.channel AS channel,
    payload.info.profile_subsession_counter AS profile_subsession_counter,
    payload.info.subsession_start_date AS subsession_start_date
  FROM
    `moz-fx-data-shared-prod.telemetry.main`
  WHERE
    sample_id = 42
    AND environment.build.version IS NOT NULL
    AND DATE(submission_timestamp) >= @date_from
    AND DATE(submission_timestamp) <= @date_to
),

longitudinal AS (
  SELECT
    client_id,
    ARRAY_AGG(version ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000) AS version,
    ARRAY_AGG(major_version ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000) AS major_version,
    ARRAY_AGG(application_name ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000) AS application_name,
    ARRAY_AGG(channel ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000) AS channel,
    ARRAY_AGG(subsession_start_date IGNORE NULLS ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000) AS subsession_start_date
  FROM
    main_sample
  GROUP BY
    client_id
)

SELECT
  COUNTIF(major_version[OFFSET(0)] >= @up_to_date_low AND major_version[OFFSET(0)] < @up_to_date_high) AS versionUpToDate,
  COUNTIF(major_version[OFFSET(0)] < @up_to_date_low AND major_version[OFFSET(0)] >= @min_version) AS versionOutOfDate,
  COUNTIF(major_version[OFFSET(0)] < @min_version) AS versionTooLow,
  COUNTIF(major_version[OFFSET(0)] > @up_to_date_high) AS versionTooHigh,
  COUNTIF(NOT major_version[OFFSET(0)] > 0) AS versionMissing
FROM
  longitudinal
WHERE
  application_name[OFFSET(0)] = 'Firefox'
  AND channel[OFFSET(0)] = @channel
  AND (REGEXP_CONTAINS(version[OFFSET(0)], r'^[0-9]{2,3}\.0[\.0-9]*$') OR version[OFFSET(0)] = '50.1.0')
  AND DATE_DIFF(DATE(SUBSTR(subsession_start_date[OFFSET(0)], 0, 10)), @min_report_date, DAY) >= 0
  AND DATE_DIFF(DATE(SUBSTR(subsession_start_date[OFFSET(0)], 0, 10)), @max_report_date, DAY) < 0
