-- Replaces the BigQuery-aggregation + AVRO-export + Spark-shim stages of the
-- legacy job.
--
-- It rebuilds the per-client "longitudinal" shape the legacy python expects
-- (arrays of pings ordered most-recent-first) and applies the
-- `out_of_date_details` WHERE clause so only the ~100k candidate clients are
-- returned to python (rather than the ~3.5M client rows the aggregation
-- produces before filtering).
--
-- Performance note: histograms are reduced to their final python shapes here
-- (the legacy Spark UDFs' work, done in SQL) and each per-ping histogram column
-- is emitted as ONE JSON string via TO_JSON_STRING.
--   - Returning histograms as nested REPEATED RECORDs makes the BigQuery client
--     deserialize tens of thousands of cells per client row (~30x slower fetch),
--     and the Storage/Arrow API rejects the nested schema.
--   - Returning them as JSON-string histograms and parsing per-ping in python
--     costs ~140 json.loads per client (~10min over 100k clients).
-- Emitting one already-reduced JSON string per column means the python side does
-- exactly one json.loads per column, and the values are ready to use. The flat
-- STRING schema also lets the fast Storage/Arrow read path work.
--
-- Histogram shapes (matching the legacy merge_* UDFs):
--   count       -> [int|null]            bucket-0 count per ping (null = null ping)
--   enumerated  -> [{nz:[{k,v}]}|null]   SPARSE per ping: only the non-zero
--                                         buckets (k, count). The legacy shim
--                                         densified each to a fixed-length
--                                         [0..n] array, but real histograms have
--                                         a median of 1 non-zero bucket (max ~4),
--                                         so the dense form was almost all zeros
--                                         and ballooned memory ~50-100x per ping.
--                                         The python mappers only ever test
--                                         `value > 0` and use the bucket index, so
--                                         a sparse {index: count} per ping is
--                                         exactly equivalent (an absent index reads
--                                         as 0). A null ping stays null; a present
--                                         but all-zero histogram is an empty list.
--   keyed count -> [[{key,value:int}]]   per-ping (key, bucket-0) pairs; python
--                                         pivots to {key: [per-ping]} like legacy
--
-- The companion summary query (summary.sql) computes the report's top-level
-- version counts over the same population WITHOUT the major-version filter, so
-- it cannot be derived from these rows and is run separately.
--
-- Parameters:
--   @date_from           inclusive lower bound on DATE(submission_timestamp)
--   @date_to             inclusive upper bound on DATE(submission_timestamp)
--   @min_report_date     report-week start (subsession_start_date >= this)
--   @max_report_date     report-week end   (subsession_start_date <  this)
--   @channel             update channel to keep (e.g. 'release')
--   @min_version         minimum major version to keep
--   @max_up_to_date_ver  exclusive upper major-version bound
--                        (== latest_version - up_to_date_releases)

-- Bucket-0 count of a count histogram (legacy values['0']), 0 if the histogram
-- exists with no bucket 0. Callers pass NULL through for null pings.
CREATE TEMP FUNCTION count_at0(h STRING) AS (
  IFNULL((SELECT v.value FROM UNNEST(mozfun.hist.extract(h).`values`) v WHERE v.key = 0), 0)
);

-- Sparse non-zero buckets of an enumerated histogram, as (k, v) pairs ordered by
-- bucket index. NULL stays NULL (null ping); a present all-zero histogram yields
-- an empty array. The python side reads an absent index as 0, so this is exactly
-- equivalent to the legacy dense [0..n] array for every mapper (which only ever
-- tests value > 0 and uses the bucket index).
CREATE TEMP FUNCTION enum_nz(h STRING) AS (
  IF(h IS NULL, NULL,
    ARRAY(
      SELECT AS STRUCT b.key AS k, b.value AS v
      FROM UNNEST(mozfun.hist.extract(h).`values`) b
      WHERE b.value > 0
      ORDER BY b.key
    ))
);

WITH main_sample AS (
  SELECT
    submission_timestamp,
    client_id,
    CAST(environment.build.version AS STRING) AS version,
    mozfun.norm.truncate_version(environment.build.version, 'major') AS major_version,
    environment.build.application_name AS application_name,
    environment.settings.update.channel AS channel,
    environment.settings.update.enabled AS enabled,
    payload.info.profile_subsession_counter AS profile_subsession_counter,
    payload.info.subsession_start_date AS subsession_start_date,
    payload.info.subsession_length AS subsession_length,
    payload.info.session_length AS session_length,
    -- enumerated histograms, kept sparse (only non-zero buckets per ping)
    enum_nz(payload.histograms.update_check_code_notify) AS update_check_code_notify,
    enum_nz(payload.histograms.update_download_code_partial) AS update_download_code_partial,
    enum_nz(payload.histograms.update_download_code_complete) AS update_download_code_complete,
    enum_nz(payload.histograms.update_state_code_partial_stage) AS update_state_code_partial_stage,
    enum_nz(payload.histograms.update_state_code_complete_stage) AS update_state_code_complete_stage,
    enum_nz(payload.histograms.update_state_code_unknown_stage) AS update_state_code_unknown_stage,
    enum_nz(payload.histograms.update_state_code_partial_startup) AS update_state_code_partial_startup,
    enum_nz(payload.histograms.update_state_code_complete_startup) AS update_state_code_complete_startup,
    enum_nz(payload.histograms.update_state_code_unknown_startup) AS update_state_code_unknown_startup,
    enum_nz(payload.histograms.update_status_error_code_complete_startup) AS update_status_error_code_complete_startup,
    enum_nz(payload.histograms.update_status_error_code_partial_startup) AS update_status_error_code_partial_startup,
    enum_nz(payload.histograms.update_status_error_code_unknown_startup) AS update_status_error_code_unknown_startup,
    enum_nz(payload.histograms.update_status_error_code_complete_stage) AS update_status_error_code_complete_stage,
    enum_nz(payload.histograms.update_status_error_code_partial_stage) AS update_status_error_code_partial_stage,
    enum_nz(payload.histograms.update_status_error_code_unknown_stage) AS update_status_error_code_unknown_stage,
    -- count histograms, reduced to the bucket-0 scalar per ping (null = null ping)
    IF(payload.histograms.update_check_no_update_notify IS NULL, NULL,
       count_at0(payload.histograms.update_check_no_update_notify)) AS update_check_no_update_notify,
    IF(payload.histograms.update_not_pref_update_auto_notify IS NULL, NULL,
       count_at0(payload.histograms.update_not_pref_update_auto_notify)) AS update_not_pref_update_auto_notify,
    IF(payload.histograms.update_ping_count_notify IS NULL, NULL,
       count_at0(payload.histograms.update_ping_count_notify)) AS update_ping_count_notify,
    IF(payload.histograms.update_unable_to_apply_notify IS NULL, NULL,
       count_at0(payload.histograms.update_unable_to_apply_notify)) AS update_unable_to_apply_notify,
    -- keyed count histogram: per-ping (key, bucket-0 count) pairs
    ARRAY(
      SELECT AS STRUCT key, count_at0(value) AS value
      FROM UNNEST(payload.keyed_histograms.update_check_extended_error_notify)
    ) AS update_check_extended_error_notify
  FROM
    `moz-fx-data-shared-prod.telemetry.main`
  WHERE
    sample_id = 42
    AND environment.build.version IS NOT NULL
    AND DATE(submission_timestamp) >= @date_from
    AND DATE(submission_timestamp) <= @date_to
),

-- Re-aggregate to one row per client, most-recent ping first. Every per-ping
-- value is aggregated under the same ORDER BY. Each histogram column is emitted
-- as a single JSON string (see the performance note above); the array-typed
-- enumerated/keyed columns are wrapped in a STRUCT before ARRAY_AGG since
-- BigQuery can't aggregate array-typed columns into an array-of-arrays.
--
-- IGNORE NULLS on session_length/enabled/subsession_start_date/subsession_length
-- (but NOT version or the histograms) is deliberate and copied verbatim from the
-- legacy job's aggregation SQL. It means those four arrays can be SHORTER than
-- version when a ping had a null in that field, so they are not strictly aligned
-- with version by index. The python mappers index e.g. enabled[index] /
-- subsession_length[index] with an index taken from enumerating version, so this
-- can read a shifted slot. We keep the exact IGNORE NULLS choices the legacy job
-- made so the output matches it; do not "fix" this to align the arrays.
longitudinal AS (
  SELECT
    client_id,
    ARRAY_AGG(version ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000) AS version,
    ARRAY_AGG(major_version ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000) AS major_version,
    ARRAY_AGG(application_name ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000) AS application_name,
    ARRAY_AGG(channel ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000) AS channel,
    ARRAY_AGG(session_length IGNORE NULLS ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000) AS session_length,
    ARRAY_AGG(enabled IGNORE NULLS ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000) AS enabled,
    ARRAY_AGG(subsession_start_date IGNORE NULLS ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000) AS subsession_start_date,
    ARRAY_AGG(subsession_length IGNORE NULLS ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000) AS subsession_length,
    -- count histograms -> JSON [int|null]
    TO_JSON_STRING(ARRAY_AGG(update_check_no_update_notify ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000)) AS update_check_no_update_notify,
    TO_JSON_STRING(ARRAY_AGG(update_not_pref_update_auto_notify ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000)) AS update_not_pref_update_auto_notify,
    TO_JSON_STRING(ARRAY_AGG(update_ping_count_notify ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000)) AS update_ping_count_notify,
    TO_JSON_STRING(ARRAY_AGG(update_unable_to_apply_notify ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000)) AS update_unable_to_apply_notify,
    -- enumerated histograms -> JSON [{h:[{k,v}]|null}] (sparse: non-zero buckets)
    TO_JSON_STRING(ARRAY_AGG(STRUCT(update_check_code_notify AS h) ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000)) AS update_check_code_notify,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(update_download_code_partial AS h) ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000)) AS update_download_code_partial,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(update_download_code_complete AS h) ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000)) AS update_download_code_complete,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(update_state_code_partial_stage AS h) ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000)) AS update_state_code_partial_stage,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(update_state_code_complete_stage AS h) ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000)) AS update_state_code_complete_stage,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(update_state_code_unknown_stage AS h) ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000)) AS update_state_code_unknown_stage,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(update_state_code_partial_startup AS h) ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000)) AS update_state_code_partial_startup,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(update_state_code_complete_startup AS h) ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000)) AS update_state_code_complete_startup,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(update_state_code_unknown_startup AS h) ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000)) AS update_state_code_unknown_startup,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(update_status_error_code_complete_startup AS h) ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000)) AS update_status_error_code_complete_startup,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(update_status_error_code_partial_startup AS h) ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000)) AS update_status_error_code_partial_startup,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(update_status_error_code_unknown_startup AS h) ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000)) AS update_status_error_code_unknown_startup,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(update_status_error_code_complete_stage AS h) ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000)) AS update_status_error_code_complete_stage,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(update_status_error_code_partial_stage AS h) ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000)) AS update_status_error_code_partial_stage,
    TO_JSON_STRING(ARRAY_AGG(STRUCT(update_status_error_code_unknown_stage AS h) ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000)) AS update_status_error_code_unknown_stage,
    -- keyed count histogram -> JSON [{ext:[{key,value:int}]}]
    TO_JSON_STRING(ARRAY_AGG(STRUCT(update_check_extended_error_notify AS ext) ORDER BY subsession_start_date DESC, profile_subsession_counter DESC LIMIT 1000)) AS update_check_extended_error_notify
  FROM
    main_sample
  GROUP BY
    client_id
)

SELECT
  client_id,
  version,
  session_length,
  enabled,
  subsession_start_date,
  subsession_length,
  update_check_code_notify,
  update_check_extended_error_notify,
  update_check_no_update_notify,
  update_not_pref_update_auto_notify,
  update_ping_count_notify,
  update_unable_to_apply_notify,
  update_download_code_partial,
  update_download_code_complete,
  update_state_code_partial_stage,
  update_state_code_complete_stage,
  update_state_code_unknown_stage,
  update_state_code_partial_startup,
  update_state_code_complete_startup,
  update_state_code_unknown_startup,
  update_status_error_code_complete_startup,
  update_status_error_code_partial_startup,
  update_status_error_code_unknown_startup,
  update_status_error_code_complete_stage,
  update_status_error_code_partial_stage,
  update_status_error_code_unknown_stage
FROM
  longitudinal
WHERE
  application_name[OFFSET(0)] = 'Firefox'
  AND channel[OFFSET(0)] = @channel
  AND (REGEXP_CONTAINS(version[OFFSET(0)], r'^[0-9]{2,3}\.0[\.0-9]*$') OR version[OFFSET(0)] = '50.1.0')
  AND DATE_DIFF(DATE(SUBSTR(subsession_start_date[OFFSET(0)], 0, 10)), @min_report_date, DAY) >= 0
  AND DATE_DIFF(DATE(SUBSTR(subsession_start_date[OFFSET(0)], 0, 10)), @max_report_date, DAY) < 0
  AND major_version[OFFSET(0)] < @max_up_to_date_ver
  AND major_version[OFFSET(0)] >= @min_version
