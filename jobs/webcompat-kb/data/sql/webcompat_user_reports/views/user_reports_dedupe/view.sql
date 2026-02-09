with report_keys AS (
  SELECT uuid, MIN(reported_at) as reported_at
  FROM `{{ ref('user_reports_prod') }}` GROUP BY uuid
)
SELECT * FROM `{{ ref('user_reports_prod') }}`
JOIN report_keys USING (uuid, reported_at)
