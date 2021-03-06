CREATE TEMP FUNCTION qualified_name(
  meta STRUCT<project_id string, dataset_id string, table_id string>
)
RETURNS string AS (
  CONCAT(meta.project_id, ":", meta.dataset_id, ".", meta.table_id)
);

CREATE TEMP FUNCTION strip_suffix(name string)
RETURNS string AS (
    -- Get rid of the date partition if it exists in the table name, and then extract everything up to the version part.
    -- If the regex fails, just return the name without the partition.
  coalesce(
    REGEXP_EXTRACT(SPLIT(name, "$")[OFFSET(0)], r"^(.*:.*\..*_v[0-9]+)"),
    SPLIT(name, "$")[OFFSET(0)]
  )
);

  -- NOTE: this will capture all links between tables in history. If the queries change over time, then this
  -- may misrepresent the dependencies. One way to solve this is to take the most recent job_id for a destination table
  -- and *then* explode.
WITH extracted AS (
  SELECT
    user_email,
    job_id,
    creation_time,
    destination_table,
    referenced_tables,
    query
  FROM
    `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
  WHERE
    creation_time > TIMESTAMP_SUB(current_timestamp, INTERVAL 90 day)
    AND user_email LIKE "%gserviceaccount.com"
    AND error_result IS NULL
    AND state = "DONE"
    -- dont care about destination tables without references at the moment
    AND referenced_tables IS NOT NULL
    AND NOT STARTS_WITH(destination_table.dataset_id, "_")
),
transformed AS (
  SELECT
    DISTINCT * EXCEPT (destination_table, referenced_tables, project_id, dataset_id, table_id),
    strip_suffix(qualified_name(destination_table)) AS destination_table,
  FROM
    extracted,
    extracted.referenced_tables AS referenced_table
  WHERE
    NOT STARTS_WITH(referenced_table.dataset_id, "_")
)
SELECT
  destination_table,
  array_agg(query ORDER BY creation_time DESC LIMIT 1)[offset(0)] AS query,
  array_agg(
    STRUCT(job_id, user_email, creation_time)
    ORDER BY
      creation_time DESC
    LIMIT
      10
  ) AS most_recent_jobs
FROM
  transformed
GROUP BY
  1
