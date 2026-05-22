{% autoescape off %}
with json_strings as (
    SELECT 
        normalized_os as segment,
        mozfun.map.get_key(environment.experiments, "{{slug}}").branch as branch,
        JSON_EXTRACT({{histogram}}, '$.values') as hist
    FROM
      `moz-fx-data-shared-prod.telemetry.main`
    WHERE
        DATE(submission_timestamp) >= DATE('{{startDate}}')
        AND DATE(submission_timestamp) <= DATE('{{endDate}}')
        AND normalized_channel = "{{channel}}"
        AND normalized_app_name = "Firefox"
        AND {{histogram}} is not null
        AND payload.processes.parent.scalars.browser_engagement_total_uri_count > 0
        AND mozfun.map.get_key(environment.experiments, "{{slug}}").branch is not null
        {% for isp in blacklist %}
        AND metadata.isp.name != "{{isp}}"
        {% endfor %}
)
,
keyValuePairs as (
SELECT
    segment,
    branch,
    SAFE_CAST(key AS float64) as bucket,
    INT64(PARSE_JSON(hist)[key]) as count
FROM
    json_strings,
    UNNEST(bqutil.fn.json_extract_keys(hist)) as key
)
{% if include_null_branch == True %}
,
json_strings_null as (
    SELECT
        normalized_os as segment,
        "null" as branch,
        JSON_EXTRACT({{histogram}}, '$.values') as hist
    FROM
      `moz-fx-data-shared-prod.telemetry.main`
    WHERE
        DATE(submission_timestamp) >= DATE('{{startDate}}')
        AND DATE(submission_timestamp) <= DATE('{{endDate}}')
        AND normalized_channel = "{{channel}}"
        AND normalized_app_name = "Firefox"
        AND {{histogram}} is not null
        AND payload.processes.parent.scalars.browser_engagement_total_uri_count > 0
        AND ARRAY_LENGTH(environment.experiments) = 0
)
,
keyValuePairs_null as (
SELECT
    segment,
    branch,
    SAFE_CAST(key AS float64) as bucket,
    INT64(PARSE_JSON(hist)[key]) as count
FROM
    json_strings_null,
    UNNEST(bqutil.fn.json_extract_keys(hist)) as key
)
{% endif %}
SELECT
    segment,
    branch,
    bucket,
    SUM(count) as counts
FROM
{% if include_null_branch == True %}
  (
    SELECT * from keyValuePairs
    UNION ALL
    SELECT * from keyValuePairs_null
  )
{% else %}
    keyValuePairs
{% endif %}
GROUP BY
   segment, branch, bucket
ORDER BY
   segment, branch, bucket
{% endautoescape %}
