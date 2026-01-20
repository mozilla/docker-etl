{% autoescape off %}
with
{% for segment in segments %}
json_{{segment.name}} as (
    SELECT
        "{{segment.name}}" as segment,
        mozfun.map.get_key(environment.experiments, "{{slug}}").branch as branch,
        JSON_EXTRACT({{histogram}}, '$.values') as hist
    FROM
    {% if channel == "nightly" %}
        `moz-fx-data-shared-prod.telemetry.main_nightly`
    {% else %}
        `moz-fx-data-shared-prod.telemetry.main`
    {% endif %}
    WHERE
        DATE(submission_timestamp) >= DATE('{{startDate}}')
        AND DATE(submission_timestamp) <= DATE('{{endDate}}')
        AND normalized_channel = "{{channel}}"
        AND normalized_app_name = "Firefox"
        AND {{histogram}} is not null
        AND payload.processes.parent.scalars.browser_engagement_total_uri_count > 0
        AND mozfun.map.get_key(environment.experiments, "{{slug}}").branch is not null
{% for condition in segment.conditions %}
        {{condition}}
{% endfor %}
),
keyValuePairs_{{segment.name}} as (
SELECT
    segment,
    branch,
    SAFE_CAST(key AS float64) as bucket,
    INT64(PARSE_JSON(hist)[key]) as count
FROM
    json_{{segment.name}},
    UNNEST(bqutil.fn.json_extract_keys(hist)) as key
),
histogram_{{segment.name}} as (
SELECT
    segment,
    branch,
    bucket,
    SUM(count) as counts
FROM
    keyValuePairs_{{segment.name}}
GROUP BY
   segment, branch, bucket
ORDER BY
    segment, branch, bucket
),
{% endfor %}
emptyTable as
(
  SELECT * FROM json_All WHERE 1=0
)

SELECT 
    segment,
    branch,
    bucket,
    counts
FROM
    (
{% for segment in segments %}
  {% if segment.name == "All" %}
        SELECT * FROM histogram_All
  {% else %}
        UNION ALL
        SELECT * FROM histogram_{{segment.name}}
  {% endif %}
{% endfor %}
    )
ORDER BY 
    segment, branch, bucket
{% endautoescape %}
