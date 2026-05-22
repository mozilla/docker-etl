{% autoescape off %}
with histStrings as (
    SELECT 
        JSON_EXTRACT({{histogram}}, '$.values') as hist
    FROM
        `moz-fx-data-shared-prod.telemetry.main`
    WHERE
        DATE(submission_timestamp) >= DATE('{{startDate}}')
        AND DATE(submission_timestamp) <= DATE('{{endDate}}')
        AND normalized_channel = "{{channel}}"
        AND normalized_app_name = "Firefox"
        AND {{histogram}} is not null
        {% if is_experiment %}
        AND mozfun.map.get_key(environment.experiments, "{{slug}}").branch = "{{branch}}"
        {% endif %}
        {% if segmentConditions %}{{segmentConditions}}{% endif %}
),
keyValuePairs as (
SELECT
    SAFE_CAST(key AS float64) as bucket,
    INT64(PARSE_JSON(hist)[key]) as count
FROM
    histStrings,
    UNNEST(bqutil.fn.json_extract_keys(hist)) as key
)
SELECT
    bucket,
    SUM(count) as counts
FROM
    keyValuePairs
GROUP BY
   bucket
ORDER BY
    bucket
{% endautoescape %}
