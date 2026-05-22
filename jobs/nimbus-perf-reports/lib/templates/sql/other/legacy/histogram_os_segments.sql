{% autoescape off %}
with 
{% for branch in branches %}
{{branch.name}} as (
    SELECT
        normalized_os as segment,
        "{{branch.name}}" as branch,
        JSON_EXTRACT({{histogram}}, '$.values') as hist
    FROM
        `moz-fx-data-shared-prod.telemetry.main`
    WHERE
        DATE(submission_timestamp) >= DATE('{{branch.startDate}}')
        AND DATE(submission_timestamp) <= DATE('{{branch.endDate}}')    
        AND normalized_channel = "{{branch.channel}}"
        AND normalized_app_name = "Firefox"
        AND {{histogram}} is not null
        {{branch.ver_condition}}
        {{branch.arch_condition}}
{% for condition in branch.legacy_conditions %}
        {{condition}}
{% endfor %}
        {% for isp in blacklist %}
        AND metadata.isp.name != "{{isp}}"
        {% endfor %}
),
bucketCounts_{{branch.name}} as (
SELECT
    segment,
    branch,
    SAFE_CAST(key AS float64) as bucket,
    INT64(PARSE_JSON(hist)[key]) as count
FROM
    {{branch.name}},
    UNNEST(bqutil.fn.json_extract_keys(hist)) as key
),
histogram_{{branch.name}} as (
SELECT
    segment,
    branch,
    bucket,
    SUM(count) as counts
FROM
    bucketCounts_{{branch.name}}
GROUP BY
   segment, branch, bucket
ORDER BY
    segment, branch, bucket
)
{% if branch.last == False %}
,
{% endif %}
{% endfor %}

SELECT 
    segment,
    branch,
    bucket,
    counts
FROM
    (
{% for branch in branches %}
        SELECT * FROM histogram_{{branch.name}}
{% if branch.last == False %}
        UNION ALL
{% endif %}
{% endfor %}
    ) s
ORDER BY 
    segment, branch, bucket
{% endautoescape %}
