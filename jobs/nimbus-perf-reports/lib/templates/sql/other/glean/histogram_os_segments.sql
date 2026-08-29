{% autoescape off %}
with 
{% for branch in branches %}
{{branch.name}}_desktop as (
    SELECT
        normalized_os as segment,
        "{{branch.name}}" as branch,
        CAST(key as INT64)/1000000 AS bucket,
        value as count
    FROM `mozdata.firefox_desktop.metrics` as d
        CROSS JOIN UNNEST({{histogram}}.values)
    WHERE 
        DATE(submission_timestamp) >= DATE('{{branch.startDate}}')
        AND DATE(submission_timestamp) <= DATE('{{branch.endDate}}')    
        AND normalized_channel = "{{branch.channel}}"
        AND normalized_app_name = "Firefox"
        AND {{histogram}} is not null
        {{branch.ver_condition}}
        {{branch.arch_condition}}
{% for condition in branch.glean_conditions %}
        {{condition}}
{% endfor %}
        {% for isp in blacklist %}
        AND metadata.isp.name != "{{isp}}"
        {% endfor %}
),
{{branch.name}}_android as (
    SELECT
        normalized_os as segment,
        "{{branch.name}}" as branch,
        CAST(key as INT64)/1000000 AS bucket,
        value as count
    FROM `mozdata.fenix.metrics` as f
        CROSS JOIN UNNEST({{histogram}}.values)
    WHERE 
        DATE(submission_timestamp) >= DATE('{{branch.startDate}}')
        AND DATE(submission_timestamp) <= DATE('{{branch.endDate}}')    
        AND normalized_channel = "{{branch.channel}}"
        AND {{histogram}} is not null
        {{branch.ver_condition}}
        {{branch.arch_condition}}
{% for condition in branch.glean_conditions %}
        {{condition}}
{% endfor %}
        {% for isp in blacklist %}
        AND metadata.isp.name != "{{isp}}"
        {% endfor %}
)
{% if branch.last == False %}
,
{% endif %}
{% endfor %}

SELECT 
    segment,
    branch,
    bucket,
    SUM(count) as counts
FROM
    (
{% for branch in branches %}
        SELECT * FROM {{branch.name}}_android
        UNION ALL
        SELECT * FROM {{branch.name}}_desktop
{% if branch.last == False %}
        UNION ALL
{% endif %}
{% endfor %}
    ) s
GROUP BY
  segment, branch, bucket
ORDER BY 
  segment, branch, bucket
{% endautoescape %}
