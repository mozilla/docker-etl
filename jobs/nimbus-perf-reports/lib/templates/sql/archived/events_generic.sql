{% autoescape off %}
WITH buckets as (
SELECT 
    i as bucket
FROM
    UNNEST(generate_array({{minBucket}}, {{maxBucket}})) i
),
{% for segment in segments %}
eventdata_{{segment.name}} as (
SELECT
  "{{segment.name}}" as segment,
{% if is_experiment %}
  mozfun.map.get_key(ping_info.experiments, "{{slug}}").branch as branch,
{% endif %}
{% for metric in metrics %}
  SAFE_CAST((SELECT value FROM UNNEST(event.extra) WHERE key = '{{metric}}') AS int) AS {{metric}},
{% endfor %}
FROM
  `moz-fx-data-shared-prod.firefox_desktop.pageload`
CROSS JOIN
  UNNEST(events) AS event
WHERE 
  normalized_channel = "{{channel}}"
  AND DATE(submission_timestamp) >= DATE('{{startDate}}')
  AND DATE(submission_timestamp) <= DATE('{{endDate}}')  
  AND mozfun.map.get_key(ping_info.experiments, "{{slug}}").branch is not null
{% for condition in segment.conditions %}
  {{condition}}
{% endfor %}
),
aggregated_{{segment.name}} as (
SELECT
    segment,
    branch,
    bucket,
{% for metric in metrics %}
    COUNTIF({{metric}} = bucket) as {{metric}}_counts,
{% endfor %}
FROM
    eventdata_{{segment.name}}, buckets
GROUP BY
    segment, branch, bucket
ORDER BY
    segment, branch, bucket
),
{% endfor %}
emptyTable as
(
  SELECT * FROM buckets WHERE 1=0
)
SELECT
    segment,
    bucket,
    branch,
{% for metric in metrics %}
    {{metric}}_counts,
{% endfor %}
FROM
    (
{% for segment in segments %}
  {% if segment.name == "All" %}
        SELECT * FROM aggregated_all
  {% else %}
        UNION ALL
        SELECT * FROM aggregated_{{segment.name}}
  {% endif %}
{% endfor %}
    ) s
ORDER BY
    segment, branch, bucket
{% endautoescape %}
