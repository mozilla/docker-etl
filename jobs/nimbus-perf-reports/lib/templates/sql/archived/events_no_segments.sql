{% autoescape off %}
WITH buckets as (
SELECT 
    i as bucket
FROM
    UNNEST(generate_array(1, 30000)) i
),
eventdata as (
SELECT
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
{% if is_experiment %}
  AND mozfun.map.get_key(ping_info.experiments, "{{slug}}").branch = "{{branch}}"
{% endif %}
{% if segmentConditions %}
  {{segmentConditions}}
{% endif %}
)
SELECT
  bucket,
{% for metric in metrics %}
  COUNTIF({{metric}} = bucket) as {{metric}}_counts,
{% endfor %}
FROM 
  eventdata, buckets
WHERE
  load_time > 0
GROUP BY
  bucket
ORDER BY
  bucket
{% endautoescape %}
