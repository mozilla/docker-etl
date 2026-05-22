{% autoescape off %}
with desktop_eventdata as (
SELECT
  normalized_os as segment,
  mozfun.map.get_key(ping_info.experiments, "{{slug}}").branch as branch,
  SAFE_CAST((SELECT value FROM UNNEST(event.extra) WHERE key = '{{metric}}') AS int) AS {{metric}},
FROM
  `moz-fx-data-shared-prod.firefox_desktop.pageload` as d
CROSS JOIN
  UNNEST(events) AS event
WHERE
  normalized_channel = "{{channel}}"
  AND DATE(submission_timestamp) >= DATE('{{startDate}}')
  AND DATE(submission_timestamp) <= DATE('{{endDate}}')  
  AND mozfun.map.get_key(ping_info.experiments, "{{slug}}").branch is not null
  {% for isp in blacklist %}
  AND metadata.isp.name != "{{isp}}"
  {% endfor %}
)
{% if include_non_enrolled_branch == True %}
,
desktop_eventdata_non_enrolled as (
SELECT
  normalized_os as segment,
  "non-enrolled" as branch,
  SAFE_CAST((SELECT value FROM UNNEST(event.extra) WHERE key = '{{metric}}') AS int) AS {{metric}},
FROM
  `moz-fx-data-shared-prod.firefox_desktop.pageload`
CROSS JOIN
  UNNEST(events) AS event
WHERE
  normalized_channel = "{{channel}}"
  AND DATE(submission_timestamp) >= DATE('{{startDate}}')
  AND DATE(submission_timestamp) <= DATE('{{endDate}}')
  AND ARRAY_LENGTH(ping_info.experiments) = 0
)
{% endif %}
, android_eventdata as (
SELECT
  normalized_os as segment,
  mozfun.map.get_key(ping_info.experiments, "{{slug}}").branch as branch,
  SAFE_CAST((SELECT value FROM UNNEST(event.extra) WHERE key = '{{metric}}') AS int) AS {{metric}},
FROM
  `moz-fx-data-shared-prod.fenix.pageload` as f
CROSS JOIN
  UNNEST(events) AS event
WHERE
  normalized_channel = "{{channel}}"
  AND DATE(submission_timestamp) >= DATE('{{startDate}}')
  AND DATE(submission_timestamp) <= DATE('{{endDate}}')  
  AND mozfun.map.get_key(ping_info.experiments, "{{slug}}").branch is not null
  {% for isp in blacklist %}
  AND metadata.isp.name != "{{isp}}"
  {% endfor %}
)
{% if include_non_enrolled_branch == True %}
,
android_eventdata_non_enrolled as (
SELECT
  normalized_os as segment,
  "non-enrolled" as branch,
  SAFE_CAST((SELECT value FROM UNNEST(event.extra) WHERE key = '{{metric}}') AS int) AS {{metric}},
FROM
  `moz-fx-data-shared-prod.fenix.pageload`
CROSS JOIN
  UNNEST(events) AS event
WHERE
  normalized_channel = "{{channel}}"
  AND DATE(submission_timestamp) >= DATE('{{startDate}}')
  AND DATE(submission_timestamp) <= DATE('{{endDate}}')
  AND ARRAY_LENGTH(ping_info.experiments) = 0
)
{% endif %}

SELECT
  segment,
  branch,
  {{metric}} as bucket,
  COUNT(*) as counts
FROM
{% if include_non_enrolled_branch == True %}
  (
    SELECT * from desktop_eventdata
    UNION ALL
    SELECT * from desktop_eventdata_non_enrolled
    UNION ALL
    SELECT * from android_eventdata
    UNION ALL
    SELECT * from android_eventdata_non_enrolled
  )
{% else %}
  (
    SELECT * from desktop_eventdata
    UNION ALL
    SELECT * from android_eventdata
  )
{% endif %}
WHERE
  {{metric}} >= {{minVal}} AND {{metric}} <= {{maxVal}}
GROUP BY
  segment, branch, bucket
ORDER BY
  segment, branch, bucket
{% endautoescape %}
