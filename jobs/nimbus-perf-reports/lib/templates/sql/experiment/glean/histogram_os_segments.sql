{% autoescape off %}
with 
{% if available_on_desktop == True %}
desktop_data as (
    SELECT 
        normalized_os as segment,
        mozfun.map.get_key(ping_info.experiments, "{{slug}}").branch as branch,
        CAST(key as INT64)/1000000 AS bucket,
        value as count
    FROM `mozdata.firefox_desktop.metrics` as d
      CROSS JOIN UNNEST({{histogram}}.values)
    WHERE
      DATE(submission_timestamp) >= DATE('{{startDate}}')
      AND DATE(submission_timestamp) <= DATE('{{endDate}}')
      AND normalized_channel = "{{channel}}"
      AND normalized_app_name = "Firefox"
      AND {{histogram}} is not null
      AND ARRAY_LENGTH(ping_info.experiments) > 0
      AND mozfun.map.get_key(ping_info.experiments, "{{slug}}").branch is not null
      {% for isp in blacklist %}
      AND metadata.isp.name != "{{isp}}"
      {% endfor %}
),
{% else %}
desktop_data as (
  SELECT
    "" as segment,
    "" as branch,
    0 as bucket,
    0 as count
  FROM `mozdata.firefox_desktop.metrics` as d
  WHERE FALSE
),
{% endif %}
{% if available_on_android == True %}
android_data as (
    SELECT 
        normalized_os as segment,
        mozfun.map.get_key(ping_info.experiments, "{{slug}}").branch as branch,
        CAST(key as INT64)/1000000 AS bucket,
        value as count
    FROM `mozdata.fenix.metrics` as f
      CROSS JOIN UNNEST({{histogram}}.values)
    WHERE
      DATE(submission_timestamp) >= DATE('{{startDate}}')
      AND DATE(submission_timestamp) <= DATE('{{endDate}}')
      AND normalized_channel = "{{channel}}"
      AND {{histogram}} is not null
      AND ARRAY_LENGTH(ping_info.experiments) > 0
      AND mozfun.map.get_key(ping_info.experiments, "{{slug}}").branch is not null
      {% for isp in blacklist %}
      AND metadata.isp.name != "{{isp}}"
      {% endfor %}
)
{% else %}
android_data as (
  SELECT
    "" as segment,
    "" as branch,
    0 as bucket,
    0 as count
  FROM `mozdata.fenix.metrics` as f
  WHERE FALSE
)
{% endif %}
{% if include_non_enrolled_branch == True %}
{% if available_on_desktop == True %}
,desktop_data_non_enrolled as (
    SELECT 
        normalized_os as segment,
        "non-enrolled" as branch,
        CAST(key as INT64)/1000000 AS bucket,
        value as count
    FROM `mozdata.firefox_desktop.metrics` as d
      CROSS JOIN UNNEST({{histogram}}.values)
    WHERE
      DATE(submission_timestamp) >= DATE('{{startDate}}')
      AND DATE(submission_timestamp) <= DATE('{{endDate}}')
      AND normalized_channel = "{{channel}}"
      AND normalized_app_name = "Firefox"
      AND {{histogram}} is not null
      AND ARRAY_LENGTH(ping_info.experiments) = 0
),
{% else %}
,desktop_data_non_enrolled as (
  SELECT 
    "" as segment,
    "" as branch,
    0 as bucket,
    0 as count
  FROM `mozdata.firefox_desktop.metrics` as d
  WHERE FALSE
),
{% endif %}
{% if available_on_android == True %}
android_data_non_enrolled as (
    SELECT 
        normalized_os as segment,
        "non-enrolled" as branch,
        CAST(key as INT64)/1000000 AS bucket,
        value as count
    FROM `mozdata.fenix.metrics` as f
      CROSS JOIN UNNEST({{histogram}}.values)
    WHERE
      DATE(submission_timestamp) >= DATE('{{startDate}}')
      AND DATE(submission_timestamp) <= DATE('{{endDate}}')
      AND normalized_channel = "{{channel}}"
      AND {{histogram}} is not null
      AND ARRAY_LENGTH(ping_info.experiments) = 0
)
{% else %}
android_data_non_enrolled as (
  SELECT
    "" as segment,
    "" as branch,
    0 as bucket,
    0 as count
  FROM `mozdata.fenix.metrics` as f
  WHERE FALSE
)
{% endif %}
{% endif %}

SELECT
    segment,
    branch,
    bucket,
    SUM(count) as counts
FROM
    (
        SELECT * FROM desktop_data
        UNION ALL
        SELECT * FROM android_data
{% if include_non_enrolled_branch == True %}
        UNION ALL
        SELECT * FROM desktop_data_non_enrolled
        UNION ALL
        SELECT * FROM android_data_non_enrolled
{% endif %}
    ) s
GROUP BY
  segment, branch, bucket
ORDER BY
  segment, branch, bucket
{% endautoescape %}
