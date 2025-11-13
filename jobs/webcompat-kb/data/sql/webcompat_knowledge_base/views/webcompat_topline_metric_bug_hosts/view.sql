/*
 * Unique hosts that are in the top 1000 for the metric-selected countries *and* appear in at least one known site issue
 */
WITH
  countries AS (
  SELECT
    country_code
  FROM
    UNNEST(JSON_VALUE_ARRAY('["global", "us", "fr", "de", "es", "it", "mx"]')) AS country_code),
  bug_hosts AS (
  SELECT
    NET.HOST(url) AS host
  FROM
    {{ ref('scored_site_reports') }}
  GROUP BY
    NET.HOST(url)),
  match_hosts AS (
  SELECT
    host,
    host AS match_host
  FROM
    bug_hosts
  UNION ALL
  SELECT
    host,
    CONCAT("www.", host)
  FROM
    bug_hosts
  WHERE
    NOT STARTS_WITH(host, "www.")
    AND NOT STARTS_WITH(host, "m.")
  UNION ALL
  SELECT
    host,
    CONCAT("m.", host)
  FROM
    bug_hosts
  WHERE
    NOT STARTS_WITH(host, "m.")
    AND NOT STARTS_WITH(host, "www."))
SELECT
  DISTINCT match_hosts.match_host
FROM
  match_hosts
JOIN
  `{{ ref('crux_imported.origin_ranks') }}` AS crux_ranks
ON
  NET.HOST(crux_ranks.origin) = match_hosts.match_host
JOIN
  countries
ON
  crux_ranks.country_code = countries.country_code
WHERE
  crux_ranks.rank = 1000 and crux_ranks.yyyymm = 202409
