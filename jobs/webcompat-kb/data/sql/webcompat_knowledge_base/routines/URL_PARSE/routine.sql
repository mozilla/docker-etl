CREATE OR REPLACE FUNCTION `{{ ref(name) }}`(url STRING) RETURNS STRUCT<scheme STRING, host STRING, path STRING, query STRING, fragment STRING> AS (
(
    SELECT
      AS STRUCT REGEXP_EXTRACT(url, r"^([^:]+):") AS scheme,
      REGEXP_EXTRACT(url, r"^[^:]+:[/]+([^/?#]+)") AS host,
      IFNULL(REGEXP_EXTRACT(url, r"^[^:]+:[/]+[^/]+([^#?]+)?"), "/") AS path,
      REGEXP_EXTRACT(url, r"[^#]*\?([^#]+)") AS query,
      REGEXP_EXTRACT(url, r"#(.*)") AS fragment )
);
