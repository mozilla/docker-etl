CREATE OR REPLACE FUNCTION `{{ ref(name) }}`(url STRING) RETURNS STRING AS (
(
    SELECT
      CASE
        WHEN STARTS_WITH(host, "www.") THEN SUBSTR(host, 5)
        WHEN STARTS_WITH(host, "m.") THEN SUBSTR(host, 3)
        ELSE host
    END
    FROM (
      SELECT
        NET.HOST(url) AS host))
);
