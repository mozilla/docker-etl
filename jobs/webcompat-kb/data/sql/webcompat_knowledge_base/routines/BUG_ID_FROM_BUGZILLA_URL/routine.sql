CREATE OR REPLACE FUNCTION `{{ ref(name) }}`(url STRING) RETURNS INT64 AS (
(
    SELECT
      SAFE_CAST(value AS INT64)
    FROM (
      SELECT
        AS STRUCT REGEXP_EXTRACT(param, r"^([^=]+)=") AS KEY,
        REGEXP_EXTRACT(param, r"=(.+)$") AS value
      FROM
        UNNEST(REGEXP_EXTRACT_ALL(REGEXP_EXTRACT(url, r"https://bugzilla\.mozilla\.org/show_bug\.cgi\?(.+)"), r"([^=]+=[^&#]+)&?")) AS param)
    WHERE
      KEY="id")
);
