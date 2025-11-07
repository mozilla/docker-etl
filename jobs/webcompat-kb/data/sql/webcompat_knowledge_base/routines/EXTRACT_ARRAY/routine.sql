CREATE OR REPLACE FUNCTION `{{ ref(name) }}`(value JSON, json_pattern STRING) RETURNS ARRAY<STRING> AS (
ARRAY(SELECT TRIM(entry) FROM
    UNNEST(
      IFNULL(
        JSON_VALUE_ARRAY(value, json_pattern),
        IF(
          JSON_VALUE(value, json_pattern) IS NOT NULL,
          [JSON_VALUE(value, json_pattern)],
          []
        )
      )
    ) AS entry
  )
);
