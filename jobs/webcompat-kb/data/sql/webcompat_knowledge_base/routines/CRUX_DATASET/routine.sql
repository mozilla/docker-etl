CREATE OR REPLACE FUNCTION `{{ ref(name) }}`() RETURNS INT64 AS (
(
    SELECT
      -- Default value; set to NULL to use the latest value
      IFNULL(202409, (
        SELECT
          yyyymm
        FROM
          `{{ ref('crux_imported.import_runs') }}`
        ORDER BY
          yyyymm DESC
        LIMIT
          1)))
);
