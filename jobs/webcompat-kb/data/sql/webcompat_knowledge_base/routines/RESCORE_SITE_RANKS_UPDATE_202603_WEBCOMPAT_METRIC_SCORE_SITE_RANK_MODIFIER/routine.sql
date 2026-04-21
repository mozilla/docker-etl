CREATE OR REPLACE FUNCTION `{{ ref(name) }}`(url STRING, crux_yyyymm INT64) RETURNS NUMERIC AS (
  (
    SELECT
      CASE
        WHEN MIN(host_ranks.global_rank) <= 1000 THEN 15
        WHEN
          MIN(host_ranks.core_rank) <= 1000 OR
          MIN(host_ranks.india_rank) <= 1000 OR
          MIN(host_ranks.brazil_rank) <= 1000 OR
          MIN(host_ranks.indonesia_rank) <= 1000 OR
          MIN(host_ranks.mexico_rank) <= 1000 OR
          MIN(host_ranks.italy_rank) <= 1000 OR
          MIN(host_ranks.spain_rank) <= 1000 OR
          MIN(host_ranks.netherlands_rank) <= 1000
          THEN 10
        WHEN MIN(host_ranks.global_rank) <= 10000 THEN 7.5
        WHEN MIN(host_ranks.local_rank) <= 1000 THEN 5
        WHEN
          MIN(host_ranks.core_rank) <= 10000 OR
          MIN(host_ranks.india_rank) <= 10000 OR
          MIN(host_ranks.brazil_rank) <= 10000 OR
          MIN(host_ranks.indonesia_rank) <= 10000 OR
          MIN(host_ranks.mexico_rank) <= 10000 OR
          MIN(host_ranks.italy_rank) <= 10000 OR
          MIN(host_ranks.spain_rank) <= 10000 OR
          MIN(host_ranks.netherlands_rank) <= 10000
          THEN 5
        WHEN MIN(host_ranks.local_rank) <= 10000 THEN 2.5
        ELSE 1
      END
    FROM
      `{{ ref ('crux_imported.host_min_ranks') }}` AS host_ranks
    WHERE
      host_ranks.yyyymm = crux_yyyymm AND `{{ ref('WEBCOMPAT_HOST') }}`(host_ranks.host) = `{{ ref('WEBCOMPAT_HOST') }}`(url)
  )
);
