CREATE OR REPLACE FUNCTION `{{ ref(name) }}`(url STRING, crux_yyyymm INT64, user_story JSON) RETURNS NUMERIC AS (
  (
    SELECT
      CAST(CASE
        WHEN
          host_ranks.global_rank <= 1000 OR
          "global-1k" IN UNNEST(site_rank_override.ranks)
          THEN 15
        WHEN
          host_ranks.core_rank <= 1000 OR
          host_ranks.india_rank <= 1000 OR
          host_ranks.brazil_rank <= 1000 OR
          host_ranks.indonesia_rank <= 1000 OR
          host_ranks.mexico_rank <= 1000 OR
          host_ranks.italy_rank <= 1000 OR
          host_ranks.spain_rank <= 1000 OR
          host_ranks.netherlands_rank <= 1000 OR
          "core-1k" IN UNNEST(site_rank_override.ranks)
          THEN 10
        WHEN
          host_ranks.global_rank <= 10000 OR
          "global-10k" IN UNNEST(site_rank_override.ranks)
          THEN 7.5
        WHEN
          host_ranks.local_rank <= 1000 OR
          "local-1k" IN UNNEST(site_rank_override.ranks)
          THEN 5
        WHEN
          host_ranks.core_rank <= 10000 OR
          host_ranks.india_rank <= 10000 OR
          host_ranks.brazil_rank <= 10000 OR
          host_ranks.indonesia_rank <= 10000 OR
          host_ranks.mexico_rank <= 10000 OR
          host_ranks.italy_rank <= 10000 OR
          host_ranks.spain_rank <= 10000 OR
          host_ranks.netherlands_rank <= 10000 OR
          "core-10k" IN UNNEST(site_rank_override.ranks)
          THEN 5
        WHEN
          host_ranks.local_rank <= 10000 OR
          "local-10k" IN UNNEST(site_rank_override.ranks)
          THEN 2.5
        ELSE 1
      END AS NUMERIC)
    FROM
      (
        SELECT
          MIN(global_rank) AS global_rank,
          MIN(core_rank) AS core_rank,
          MIN(india_rank) AS india_rank,
          MIN(brazil_rank) AS brazil_rank,
          MIN(indonesia_rank) AS indonesia_rank,
          MIN(mexico_rank) AS mexico_rank,
          MIN(italy_rank) AS italy_rank,
          MIN(spain_rank) AS spain_rank,
          MIN(netherlands_rank) AS netherlands_rank,
          MIN(local_rank) AS local_rank
        FROM
          `{{ ref ('crux_imported.host_min_ranks') }}` AS host_ranks
        WHERE
          host_ranks.yyyymm = crux_yyyymm AND `{{ ref('WEBCOMPAT_HOST') }}`(host_ranks.host) = `{{ ref('WEBCOMPAT_HOST') }}`(url)
      ) AS host_ranks,
      (
        SELECT `{{ ref('EXTRACT_ARRAY') }}`(user_story, "$.site-rank-override") AS ranks
      ) AS site_rank_override
  )
);
