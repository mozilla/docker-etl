CREATE OR REPLACE FUNCTION `{{ ref(name) }}`(url STRING, crux_yyyymm INT64) RETURNS NUMERIC AS (
(SELECT
     IFNULL(IF(MIN(host_ranks.global_rank) <= MIN(IFNULL(host_ranks.local_rank, host_ranks.global_rank)), MAX(1.5 * global_scores.score), MAX(local_scores.score)), 1)
   FROM
     `{{ ref ('crux_imported.host_min_ranks') }}` AS host_ranks
   LEFT JOIN `{{ ref('dim_bug_score') }}` AS local_scores ON local_scores.lookup_type = 'site_rank' AND host_ranks.local_rank <= cast(local_scores.lookup_value as int64)
   LEFT JOIN `{{ ref('dim_bug_score') }}` AS global_scores ON global_scores.lookup_type = 'site_rank' AND host_ranks.global_rank <= cast(global_scores.lookup_value as int64)
   WHERE
     host_ranks.yyyymm = crux_yyyymm AND `{{ ref('WEBCOMPAT_HOST') }}`(host_ranks.host) = `{{ ref('WEBCOMPAT_HOST') }}`(url)
  )
);
