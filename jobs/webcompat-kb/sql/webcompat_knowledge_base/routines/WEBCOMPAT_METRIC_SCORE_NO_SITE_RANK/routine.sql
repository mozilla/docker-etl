CREATE OR REPLACE FUNCTION `{{ ref(name) }}`(keywords ARRAY<STRING>, user_story JSON) RETURNS NUMERIC AS (
(SELECT
    sum(if(weights.lookup_type = "impact" and weights.lookup_value = JSON_VALUE(user_story, "$.impact"), weights.score, 0)) *
    sum(if(weights.lookup_type = "platform" and weights.lookup_value in UNNEST(SPLIT(JSON_VALUE(user_story, "$.platform"))), weights.score, 0)) *
    sum(if(weights.lookup_type = "configuration" and weights.lookup_value = IFNULL(JSON_VALUE(user_story, "$.configuration"), "general"), weights.score, 0)) *
    sum(if(weights.lookup_type = "users_affected" and weights.lookup_value = IFNULL(JSON_VALUE(user_story, "$.affects"), "all"), weights.score, 0)) *
    sum(if(weights.lookup_type = "patch_applied" and weights.lookup_value = IF("webcompat:sitepatch-applied" IN UNNEST(keywords), IF("webcompat:platform-bug" IN UNNEST(keywords), "platform-bug", "site-bug"), "none"), weights.score, 0)) *
    sum(if(weights.lookup_type = "branch" and weights.lookup_value = IFNULL(JSON_VALUE(user_story, "$.branch"), "release"), weights.score, 0))
  FROM `{{ ref('dim_bug_score') }}` as weights
  )
);
