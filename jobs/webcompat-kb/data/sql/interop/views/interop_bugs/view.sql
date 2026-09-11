WITH

linked_bugs AS (
  SELECT interop_proposals.issue, bugs.number
  FROM `{{ ref('interop_proposals') }}` AS interop_proposals
  JOIN `{{ ref('webcompat_knowledge_base.bugzilla_bugs') }}` as bugs ON bugs.number IN UNNEST(interop_proposals.bugs)
),

feature_bugs AS (
  SELECT interop_proposals.issue, bugs.number
  FROM `{{ ref('interop_proposals') }}` AS interop_proposals
  JOIN `{{ ref('webcompat_knowledge_base.bugs_platform_data') }}` as bugs ON bugs.feature IN UNNEST(interop_proposals.features)
),

see_also_bugs_all AS (
  SELECT number, CAST(ARRAY_LAST(SPLIT(`{{ ref('webcompat_knowledge_base.URL_PARSE') }}`(see_also).path, "/")) as INTEGER) as issue
  FROM `{{ ref('webcompat_knowledge_base.bugzilla_bugs') }}`
  JOIN UNNEST(see_also) AS see_also
  WHERE starts_with(see_also, "https://github.com/web-platform-tests/interop/issues/")
),

see_also_bugs AS (
  SELECT number, issue
  FROM see_also_bugs_all
  JOIN `{{ ref('interop_proposals') }}` AS interop_proposals USING(issue)
),

all_bugs AS (
  SELECT issue, ARRAY_AGG(number) as bugs FROM (
    SELECT DISTINCT issue, number FROM (
      SELECT issue, number FROM linked_bugs
      UNION ALL
      SELECT issue, number FROM feature_bugs
      UNION ALL
      SELECT issue, number FROM see_also_bugs
    )
  )
  GROUP BY issue
)

SELECT interop_proposals.year, interop_proposals.issue, interop_proposals.title, interop_proposals.proposal_type, interop_proposals.features, interop_proposals.updated_at, interop_proposals.state, all_bugs.bugs as bugs
FROM `{{ ref('interop_proposals') }}` AS interop_proposals
LEFT JOIN all_bugs USING(issue)
ORDER BY issue ASC
