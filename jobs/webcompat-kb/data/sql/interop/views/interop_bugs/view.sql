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

all_bugs AS (
  SELECT issue, ARRAY_AGG(number) as bugs FROM (
    SELECT DISTINCT issue, number FROM (
      SELECT issue, number FROM linked_bugs
      UNION ALL
      SELECT issue, number FROM feature_bugs
    )
  )
  GROUP BY issue
)

SELECT interop_proposals.issue, interop_proposals.title, interop_proposals.proposal_type, interop_proposals.features, interop_proposals.state, all_bugs.bugs as bugs
FROM `{{ ref('interop_proposals') }}` AS interop_proposals
LEFT JOIN all_bugs USING(issue)
ORDER BY issue ASC
