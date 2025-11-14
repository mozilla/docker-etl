/* All core bugs, including knowledge base entries that are themselves core bugs */
SELECT
  core_bugs.knowledge_base_bug,
  core_bugs.core_bug
FROM
  `{{ ref('core_bugs') }}` AS core_bugs
UNION ALL
SELECT
  kb_bugs.number AS knowledge_base_bug,
  kb_bugs.number AS core_bug
FROM
  `{{ ref('kb_bugs') }}` AS kb_bugs
JOIN
  `{{ ref('bugzilla_bugs') }}` AS bugs
ON
  bugs.number = kb_bugs.number
WHERE
  bugs.product != "Web Compatibility";
