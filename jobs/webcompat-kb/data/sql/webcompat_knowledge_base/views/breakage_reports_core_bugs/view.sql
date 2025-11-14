SELECT breakage_reports.breakage_bug, core_bugs.core_bug
FROM `{{ ref('breakage_reports') }}` AS breakage_reports
JOIN `{{ ref('core_bugs_all') }}` as core_bugs on breakage_reports.knowledge_base_bug = core_bugs.knowledge_base_bug
