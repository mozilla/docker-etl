with relative_priorities as (SELECT
features.bug,
features.feature,
title,
team,
features.size_estimate,

CASE size_estimate
  WHEN "XS" THEN 1
  WHEN "S" THEN 2
  WHEN "M" THEN 3
  WHEN "L" THEN 5
  WHEN "XL" THEN 8
  ELSE NULL
END as effort,

LEAST(GREATEST(
  CASE
    WHEN features.in_interop THEN 9
    WHEN EXISTS (SELECT 1 FROM features.webcompat_bugs as webcompat_bugs where webcompat_priority = "P1")  THEN 7
    WHEN EXISTS (SELECT 1 FROM features.webcompat_bugs as webcompat_bugs where webcompat_priority = "P2") THEN 5
    ELSE 3
  END +
  IF(features.chrome_implemented AND features.safari_implemented, 3, IF(features.chrome_implemented OR features.safari_implemented, 1, 0)) +
  IF(NOT features.safari_implemented,
    CASE features.webkit_standards_position
      WHEN "support" THEN 1
      WHEN "oppose" THEN -2
      ELSE 0
    END, 0) +
  IF(features.chrome_supported_date < DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR) AND
     features.safari_supported_date < DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR) AND
     ARRAY_LENGTH(features.webcompat_bugs) = 0, -2, 0) +
  IF(features.has_polyfill = "Full", -1, 0) +
  IF(features.cosmetic_only = "TRUE", -1, 0) +
  CASE features.webcompat_risk
    WHEN "high" THEN 2
    WHEN "moderate" THEN 1
    WHEN "low" THEN -2
    ELSE 0
  END,
1), 9) as risk,

LEAST(GREATEST(
  3 +
  CASE
    WHEN features.gecko_priority = "High" THEN 4
    WHEN features.gecko_priority = "Medium" THEN 2
    ELSE 0
  END +
  CASE features.partner_request
    WHEN "High Priority" THEN 4
    WHEN "Medium Priority" THEN 2
    ELSE 0
  END +
  CASE features.a11y_impact
    WHEN "Positive" THEN 2
    WHEN "Negative" THEN -1
    ELSE 0
  END +
  CASE features.performance_impact
    WHEN "High" THEN 2
    WHEN "Medium" THEN 1
    WHEN "Negative" THEN -1
    ELSE 0
  END +
  CASE features.privacy_impact
    WHEN "High" THEN 2
    WHEN "Medium" THEN 1
    WHEN "Negative" THEN -1
    ELSE 0
  END +
  CASE features.state_of
    WHEN "Top Request" THEN 3
    WHEN "Little Interest" THEN -1
    ELSE 0
  END +
  CASE
    WHEN chrome_use_counter_override = "Verified High" THEN 1
    WHEN chrome_use_counter_override = "Verified Low" THEN -1
    WHEN chrome_use_counter_override = "Verified Medium" THEN 0
    WHEN chrome_use_counter > 5 THEN 1
    WHEN chrome_use_counter < 0.1 THEN -1
    ELSE 0
  END +
  CASE features.other_dev_interest
    WHEN "High" THEN 2
    WHEN "Low" THEN -1
    ELSE 0
  END
, 1), 9) as benefit,

4 as uncertainty,

 FROM `{{ ref('platform_priorities') }}` as features),

weights AS (SELECT 2  risk, 1 AS benefit, 0.5 AS uncertainty, 1 AS effort),

weighted_scores as (
SELECT
  relative_priorities.*,
  relative_priorities.risk * weights.risk as weighted_risk,
  relative_priorities.benefit * weights.benefit as weighted_benefit,
  relative_priorities.effort * weights.effort as weighted_effort,
  relative_priorities.uncertainty * weights.uncertainty as weighted_uncertainty
FROM relative_priorities
CROSS JOIN weights),

relative_scores as (
  SELECT relative_priorities.*,
  100 * weights.risk * relative_priorities.risk / (SELECT SUM(risk) FROM relative_priorities) as relative_risk,
  100 * weights.benefit * relative_priorities.benefit / (SELECT SUM(benefit) FROM relative_priorities) as relative_benefit,
  100 * weights.effort * relative_priorities.effort / (SELECT SUM(effort) FROM relative_priorities) as relative_effort,
  100 * weights.uncertainty * relative_priorities.uncertainty / (SELECT SUM(uncertainty) FROM relative_priorities) as relative_uncertainty,
  FROM relative_priorities
  CROSS JOIN weights
)

 SELECT *,
  relative_risk + relative_benefit as value,
  (relative_risk + relative_benefit) / relative_uncertainty as uncertainty_adjusted_value,
  relative_uncertainty + relative_effort as cost,
  (relative_risk + relative_benefit) / (relative_uncertainty + relative_effort) AS relative_priority
 FROM relative_scores
 ORDER BY uncertainty_adjusted_value desc, value DESC, bug
