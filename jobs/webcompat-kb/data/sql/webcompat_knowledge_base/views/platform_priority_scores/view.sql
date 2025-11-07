with rice_fields as (SELECT
features.bug,
features.feature,
title,
team,
features.size_estimate,

CASE size_estimate
  WHEN "XS" THEN 0.75
  WHEN "S" THEN 1.5
  WHEN "M" THEN 3
  WHEN "L" THEN 6
  WHEN "XL" THEN 12
  ELSE NULL
END as size_estimate_months,

LEAST(GREATEST(
CASE
  WHEN features.in_interop THEN 4
  WHEN EXISTS (SELECT 1 FROM features.webcompat_bugs as webcompat_bugs where webcompat_priority = "P1") THEN 3
  WHEN features.gecko_priority = "High" THEN 3
  WHEN EXISTS (SELECT 1 FROM features.webcompat_bugs as webcompat_bugs where webcompat_priority = "P2") THEN 2
  WHEN features.gecko_priority = "Medium" THEN 2
  WHEN FALSE THEN 0
  ELSE 1
END +
  IF(features.chrome_implemented, 1, 0) +
  IF(features.safari_implemented, 1,
    IF(features.webkit_standards_position ="support", 0.25,
      IF(features.webkit_standards_position ="oppose", -0.5, 0.))) +
  IF(features.chrome_supported_date < DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR) AND
     features.safari_supported_date < DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR) AND
     ARRAY_LENGTH(features.webcompat_bugs) = 0, -1, 0) +
     IF(features.has_polyfill = "Full", -0.5, 0) +
  IF(features.cosmetic_only = "TRUE", -0.5, 0) +
  CASE features.webcompat_risk
    WHEN "high" THEN 1
    WHEN "moderate" THEN 0.5
    WHEN "low" THEN -0.5
    ELSE 0
  END,
0), 4) as risk,

LEAST(GREATEST(
CASE
  WHEN EXISTS (SELECT 1 FROM features.webcompat_bugs as webcompat_bugs where webcompat_priority = "P1") THEN 4
  WHEN EXISTS (SELECT 1 FROM features.webcompat_bugs as webcompat_bugs where webcompat_priority = "P2") THEN 3
  WHEN features.partner_request IS NOT NULL THEN 3
  ELSE 2
END +
  IF(features.state_of = "Top Request", 1, IF(features.state_of = "Little Interest", -0.5, 0)) +
  CASE
    WHEN chrome_use_counter_override = "Verified High" THEN 1
    WHEN chrome_use_counter_override = "Verified Low" THEN -1
    WHEN chrome_use_counter_override = "Verified Medium" THEN 0
    WHEN chrome_use_counter > 5 THEN 1
    WHEN chrome_use_counter < 0.1 THEN -1
    ELSE 0
  END +
  IF(features.other_dev_interest = "High", 1, IF(features.other_dev_interest = "Low", -0.5, 0))
, 0), 4) as reach,

LEAST(GREATEST(
  1 +
  IF(features.a11y_impact = "Positive", 1, IF(features.a11y_impact = "Negative", -1, 0)),
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
  END
  -- gecko leadership priority
, 0), 4) as impact,

CASE
  WHEN features.chrome_implemented AND features.safari_implemented THEN 1
  WHEN EXISTS(SELECT 1 FROM features.webcompat_bugs as webcompat_bugs where webcompat_priority = "P1" OR webcompat_priority = "P2") THEN 1
  WHEN features.chrome_implemented AND features.webkit_standards_position IS NOT NULL OR features.safari_implemented THEN 0.8
  ELSE 0.5
END AS risk_confidence,

CASE
  WHEN features.partner_request IS NOT NULL AND features.partner_request != "None" THEN 1
  WHEN EXISTS(SELECT 1 FROM features.webcompat_bugs as webcompat_bugs where webcompat_priority = "P1" OR webcompat_priority = "P2") THEN 1
  WHEN STARTS_WITH(chrome_use_counter_override, "Verified") AND state_of IS NOT NULL THEN 1
  WHEN STARTS_WITH(chrome_use_counter_override, "Verified") OR features.state_of IS NOT NULL THEN 0.8
  ELSE 0.5
END AS reach_confidence,

CASE
  WHEN features.a11y_impact IS NOT NULL AND features.performance_impact IS NOT NULL AND features.privacy_impact IS NOT NULL THEN 1
  WHEN (SELECT COUNTIF(impact IS NOT NULL) FROM UNNEST([features.a11y_impact, features.performance_impact, features.privacy_impact]) as impact) > 1 THEN 0.8
  ELSE 0.5
END AS impact_confidence

 FROM `{{ ref('platform_priorities') }}` as features),

 intermediate_scores as (SELECT *, cast(risk * reach * impact AS NUMERIC) AS risk_reach_impact, cast(risk_confidence * reach_confidence * impact_confidence AS NUMERIC) AS confidence FROM rice_fields)

 SELECT *, risk_reach_impact * confidence as priority_score, CAST(risk_reach_impact * confidence / size_estimate_months AS NUMERIC) as rice_score FROM intermediate_scores
 ORDER BY priority_score desc, rice_score DESC, bug
