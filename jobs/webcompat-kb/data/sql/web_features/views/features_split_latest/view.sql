SELECT *
FROM `{{ ref('features_split') }}` as features_split
WHERE features_split.release = (
  SELECT name
  FROM `{{ ref('releases') }}` as releases
  ORDER BY releases.version.major DESC, releases.version.minor DESC, releases.version.patch DESC
  LIMIT 1
)
