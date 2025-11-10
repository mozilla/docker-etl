SELECT *
FROM `{{ ref('features') }}` as features
WHERE features.release = (
  SELECT name
  FROM `{{ ref('releases') }}` as releases
  ORDER BY releases.version.major DESC, releases.version.minor DESC, releases.version.patch DESC
  LIMIT 1
)
