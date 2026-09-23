SELECT *
FROM `{{ ref('features_moved') }}` as features_moved
WHERE features_moved.release = (
  SELECT name
  FROM `{{ ref('releases') }}` as releases
  ORDER BY releases.version.major DESC, releases.version.minor DESC, releases.version.patch DESC
  LIMIT 1
)
