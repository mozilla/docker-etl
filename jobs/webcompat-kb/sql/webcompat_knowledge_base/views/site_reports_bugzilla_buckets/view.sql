SELECT
  number,
  triage_score as score,
  CASE
    WHEN score <= 10 THEN 1
    WHEN score <= 20 THEN 2
    WHEN score <= 50 THEN 3
    WHEN score <= 100 THEN 4
    WHEN score <= 200 THEN 5
    WHEN score <= 500 THEN 6
    WHEN score <= 750 THEN 7
    WHEN score <= 1000 THEN 8
    WHEN score <= 2000 THEN 9
    ELSE 10
END as score_bucket,
  CASE
    WHEN score < 100 THEN 3
    WHEN score < 750 THEN 2
    ELSE 1
END as webcompat_priority
FROM
  `{{ ref('scored_site_reports') }}`
WHERE triage_score is not null
