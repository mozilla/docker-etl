CREATE OR REPLACE FUNCTION `{{ ref(name) }}`(keywords ARRAY<STRING>, user_story JSON) RETURNS STRING AS (
CASE
  WHEN "webcompat:blocked-resources" IN UNNEST(keywords) THEN "resources"
  WHEN "webcompat:blocked" IN UNNEST(keywords) THEN
    CASE
      WHEN "spec-needed" IN UNNEST(keywords) THEN "spec"
      WHEN "webcompat:needs-diagnosis" IN UNNEST(keywords) AND "webcompat:needs-login" IN UNNEST(keywords) THEN "needs-login"
      WHEN "webcompat:needs-contact" IN UNNEST(keywords) THEN "needs-contact"
      ELSE "other"
  END
  ELSE NULL
END
);
