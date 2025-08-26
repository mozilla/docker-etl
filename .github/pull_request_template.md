Checklist for reviewer:

- [ ] Commits should reference a bug or github issue, if relevant (if a bug is
  referenced, the pull request should include the bug number in the title)
- [ ] Scan the PR and verify that no changes (particularly to
  `.circleci/config.yml`) will cause environment variables (particularly
  credentials) to be exposed in test logs
- [ ] Ensure the container image will be using permissions granted to
  [telemetry-airflow](https://github.com/mozilla/telemetry-airflow/)
  responsibly.

**Note for deployments:** In order to push images built by this PR, the user who merges the PR
must be in the [telemetry Github team](https://github.com/orgs/mozilla/teams/telemetry).
This is because deploys depend on the
[data-eng-airflow-gcr CircleCI context](https://app.circleci.com/settings/organization/github/mozilla/contexts/e1876f84-dfea-47ce-b950-a9eb9e0d4d64).
See [DENG-8850](https://mozilla-hub.atlassian.net/browse/DENG-8850) for additional discussion.
