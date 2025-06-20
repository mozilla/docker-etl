# Web compatibility Knowledge Base ETL

This job fetches bugzilla bugs from Web Compatibility > Knowledge Base
component, as well as their core bugs dependencies and breakage
reports and puts them into BQ. It also has additional sub-jobs to
record the webcompat metric score, and changes to the score, and to
ensure that we have fresh data from external sources such as CrUX.

## Usage

This script is intended to be run in a docker container.
Build the docker image with:

```sh
docker build -t webcompat-kb .
```

### Running locally

First authenticate with gcloud. You also need to ensure Google Drive
access is enabled:

```sh
gcloud auth login --enable-gdrive-access --update-adc
```

This will open an authentication flow in your web browser. It has to
be rerun when the access token expires.

It is highly recommended to use [uv](https://docs.astral.sh/uv/) to
run the project. Assuming uv is installed starting the ETL locally
should be as simple as:

And then run the script after authentication with gcloud:

```sh
uv run webcompat-etl --bq-project=<your_project_id> --bq-kb-dataset=<your_dataset_id> --no-write
```

By default all the jobs that run in production are run. Specific jobs
can be specified by name; see `webcompat-etl --help` for more details.

## Development

Run tests with:

```sh
./test.sh
```

Ruff is used for code formatting:
```sh
uv run ruff format .
```

## Metric Changes and Rescoring

If the webcompat metric scoring is changed, follow these steps to
correctly record the impact of the change:

* Create a copy of the `scored_site_reports` table with a different
  name e.g. `scored_site_reports_new`.  Update the definition of this
  view to reflect the new scoring.

* If changes are also required to routines used to compute the metric
  score similarly create copies of modified routines implementing the
  new logic, for example, if updating the logic in
  `WEBCOMPAT_METRIC_SCORE_NO_HOST` create a copy of the routine with a
  name like `WEBCOMPAT_METRIC_SCORE_NO_HOST_NEW` implementing the new
  logic, and use that inside `scored_site_reports_new`.

* Validate that the score changes have the anticipated effect (e.g. by
  checking the bugs that change between `scored_site_reports` and
  `scored_site_reports_new`.

Once the change is validated and you're ready to deploy:

* Arrange for the production deployment of the ETL to be paused, to
  avoid multiple processes trying to make conflicting writes.

* Run `webcompat-etl` locally as documented above, specifying the jobs
  `bugzilla`, `metric_changes` and `metric_rescore`.

  In addition to the standard command line arguments needed to
  configure the ETL to point at the correct project and dataset, the
  following are required:

  * `--metric-rescore-new-scored-site-reports` - the name of the view
    you created with the new metric logic
    e.g. `scored_site_reports_new`.

  * `--metric-rescore-update-routine` - once for each routine that
    needs to be replaced by a new version. This has the form
    `canonical_name:replacement_name`
    e.g. `WEBCOMPAT_METRIC_SCORE_NO_SITE_RANK:WEBCOMPAT_METRIC_SCORE_NO_SITE_RANK_NEW`.

  * `--metric-rescore-reason` - A string summarising the change.

* This will move the existing `scored_site_reports` to
  `scored_site_reports_before_{timestamp}` and the new scored site
  reports to `scored_site_reports`. Similarly the existing routines
  that have been updated will be renamed to
  `ROUTINE_NAME_BEFORE_{timestamp}` and the new version of the
  routines will be given the original name. The queries in the view
  will be udpated to point at the correct routine names (using string
  replacement, so this should be verified manually after running).

* It will also add a row to `webcompat_topline_metric_rescores` with
  the final scores computed with the old version of the query and the
  first scores computed with the new version of the query, and entries
  in `webcompat_topline_metric_changes` for each bug that contributed
  to a score change.

* The new version of routines and queries with their pre-rescore names
  (e.g. `scored_site_reports_new`) are not removed; this should be
  done manually once it has been verified that the rescore was
  successful.

## Adding new metric types

Adding a new metric type (e.g. all sites that are top 1000 in the EU)
requires the following steps:

* Add a nullable integer column to `crux_imported.host_min_ranks` with
  any additional ranking data required e.g. in this example eu_rank.

* Update `update_min_rank_data` in the `site-rank` job to populate the
  new column. It can also be backfilled by running an `INSERT` just
  targeting that specific column.

* Add an `is_{type}` column to the `scored_site_reports` view, defined
  to be `true` if the site report is in the metric category and
  `false` if it isn't. This will typically chek the
  `crux_imported.host_min_ranks` for the ranking of the site report
  host being below a given value.

* Create tables `webcompat_kb_dataset.webcompat_topline_metric_{type}`
  and `webcompat_kb_dataset.webcompat_topline_metric_{type}_history`
  e.g. `webcompat_kb_dataset.webcompat_topline_metric_eu` and
  `webcompat_kb_dataset.webcompat_topline_metric_eu_history`. These
  must have the same schemas as
  ``webcompat_kb_dataset.webcompat_topline_metric_all` and
  `webcompat_kb_dataset.webcompat_topline_metric_all_history`,
  respectively.

* Update the `update_metric_history` function in the `metric` job to
  write to the new tables.

* Add `bug_count_{type}`, `needs_diagnosis_score_{type}`,
  `not_supported_score_{type}`, and `total_score_{type}` columns to
  `webcompat_kb_dataset.topline_metric_daily`.

* Update the `update_metric_daily` function in the `metric` job to
  record the new scores.

* Add `before_bug_count_{type}`,
  `before_needs_diagnosis_score_{type}`,
  `before_not_supported_score_{type}`, `before_total_score_{type}`,
  `after_bug_count_{type}`, `after_needs_diagnosis_score_{type}`,
  `after_not_supported_score_{type}`, and `after_total_score_{type}`
  columns to `webcompat_kb_dataset.topline_metric_rescores`.

* Update `score_bug_changes` in the `metric-rescore` job to include
  the new metric type in the list of score changes.

* Update `insert_metric_changes` in the `metric-rescore` job to
  include the new score type in the list of score types.
