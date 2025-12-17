# Metrics

## Metrics Defintions

The definitions for metrics are stored under `data/metrics` as a
`metrics.toml` file.

Each defined metric is a separate table in the file with the following structure:

```toml
[metric_name]
type = "site_reports_field"
host_min_ranks_condition = "condition"
conditions = ["condition1", "condition2"]
```

The metric name must be valid as a column name in SQL. By convention
it should consider of lowercase alphanumeric characters from ASCII,
plus `_`, i.e. it should match `[a-z0-9_]+`. The other fields are
defined as:

* `type` - either `"unconditional"` if the metric contains all
  scored site reports, or `"site_reports_field"` if the metric
  contains conditions to select the required site reports.

Metrics which are of type `"site_reports_field"` define a boolean
column on `scored_site_reports` with the name `is_{metric_name}`. The
column is true when the site report is part of the metric. For metrics
of this type, one or both of the following addition fields can be
defined:

* `host_min_ranks_condition` - A SQL fragment that defines a column
  in the `host_categories` CTE in `scored_site_reports`. The input is
  the `crux_imported.host_min_ranks` table, and this condition is used
  to define which hosts are part of the metric. The output column will
  have the name `is_{metric_name}`. If this column is ommitted nothing
  is added to the definition of `host_categories`.
* `conditions` - List of conditions applied to `site_reports` and
  `host_categories` when building `scored_site_reports` that
  determines whether the report is part of the metric. Conditions are
  joined with `AND`. When this is not supplied the default condition
  is `host_categories.is_{metric_name}`.

## Site Rank Definitions

Metrics often depend on the popularity of a site. This data is stored
in the `crux_imported.host_min_ranks` table, which is derived from
CrUX and Tranco data.

Columns in this table are defined in `metrics/ranks.toml`.

The order of the sections in the file is important; it must match the
column order in the `host_min_ranks` table.

The structure of the entries is:

```toml
[column_name]
crux_include = ["country_code"]
crux_exclude = ["other_country_code"]
rank = "sql condition"
```

The column name should consider of lowercase alphanumeric characters
from ASCII, plus `_`, i.e. it should match `[a-z0-9_]+`. The other
fields are defined as:

* `crux_include` - Optional list of two-letter country codes (or
  `"global"`) corresponding to CrUX countries to include in the rank
  definition.
* `crux_exclude` - Optional list of two-letter country codes (or
  `"global"`) corresponding to CrUX countries to exclude in the rank
  definition. `crux_exclude` and `crux_include` cannot both be provided.
* `rank` - Optional SQL condition to define the site rank. The inputs are a
  `crux_ranks` and `tranco_ranks` tables. By default, if this is not
  supplied it is defined as `MIN(IF({column_name}, crux_ranks.rank, NULL))`

At least one field must be provided.

## Adding new metric types

Adding a new metric type (e.g. all sites that are top 1000 in the EU)
requires the following steps:

* If a new site rank type is required, add the definition to
  `data/metrics/ranks.toml`.

* Add the metric definition in `data/metrics/metrics.toml`.

* Run `webcompat-add-metric` to create schemas for the tables
  associated with the new metric.

* To test the changes run `webcompat-etl --bq-project-id <id> --stage
  update-schema`. This should update existing tables, and create new
  tables, with a `_test` suffix.

* Commit the changes, and create a PR. Once this lands, wait for the
  the ETL to run with the updated table definitions.

* Run `webcompat-backfill-metric <name>` to add extrapolate the metric
  backwards in time, based on the current state of the bugs.

* Create tables `webcompat_kb_dataset.webcompat_topline_metric_{metric_name}`
  and `webcompat_kb_dataset.webcompat_topline_metric_{metric_name}_history`
  e.g. `webcompat_kb_dataset.webcompat_topline_metric_eu` and
  `webcompat_kb_dataset.webcompat_topline_metric_eu_history`. These
  must have the same schemas as
  ``webcompat_kb_dataset.webcompat_topline_metric_all` and
  `webcompat_kb_dataset.webcompat_topline_metric_all_history`,
  respectively.

## Metric Changes and Rescoring

When planning a change that will affect the metric, run the following
steps:

* `uv run webcompat-metric-rescore --project=<project_id>
  create-schema <rescore_name> --reason <reason for rescore>`. This
  will create a copy of `scored_site_reports` in the
  `data/sql/webcompat_knowledge_base/views/` directory with a name
  like `scored_site_reports-rescore_<rescore_name>`. If the rescore
  affects any routines the name of these routines can be specified as
  `--routine` arguments to the above command; this will similarly
  generate copies of the existing routine templates with a name line
  `ROUTINE_NAME-RESCORE_<RESCORE_NAME>`. In addition a view template
  called `rescore_<rescore_name>_delta` will be created, with a query
  containing the per-bug delta between the current
  `scored_site_reports` and the post-rescore `scored_site_reports`.

* Edit the created templates for
  `scored_site_reports-rescore_<rescore_name>` and any updated
  routines to reflect the post-rescore scoring.

* Commit the templates, open a PR and get review for the changes. Once
  landed this will cause the prod schema to be updated next time the
  ETL runs.

* Observe the impact on the scores, make any additional changes
  required for the scoring logic, and get consensus that the rescore
  should be deployed.

* `uv run webcompat-metric-rescore --project=<project_id>
  prepare-deploy <rescore_name>`. This will copy the current canonical
  schema to the `webcompat_knowledge_base_archive` dataset (adjusting
  references as required), and copy the updated schema to the
  canonical locations, deleting the templates for the provisional
  views/routines.

* File a PR and deploy. This will cause the prod schema to be updated
  next time the ETL runs.

* If changes are also required to routines used to compute the metric
  score similarly create copies of modified routines implementing the
  new logic, for example, if updating the logic in
  `WEBCOMPAT_METRIC_SCORE_NO_HOST` create a copy of the routine with a
  name like `WEBCOMPAT_METRIC_SCORE_NO_HOST_NEW` implementing the new
  logic, and use that inside `scored_site_reports_new`.

* Validate that the score changes have the anticipated effect (e.g. by
  checking the bugs that change between `scored_site_reports` and
  `scored_site_reports_new`.
