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

* If a new `host_min_rank` column is required, update
  `metrics/ranks.toml` with the column, then ensure that the
  `site-ranks` ETL job has run with the
  `--site-ranks-force-tranco-update` flag to create the column (note:
  this doesn't actually backfill the data, that currently requires a manual `INSERT`).

* Add the metric definition in `metrics/metrics.toml`.

* Create tables `webcompat_kb_dataset.webcompat_topline_metric_{metric_name}`
  and `webcompat_kb_dataset.webcompat_topline_metric_{metric_name}_history`
  e.g. `webcompat_kb_dataset.webcompat_topline_metric_eu` and
  `webcompat_kb_dataset.webcompat_topline_metric_eu_history`. These
  must have the same schemas as
  ``webcompat_kb_dataset.webcompat_topline_metric_all` and
  `webcompat_kb_dataset.webcompat_topline_metric_all_history`,
  respectively.
