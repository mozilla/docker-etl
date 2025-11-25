# WebCompat ETL Commands

All commands are expected to be run using `uv` e.g. `uv run webcompat-etl`.

## webcompat-etl

Run the main ETL to import data into BigQuery. The command has the form:

```
webcompat-etl --bq-project-id <project_id> [job*]
```

`jobs` is the list of update jobs that will be run. When omitted all
jobs are run, otherwise jobs are run in the order specified on the
command line.

### Important Arguments

* `--no-write` - Don't write updates, but output the data that would be written.
* `--stage` - Write output to a staging deployment (currently: the
  same project datasets but with a `_test` suffix on the datasets).

## webcompat-check-templates

Perform linting-type checks on all the templates defined in the `data`
directory.

```
webcompat-check-templates --bq-project-id <project_id>
```

## webcompat-render

Render provided schema templates to stdout.

```
webcompat-render --bq-project-id <project_id> [schema_id+]
```

`schema_id` is a schema id, usually of the form `dataset.name`
e.g. `webcompat_knowledge_base.scored_site_reports`. This works for
tables (in which case the table schema will be output in TOML format),
views and routines (which both output SQL).

## webcompat-validate

Validate that provided view or routine template renders to valid SQL.

```
webcompat-validate --bq-project-id <project_id> [schema_id*]
```

`schema_id` is a schema id, usually of the form `dataset.name`
e.g. `webcompat_knowledge_base.scored_site_reports`. Unlike
`webcompat-render` this communicates with BigQuery.

## webcompat-update-staging-data

Update the data in staging datasets based on current data in production.

```
webcompat-update-staging-data --bq-project-id <project_id> [--update-views]
```

Copies tables from `dataset` to `dataset_test`. With `--update-views`
also updates views in `--dataset-test` to match the ones defined in
the source tree.

## webcompat-add-metric

Create the views and tables for metrics based on the metric definition
files.

```
webcompat-add-metric --bq-project-id <project_id>
```
