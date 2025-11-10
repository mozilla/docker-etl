# Schema Definitions

The definitions for tables, views and routines are stored under
`data/sql`. Each definition consists of a `toml` file containing
metadata relating to the schema, and a `sql` file containing the
definition, using
[jinja](https://jinja.palletsprojects.com/en/stable/templates/) as a
templating language.

## Metadata

Metadata is stored as [TOML](https://toml.io/en/) files, and is
largely common between tables, views and routines. The following
fields are supported:

* _name_ - The name of the table, view or routine
* _desription_ - A human-readable description

## Tables

Each table is in a directory:

```
 data/sql/<dataset>/tables/<name>/
```

The metadata is in `meta.toml` and the view defintion is in `table.toml`.

The `table.toml` file has the following structure:

```toml
[field_name]
type = <type>
mode = <mode>
```

Here `type` is the SQL datatime e.g. `INTEGER` or `DATETIME`, and
`mode` is `NULLABLE`, `REQUIRED` or `REPEATED`, defaulting to
`NULLABLE`.

The TOML file may be templated using Jinja templates; templates have
access to `metric_types` which is a list of `MetricType` objects and
`metrics` which is a dictionary mapping metric names to `Metric`
objects.

## Views

Each view is in a directory:

```
 data/sql/<dataset>/views/<name>/
```

The metadata is in `meta.toml` and the view defintion is in `view.sql`.

`view.sql` is processed as a Jinja template. It has access to the same
`metrics` and `metric_types` entries as for tables, plus `dataset`
which the name of the view's dataset, `name` which is the name of the
view, and `ref` which is a function used for constructing references
(see below).

## Routines

Each routine is in a directory:

```
 data/sql/<dataset>/routines/<name>/
```

The metadata is in `meta.toml` and the view defintion is in `routine.sql`.

`routine.sql` is processed as a Jinja template with access to the same
variables as for views.

Routines _must_ start with the SQL to create a function; in general
the file is expected to start with the string:

```
CREATE OR REPLACE FUNCTION `{{ ref(name) }}`
```

followed by the routine arguments, return type and query e.g.

```
CREATE OR REPLACE FUNCTION `{{ ref(name) }}(input STRING) return STRING AS (
  SELECT input
)`
```

## References

In views and routines, references to other tables, views or routines
_must_ be made using the `ref()` function in templates. It is an error
to directly reference another schema. This allows references to be
tracked, and ensures that we create schemas after their
dependencies. It also allows references to automatically be updated
for staging vs production.

The `ref()` function takes a string, which is interpreted as a name
relative to the current schema. So for example when generating
`test_view` in `test_project` and `test_dataset`, a query like:

```
SELECT `{{ ref("test_function") }}`(column)
FROM `{{ ref("other_dataset.table" }}}`

```

will be converted into:

```sql
SELECT `test_project.test_dataset.test_function`(column)
FROM `test_project.other_dataset.table`
```

## Testing changes

Changes can be tested using the `webcompat-etl` command with the
`update-schema` job and the `--stage` command line
argument. This will write changes to copies of the prod datasets
suffixed with `_test`.

The full command is:

```
uv run webcompat-etl --fail-on-error --bq-project moz-fx-dev-dschubert-wckb --bq-kb-dataset webcompat_knowledge_base --stage update-schema
```

To avoid writing to the remote at all, but just see what would be
deployed, use `--no-write` (note that this works better with tables
and views than routines, as routines are always redeployed and we
currently can't generate a diff at all.
