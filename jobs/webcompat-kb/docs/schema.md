# Schema Definitions

The definitions for views and routines are stored under `sql`. Each
definition consists of a `toml` file containing metadata relating to
the schema, and a `sql` file containing the definition, using
[jinja](https://jinja.palletsprojects.com/en/stable/templates/) as a
templating language.

## Metadata

Metadata is stored as [TOML](https://toml.io/en/) files, and is
largely common between views and routines. The following fields are
supported:

* _name_ - The name of the view or routine
* _desription_ - A human-readable description of the view/routine

## Views

Each view is in a directory:

```
 sql/<dataset>/views/<name>/
```

The metadata is in `meta.toml` and the view defintion is in `view.sql`.

## Routines

Each routine is in a directory:

```
 sql/<dataset>/routines/<name>/
```

The metadata is in `meta.toml` and the view defintion is in `routine.sql`.

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

## Testing changes

Changes can be tested using the `webcompat-etl` command with the
`update-schema` job and the `--update-schema-stage` command line
argument. This will write changes to copies of the prod datasets
suffixed with `_test`.

The full command is:

```
uv run webcompat-etl --fail-on-error --bq-project moz-fx-dev-dschubert-wckb --bq-kb-dataset webcompat_knowledge_base --update-schema-stage update-schema
```

To avoid writing to the remote at all, but just see what would be
deployed, use `--no-write` (note that this works better with views
than routines, as routines are always redeployed and we currently
can't generate a diff at all.
