# Redash Definitions

The definitions for Redash queries are stored under `data/redash`.

The directory structure is:

```
data/redash/<dashboard>/<query>
```

## Dashboards

By convention queries are grouped in to "Dashboards". This is
currently for organizational purposes only, there isn't any
relationship between a dashboard directory and an actual Redash
dashboard. However it's generally convenient to group together all
queries relating to a specific dashboard.

Dashboard metadata is stored in a `meta.toml` file. The following
fields are supported:

* _name_ - The human-readable name of the dashboard
* _description_ - A human-readable description

Dashboards may also define parameters that are available in all
queries, using a `parameters.toml` file which matches the format used
for queries (below).

## Queries

Queries correspond directly to upstream Redash queries. Each query is composed of two or three files:

* `meta.toml`- An untemplated TOML file containing metadata about the query
* `query.sql` - A Jinja2-templated SQL query definition
* `parameters.toml` [optional] - A Jinja2-templated TOML file defining query parameters

### Query Metadata `meta.toml`

Queries can contain the following metadata:

* `id` - An optional integer id specifying the Redash query
  id. Typically this is unset until the query is deployed to Redash
  and then set automatically by the deployment code. If this is not
  specified deployment will create a new query.

* `name` - A human-readable name for the query.

* `description` - An optional description for the query.

### Query Definition `query.sql`

`query.sql` is processed as a Jinja template. It has access to the following variables:

* `metrics` - A dictionary of `{metric_name: Metric}`
* `metric_types` - A list of `MetricType` values
* `dashboard_metrics` - A list of `Metric` objects that have the name
  of the current dashboard as part of their `dashboards` property.
* `ref` - A function for generating a schema reference (by adding the
  default project id, if none is present).
* `param` - A function for generating a parameter reference, including
  the `{{` `}}` delimiters that clash with Jinja syntax.

### Parameter Definition `parameters.toml`

Parameters are defined in a TOML file in the following format:

```toml
[parameter_name]
type = <parameter type>
title = <string>
value = <Optional parameter-type-specfic>
```

Depending on the specified type some other fields may be available or required. The possible types are:

* `text` - `value` is a string
* `number` - `value` is a number
* `enum` - `enum_options` must be provided, which is a list of string values representing choices for the enum. `value` is a string, or a list of strings if `multi_values_options` is provided.
* `query` - `query_id` must be provided. It is an integer query id referencing a Redash query that will return the parameter options. `value` and `multi_value_options` work as for `enum`
* `date` - `value` is a string representing a date in iso format.
* `datetime-local` - `value` is a string representing a datetime in iso format.
* `datetime-with-seconds` - `value` is a string representing a datetime in iso format.
* `date-range` - `value` is a range object (see below) with start and end strings representing dates in iso format.
* `datetime-range` - `value` is a range object (see below) with start and end strings representing datetimes in iso format.
* `datetime-range-with-seconds` - `value` is a range object (see below) with start and end strings representing datetimes in iso format.

#### Multi-valued Options

Enums that take more than one value must specify how values are combined. This is done with a TOML table with the following properties:

* `prefix` - A string representing the initial delimiter. Can be the empty string, `"` or `'`.
* `suffix` - A string representing the final delimiter. Can be the empty string, `"` or `'`.
* `seperator` - A string representing the character used to separate entries.

#### Range objects

Date and datetime ranges are specified using a TOML table with `start` and `end` properties.

## Deploying Changes

Due to the lack of service accounts Redash dashboards aren't
automatically deployed by the ETL. Instead they must be deployed
manually using the command:

```sh
uv run webcompat-update-redash --bq-project <project> --redash-api-key=<key>
```

This supports a `--no-write` parameter to render the templates without
trying to deploy the results to Redash.

Your Redash API key can be found on your user profile page in
Redash. You must have write access to all the queries you are trying
to deploy.

If deployment creates a new Redash query the corresponding `meta.toml`
will be updated with the query id. This change must then be committed
to the repository so that future deployments target the existing
query rather than writing a new one.

Individual dashboards can be deployed using the `--dashboard`
parameter with the name of the `data/redash` subdirectory to deploy.
