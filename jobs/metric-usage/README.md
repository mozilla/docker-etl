# Metric usage

Finds every place a Glean metric or legacy telemetry probe is used, so whoever
owns a metric or depends on one can see what would break before changing or
removing it. The job parses SQL from each source below, follows each metric
column by column through bigquery-etl's lineage, and writes BigQuery tables
that a page reads per metric.

Frontend: [bigquery-metric-lineage](https://github.com/mozilla/bigquery-metric-lineage),
deployed at https://benwu-metric-usage.quick.mozilla.cloud/.

| Source               | What is read                                                       | Counted per window                            |
|----------------------|--------------------------------------------------------------------|-----------------------------------------------|
| bigquery-etl         | Every deployed query and view (the `generated-sql` branch)         | Reads and readers of each table               |
| Redash               | Live saved queries on BigQuery, with their dashboards and owners   | Views and viewers (recorded since 2025-09-17) |
| Looker               | Dashboards, Looks and explores, through the SQL Looker generates   | Runs and users, and the queries run           |
| GLAM                 | Firefox Desktop and Fenix metrics opened                           | Days each metric was opened                   |
| metric-hub           | Metric definitions and the OpMon projects using them               | OpMon projects active in the window           |
| BigQuery job history | Queries run directly by people, tools and Jetstream                | Days and jobs with a query naming the metric  |

## Time windows

Use is counted over windows of 30, 90, 180 and 365 days ending yesterday (UTC,
matching BigQuery dates). Code and saved content, such as a bigquery-etl query or
a Looker tile, counts in every window whether or not it ran; views, runs and ad
hoc queries count only in the windows they happened in.

`INFORMATION_SCHEMA.JOBS_BY_PROJECT` keeps 180 days, so `adhoc` appends each new
day to `adhoc_jobs`, and ad hoc history grows to a year over a year of runs. A
gap of more than 179 days between runs is lost; to recapture days, delete their
partitions from `adhoc_jobs` and rerun `adhoc`.

## Tables

All are written to `moz-fx-data-shared-prod.metric_usage_tracking_derived`, each
name with the suffix `_v1` (see [Settings](#settings)). Each step replaces its
tables on every run, except `adhoc_jobs`, which is appended to.

The tables are defined in bigquery-etl, with a `metadata.yaml` and `schema.yaml`
each and no query. bigquery-etl does not create or alter such a table, so the job
creates each one on its first run and its schemas (`ref_schema()` and the others)
are the ones that hold. A change to a schema here needs the same change to the
table's `schema.yaml` there. The `refs_v1` and `artifacts_v1` views over the
sources' tables are bigquery-etl views too, and a new source has to be added to
them and to `SOURCES`. The page reads the user-facing views over these tables in
`mozdata.metric_usage_tracking`. bigquery-etl sets each table's description from
its `metadata.yaml`, so the job sets descriptions only in other datasets.

| Table                                          | One row per                                                     |
|------------------------------------------------|-----------------------------------------------------------------|
| `metrics`                                      | Metric or probe per app, with the columns that hold it          |
| `carries`                                      | Warehouse table column and a metric it carries                  |
| `refs_<source>`, and the `refs` view           | Metric used by an artifact (a table, query, dashboard, account) |
| `artifacts_<source>`, and the `artifacts` view | Artifact, with its owners and use per window                    |
| `summary`                                      | (window, app, metric), for the page's search                    |
| `definitions`                                  | (window, metric definition), for the page's little-used list    |
| `windows`                                      | Window, its first and last day, and where job history starts    |
| `adhoc_jobs`                                   | (day, project, query key, account), kept 366 days               |
| `resolved_adhoc`                               | Ad hoc query key and the metrics it resolved to                 |

`<source>` is `bqetl`, `redash`, `looker`, `glam`, `metrichub` or `adhoc`.

## Running

Each step is a module, `python -m metric_usage.<step>`, run in this order by
`script/run_all.sh`:

1. `script/fetch_generated_sql.sh` and `script/fetch_metric_hub.sh`
2. `inventory`, then `lineage`, then `metric_hub`
3. `redash`, `looker`, `glam` and `adhoc`, in any order
4. `summary`

Run them all on the same day, since each ends its windows yesterday. A
scheduler can run each step as its own task in the same image, fetch scripts
first, so a failed source can be retried alone.

```sh
docker build -t metric-usage .
docker run metric-usage                                   # every step
docker run metric-usage python -m metric_usage.looker     # one step
```

Locally, with Python 3.14, from this directory:

```sh
python3.14 -m venv .venv && .venv/bin/pip install -r requirements.txt
script/run_all.sh                                    # fetch, then rebuild every table
FETCH=0 script/run_all.sh                            # same, without refreshing the clones
.venv/bin/python -m metric_usage.<step> --dry-run    # most steps: everything but writing
```

`adhoc` keeps what it resolved in `resolved_adhoc`, so a daily run only resolves
new queries. Any edit to `lineage.py` or `redash.py` changes the resolver's digest,
and the next `adhoc` run resolves every query again (about 15 minutes and 60 GiB
of query text read).

The job needs Google application default credentials that can read the
warehouse, `bigquery.jobs.listAll` in the projects `adhoc.py` lists, and write
access to its dataset. The Looker API key is read from the system keyring as
`looker-api-client-id` and `looker-api-client-secret`, or from
`LOOKERSDK_CLIENT_ID` and `LOOKERSDK_CLIENT_SECRET`, as in the container.

### Settings

| Env Variable           | Default                                                 | Meaning                                                                                                                |
|------------------------|---------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------|
| `METRIC_USAGE_DATASET` | `moz-fx-data-shared-prod.metric_usage_tracking_derived` | `project.dataset` the tables are written to                                                                            |
| `METRIC_USAGE_PREFIX`  | empty                                                   | Table name prefix, for a working copy; empty in the default dataset, and starting with `benwu_metrics_usage_` in `mozdata.analysis` |
| `METRIC_USAGE_CACHE`   | `1` (`0` in the image)                                  | `0` keeps the cache in memory instead of under `.cache`                                                                |
| `METRIC_USAGE_WORKERS` | usable CPUs                                             | Worker processes that parse queries; by default the CPUs within any cgroup CPU limit                                   |

A working copy, such as `METRIC_USAGE_DATASET=mozdata.analysis
METRIC_USAGE_PREFIX=benwu_metrics_usage_mybranch_`, gets its own `refs_v1` and
`artifacts_v1` views from the job, and its tables expire 180 days after they are
created (the analysis dataset's default), however often they are rewritten.

### When a source fails

A step that can't fetch what it needs (persistent probeinfo or BigQuery errors,
or more than 1% of Looker queries failing transiently) exits without writing, so
its tables keep the last complete run and it can be rerun alone. Projects the
account can't read and app variants missing from probeinfo are skipped.

Each artifact's `note` says how its SQL was read: `ok` (fully evaluated),
`partial: N scopes unread` (a metric named only in those subqueries is missed),
`resolve failed: worker died` (recorded, not retried), or the fallback used,
such as regex.

## Handling query text

Redash, ad hoc and Looker-generated SQL is parsed in memory and never written to
BigQuery; Redash and ad hoc text is not cached either. Tables hold metric and
artifact ids with metadata such as titles, owners and accounts. `resolved_adhoc`
keys each ad hoc query by a hash and lists only the bigquery-etl tables and
columns it mentions.

## Tests and accuracy

```sh
.venv/bin/python -m pytest
.venv/bin/black --check . && .venv/bin/flake8 .
```

CI runs both in the image, and fails on either.

The tests are offline, with synthetic queries; Redash, Looker and ad hoc query
text never goes in `tests/fixtures`. `python -m metric_usage.accuracy` compares
the parser with a plain text search on a hand-labelled sample, and
`python -m metric_usage.check` scores it; both write only to `labels/`, which is
not in git.

## Scope

Event extra keys are not matched: a query on an extra is credited to its event.

SQL that cannot be parsed falls back to matching names, which skips columns
that carry five or more metrics.
