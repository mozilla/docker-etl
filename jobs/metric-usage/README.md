# Metric usage

Finds every place a Glean metric or legacy telemetry probe is used, so whoever
owns a metric or depends on one can see what would break before changing or
removing it. The job parses SQL from each source below, follows each metric
column by column through bigquery-etl's lineage, and writes BigQuery tables
that a page reads per metric.

The frontend portion of this is in https://github.com/mozilla/bigquery-metric-lineage, deployed to
https://benwu-metric-usage.quick.mozilla.cloud/.

| Source               | What is read                                                                                  |
|----------------------|-----------------------------------------------------------------------------------------------|
| bigquery-etl         | Every deployed query and view (the `generated-sql` branch), and each table's writes and reads |
| Redash               | Live saved queries on BigQuery, with their dashboards, owners and views                       |
| Looker               | Dashboards, Looks and explore queries, through the SQL Looker generates, and their runs       |
| GLAM                 | How often each Firefox Desktop and Fenix metric is opened                                     |
| metric-hub           | Metric definitions and the OpMon projects using them                                          |
| BigQuery job history | Queries people, tools and Jetstream ran themselves, per account or experiment                 |

## Time windows

Use is counted over windows of 30, 90, 180 and 365 days ending yesterday, in UTC
wherever the job runs, since that is the day BigQuery's dates use. Code
and saved content (a bigquery-etl query, a saved Redash query, a Looker tile, a
metric-hub definition) counts in every window whether or not it ran; their
views and runs are counted per window. Use that only shows up when something
runs counts in the windows it was seen in:

| Source               | Counted per window                                                        |
|----------------------|---------------------------------------------------------------------------|
| bigquery-etl         | Reads and readers of each table                                           |
| Redash               | Views and viewers of each query and dashboard (recorded since 2025-09-17) |
| Looker               | Runs and users of each dashboard, Look and explore, and the queries run   |
| GLAM                 | Days each metric was opened                                               |
| metric-hub           | OpMon projects whose dates overlap the window                             |
| BigQuery job history | Days and jobs with a query naming the metric                              |

`INFORMATION_SCHEMA.JOBS_BY_PROJECT` keeps only 180 days, so the job keeps the
job list itself in `adhoc_jobs` and adds the days it lacks on each run. The
first run fills the last 179 days, and ad hoc use goes back only as far as that
table does, so it reaches a year after a year of runs. A gap of more than 179
days between runs is lost for good. To capture days again, delete their
partitions from `adhoc_jobs` and run `adhoc`.

## Tables

All are written to `mozdata.analysis` with the prefix `benwu_metrics_usage_`
(see [Settings](#settings)). Each step replaces its tables on every run, except `adhoc_jobs`,
which is appended to. These tables are temporary and will be put in a bigquery-etl dataset later.

| Table                                          | One row per                                                     |
|------------------------------------------------|-----------------------------------------------------------------|
| `metrics`                                      | Metric or probe per app, with the columns that hold it          |
| `carries`                                      | Warehouse table column and a metric it carries                  |
| `refs_<source>`, and the `refs` view           | Metric used by an artifact (a table, query, dashboard, account) |
| `artifacts_<source>`, and the `artifacts` view | Artifact, with its owners and use per window                    |
| `summary`                                      | (window, app, metric), for the page's search                    |
| `definitions`                                  | (window, metric definition), for the page's little-used list    |
| `windows`                                      | Window, with its first and last day                             |
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

Locally, with Python 3.11, from this directory:

```sh
python3.11 -m venv .venv && .venv/bin/pip install -r requirements.txt
script/run_all.sh                                    # fetch, then rebuild every table
FETCH=0 script/run_all.sh                            # same, without refreshing the clones
.venv/bin/python -m metric_usage.<step> --dry-run    # most steps: everything but writing
```

The job needs Google application default credentials that can read the
warehouse, `bigquery.jobs.listAll` in the projects `adhoc.py` lists, and write
access to its dataset. The Looker API key is read from the system keyring as
`looker-api-client-id` and `looker-api-client-secret`, or from
`LOOKERSDK_CLIENT_ID` and `LOOKERSDK_CLIENT_SECRET`, as in the container.

### Settings

| Env Variable           | Default                | Meaning                                                                                       |
|------------------------|------------------------|-----------------------------------------------------------------------------------------------|
| `METRIC_USAGE_DATASET` | `mozdata.analysis`     | `project.dataset` the tables are written to                                                   |
| `METRIC_USAGE_PREFIX`  | `benwu_metrics_usage_` | Table name prefix, for a working copy; in `mozdata.analysis` it has to start with the default |
| `METRIC_USAGE_CACHE`   | `1` (`0` in the image) | `0` keeps the cache in memory instead of under `.cache`                                       |
| `METRIC_USAGE_WORKERS` | usable CPUs            | Worker processes that parse queries; by default the CPUs within any cgroup CPU limit          |

A table in a dataset with a default expiration (180 days in analysis) expires
that long after it is created, however often it is rewritten.

Locally, `.cache` keeps what each step fetched between runs (`--force`
refetches). The image keeps nothing on disk between runs; `adhoc`, the one slow
step, keeps what it resolved in `resolved_adhoc` instead, so a daily run only
resolves new queries.

### When a source fails

A step that cannot fetch what it needs stops without writing, so its tables keep
the last complete run, and it can be run again on its own. That includes a
probeinfo or BigQuery error that persists after retries, a System Activity query
that keeps failing, and more than 1% of Looker's queries failing for reasons that
may pass (a timeout, a 429 or a 5xx). Two failures are expected and skipped: a
project in `adhoc.py` the account cannot read, and an app variant probeinfo does
not list.

Each artifact's `note` says how its SQL was read. `ok` means every part of it was
evaluated. `partial: N scopes unread` means some subquery or CTE could not be,
so a metric named only there is missing. `resolve failed: worker died` means
parsing it crashed its worker process; it is recorded rather than retried, so one
query does not fail every run. The other notes name the fallback used, such as
reading the text by regex.

## Handling query text

Redash queries, ad hoc queries, and the SQL that Looker generates are read to be
parsed, but the text itself is not written to BigQuery. The tables hold metric and
artifact ids and their metadata, such as titles, owners and accounts.
`resolved_adhoc` also lists the warehouse tables and columns each ad hoc query
mentions, so it can be redone when one of them changes. Only tables bigquery-etl
deploys and their columns are listed; other dotted names in the text, such as an
alias and column (`t.col`) or a literal (`example.com`), are not kept. Redash and ad hoc text 
is never cached, and `resolved_adhoc` keys each query by a hash, not its text.

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
