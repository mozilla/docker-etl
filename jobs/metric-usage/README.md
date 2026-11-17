# Metric usage

Finds every place a Glean metric or legacy telemetry probe is used, so whoever
owns a metric or depends on one can see what would break before changing or
removing it. The job parses SQL from each source below with sqlglot, follows
each metric column by column through bigquery-etl's lineage, and writes
BigQuery tables that a page reads per metric.

| Source               | What is read                                                                                                                                            |
|----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|
| bigquery-etl         | Every deployed query and view, from the `generated-sql` branch, and each table's last write and reads from `monitoring_derived.jobs_by_organization_v1` |
| Redash               | Live saved queries on BigQuery data sources, with dashboards, owners and views from `stmo_external`                                                     |
| Looker               | Dashboards, Looks and explore queries through the SQL Looker generates for them, use from System Activity, and the metrics each explore exposes         |
| GLAM                 | How often each Firefox Desktop and Fenix metric was opened in GLAM, as a band                                                                           |
| metric-hub           | Metric definitions, the OpMon projects using them, and the Jetstream experiments that chose them                                                        |
| BigQuery job history | Queries people and tools ran themselves, from `INFORMATION_SCHEMA.JOBS_BY_PROJECT` in nine projects, per account                                        |

Use is counted over windows of 30, 90, 180 and 365 days ending yesterday; see
[Time windows](#time-windows) for what each source counts in them.

## Time windows

Every window ends yesterday, the last whole day (`common.WINDOWS`). Each source
reads the longest window once and counts each window within it, so the page
switches windows without a rebuild. 365 is the longest because Looker's System
Activity keeps no more. Every step ends its windows on the day before it runs,
so all of them should run on the same day; `summary` writes the days each
window covers to `windows`.

A ref, a metric used by an artifact, is one of two kinds. One from code or
saved content (a bigquery-etl query, a saved Redash query, a dashboard tile, a
metric-hub definition) holds in every window, whether or not anything ran it,
and its `seen_within` is null. One that exists only because of use (an ad hoc
query, a Looker query that was run, an experiment, an OpMon project) has
`seen_within`, the shortest window it was seen in, and counts in that window
and the longer ones. Use is counted apart from refs: an artifact's
`views_<n>d` and `users_<n>d` are its use in each window, and a ref's
`observed_days_<n>d` the days in each window with a use.

| Source | Refs | Use per window | Notes |
|---|---|---|---|
| bigquery-etl | From code, in every window | Each table's reads and readers per window, and its last successful write (`last_run`) and whether Airflow wrote it (`is_scheduled`) in the longest, from `jobs_by_organization_v1` | Views have no run history, since the view deploy rewrites every view weekly. Tables in `moz-fx-data-experiments` have none either, since `jobs_by_organization_v1` does not cover that project |
| Redash | From saved queries, in every window; a dashboard's from its queries | Views and distinct viewers per window from `object_views_daily`, which starts on 2025-09-17; `last_used` is the last day in the longest window with a view | `last_run` (the latest result) and `is_scheduled` (a schedule not past its end) are as of the run, not per window |
| Looker | From dashboard tiles and saved Looks, in every window. From a query run on a dashboard, Look or explore, from the shortest window with a run of one naming the metric. An explore's fields, as refs that are not named, in every window | Runs and users per window from System Activity history; `last_run` and `is_scheduled` in the longest | History keeps 365 days back from the time of the run, so the oldest day of the longest window can be partial |
| GLAM | One per metric opened in GLAM | Days with a fetch per window (`observed_days_<n>d`) and over the year (`observed_days`); `top_percent` ranks the year | The usage log is read for the 365 days ending yesterday, UTC |
| metric-hub | Definitions, in every window. An OpMon project's from the shortest window its start and end dates overlap, so one still running is in all of them | Projects whose dates overlap the longest window | A project's `last_observed` is its end date, capped at the window's end, or the window's end if it has none |
| BigQuery job history | Per account and per Jetstream experiment, from the shortest window with a query naming the metric | Days per window with such a query, and jobs per window (`views_<n>d`) | Covers only the days in `adhoc_jobs`; see below |

`JOBS_BY_PROJECT` keeps only 180 days, so `adhoc.py` keeps the job list itself
in `adhoc_jobs`, a partition per day, kept for the longest window. Every run
adds the days `JOBS_BY_PROJECT` has whole that the table lacks, so the first
run fills the last 179 days, a daily run adds yesterday, and a run after a gap
fills it, as long as the gap is under 179 days. Ad hoc use goes back only as
far as the table does, so until it holds a year its 365-day counts are its
180-day ones. A query whose last run is older than `JOBS_BY_PROJECT` keeps the
result it was resolved to while its text was there, since it cannot be
resolved again. To capture days again, delete their partitions from
`adhoc_jobs` and run the step; only days still in `JOBS_BY_PROJECT` can be.

In `summary`, a metric's counts for a window take every ref that holds in it.
`definitions` counts a metric as used in a window by an ad hoc query from a
person or service account, a Jetstream experiment or OpMon project that chose
it, a Looker dashboard, Look or explore query or saved Redash query that names
it and was viewed or run in the window, or a GLAM page opened in the window.
Its counts of saved content that would need fixing (tables, views, Redash
queries, Looker content, metric-hub metrics) and its `last_used` are over the
longest window, the same in every window.

## Layout

```
metric_usage/        the package; each step is a module run with python -m
  common.py          shared helpers, table names, and the refs and artifacts schemas
  inventory.py       metrics table: every metric and probe from probeinfo, with its columns
  lineage.py         the resolver, and bigquery-etl lineage (carries, refs_bqetl)
  runs.py            bigquery-etl tables' last write and reads
  metric_hub.py      metric-hub metrics and OpMon projects; experiment configs
  redash.py          Redash saved queries and dashboards
  looker.py          Looker through the API; looker_api.py holds the client
  glam.py            how often metrics are opened in GLAM
  adhoc.py           ad hoc queries from BigQuery job history, per account
  summary.py         per-metric counts for the page's search and verdict line
  accuracy.py        text-search comparison on a frozen sample; check.py scores it
script/
  run_all.sh         every step, in order
  fetch_generated_sql.sh, fetch_metric_hub.sh
                     shallow clones of bigquery-etl's generated-sql branch and metric-hub
tests/               offline resolver tests
```

## Running

Each step is a module: `python -m metric_usage.<step>`. Order matters, and
`script/run_all.sh` keeps it:

1. `script/fetch_generated_sql.sh` and `script/fetch_metric_hub.sh`
2. `inventory`
3. `lineage`, which the other sources resolve against through its `carries` table
4. `metric_hub`, whose OpMon projects and experiment configs `looker` and `adhoc` use
5. `redash`, `looker`, `glam` and `adhoc`, in any order
6. `summary`, which reads every source's tables

The image runs `script/run_all.sh`. A scheduler can instead run each step as
its own task in the same image, with the fetch scripts first, so a failed
source can be retried without rebuilding the rest.

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
warehouse and the tables above, `bigquery.jobs.listAll` in the projects
`adhoc.py` lists, and write access to the dataset it writes. The Looker API key
is read from the system keyring as `looker-api-client-id` and
`looker-api-client-secret`, or from `LOOKERSDK_CLIENT_ID` and
`LOOKERSDK_CLIENT_SECRET` where there is no keyring, as in the container.

Tables are written to `mozdata.analysis` with the prefix
`benwu_metrics_usage_`. `METRIC_USAGE_DATASET` (as `project.dataset`) moves
them, and `METRIC_USAGE_PREFIX` gives a working copy its own set; in
`mozdata.analysis` the prefix has to start with `benwu_metrics_usage_`. A
table created in a dataset with a default expiration (180 days in analysis)
takes it from its creation, and rewriting it does not renew it.

The job runs with no disk that outlives it, so what is worth keeping between
runs is kept in BigQuery. `adhoc.py` is the step that needs it, since a cold
run resolves every distinct query in the job history and most of
them are the same from one day to the next. It keeps them in
`resolved_adhoc`: each entry there is a query's key, its refs, and for each
table it names the columns of it the query mentions with their digest, and it
is reused while those digests and the resolver's own hold. A metric added
elsewhere in a table leaves it alone. An entry is also redone after 15 to 30
days if its query still runs, in case a dependency was missed. No query text
is kept, only keys and schema names. `inventory`, `lineage`, `metric_hub`,
`redash` and `glam` take a few minutes each from nothing, so they keep
nothing between runs.

Locally, `.cache` keeps what each step fetched from run to run: HTTP
responses and query results are keyed on the URL or the SQL and kept until a
step is run with `--force`, and a query of the job's own tables is kept until
one of them is rewritten. Looker's System Activity is kept for the day and its
generated SQL until `--refresh-sql`. The image sets `METRIC_USAGE_CACHE=0`,
which keeps the same cache in memory for each step instead, since a container
that runs once would only fill its disk with it; a later step then fetches
again what an earlier one did, which costs a few BigQuery reads. Either way
`.cache` holds the generated-sql and metric-hub clones.

## Handling query text

Redash queries, ad hoc queries from job history and the SQL Looker generates
are read in full to be parsed. None of it is written to BigQuery. Only
metric ids from the inventory and artifact ids reach the tables, never column
aliases or string literals from a query. Redash and ad hoc text stays in
memory and is never cached; ad hoc text never passes through
`common.query()`, which caches results. `resolved_adhoc` holds each ad hoc
query's key (a hash of its text) and the schema columns it mentions, never
the text. Looker's generated SQL is cached under `.cache`.

`adhoc.py` stores the email of each account whose own queries named a metric,
since knowing who reads a metric is what lets someone ask before changing it.
`jobs_by_organization_v1` already publishes the same emails to the same
audience.

## Tests and accuracy

```sh
.venv/bin/python -m pytest
```

The tests need no BigQuery and no fetch: each lists the columns of the tables
a query reads and the metric each holds, resolves the query, and checks what
each output column carries. Persistent UDFs and whole queries the tests
expand are copied from bigquery-etl into `tests/fixtures`. Redash, Looker and
ad hoc query text never goes there; a test uses a synthetic query with the
same pattern instead.

`python -m metric_usage.accuracy` compares the parser with a plain text search
on a frozen, hand-labelled sample of metrics, and `python -m
metric_usage.check` scores it. They write `labels/`, which holds snippets of
query text for review and is not in git.

## Scope

Event extra keys are not matched. A query on an extra is
credited to the event it names, which it nearly always does, but the key
itself is not recorded. Extras are part of their event's definition, so the
event's page already answers who uses them.

SQL that cannot be parsed (Python that builds SQL, Redash scripts with control
flow, broken SQL) falls back to matching names, which skips columns that
carry five or more metrics.
