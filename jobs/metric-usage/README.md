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

Use is counted over windows of 30, 90 and 180 days ending yesterday. 180 is
the longest because `JOBS_BY_PROJECT` keeps no more.

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
`adhoc.py` lists, and write access to `mozdata.analysis`. The Looker API key
is read from the system keyring as `looker-api-client-id` and
`looker-api-client-secret`, or from `LOOKERSDK_CLIENT_ID` and
`LOOKERSDK_CLIENT_SECRET` where there is no keyring, as in the container.

Tables are written to `mozdata.analysis` with the prefix
`benwu_metrics_usage_`. `METRIC_USAGE_PREFIX` gives a working copy its own
set, and has to start with that prefix.

Everything fetched is cached under `.cache`, so a rerun only asks for what
changed. HTTP responses and query results are keyed on the URL or the SQL and
kept until a step is run with `--force`; a query of the job's own tables is
kept until one of them is rewritten. Looker's System Activity is kept for the
day, its generated SQL until `--refresh-sql`, and each source's resolved
results until lineage is rebuilt. A container that starts with an empty cache
fetches and resolves everything again.

## Handling query text

Redash queries, ad hoc queries from job history and the SQL Looker generates
are read in full to be parsed. None of it is written to BigQuery. Only
metric ids from the inventory and artifact ids reach the tables, never column
aliases or string literals from a query. Redash and ad hoc text stays in
memory and is never cached; ad hoc text never passes through
`common.query()`, which caches results. Looker's generated SQL is cached
under `.cache`.

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
