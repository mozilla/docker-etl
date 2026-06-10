# Firefox Application Update Out Of Date dashboard job

This job produces the data behind the Firefox Application Update Out Of Date
dashboard (<https://telemetry.mozilla.org/update-orphaning/>), a public dashboard
that tracks how much of the Firefox release population is running an out of date
version, and of those, how many are stuck in a way the team can act on: updates
disabled, an unsupported OS, repeated download or staging failures, and so on.

The dashboard is a static site that fetches a per-report week JSON file from
`gs://moz-fx-data-static-websit-8565-analysis-output/app-update/data/out-of-date/`
(served at
`https://analysis-output.telemetry.mozilla.org/app-update/data/out-of-date/`).
This job computes that JSON file from Firefox telemetry and uploads it.

## What the job produces

One file per run, named for the report week's Sunday (`<YYYYMMDD>.json`). The
dashboard's Report Week dropdown lists the files that exist, and each one holds a
self-contained snapshot for that week:

| Section          | What it holds                                                                                           |
| ---------------- | ------------------------------------------------------------------------------------------------------- |
| `reportDetails`  | the run's parameters: latest version, the report and subsession date windows, the ping count thresholds |
| `summary`        | the top level version counts over the whole report week population: up to date, out of date, too low, too high, missing |
| categorized keys | counts of the out of date clients broken down by the reasons they are or are not "of concern"           |

The categorized keys are the main part of the report. They walk the out of date
clients through a funnel (do they have a recent enough version, enough update
pings, a long enough session, a supported OS, the ability to apply an update,
updates enabled) and then bucket the survivors by what is keeping them out of
date. The keys and their boolean or enumerated sub keys match the legacy output
exactly, since the same frontend reads both.

## How it works

Two stages: aggregate in BigQuery, categorize in Python.

1. SQL (`update_orphaning_dashboard/sql/`) reads the 1% sample
   (`sample_id = 42`) of `moz-fx-data-shared-prod.telemetry.main` over a roughly
   six month window. Two queries:
   - `summary.sql` returns the five top level version counts over the report
     week population, computed with `COUNTIF`. This deliberately leaves out the
     major version filter, because the summary describes everyone, not just the
     candidates.
   - `out_of_date_details.sql` aggregates each client's pings into a most recent
     first "longitudinal" shape and applies the out of date filter (Firefox,
     release channel, a valid version that is at least two releases behind), so
     only the candidate clients come back. The histogram reshaping the legacy
     Spark UDFs did is done here too, in SQL.

2. Python (`update_orphaning_dashboard/`) runs the queries and finishes the job:
   - `processing.Ping` parses each detail row's histogram columns back into the
     per-ping lists and keyed dicts the categorization expects.
   - `processing.categorize` streams the clients through the per-client funnel
     and counting, and returns the categorized count dicts.
   - `main.py` assembles those plus the summary into the report dict, serializes
     it, and uploads it to GCS (or writes it locally under `--dry-run`).

### Histograms travel as JSON strings

A client's histograms are arrays, and per client they are arrays of arrays (one
per ping). BigQuery will not `ARRAY_AGG` an array-typed column, and will not
return a nested array from a subquery. So `out_of_date_details.sql` reduces each
histogram in SQL and emits each column as a single JSON string per client via
`TO_JSON_STRING(ARRAY_AGG(...))`. `processing.Ping` then does exactly one
`json.loads` per column. This flat, all-string output schema is also what lets
the job read results through the fast BigQuery Storage API (see "Reading results"
below).

The enumerated histograms are kept sparse: each ping carries only its non-zero
buckets as `{index: count}` rather than a dense `[0..n]` array. Real histograms
have a median of one non-zero bucket (the largest tops out around four), so the
dense form was almost entirely zeros and, with up to 1000 pings per client,
inflated to many gigabytes once parsed into Python objects. The mappers only ever
test whether a bucket is non-zero and use its index, so a sparse dict where an
absent index reads as 0 is exactly equivalent. `categorize` also consumes the
result row by row from a streaming Arrow iterator (`main.iter_query_rows`) and
classifies each client before discarding it, so peak memory is one client rather
than the whole result set. Together these keep the details pass within the GKE
memory budget.

### Report dates

`ReportDates` derives everything from `--run-date` (the Airflow `ds_nodash`, a
Monday by schedule): the report week (the prior Sunday through Saturday), the
six month aggregation window, and the date used to look up the latest Firefox
major version from product details
(`firefox_history_major_releases.json`). This reproduces the date math at the
top of the legacy job.

### Reading results

The details query returns roughly 100k rows whose histogram columns are large
JSON strings. The default REST row iterator deserializes those one row at a time,
which takes about fifteen minutes; the Storage API streams them as Arrow in well
under a minute. The flat all-string schema above is what makes the Storage API
usable here, since it rejects the deeply nested record schema the legacy job's
transport needed.

## Running locally

```bash
pip install -r requirements.txt
pip install -e .
# --dry-run writes the JSON to ./test_output/ instead of uploading to GCS:
python -m update_orphaning_dashboard.main --run-date 2026-06-08 --dry-run
```

Authenticate with `gcloud auth application-default login`. `--dry-run` writes
the file to `test_output/` (override with `--test-output-dir`) instead of
uploading, and it is byte for byte what would be uploaded, so you can inspect or
diff it. `--billing-project` selects the GCP project the BigQuery queries run and
bill in (they read `moz-fx-data-shared-prod` by fully qualified name regardless);
it defaults to `mozdata`, which is what most people running this locally will
want. The CLI uses [click](https://click.palletsprojects.com/); run with
`--help` for all options.

### Verifying visually in the dashboard

`serve_frontend.py` renders a dry run's output in the actual dashboard UI. It
fetches a fresh copy of the [dashboard
site](https://github.com/mozilla/telemetry-dashboard/tree/gh-pages/update-orphaning)
at run time, patches it to read from your local dry run output, makes the dry
run's report dates selectable in the Report Week dropdown, and serves it over
HTTP (so nothing stale is checked in):

```bash
# Generate the JSON (writes to test_output/ by default):
python -m update_orphaning_dashboard.main --run-date 2026-06-08 --dry-run
# Then fetch, patch, and serve the frontend over that output:
python serve_frontend.py   # then open http://localhost:8000/ and pick the "(local)" date
```

By default it renders `test_output/`; point at a different dry run directory with
`--data-dir`, or change the port with `--port`. It is a local verification aid
only and is not part of the job's container. Run with `--help` for all options.

### Tests

```bash
pytest
black --check .
flake8 .
```

---

# Migration notes

This job replaces the legacy Spark/Dataproc job
[`jobs/update_orphaning_dashboard_etl.py`](https://github.com/mozilla/telemetry-airflow/blob/main/jobs/update_orphaning_dashboard_etl.py)
in telemetry-airflow. That job ran on a 20 node Dataproc cluster, but nothing in
it was actually distributed work. It aggregated the 1% sample in BigQuery,
dumped the result to AVRO in GCS, loaded the AVRO into Spark only because the
BigQuery Storage API could not handle the schema, used Spark UDFs to reshape
histograms, and then ran an RDD map/filter/`countByKey` pipeline of row
independent Python functions. The output JSON, the dashboard frontend, and the
GCS location are all unchanged.

## Key decisions

- Do it without Spark. After the report week, version, and channel filters the
  working set is only about 10k to 15k clients, which fits comfortably in
  memory. There is no temp table, no AVRO dump, no Spark, and no Dataproc
  cluster.
- Aggregate in BigQuery, thin client. The histogram reshaping and the out of
  date filter both run in SQL, so only the candidate clients come back. The
  Python only finalizes histograms and runs the categorization funnel. The
  legacy `countByKey` becomes `collections.Counter` and the `.filter(...)`
  stages become list comprehensions.
- Two queries, split by population. `summary.sql` describes the whole report
  week without the major version filter; `out_of_date_details.sql` returns only
  the out of date candidates. They are kept separate because they answer
  different questions, not for performance.
- docker-etl only, no bigquery-etl. This is a low importance job not expected to
  see further development, so it optimizes for the lowest operational surface:
  the SQL is embedded in the package and run ad hoc rather than promoted to a
  scheduled derived table. Query cost is handled by on demand billing
  (`--billing-project`). We give up catalog and lineage visibility, which only
  matters under active development.
- The CLI uses `click` (the repo standard), not the legacy job's argument
  parsing.

## Output parity is exact, on purpose

The same dashboard frontend reads this output, so it has to match the legacy
output, including some quirks worth calling out:

- The categorization functions in `processing.py` are ported as close to
  verbatim as possible, including bug for bug behaviors. For example
  `_has_min_update_ping_count` advances its index by two per qualifying ping, so
  a client effectively needs about twice `min_update_ping_count` pings of the
  current version to pass. This is preserved deliberately.
- Histogram densification reproduces the legacy fixed lengths (`n_values + 1`)
  and the "all pings null produces `None`" rule.
- `main.to_json` reproduces the python2 era boolean key capitalization: the
  dict keyed counts have Python `True`/`False` keys, which `json` renders as
  `"true"`/`"false"`, but the legacy job emitted them capitalized as `"True"`
  and `"False"`. The frontend reads the capitalized form, so the serializer
  rewrites them after dumping.

## Validating against the legacy output

Run a `--dry-run` for a recent report week and diff the JSON against the file the
legacy job produced for the same date:

```bash
python -m update_orphaning_dashboard.main --run-date 2026-06-08 --dry-run
fn=$(ls -t test_output/*.json | head -1 | xargs basename)
curl -s "https://analysis-output.telemetry.mozilla.org/app-update/data/out-of-date/$fn" > /tmp/prod.json
diff <(jq -S . /tmp/prod.json) <(jq -S . "test_output/$fn")
```

Both jobs read the same 1% sample, so the counts should line up closely. Small
differences might be from shredder; large differences in any category signal a regression.
