# Highwind experiment analysis

Covariate-adjusted sequential analysis of every live Firefox Desktop Nimbus experiment. Per-branch
sufficient statistics are aggregated in BigQuery and turned into confidence intervals with
[gbstats](https://github.com/growthbook/growthbook/tree/main/packages/stats), so no per-client row
leaves the warehouse.

A proof of concept. What it does not cover is under [Proof-of-concept scope](#proof-of-concept-scope).

## What it does

1. Reads the Experimenter mirror for experiments to analyse, skipping rollouts, recipes over a year
   old, recipes with no usable reference branch, and recipes whose randomization unit this job has
   no analysis unit for. Each skip is reported rather than silent.
2. Materializes one cohort of enrolled units for the whole run, then runs one query per source table
   per analysis unit, each grouped by slug. Sharing the scan matters because every experiment's date
   range overlaps almost every other's, so scanning per experiment would read the same calendar day
   repeatedly.
3. Each source query returns six aggregates per (slug, branch, window, metric): `n`, `sum`, `sumsq`,
   `pre_sum`, `pre_sumsq` and `xp`. Those are everything a covariate-adjusted mean comparison needs,
   so the per-unit stages stay inside the query that aggregates them away.
4. gbstats fits the CUPED coefficient over the two arms being compared and computes an always-valid
   two-sided interval on the relative difference.
5. Every declared cell is written, in one of five states: `not_started`, `insufficient_data`,
   `forming`, `confident` or `error`. Cells that produced nothing are written too, so an experiment
   whose queries failed cannot report a clean error rate by writing no rows at all.

Windows are anchored to each unit's own enrollment rather than to the calendar, so `cumu:2` covers a
client's own first two weeks and cells are comparable across clients who enrolled on different days.
`analysis.py`'s `run_daily_job` is the entry point, and the module docstrings carry the reasoning for
each step.

A unit is whatever the recipe randomized on, resolved in `units.py` from the mirror's `app_name` and
`randomization_unit` together. A desktop recipe on `group_id` is analysed at `profile_group_id` and
one on `normandy_id` at the client id, because a comparison whose grain is finer than assignment
reports intervals narrower than its data supports. A pair the table does not list is refused rather
than defaulted.

## What it writes

| Output | Grain |
| --- | --- |
| `moz-fx-data-experiments.monitoring.highwind_statistics_v1` | (slug, metric, window, comparison) |
| `moz-fx-data-experiments.monitoring.highwind_sufficient_stats_v1` | (slug, metric, window, branch) |
| `gs://mozanalysis/highwind/<slug>.json` | experiment, the seam Experimenter reads |

All three come from one computation, which is why they are one job rather than three. Both tables are
partitioned on `as_of_date`, and a run replaces its own date's partition rather than appending to it,
so a retry produces the same table as the first run instead of doubling it.

## Usage

```sh
docker build -t highwind jobs/highwind
docker run highwind --date 2026-08-01 --dry-run --limit 1
```

Locally, which needs Python 3.11 and gbstats on `PYTHONPATH` (the image is the easier path):

```sh
pip install -r requirements.txt && pip install -e .
python -m highwind.main --date 2026-08-01 --dry-run --limit 1
```

Requires authenticating through `gcloud` in order to query BigQuery:

```sh
gcloud auth application-default login
```

| Option | Default | Meaning |
| --- | --- | --- |
| `--date` | required | Run date. Windows mature against it, and it is the partition written |
| `--workers` | 8 | Experiments whose statistics are computed concurrently |
| `--slug` | every experiment | Analyse only this one. Repeat the flag for several |
| `--limit` | no limit | Analyse only the first N, shortest-running first |
| `--dry-run` | off | Write the per-experiment JSON to `--local-output`, skip the tables |
| `--local-output` | `test_output` | Directory `--dry-run` writes to |
| `--validate-sql` | off | Submit every query for validation without executing it, write nothing |
| `--sample-percent` | no sampling | Restrict the cohort to this percent of clients |
| `--billing-project` | `mozdata` | Project the queries run and bill in |

`--slug` and `--limit` write blobs but never tables. A filtered run is not the day's complete output,
and the table write replaces the whole partition, so letting one through would delete every
experiment the filter excluded.

`--dry-run` still runs the queries, so it costs what a real run costs; what changes is where the
output goes. `--validate-sql` is the opposite, checking that the generated SQL is well formed and
typed without executing any of it. `--sample-percent` samples whole analysis units, and widens the
intervals, so it is for development rather than for reading. Where the unit is the client id it uses
the `sample_id` the source tables are clustered on and the scan falls with the sample; where it is
not, keeping a unit whole costs that pruning, because sampling on the clustered column would split
units across the sample boundary and aggregate partial ones.

## Development

```sh
pytest
flake8 highwind/ tests/
```

The tests need no BigQuery and no network, but they do import gbstats, so run them in the image if it
is not installed locally:

```sh
docker run highwind -m pytest
docker run highwind -m flake8 highwind/ tests/
```

## Notes

gbstats comes from GrowthBook's published image at a frozen digest rather than from PyPI, because the
PyPI package of the same version pins a pandas that conflicts with the one this job runs on. Only the
Python package is copied out of that image; scipy, the one dependency it lacks, is declared in
`requirements.txt`. The base image is `python:3.11-slim` to match the frozen tree.

The job declares both table schemas itself, in `output_writing.py`, and passes them explicitly on
every load. Schema inference is off deliberately: a cell in the `error` or `not_started` state
carries no interval, so an inferred schema would vary with the day's mix of states.

## Proof-of-concept scope

- Firefox Desktop only.
- A hard-coded guardrail metric set, so the prototype has no cross-repo dependency. In the real
  system these come from metric-hub through metric-config-parser.
- A full recompute every run. Nothing is accumulated across days.
- Rollouts excluded. They have no randomized control to compare against until the synthetic-control
  work lands, and Jetstream skips them today.

The Airflow DAG that schedules this lives in
[mozilla/telemetry-airflow](https://github.com/mozilla/telemetry-airflow) and is a follow-up.
