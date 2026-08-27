# crash-correlations

Finds attributes over-represented in a Firefox crash signature compared to all crashes on the
same channel, such as a graphics driver version, an add-on or a loaded module. Engineers
triaging a top crasher can use it to guess at a cause: if 90% of the crashes on a signature have one
driver version and only 5% of the channel does, that is a lead.

Used in the [crash stats correlations tab](https://crash-stats.mozilla.org/signature/?product=Firefox&signature=OOM%20%7C%20large%20%7C%20NS_ABORT_OOM%20%7C%20nsTSubstring%3CT%3E%3A%3AAllocFailed%20%7C%20AppendUTF16toUTF8%20%7C%20NS_ConvertUTF16toUTF8%3A%3ANS_ConvertUTF16toUTF8%20%7C%20mozilla%3A%3Adom%3A%3AserviceWorkerScriptCache%3A%3A%28anonymous%20namespace%29%3A%3ACompareManager%3A%3AWriteToCache&version=155.0b2&date=%3C2026-08-20T16%3A29%3A19%2B00%3A00&date=%3E%3D2026-08-13T16%3A29%3A19%2B00%3A00#correlations).

This replaces the PySpark `top_signatures_correlations` job in 
[python_mozetl](https://github.com/mozilla/python_mozetl/blob/main/mozetl/symbolication/top_signatures_correlations.py)
(`mozetl/symbolication/`) that pulls code from https://github.com/marco-c/crashcorrelations.
The feature table moved into BigQuery and the rest is plain Python, so there is no Spark or Dataproc.
The old job had several bugs that changed its output, so its numbers are not a reference to check
this one against; see "Changes from the old job".

## Input

|                                                              |                                                                                                                                |
|--------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| `moz-fx-data-shared-prod.telemetry_derived.socorro_crash_v2` | The crashes. One row per crash report, read over a 5 day window ending at `--date`                                             |
| `product-details.mozilla.org`                                | Which Firefox versions are live on each channel, as of `--date`                                                                |
| `crash-stats.mozilla.com` SuperSearch                        | The top signatures per channel, which is what gets analysed                                                                    |
| `searchfox.org`                                              | The app note and graphics error strings to look for, which are string literals in mozilla-central rather than a queryable list |

From each crash it takes about 50 attributes: platform and OS version, CPU, graphics adapter and
driver, process and crash reason, e10s and fission flags, locale, theme, plugin, and the loaded
modules and installed add-ons. Add-ons and modules are per crash lists rather than single values,
so they become one column per popular add-on or module.

## Output

Files land in the `moz-fx-data-static-websit-8565-analysis-output` bucket, served at
`https://analysis-output.telemetry.mozilla.org/top-signatures-correlations/data/`.
The consumer is the Crash Stats frontend, which fetches JSON over HTTP from
the browser to fill the Correlations tabs on the signature report and crash report pages (Desktop
only).

```
all.json.gz                       the run's date and per channel crash totals
addon_related_signatures.json.gz  signatures with addon correlations
<channel>/<sha1(signature)>.json.gz
```

Each per signature file, where `total` is the crashes on this signature, `count_group` the ones
carrying this attribute, and `count_reference` how often it occurs channel-wide:

```json
{
  "total": 592,
  "results": [
    {
      "item": {"moz_crash_reason": "[unhandlable oom] Failed to allocate ..."},
      "count_reference": 7624.0,
      "count_group": 260.0,
      "prior": null
    }
  ]
}
```

`prior` is set when another correlation already explains this one, so the tab can nest them: a
driver version that only shows up because of a particular graphics card points at the card. It
holds the explaining item plus its own counts, which the tab renders as the trailing clause in

```
Module "xmllite.dll" = true [100.0% vs 40.27% if platform_version = 10.0.19045]
```

Filenames are `sha1(signature.encode("utf-8")).hexdigest()`, gzipped, uploaded with
`content_encoding="gzip"` and `content_type="application/json"`. All of that has to stay
byte-compatible or the tabs break, including the counts being floats rather than ints.

Each run replaces the whole prefix, because a signature that dropped out of the top 200 has to
disappear rather than linger as a stale file the frontend would still serve. So there is no state
to migrate and a bad run is fixed by rerunning with the same `--date`. Two runs on the same input
produce byte identical output.

The whole output is about 1 MB gzipped across ~645 files. That is a ceiling rather than a number
that grows with the data, since `--top-signatures` caps it at 200 per channel.

## Running it

Writing locally is the default and uploading is opt-in, so a dev run doesn't need a bucket and write
credentials. The local layout mirrors the bucket and the bytes are identical.

The job requires Python 3.11+.

Requires authenticating through `gcloud` in order to query BigQuery:
```sh
gcloud auth application-default login
```

```sh
pip install -r requirements.txt

# all four channels, the 5 day window ending 2026-08-19
python -m crash_correlations.main --date 2026-08-19

# then look at it the way Crash Stats would display it
python render_frontend.py --data-dir test_output --signature "OOM | small"
```

A full 200 signature release run takes 1 to 2 minutes.

|                                    |                                                                                                                                                                                                                                                               |
|------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--date`                           | end of the crash window, exclusive. Defaults to today (UTC). Also scopes the version list, so a rerun for a past date reproduces that date's run                                                                                                              |
| `--channel`                        | repeat for several. Replaces the default of all four rather than adding to it                                                                                                                                                                                 |
| `--top-signatures`                 | signatures per channel, default 200. Cost grows with this, since candidates are generated per signature                                                                                                                                                       |
| `--window-days`                    | default 5                                                                                                                                                                                                                                                     |
| `--versions`                       | pin the version list instead of resolving it, e.g. `--versions 153.0.4`. Only valid with a single `--channel`, and refused against the production bucket. Needed when the resolved answer is a version that has just shipped and so has almost no crashes yet |
| `--results-bucket`                 | also upload here, without the `gs://` prefix. The prefix is cleared before uploading, so point it at a scratch bucket when testing                                                                                                                            |
| `--no-local`                       | upload only. Requires `--results-bucket`                                                                                                                                                                                                                      |
| `--output-dir`                     | default `test_output`. Cleared first                                                                                                                                                                                                                          |
| `--min-support-diff`, `--min-corr` | how aggressively candidates are filtered, 0.15 and 0.03. The defaults match the job this replaces                                                                                                                                                             |

### Which versions get analysed

Per channel, versions resolve as of `--date` from dated product-details history, which is what
makes a rerun reproducible:

| channel | source | as of a past date |
|---|---|---|
| release | `firefox_history_major_releases` + `_stability_releases` | yes, exact |
| beta | `firefox_history_development_releases` | yes, exact |
| esr | `firefox.json` `releases`, the only place esr builds are dated | yes, exact |
| nightly | `firefox_versions.json` only | no, no history is published |

Nightly is the gap. Only a current value is published, so a rerun for an older date still gets
today's build. It ships daily, so this is nearly always right for a recent window, and
`--versions` pins it when it isn't.

Only the newest shipped major is analysed, per channel, which is the version people likely monitor
when triaging crashes. Every point release within that major is included, so release resolves to
something like `['153.0', '153.0.1', '153.0.4']` rather than a single build. Note this means the
days right after a major ships cover little of the channel, since the new version starts at a
share of nearly zero: on 2026-08-19 that was 1,021 of release's 86,011 crashes. `--versions` pins
the list when that gets in the way.

esr version strings keep their `esr` suffix, so they match crash data. The old job stripped it and
resolved esr from the release history, which matched zero esr rows; see "Changes from the old
job".

## Seeing what the tab would say

`render_frontend.py` runs Crash Stats' own `correlation.js` against a bucket or a local directory
and prints the lines the Correlations tab would show. No Socorro checkout, no bundler, no browser:
that file's `getCorrelations()` returns an array of strings, so node is enough. It is also the
quickest way to see whether priors are attaching, since a missing `prior` shows up as a missing
trailing clause.

```sh
# what a dev run's output looks like on the tab
python render_frontend.py --bucket my-scratch-bucket --signature "OOM | small"

# the same signature from production, to compare
python render_frontend.py --production --signature "OOM | small"

# the files are named sha1(signature), so to find out what's in a bucket
python render_frontend.py --bucket my-scratch-bucket --top-signatures
```

Requires node 16 or newer on the path, and `gsutil` for `--bucket`. There is no `npm install`:
`correlation.js`'s one dependency is patched out in favour of node's built in `crypto`. This
checks the data contract, not the deployment; CSP, CORS and gzip negotiation are the browser's job
and aren't covered. It is a dev tool and deliberately isn't copied into the image.

## Deploying

CI builds, tests and pushes the image from `.github/workflows/job-crash-correlations.yml`,
regenerated from `ci_job.yaml` with `script/update_ci_config` at the repo root.

The DAG passes `--results-bucket moz-fx-data-static-websit-8565-analysis-output` and requests 4Gi
of memory for the pod, which is above the default allocation:

```python
container_resources=k8s.V1ResourceRequirements(
    requests={"memory": "4Gi"},
),
```

Measured peak RSS on a full 200 signature run is 1.77 GB on release and 2.19 GB on esr, so that
leaves about 1.8Gi of headroom over the worst channel.

## Development

```sh
pip install -r requirements.txt
pytest
flake8 crash_correlations/ tests/
```

The tests and `.flake8` are copied into the image and CI runs both against it, so the image's
entrypoint is a bare `python` and normal runs rely on the `CMD`.

## How it fits together

Per channel: resolve which versions are live, ask Crash Stats for the top signatures, build a
feature table in BigQuery, count attribute combinations over it in Python, filter down to the ones
worth reporting, and write a file per signature. The counting is Python rather than more SQL
because it measured faster for this shape of problem, so there is only one substantial query.

| | |
|---|---|
| `main.py` | arguments, the per channel loop, and the totals in `all.json.gz` |
| `queries.py` | everything that talks to something outside the process, plus the SQL runners |
| `sql/feature_table.sql` | one row per crash, one column per attribute |
| `sql/frequent_values.sql` | the module and addon columns, plus their single-attribute counts |
| `sql/frequent_substrings.sql` | which searchfox strings occur often enough to be worth a column |
| `mining.py` | the counting, single attributes and then pairs |
| `pruning.py` | drops combinations that aren't interesting, and the significance tests |
| `priors.py` | which attributes can explain which, used to set `prior` |
| `filtering.py` | turns counted candidates into the output rows |
| `output.py` | file naming, gzip, the local write and the GCS upload |
| `render_frontend.py` | prints what the Correlations tab would show. Dev tool, not in the image |

## Changes from the old job

Only of interest if you are comparing against the old job or wondering why something looks odd.
Every bug below was confirmed in production output or logs; how each was diagnosed is in
python_mozetl's `migration_plan.md`. Vocabulary and structure were kept close to the original so
the two could be diffed during validation.

| Bug | Here |
|---|---|
| **Null counts are wrong and nondeterministic.** Published counts for null-valued attributes fall far below the true row count, and two computations of the same rows disagree. | Fixed. Counting is a Python dict keyed on the value, so `None` is one key like any other. Verified against BigQuery `COUNTIF`. |
| **Output contains impossible arithmetic**, a superset counting higher than its own subset: 5,138 cases across all 573 published files, every one involving a null. | Fixed. |
| **A channel with no usable signatures crashes the run**, losing the channels that already succeeded. Hit production when a release shipped and beta and esr fell below the minimum crash count. | Fixed. The channel is skipped and its total set to 0. |
| **The esr channel analyses release crashes.** esr is routed down the release branch of `get_versions`, whose history file holds no `esr`-suffixed builds, so the filter matches zero esr rows and collides with release: the two channels published byte-identical output, and the esr tab showed release data. | Fixed. esr resolves from `firefox.json`, the only place esr builds are dated, and version strings keep their `esr` suffix. The two channels no longer duplicate each other. |
| **Addon correlations have never worked.** The addon pass gets a `Row` where it expects a string, so every addon is dropped and `addon_related_signatures.json.gz` is always `{}` in production. | Fixed, not by choice: addons are counted in SQL here, which never had the type confusion. That file is populated for the first time. |
| **`--date` doesn't control the version list**, so a rerun can't reproduce an older run. | Fixed for release, beta and esr, which resolve from dated history. nightly still uses the live value, since none is published. |

Two inherited quirks were kept on purpose, because changing either moves output for reasons
unrelated to the migration. The counts are emitted as floats because of a `count * total / total`
no-op multiplication at `crash_deviations.py:317`, and the frontend now expects floats. And the
significance threshold leaks across signatures: upstream initialises it outside the per-signature
loop and only ever shrinks it, so what a signature is judged against depends on how many
candidates the signatures before it produced.

### Why the row sets don't match production

Don't expect the same rows per signature, in either direction, even where the populations match
exactly. The null undercount invents correlations that production publishes and this job doesn't,
so it emits fewer rows; production's module feature selection is nondeterministic and keeps an
arbitrary subset of the modules that qualify, so it emits more. Counts agree on the rows the two
have in common, so compare per-item counts on shared signatures rather than row sets.
