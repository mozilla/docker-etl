# Crash Missing Symbols

Weekly email report of modules that appeared in Firefox crash reports without
symbols, so the people who maintain our symbols coverage know what to chase.

Ported from the PySpark job at `mozetl/symbolication/modules_with_missing_symbols.py`
in python_mozetl, which ran on a Dataproc image that is past end of life. The
Spark work was one explode, one dedupe and one group-by; that is now
`sql/modules_with_missing_symbols.sql` and there is no Spark dependency.

## What it does

1. Queries `moz-fx-data-shared-prod.telemetry_derived.socorro_crash_v2` for
   modules flagged `missing_symbols` over the last three days, counting distinct
   crash reports per (name, version, debug ID, debug file).
2. Drops modules listed in [marco-c/missing_symbols](https://github.com/marco-c/missing_symbols)
   `known_modules`, which is where people go to suppress false positives, and
   drops Firefox modules old enough that their symbols have expired off the
   symbols server.
3. HEADs `symbols.mozilla.org` for each remaining module, marking with `(*)` any
   whose symbols have shown up since the crash. Those are candidates for
   reprocessing.
4. Sends the HTML table via SES to the `--recipient` addresses, which default to
   mcastelluccio@mozilla.com, release-mgmt@mozilla.com and stability@mozilla.org.

## Usage

```sh
docker build -t crash-missing-symbols jobs/crash-missing-symbols
docker run crash-missing-symbols --dry-run
```

Locally:

This job was only built and tested with Python 3.14, so other versions aren't
guaranteed to work (but probably will).

Requires authenticating through `gcloud` in order to query BigQuery:
```sh
gcloud auth application-default login
```

```sh
pip install -r requirements.txt && pip install -e .
python -m crash_missing_symbols.main --dry-run
```

`--dry-run` prints the email instead of sending it, and needs no AWS
credentials. The report goes to stdout and the progress lines to stderr, so you
can redirect it and open the result in a browser:

```sh
python -m crash_missing_symbols.main --dry-run > report.html
```

It still reads BigQuery, so you need application default credentials with access
to `socorro_crash_v2`.

Useful options:

| Option | Default | Meaning |
| --- | --- | --- |
| `--date` | today (UTC) | End of the counting window |
| `--window-days` | 3 | Days of crash reports to count |
| `--min-crash-count` | 70 | Modules at or below this are left out |
| `--run-on-days` | send every day | Days of week to send email on (0 is Sunday) |
| `--billing-project` | mozdata | Project the query bills to |
| `--recipient` | the three addresses above | Where to send the report |
| `--sender` | telemetry-alerts@mozilla.com | From address, must be verified in SES |
| `--dedupe-key` | `module-struct` | How repeated modules are counted, see below |
| `--fix-availability-args` | off | Look up symbols at the URL that exists |
| `--dry-run` | off | Print the email instead of sending it |

`--recipient` replaces the defaults rather than adding to them, so a test run
only mails you. Repeat the flag or pass one comma separated list, whichever is
easier from the caller:

```sh
python -m crash_missing_symbols.main --recipient me@mozilla.com
python -m crash_missing_symbols.main --recipient "me@mozilla.com,you@mozilla.com"
```

`--run-on-days` exists because Airflow can't give one task in a DAG its own
schedule. The report is weekly but the DAG is daily, so on the other six days the
job still runs in full and prints the report, it just doesn't send it. That way a
broken query or a symbols server outage shows up the day it breaks instead of
waiting for send day.

## Development

```sh
pytest
flake8 crash_missing_symbols/ tests/
```

The tests cover the URL construction and both availability modes, the old-version
filter, the HTML rendering, recipient parsing, and the query text (window bound,
dedupe gating, parameter binding). They don't hit BigQuery or the network; use
`--dry-run` for that.

## Parity with the Spark job

The migration prioritises matching the old job over fixing it, so the output can
be diffed to prove nothing was lost. Two known defects in the old job are
therefore reproduced by default, each behind a flag that turns the fix on. The
intent is to switch the defaults once the migration is signed off, not to keep
the bugs indefinitely.

### `--dedupe-key` (default `module-struct`)

Spark's `dropDuplicates(["uuid", "module"])` keyed on the whole module struct,
including `base_addr` and `end_addr`, so a file mapped at several addresses in
one crash report counted once per mapping. `module-struct` reproduces that.
`crash-report` counts each report once per module, which is what the email's
"# of crash reports" column says the number means.

The gap is large, and it's memory mapped files rather than loaded libraries that
drive it. For the 3 days ending 2026-08-07, `module-struct` gives 261 rows
totalling 70,285 against `crash-report`'s 225 and 46,665. `nvidiactl` alone was
counted 11,393 times across 207 crash reports; font files, `icon-theme.cache` and
`omni.ja` behave the same way. Under `crash-report` several of those drop below
the reporting threshold and `nvidiactl` is no longer the top entry, so the
ranking changes visibly.

Reproducing the struct dedupe only needs `base_addr`, `end_addr` and `code_id`:
the other fields never vary within a group that is otherwise identical, verified
against `TO_JSON_STRING(m.element)`.

### `--fix-availability-args` (default off)

The old job called `are_symbols_available(debug_id, debug_file)` into parameters
declared `(debug_file, debug_id)`, so every request asked for
`<debug_id>/<debug_file>/<debug_id>`, got a 404, and no module was ever marked
`(*)`. Off by default reproduces that. With the flag, the lookup uses the real
URL, confirmed against modules whose symbols are present on the server.

In practice this changes little: for the 3 days ending 2026-08-06 the corrected
check found 0 of 251 modules available, so the reports are usually identical
either way.

### Not reproduced

The footer said "at least 2,000 crash reports" while the code filtered at more
than 70. The text now reflects the actual threshold. `symbol_url` also
percent-encodes the path, which the old job didn't; since the transposed lookup
404s regardless, that can't affect parity, and it avoids raising on module names
with spaces or non-ASCII characters.

## Comparing against the baseline

Parity was checked against the old Spark code run locally with a fixed window and
a pinned copy of the module filter lists, so runs are reproducible. With default
flags the query matches it exactly: same row set and same counts for the windows
ending 2026-08-06 (282 rows) and 2026-08-07 (261).

That baseline is not identical to the deployed job, so "parity" means parity with
the baseline, not with a past production email:

- The deployed job filters on `crash_date >= utcnow() - 3` with **no upper
  bound**, so it includes partial-today data. The baseline and this job both use
  `crash_date < end_date`. On 2026-08-06 the unbounded window gives 214 rows
  totalling 113,713 against 170 and 84,153. Keeping the bound exclusive is
  deliberate: an unbounded window makes the counts depend on what time the DAG
  fires, and can't be pinned for testing.
- The deployed job's window depends on when it ran, not on the date it was asked
  to run for. It reads the clock in four places, and its `--date` argument feeds
  only the day-of-week check, so the query window and the subject line always
  track wall clock. Passing `--date` cannot move them. Here `--date` sets the
  window, the subject and the expiry cutoff together, so a run is reproducible
  and a backfill reports the window it was asked for. The practical difference is
  small, since both normally run once a day on the same schedule, but it means a
  retry hours later or a backfill of an old date produces different output under
  the two jobs.
- The deployed job clones `missing_symbols` at HEAD, so its filter lists drift if
  anyone lands a commit there. No effect today, since HEAD hasn't moved since
  January 2024.
