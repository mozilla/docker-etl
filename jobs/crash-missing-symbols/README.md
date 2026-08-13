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

`--dry-run` prints the email instead of sending it, so it needs no AWS
credentials. The report goes to stdout and progress lines to stderr, so you can
redirect it and open the result in a browser:

```sh
python -m crash_missing_symbols.main --dry-run > report.html
```

Useful options:

| Option | Default | Meaning |
| --- | --- | --- |
| `--date` | today (UTC) | End of the counting window |
| `--window-days` | 3 | Days of crash reports to count |
| `--min-crash-count` | 70 | Modules at or below this are left out |
| `--run-on-days` | never send | Days of week to send email on (0 is Sunday) |
| `--billing-project` | mozdata | Project the query bills to |
| `--recipient` | the three addresses above | Where to send the report |
| `--sender` | telemetry-alerts@mozilla.com | From address, must be verified in SES |
| `--dedupe-key` | `module-struct` | How repeated modules are counted, see below |
| `--fix-availability-args` | off | Look up symbols at the URL that exists |
| `--dry-run` | off | Print the email instead of sending it |

`--recipient` replaces the defaults rather than adding to them, so a test run
only mails you. One address per flag, repeated for several:

```sh
python -m crash_missing_symbols.main --recipient me@mozilla.com
python -m crash_missing_symbols.main --recipient me@mozilla.com --recipient you@mozilla.com
```

`--run-on-days` exists because Airflow can't give one task in a DAG its own
schedule. The report is weekly but the DAG is daily, so on the other six days the
job still runs in full and prints the report without sending it, which surfaces a
broken query or a symbols server outage the day it breaks.

Omitting the flag means nothing is ever mailed, so the DAG has to pass it (e.g.
`--run-on-days 3` for Wednesday). Defaulting to silence keeps a deploy that drops
the flag from mailing the distribution lists daily.

## Development

```sh
pytest
flake8 crash_missing_symbols/ tests/
```

The tests don't hit BigQuery or the network; use `--dry-run` for that.

## Parity with the Spark job

The Spark job was changed to print its report so the two could be compared
directly. With default flags the output is identical: same modules, versions,
debug IDs and counts, in the same order.

Matching the old job was prioritised over fixing it, so two of its defects are
reproduced by default, each behind a flag that turns the fix on. The intent is to
switch the defaults once the migration is signed off.

- `--dedupe-key` (default `module-struct`) counts a module once per mapped
  address, as Spark's whole-struct dedupe did, rather than once per crash report
  as the email's "# of crash reports" column claims. The gap is large and driven
  by memory mapped files: for the 3 days ending 2026-08-07, `module-struct` gives
  261 rows totalling 70,285 against `crash-report`'s 225 and 46,665. `nvidiactl`
  alone was counted 11,393 times across 207 crash reports, and under
  `crash-report` it's no longer the top entry, so the ranking changes visibly.
- `--fix-availability-args` (default off) reproduces a transposed argument pair
  that made every symbols lookup 404, so no module was ever marked `(*)`. Turning
  it on changes little: for the 3 days ending 2026-08-06 the corrected check found
  0 of 251 modules available.

Deliberate differences that don't change the output:

- The footer said "at least 2,000 crash reports" while the code filtered at more
  than 70. The text now reflects the actual threshold.
- Symbol URLs are percent-encoded, so module names with spaces or non-ASCII
  characters no longer raise.
- The `(deleted)` filter uses `CONTAINS_SUBSTR`, which is case-insensitive where
  Spark's Python `in` was not. Over the 60 days ending 2026-08-07 no filename
  matched in any other case, and both drop the same 209,383 of 331,087 rows in the
  3 day window. Use `STRPOS(filename, '(deleted)') = 0` if it ever needs to be
  case sensitive.
- The query window is half open (`crash_date < end_date`), where Spark had no
  upper bound and so included partial-today data. An unbounded window makes counts
  depend on what time the DAG fires and can't be pinned for testing.
