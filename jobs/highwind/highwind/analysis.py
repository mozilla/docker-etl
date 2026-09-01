"""The Highwind daily analysis, top down.

Reading order is this file, then the modules it calls into:

    discovery.py        which experiments, and which windows each metric gets
    units.py            the unit each experiment is analysed at
    metrics.py          SECTION 1: metric definitions
    sql_generation.py   SECTION 2: SQL generation
    sql_running.py      SECTION 3: SQL running
    gbstats_compute.py  SECTION 4: gbstats compute
    output_writing.py   SECTION 5: output writing

The entry point is main.py, which parses the arguments Airflow passes and calls `run_daily_job`
here.

Desktop experiments only, guardrail metrics only, full recompute every run. Each of those is a
deliberate limit of the proof of concept and is noted where it bites.
"""

import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import storage  # type: ignore
from google.cloud import bigquery

from . import discovery, gbstats_compute
from . import metrics as metric_definitions_module
from . import output_writing, sql_generation, sql_running

# The covariate window: days before a unit's enrollment used to predict its post-enrollment value.
# Fixed rather than matched to each window's length, so one pre-period serves every window of a
# metric.
COVARIATE_DAYS = 28

# The project the queries run and bill in. Every table they read is named by its own project, so
# this selects who pays rather than what is read, and this job's consumption is attributable to the
# project that owns its results.
DEFAULT_BILLING_PROJECT = "moz-fx-data-experiments"


def run_daily_job(
    as_of,
    only_slugs=None,
    limit=None,
    validate_only=False,
    workers=8,
    live_only=False,
    sample_percent=None,
    outputs=None,
    billing_project=DEFAULT_BILLING_PROJECT,
):
    """Analyse every discoverable experiment for one run date.

    The expensive part is a handful of queries for the whole run rather than a handful per
    experiment: one cohort scan, then one per source per analysis unit, each grouped by slug. Every
    experiment's date range overlaps almost every other's, so a per-experiment scan would read the
    same calendar day once per experiment covering it.

    The cost of sharing them is failure isolation on the query step: a source query that fails fails
    every experiment rather than one. That is a fair description of reality, since a broken source
    query IS broken for everyone, and it is the case `systemic_failure` exists to catch. Isolation
    still holds for the per-experiment statistics below, which is where experiment-specific faults
    arise.
    """
    outputs = outputs or output_writing.Outputs()
    client = bigquery.Client(project=billing_project)
    storage_client = storage.Client()
    experiments, skipped = discovery.discover(
        client, as_of, limit=limit, only_slugs=only_slugs, live_only=live_only
    )
    report_selection(experiments, skipped, as_of)
    if not experiments:
        return []

    metrics_by_source = metric_definitions_module.metric_definitions()
    windows_by_slug = {
        e.slug: run_windows(e, as_of, metrics_by_source) for e in experiments
    }
    run = sql_generation.Run(
        experiments, windows_by_slug, as_of, COVARIATE_DAYS, sample_percent
    )

    cells_by_slug, timings, failure = gather_sufficient_statistics(
        client, run, metrics_by_source, as_of, validate_only
    )
    write_blobs = writes_blobs(validate_only, sample_percent, outputs)

    # No lock: `as_completed` yields in this thread, so the accumulation below is single-threaded.
    summaries, results_by_slug, done = [], {}, 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(
                analyze_experiment,
                storage_client,
                experiment,
                as_of,
                windows_by_slug[experiment.slug],
                metrics_by_source,
                cells_by_slug.get(experiment.slug, {}),
                failure,
                outputs,
                write_blobs,
            )
            for experiment in experiments
        ]
        for future in as_completed(futures):
            summary, results = future.result()
            summaries.append(summary)
            results_by_slug[summary["slug"]] = results
            done += 1
            report_progress(summary, done, len(experiments))

    if is_partial_run(only_slugs, limit, sample_percent):
        # A partial run is not the day's complete output, and the write replaces the whole
        # partition, so letting it through would overwrite the full run's rows. A filter drops
        # experiments the partition should still hold; a sample keeps every experiment but computes
        # each from a fraction of its cohort, which is the more dangerous of the two because the
        # rows it writes look complete.
        blobs = "blobs written" if write_blobs else "blobs skipped"
        print(
            f"\npartial run ({len(experiments)} experiments): {blobs}, tables skipped "
            f"so the {as_of} partition keeps the full run's rows"
        )
    elif outputs.local_blob_dir:
        # Blobs went to a directory, so there is nowhere for the tables to be part of the same
        # output. Writing them would put a local run's numbers in the production partition.
        print(f"\nlocal run: blobs written to {outputs.local_blob_dir}, tables skipped")
    elif not validate_only:
        # After the loop, not inside it. Every experiment's rows share one daily partition, so a
        # per-experiment write could only append, and appending makes a retried run double its own
        # output. Writing the run together lets the load replace the partition instead.
        written = output_writing.write_tables(
            client, as_of, results_by_slug, cells_by_slug, outputs
        )
        print(
            f"\nwrote {written[0]:,} statistics rows and {written[1]:,} "
            f"sufficient-statistics rows for {as_of}"
        )
    report_run(summaries, timings)
    if not validate_only:
        report_anomalies(summaries)
    return summaries


def is_partial_run(only_slugs, limit, sample_percent):
    """Whether this run covers less than the whole day, and so must not replace the partition.

    Three ways to be partial, and the sample is the one worth naming: a filter drops experiments the
    partition should still hold, which is visibly incomplete, while a sample keeps every experiment
    and computes each from a fraction of its cohort, so the rows it writes look complete and are
    not.
    """
    return bool(only_slugs or limit or sample_percent)


def writes_blobs(validate_only, sample_percent, outputs):
    """Whether this run may write the per-experiment blobs Experimenter reads.

    A blob is named by its experiment alone, so writing one replaces that experiment's published
    answer. A filtered run's experiments are each computed from their whole cohort, so its blobs
    are correct for the experiments it covered and are written. A sampled run's are not: every one
    of its numbers comes from a fraction of a cohort, and the name it would be published under
    carries no sign of that, so a sampled run publishes nothing. Directed at a local directory a
    sample is inspectable rather than published, which is how its output is read.
    """
    if validate_only:
        return False
    return sample_percent is None or outputs.local_blob_dir is not None


def gather_sufficient_statistics(client, run, metrics_by_source, as_of, validate_only):
    """Run the shared scan: cohort once, then one query per source and unit, split by slug.

    Returns the cells keyed by slug plus whatever went wrong, rather than raising, so a source
    failure becomes an error grid on every experiment instead of an exception that ends the run
    having written nothing.
    """
    if not run.experiments:
        return {}, [], None
    try:
        cohort_table, cohort_timing = sql_running.materialize_cohort(
            client, sql_generation.cohort_query(run), as_of, validate_only
        )
        queries = sql_generation.build_queries(run, metrics_by_source, cohort_table)
        cells_by_slug, timings = sql_running.run_queries(
            client, queries, validate_only=validate_only
        )
        return cells_by_slug, [cohort_timing, *timings], None
    # Recorded against every experiment rather than raised, so a source failure becomes an error
    # grid instead of an exception that ends the run having written nothing.
    except Exception as error:
        traceback.print_exc()
        return {}, [], error


def report_progress(summary, done, total):
    """One line per finished experiment.

    Flags on the summary's error as well as on error cells. An experiment that failed before
    producing any cells has an error but no error cells, so keying only on cell states reports the
    worst case as `ok`.
    """
    flag = "ERROR" if (summary["states"].get("error") or summary["error"]) else "ok   "
    reason = f"  {summary['error']}" if summary["error"] else ""
    print(
        f"[{done:3d}/{total}] {flag} {summary['slug'][:52]:52s} "
        f"{summary['cells']:5d} cells{reason}"
    )


def analyze_experiment(
    storage_client,
    experiment,
    as_of,
    windows,
    metrics_by_source,
    cells,
    shared_failure,
    outputs,
    write_blobs=False,
):
    """One experiment's statistics and outputs, from cells the shared scan already produced.

    Wrapped so a failure is recorded against this experiment's cells and the loop continues. The
    unit of isolation is the experiment because that is the unit a user reads: one experiment whose
    statistics blow up should not cost the others their results.
    """
    try:
        if shared_failure is not None:
            return failed_summary(
                storage_client,
                experiment,
                as_of,
                windows,
                metrics_by_source,
                shared_failure,
                outputs,
                write_blobs,
            )
        if not windows:
            # Younger than the shortest window, so no unit has completed one and there is nothing
            # to compute. Its cells are all `not_started` by construction.
            return experiment_summary(experiment, []), []
        results = gbstats_compute.compute_statistics(
            experiment, all_metrics(metrics_by_source), windows, cells
        )
        if write_blobs:
            output_writing.write_blob(
                storage_client, experiment, as_of, results, outputs
            )
        return experiment_summary(experiment, results), results
    # The whole point is to record it, not to raise.
    except Exception as error:
        try:
            return failed_summary(
                storage_client,
                experiment,
                as_of,
                windows,
                metrics_by_source,
                error,
                outputs,
                write_blobs,
            )
        except Exception as while_recording:
            # A failure while recording a failure must not propagate: it would come back out of
            # future.result() and cost every other experiment its results, which is exactly the
            # isolation this design claims to have.
            traceback.print_exc()
            return (
                experiment_summary(
                    experiment,
                    [],
                    error=f"{error} (recording it also failed: {while_recording})",
                ),
                [],
            )


def failed_summary(
    storage_client,
    experiment,
    as_of,
    windows,
    metrics_by_source,
    error,
    outputs,
    write_blobs,
):
    """Record a query-level failure across every cell this experiment expected to produce.

    Without this the error rate cannot see an outage: an experiment that produced nothing would
    write no rows and so contribute nothing to the denominator, making total failure look like
    perfect health.
    """
    results = gbstats_compute.compute_statistics(
        experiment, all_metrics(metrics_by_source), windows, cells={}, failure=error
    )
    if write_blobs:
        try:
            output_writing.write_blob(
                storage_client, experiment, as_of, results, outputs
            )
        except Exception:
            # Recording the error grid must not depend on the thing that just failed. The grid is
            # returned either way, so the failure stays visible in this run's accounting and in the
            # table written at the end, even when the blob could not be written.
            traceback.print_exc()
    return experiment_summary(experiment, results, error=error), results


def run_windows(experiment, as_of, metrics_by_source=None):
    """List every distinct window any metric declares, out to the maturity frontier."""
    tenure = experiment.tenure_days(as_of)
    metrics_by_source = (
        metrics_by_source or metric_definitions_module.metric_definitions()
    )
    seen, windows = set(), []
    for metric in all_metrics(metrics_by_source):
        for window in discovery.windows_for(metric, tenure):
            if window.label not in seen:
                seen.add(window.label)
                windows.append(window)
    return windows


def all_metrics(metrics_by_source):
    """Flatten the per-source metric mapping into one list."""
    return [metric for metrics in metrics_by_source.values() for metric in metrics]


def experiment_summary(experiment, results, error=None):
    """Summarise what this experiment produced, for the run report and the health measurement.

    Carries no scan consumption: the scan is shared across the run, so bytes and slot-hours are
    properties of the run and are reported there.
    """
    return dict(
        slug=experiment.slug,
        cells=len(results),
        states=output_writing.state_counts(results),
        error=None if error is None else f"{type(error).__name__}: {error}"[:200],
    )


def report_selection(experiments, skipped, as_of):
    """Print which experiments the run will analyse, and why the others were skipped."""
    print(
        f"as_of {as_of}: {len(experiments)} experiments to analyse, {len(skipped)} skipped"
    )
    for slug, why in skipped:
        print(f"   skipped {slug}: {why}")


def report_run(summaries, timings):
    """Print the per-run operational summary: what it consumed and how many cells failed.

    Bytes scanned and slot-hours are consumption rather than a price, since the slots are a shared
    reservation. Both are properties of the run and not of an experiment: one pass over each source
    serves all of them, so there is no per-experiment byte count to report.
    """
    cells = sum(summary["cells"] for summary in summaries)
    errors = sum(summary["states"].get("error", 0) for summary in summaries)
    failed = sum(1 for summary in summaries if summary["error"])
    print(
        f"\n{len(summaries)} experiments ({failed} failed outright), {cells:,} cells, "
        f"{sum(t['gb_scanned'] for t in timings) / 1000:.2f} TB scanned, "
        f"{sum(t['slot_hours'] or 0 for t in timings):.1f} slot-hours "
        f"over {len(timings)} queries"
    )
    for timing in timings:
        print(
            f"   {timing['source']:30s} {timing['wall_s']:8.1f}s "
            f"{timing['gb_scanned'] / 1000:7.2f} TB "
            f"{timing['slot_hours'] if timing['slot_hours'] is not None else '-'} slot-h "
            f"{timing['rows']:,} rows"
        )
    print(
        f"error rate {errors / cells:.2%} ({errors:,} of {cells:,} cells)"
        if cells
        else "no cells produced"
    )


def systemic_failure(summaries):
    """Report whether the run failed as a whole, rather than losing individual experiments.

    Keyed on cell STATE, not on how many cells there are. A failing experiment still produces a
    full grid, every cell of it an error, precisely so the failure is visible in the output rather
    than absent from it; counting cells therefore cannot distinguish a total outage from a good run.

    Per-experiment isolation means one bad experiment must not fail the run, so this asks whether
    ANY experiment produced a cell in a state other than error. A quiet day on which every
    experiment is simply too young to have matured a window produces no cells and no errors, and is
    not a failure.
    """
    if not summaries:
        return False
    produced = any(
        count
        for summary in summaries
        for state, count in summary["states"].items()
        if state != gbstats_compute.ERROR
    )
    failed = any(
        summary["error"] or summary["states"].get(gbstats_compute.ERROR)
        for summary in summaries
    )
    return failed and not produced


def report_anomalies(summaries):
    """Results that ran without erroring but do not look like an analysis.

    Worth reporting separately because the failure that cost us most in this prototype produced no
    error at all: an identifier mismatch made every metric sum to zero across a full cohort, and the
    run reported perfect health. A clean error rate is not evidence the numbers mean anything.
    """
    suspicious = []
    for summary in summaries:
        states = summary["states"]
        if summary["error"]:
            suspicious.append((summary["slug"], f"failed: {summary['error'][:90]}"))
            continue
        if summary["cells"]:
            if states.get("not_started", 0) == summary["cells"]:
                suspicious.append(
                    (summary["slug"], "every cell not_started: cohort or join empty")
                )
            elif states.get("insufficient_data", 0) == summary["cells"]:
                suspicious.append((summary["slug"], "every cell insufficient_data"))
        if summary["cells"] == 0:
            suspicious.append((summary["slug"], "no cells: no window has matured yet"))
    if suspicious:
        print(f"\n{len(suspicious)} experiments to look at:")
        for slug, why in suspicious:
            print(f"   {slug[:56]:56s} {why}")
