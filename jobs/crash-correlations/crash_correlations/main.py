"""Attributes over-represented in a Firefox crash signature, per channel.

Finds what is unusual about the crashes on a signature compared to its whole channel,
and writes the answer as JSON for the Correlations tabs on Crash Stats. See README.md.

Writes locally by default, because the whole output is about 1 MB gzipped across ~645
files, so a dev run needs no bucket and no write credentials:

    python -m crash_correlations.main --date 2026-08-14

Uploading is opt in:

    python -m crash_correlations.main --date 2026-08-14 \\
        --results-bucket my-scratch-bucket

Point --results-bucket at a scratch bucket unless you mean it. The production bucket is
the one Crash Stats reads, and the prefix is cleared before uploading.
"""

import datetime
import pathlib
import sys

import click

from crash_correlations import filtering, mining, output, priors, pruning, queries


CHANNELS = ("release", "beta", "nightly", "esr")

DEFAULT_WINDOW_DAYS = 5

DEFAULT_TOP_SIGNATURES = 200
DEFAULT_BILLING_PROJECT = "mozdata"
DEFAULT_OUTPUT_DIR = "test_output"

# The bucket Crash Stats reads. Deliberately not the default for --results-bucket;
# uploading anywhere is always explicit.
PRODUCTION_BUCKET = "moz-fx-data-static-websit-8565-analysis-output"

# Defaults of crash_deviations.find_deviations.
DEFAULT_MIN_SUPPORT_DIFF = 0.15
DEFAULT_MIN_CORR = 0.03


def process_channel(
    channel,
    run_date,
    window_days,
    top_signatures,
    billing_project,
    min_support_diff,
    min_corr,
    versions=None,
):
    """Run the whole pipeline for one channel.

    Returns (total_reference, totals_by_signature, results), or None when the channel
    has nothing to analyse. That case used to take the entire run down; see "Empty
    channel crash" in python_mozetl's migration_plan.md.

    versions overrides the product-details lookup; see the --versions help.
    """
    if versions:
        click.echo(f"{channel}: using pinned versions {list(versions)}", err=True)
    else:
        versions = queries.channel_versions(channel, as_of=run_date)
    if not versions:
        click.echo(f"{channel}: no versions from product-details, skipping", err=True)
        return None
    click.echo(f"{channel}: versions {list(versions)}", err=True)

    signatures = queries.top_signatures(
        top_signatures, versions, run_date, window_days
    )
    if not signatures:
        click.echo(f"{channel}: no signatures from Crash Stats, skipping", err=True)
        return None

    features = queries.frequent_values(
        billing_project, run_date, window_days, versions, signatures
    )
    click.echo(
        f"{channel}: {len(features.module_columns)} modules, "
        f"{len(features.addon_columns)} addons, "
        f"{len(features.app_note_columns)} app notes, "
        f"{len(features.gfx_error_columns)} gfx errors",
        err=True,
    )

    table, columns = queries.feature_table(
        billing_project, run_date, window_days, versions, features
    )
    if not len(table):
        click.echo(
            f"{channel}: no crashes for versions {versions}, skipping the channel",
            err=True,
        )
        return None
    click.echo(
        f"{channel}: {len(table)} crashes, {len(columns)} feature columns", err=True
    )

    # Level 1. The module and addon features were already counted in SQL, so they're
    # excluded here rather than counted twice; see the note at the top of
    # frequent_values.sql.
    scalar_columns = [c for c in columns if c not in features.counted_columns]
    counts, totals = mining.count_level_1(table, scalar_columns, signatures)
    for group, group_counts in features.counts.items():
        counts.setdefault(group, {}).update(group_counts)

    counted = pruning.Counts(counts=counts, totals=totals)
    groups = [s for s in signatures if totals.get(s, 0) >= mining.MIN_COUNT]

    level_1 = {
        group: pruning.filter_level_1(
            counted,
            group,
            [i for i in counts.get(group, ()) if len(i) == 1],
            features.addon_version_columns,
            min_support_diff,
        )
        for group in groups
    }

    candidates_2, parents = mining.generate_candidates(
        level_1, module_columns=features.module_columns
    )
    level_2_counts = mining.count_level_2(table, columns, candidates_2)
    for group, group_counts in level_2_counts.items():
        counted.counts.setdefault(group, {}).update(group_counts)

    level_2 = {
        group: pruning.filter_level_2(
            counted,
            group,
            candidates_2.get(group, ()),
            parents.get(group, {}),
            min_support_diff,
        )
        for group in groups
    }

    all_candidates = {group: level_1[group] + level_2[group] for group in groups}
    counts_by_level = {
        group: {1: len(level_1[group]) or 1, 2: len(level_2[group]) or 1}
        for group in groups
    }

    reachable = priors.reachability(
        priors.build_graph(
            app_notes=features.app_note_columns,
            gfx_errors=features.gfx_error_columns,
            modules=features.module_columns,
            addons=features.addon_columns,
            addon_versions={
                presence: version
                for version, presence in features.addon_version_columns.items()
            },
        )
    )

    results = filtering.filter_all(
        counted,
        groups,
        mining.order_candidates(all_candidates),
        reachable,
        features.labels,
        min_support_diff=min_support_diff,
        min_corr=min_corr,
        candidate_counts_by_level=counts_by_level,
    )

    return (
        totals[mining.REFERENCE],
        {group: totals[group] for group in groups},
        {group: rows for group, rows in results.items() if rows},
    )


@click.command(help=__doc__)
@click.option(
    "--date",
    "run_date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help=(
        "End of the window, exclusive. Defaults to today (UTC). Scopes both the crash "
        "window and the version list, which resolves from dated product-details "
        "history, so a rerun for a past date reproduces that date's run. The exception "
        "is nightly, which has no dated history and so always resolves to today's "
        "build; use --versions to pin it."
    ),
)
@click.option(
    "--versions",
    multiple=True,
    default=None,
    help=(
        "Use these versions instead of resolving them, e.g. --versions 153.0.4 "
        "--versions 153.0.3. Only valid with a single --channel, since the list is "
        "per channel. Mainly useful for nightly, which has no dated history and so "
        "always resolves to today's build whatever --date says, and for investigating "
        "one specific build."
    ),
)
@click.option("--window-days", default=DEFAULT_WINDOW_DAYS, show_default=True)
@click.option(
    "--top-signatures",
    default=DEFAULT_TOP_SIGNATURES,
    show_default=True,
    help="Signatures per channel. Cost grows with this; candidates are per signature.",
)
@click.option(
    "--channel",
    "channels",
    multiple=True,
    type=click.Choice(CHANNELS),
    default=CHANNELS,
    show_default=True,
    help="Repeat for several. Replaces the defaults rather than adding to them.",
)
@click.option("--billing-project", default=DEFAULT_BILLING_PROJECT, show_default=True)
@click.option(
    "--output-dir",
    default=DEFAULT_OUTPUT_DIR,
    show_default=True,
    help="Where to write the output. Cleared first, so it mirrors the bucket exactly.",
)
@click.option(
    "--results-bucket",
    default=None,
    help=(
        "Also upload to this GCS bucket, without the gs:// prefix. Omit to write only "
        f"locally. Production is {PRODUCTION_BUCKET}; point this at a scratch bucket "
        "when testing, the prefix is cleared before uploading."
    ),
)
@click.option(
    "--no-local",
    is_flag=True,
    help="Skip the local write. Only meaningful with --results-bucket.",
)
@click.option(
    "--min-support-diff", default=DEFAULT_MIN_SUPPORT_DIFF, show_default=True
)
@click.option("--min-corr", default=DEFAULT_MIN_CORR, show_default=True)
def main(
    run_date,
    versions,
    window_days,
    top_signatures,
    channels,
    billing_project,
    output_dir,
    results_bucket,
    no_local,
    min_support_diff,
    min_corr,
):
    run_date = (
        run_date.date() if run_date else datetime.datetime.now(datetime.UTC).date()
    )
    if no_local and not results_bucket:
        raise click.UsageError(
            "--no-local needs --results-bucket, or nothing would be written"
        )
    if versions and len(channels) != 1:
        raise click.UsageError(
            "--versions applies to one channel's version list, so pass exactly one "
            f"--channel with it (got {len(channels)}: {list(channels)})"
        )
    if versions and results_bucket == PRODUCTION_BUCKET:
        raise click.UsageError(
            "--versions produces output that doesn't match the current versions. "
            "Point --results-bucket at a scratch bucket to use it."
        )

    writer = output.Writer(output_dir, bucket=results_bucket)
    totals = {"date": str(run_date)}
    addon_related = {}

    for channel in channels:
        processed = process_channel(
            channel,
            run_date,
            window_days,
            top_signatures,
            billing_project,
            min_support_diff,
            min_corr,
            versions=versions,
        )
        if processed is None:
            # The channel key stays in all.json.gz so its shape doesn't change.
            totals[channel] = 0
            continue

        total_reference, totals_by_signature, results = processed
        totals[channel] = total_reference

        for signature, rows in results.items():
            writer.add_signature(
                channel, signature, totals_by_signature[signature], rows
            )

        entries = output.addon_related(results, totals_by_signature, total_reference)
        if entries:
            addon_related[channel] = entries

        click.echo(f"{channel}: {len(results)} signatures with results", err=True)

    writer.add("all.json.gz", totals)
    writer.add("addon_related_signatures.json.gz", addon_related)

    click.echo(
        f"{len(writer.files)} files, {writer.total_bytes() / 1024:.0f} KiB gzipped",
        err=True,
    )

    if not no_local:
        writer.write_local()
        click.echo(f"wrote {pathlib.Path(output_dir).resolve()}", err=True)

    if results_bucket:
        from google.cloud import storage

        writer.gcs_client = storage.Client(project=billing_project)
        writer.upload()
        click.echo(
            f"uploaded to gs://{results_bucket}/{output.JOB_NAME}/data/", err=True
        )


if __name__ == "__main__":
    sys.exit(main())
