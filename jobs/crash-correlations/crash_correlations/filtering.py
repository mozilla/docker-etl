"""The final rules filtering, which turns counted candidates into output rows.

Ported from crash_deviations.py:778-910. The last stage before the JSON is written,
and the most intricate: it drops candidates that aren't statistically significant,
attaches a "prior" when the priors graph says another correlation explains this one,
and suppresses whole families of rules via the `to_skip` accumulator.

Two properties of the original worth knowing before changing anything here.

`alpha_k` leaks across groups. Upstream initialises it once at line 781, outside the
per-signature loop, and only ever shrinks it with min(). So the significance threshold
a signature is judged against depends on how many candidates the signatures processed
before it had. That is preserved here, because changing it would change which
correlations appear for every signature but the first. `share_alpha_k=False` opts into
the per-signature behaviour if that's ever wanted.

The stage is order dependent by construction: `to_skip` grows as candidates are
visited, and the prior lookup only sees results already recorded. Feed candidates in a
stable order (mining.order_candidates) or the output varies between runs.
"""

import datetime

from crash_correlations import mining, priors as priors_module, pruning


ALPHA = 0.05


def clean_item(itemset, labels):
    """The candidate as the output JSON's "item" object.

    crash_deviations.py:754. `labels` maps a feature column name to the label Crash
    Stats should show, e.g. MOD0 -> 'Module "xul.dll"'. Upstream computes those inline
    from module_ids, all_addons, all_gfx_critical_errors and all_app_notes; keeping it
    a plain dict means this function doesn't need any of that context.

    Dates are stringified because json.dump can't serialise datetime.date.

    Sorted before building the dict. An itemset is a frozenset, so iterating it directly
    gives an order that varies with the hash seed, and json.dump preserves insertion
    order: without the sort, two runs over identical data produce byte-different files
    whose parsed contents are the same. Measured before fixing it, 9 of 130 beta files
    differed that way. The sort is on the internal column name rather than the label, so
    it doesn't shift when a label changes.
    """
    item = {}
    for column, value in sorted(itemset, key=lambda pair: (pair[0], repr(pair[1]))):
        key = labels.get(column, column)
        item[key] = str(value) if isinstance(value, datetime.date) else value
    return item


def _prior_payload(prior, candidate, counts, group, labels):
    """The "prior" object attached to a result. crash_deviations.py:837."""
    return {
        "item": clean_item(prior, labels),
        "count_reference": float(counts.count(candidate, mining.REFERENCE)),
        "count_group": float(counts.count(candidate, group)),
        "total_reference": float(counts.count(prior, mining.REFERENCE)),
        "total_group": float(counts.count(prior, group)),
    }


def _apply_priors(candidate, counts, group, results, reachable, labels,
                  min_support_diff):
    """Attach this candidate's finding to an existing result as a prior.

    crash_deviations.py:801-846. Returns True when a prior was found, meaning the
    candidate itself should not be emitted: its information has been folded into the
    result for the remaining items instead.

    Also appends to `to_skip` via the returned list, for the case where conditioning
    on the prior removes the difference entirely.
    """
    count_reference = counts.count(candidate, mining.REFERENCE)
    count_group = counts.count(candidate, group)

    elems = [frozenset((item,)) for item in candidate]
    got_prior = False
    newly_skipped = []

    for prior in priors_module.possible_priors(candidate, reachable):
        others = frozenset().union(*[e for e in elems if e != prior])
        if others not in results:
            continue

        count_prior_reference = counts.count(prior, mining.REFERENCE)
        count_prior_group = counts.count(prior, group)
        if not count_prior_reference or not count_prior_group:
            # Upstream divides by these unguarded. Unreachable for candidates whose
            # parents survived pruning, but a zero here would be a crash not a result.
            continue

        others_support_group = counts.support(others, group)
        others_support_reference = counts.support(others, mining.REFERENCE)
        support_group_given_prior = count_group / count_prior_group
        support_reference_given_prior = count_reference / count_prior_reference

        # Conditioning on the prior removes the difference, so the remaining items
        # aren't interesting either: skip anything containing them.
        if (
            abs(support_reference_given_prior - support_group_given_prior)
            < min_support_diff
        ):
            got_prior = True
            newly_skipped.append(others)
            continue

        threshold = min(0.05, min_support_diff / 2)
        if (
            abs(others_support_group - support_group_given_prior) < threshold
            and abs(others_support_reference - support_reference_given_prior)
            < threshold
        ):
            continue

        got_prior = True

        existing = results[others].get("prior")
        if existing is not None:
            # Keep whichever prior explains the difference best.
            existing_diff = abs(
                existing["count_reference"] / existing["total_reference"]
                - existing["count_group"] / existing["total_group"]
            )
            if (
                abs(support_reference_given_prior - support_group_given_prior)
                > existing_diff
            ):
                continue

        results[others]["prior"] = _prior_payload(
            prior, candidate, counts, group, labels
        )

    return got_prior, newly_skipped


def filter_group(
    counts,
    group,
    candidates,
    reachable,
    labels,
    min_support_diff,
    min_corr,
    alpha_k=ALPHA,
    candidate_counts_by_level=None,
):
    """Run the final filtering for one signature.

    `candidates` must already be in a stable order. `candidate_counts_by_level` is
    {level: number of candidates at that level for this group}, used by the alpha
    correction; upstream reads len(candidates[level][group_name]).

    Returns (results, alpha_k) where results is {itemset: output row} and alpha_k is
    the possibly-tightened threshold to carry into the next group.
    """
    results = {}
    to_skip = []

    for candidate in candidates:
        count_reference = counts.count(candidate, mining.REFERENCE)
        count_group = counts.count(candidate, group)
        support_reference = counts.support(candidate, mining.REFERENCE)
        support_group = counts.support(candidate, group)

        if any(skip <= candidate for skip in to_skip):
            continue

        if len(candidate) > 1:
            got_prior, newly_skipped = _apply_priors(
                candidate,
                counts,
                group,
                results,
                reachable,
                labels,
                min_support_diff,
            )
            to_skip.extend(newly_skipped)
            if got_prior:
                continue

        # The signature has to differ from the channel by enough to be worth showing.
        if abs(support_reference - support_group) < min_support_diff:
            continue

        if len(candidate) != 1:
            if pruning.independent_support_check(counts, group, candidate):
                continue
            if pruning.fisher_p(counts, group, candidate) > alpha_k:
                continue

        p, phi = pruning.significance(counts, group, candidate)

        # Bonferroni-style correction, halving per level and dividing by the number of
        # candidates at that level. Only ever tightens, and upstream lets it carry
        # across signatures; see the module docstring.
        level = len(candidate)
        num_candidates = (candidate_counts_by_level or {}).get(level, 1) or 1
        alpha_k = min((ALPHA / pow(2, level)) / num_candidates, alpha_k)

        if p > alpha_k:
            continue
        if phi < min_corr:
            continue

        results[candidate] = {
            "item": clean_item(candidate, labels),
            "count_reference": float(count_reference),
            "count_group": float(count_group),
            "prior": None,
        }

    # to_skip is applied again at the end, because a family can be marked for skipping
    # after some of its members were already recorded.
    for candidate in list(results):
        if any(skip <= candidate for skip in to_skip):
            del results[candidate]

    return results, alpha_k


def filter_all(
    counts,
    groups,
    candidates_by_group,
    reachable,
    labels,
    min_support_diff=0.15,
    min_corr=0.03,
    candidate_counts_by_level=None,
    share_alpha_k=True,
):
    """Run the filtering for every signature.

    `groups` is the ordered list of signatures. Order matters when share_alpha_k is
    True, which is the default because it's what upstream does.

    Returns {group: list of output rows}, which is what goes into the per signature
    JSON files as "results".
    """
    output = {}
    alpha_k = ALPHA
    for group in groups:
        group_alpha = alpha_k if share_alpha_k else ALPHA
        results, next_alpha = filter_group(
            counts,
            group,
            candidates_by_group.get(group, ()),
            reachable,
            labels,
            min_support_diff,
            min_corr,
            alpha_k=group_alpha,
            candidate_counts_by_level=(candidate_counts_by_level or {}).get(group),
        )
        if share_alpha_k:
            alpha_k = next_alpha
        output[group] = list(results.values())
    return output
