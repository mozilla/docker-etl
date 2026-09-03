"""Pruning and redundancy suppression over the counted itemsets.

Ported from crash_deviations.py with the logic unchanged. None of this touched Spark
in the original: should_prune, ignore_rule, the priors graph and the final
significance filtering are all arithmetic over the counts, so this is a re-hosting
rather than a rewrite. Line references below are to the vendored copy in
python_mozetl at mozetl/symbolication/crashcorrelations/crash_deviations.py.

The one structural change is that upstream is a single 900 line function whose
stages read a dozen closure variables. Here the counts and totals are passed
explicitly as a Counts object, so each stage can be tested on its own.
"""

import dataclasses
import math
import operator
from functools import reduce

import scipy.stats

from crash_correlations import mining


# Significance level before the per level Bonferroni-style correction.
ALPHA = 0.05


@dataclasses.dataclass
class Counts:
    """Everything the filtering stages need to look up.

    counts is {group: {itemset: count}} as produced by mining.count_level_1 and
    mining.count_level_2, merged. totals is {group: row count}, including
    mining.REFERENCE.
    """

    counts: dict
    totals: dict

    def count(self, itemset, group):
        """Count of an itemset within a group, 0 if it was never counted.

        Upstream's get_count() raises KeyError instead. It gets away with that
        because every itemset it asks about came from a counting pass over the same
        group, but the pruning reads parent counts and single-item counts of 2-item
        candidates, so a miss is reachable. 0 is the honest answer: the itemset
        appeared in fewer than MIN_COUNT rows, which is why it wasn't kept.
        """
        return self.counts.get(group, {}).get(itemset, 0)

    def total(self, group):
        return self.totals[group]

    def support(self, itemset, group):
        total = self.totals[group]
        return self.count(itemset, group) / total if total else 0.0


def should_prune(counts, group, candidate, parents, min_support_diff):
    """Whether a candidate is too weak or too redundant to expand.

    crash_deviations.py:527. parents is the (left, right) pair the candidate was
    built from, or None for level 1 where there are no parents.
    """
    count_reference = counts.count(candidate, mining.REFERENCE)
    count_group = counts.count(candidate, group)
    support_reference = counts.support(candidate, mining.REFERENCE)
    support_group = counts.support(candidate, group)

    if count_reference < mining.MIN_COUNT:
        return True
    if count_group < mining.MIN_COUNT:
        return True
    if support_reference < min_support_diff and support_group < min_support_diff:
        return True

    if parents is None:
        return False

    parent1, parent2 = parents

    # Support of the candidate conditioned on each parent, against the parent's own
    # support. If adding the second item barely moves the support, the candidate says
    # nothing the parent didn't.
    stats = []
    for parent in (parent1, parent2):
        parent_count_reference = counts.count(parent, mining.REFERENCE)
        parent_count_group = counts.count(parent, group)
        if not parent_count_reference or not parent_count_group:
            # Upstream would divide by zero here. Can only happen if a parent fell
            # below MIN_COUNT, in which case the candidate cannot be interesting.
            return True
        stats.append(
            (
                counts.support(parent, mining.REFERENCE),
                counts.support(parent, group),
                count_reference / parent_count_reference,
                count_group / parent_count_group,
            )
        )

    threshold = min(0.05, min_support_diff / 2)

    (p1_sup_ref, p1_sup_grp, p1_given_ref, p1_given_grp) = stats[0]
    (p2_sup_ref, p2_sup_grp, p2_given_ref, p2_given_grp) = stats[1]

    # Keep it if conditioning on both parents shifts the support materially.
    if (
        abs(p1_sup_ref - p1_given_ref) > threshold
        or abs(p1_sup_grp - p1_given_grp) > threshold
    ) and (
        abs(p2_sup_ref - p2_given_ref) > threshold
        or abs(p2_sup_grp - p2_given_grp) > threshold
    ):
        return False

    # Prune if the candidate's support is nearly a parent's own support.
    if (
        abs(p1_sup_ref - support_reference) < threshold
        and abs(p1_sup_grp - support_group) < threshold
    ) or (
        abs(p2_sup_ref - support_reference) < threshold
        and abs(p2_sup_grp - support_group) < threshold
    ):
        return True

    # Prune if neither extension is statistically significant.
    p_values = []
    for parent in (parent1, parent2):
        table = [
            [counts.count(parent, group), count_group],
            [counts.count(parent, mining.REFERENCE), count_reference],
        ]
        p_values.append(scipy.stats.chi2_contingency(table)[1])
    if p_values[0] > 0.5 and p_values[1] > 0.5:
        return True

    return False


def ignore_rule(candidate, kept, counts, group, addon_versions):
    """Whether a level 1 rule is redundant with another that's already kept.

    crash_deviations.py:710. `kept` is the set of surviving 1-item itemsets for the
    group, which the tests below look into.

    `addon_versions` maps an addon's version column to its presence column, i.e.
    {'ADDON3_VERSION': 'ADDON3'}. Upstream took the set of addon names instead and
    recovered the pairing by stripping a '-version' suffix, which is how it named those
    columns. Passing the mapping keeps this from depending on a naming convention: the
    columns here are ADDON<n>_VERSION, and while the rule still matched '-version' it
    never fired at all, so every 'Not installed' row reached the output.

    This is where the value types matter. The checks are `is False`, `is None` and
    equality against the strings '1' and 'Active', so a bool has to be a real bool
    and a NULL a real None. Upstream's addon name UDF declared StringType while
    returning a bool, which made the first check permanently false for years.
    """
    ((column, value),) = candidate

    # addon_X=False is redundant when addon_X=True is also kept.
    if value is False and frozenset(((column, True),)) in kept:
        return True

    # is_garbage_collecting=None alongside ='1', accessibility=None alongside
    # ='Active': the null case adds nothing.
    if value is None and frozenset(((column, "1"),)) in kept:
        return True
    if value is None and frozenset(((column, "Active"),)) in kept:
        return True

    # Only the true case of submitted_from_infobar is meaningful.
    if column == "submitted_from_infobar" and value is not True:
        return True

    if column in addon_versions:
        # An unavailable version says nothing.
        if value is None or value == "Not installed":
            return True
        # Nor does a version that tracks the addon's mere presence.
        presence = frozenset(((addon_versions[column], True),))
        if presence in kept:
            group_total = counts.total(group)
            if group_total:
                delta = abs(
                    counts.count(candidate, group) / group_total
                    - counts.count(presence, group) / group_total
                )
                if delta <= 0.01:
                    return True

    return False


def filter_level_1(counts, group, itemsets, addon_versions, min_support_diff):
    """Level 1 pruning followed by the redundancy rules.

    crash_deviations.py:704 and 737. Ordered so a rerun gives the same result; see
    mining.itemset_sort_key.

    `addon_versions` is queries.Features.addon_version_columns; see ignore_rule.
    """
    surviving = {
        itemset
        for itemset in itemsets
        if not should_prune(counts, group, itemset, None, min_support_diff)
    }
    return [
        itemset
        for itemset in sorted(surviving, key=mining.itemset_sort_key)
        if not ignore_rule(itemset, surviving, counts, group, addon_versions)
    ]


def filter_level_2(counts, group, itemsets, parents, min_support_diff):
    """Level 2 pruning. crash_deviations.py:654.

    `itemsets` must already be ordered by mining.itemset_sort_key, which is what
    generate_candidates returns. Output order follows input order.

    Not sorted here, unlike filter_level_1, whose input is a set. Re-sorting cost 7.6s of
    this stage's 30.3s on release: timsort is cheap on already-sorted input but the key
    function still runs once per itemset, and there are 2.2 million of them. The order is
    load-bearing rather than cosmetic, since the filtering downstream is order dependent,
    so this is a precondition rather than a detail; test_requires_sorted_input pins it.
    """
    return [
        itemset
        for itemset in itemsets
        if not should_prune(
            counts, group, itemset, parents.get(itemset), min_support_diff
        )
    ]


def independent_support_check(counts, group, candidate):
    """True when a multi-item candidate's support is what independence predicts.

    crash_deviations.py:855. If the items co-occur about as often as their individual
    rates imply, the combination isn't telling you anything.
    """
    total_group = counts.total(group)
    if not total_group:
        return True
    support_group = counts.support(candidate, group)
    independent = reduce(
        operator.mul,
        [
            counts.count(frozenset((item,)), group) / total_group
            for item in candidate
        ],
    )
    return abs(independent - support_group) <= max(0.01, 0.15 * support_group)


def fisher_p(counts, group, candidate):
    """Fisher exact p for a 2-item candidate. crash_deviations.py:863.

    The contingency table upstream builds is not the textbook one, and it's
    reproduced as-is rather than corrected, since changing it would change which
    correlations appear. The TODO there about not assuming two elements still
    applies: it reads exactly two single-item counts, which is safe only because the
    level cap is 2.
    """
    total_group = counts.total(group)
    count_group = counts.count(candidate, group)
    singles = [
        counts.count(frozenset((item,)), group)
        for item in sorted(candidate, key=lambda pair: (pair[0], repr(pair[1])))
    ]
    elem1, elem2 = singles[0], singles[1]
    table = [
        [count_group, total_group - elem1],
        [total_group - elem2, total_group - count_group],
    ]
    return scipy.stats.fisher_exact(table)[1]


def significance(counts, group, candidate):
    """Chi-squared p and the phi coefficient. crash_deviations.py:879."""
    count_reference = counts.count(candidate, mining.REFERENCE)
    count_group = counts.count(candidate, group)
    total_reference = counts.total(mining.REFERENCE)
    total_group = counts.total(group)
    table = [
        [count_group, count_reference],
        [total_group - count_group, total_reference - count_reference],
    ]
    chi2, p = scipy.stats.chi2_contingency(table)[:2]
    phi = math.sqrt(chi2 / (total_reference + total_group))
    return p, phi
