"""Tests for the pruning and redundancy rules.

These pin the behaviour that a port can silently get wrong: which of the several
early-exit conditions fires, and the type sensitivity in ignore_rule. A wrong answer
here doesn't raise, it just changes which correlations reach the tab.
"""

from crash_correlations import mining, pruning


def item(column, value):
    return frozenset(((column, value),))


# {version column: presence column}, the shape queries.Features.addon_version_columns
# produces. The generated names are what the job really uses; a rule keyed on a naming
# convention passed its tests here while never firing on a real run.
ADDON_VERSIONS = {"ADDON0_VERSION": "ADDON0"}


def counts_fixture(reference=None, group=None, total_reference=1000, total_group=100):
    return pruning.Counts(
        counts={
            mining.REFERENCE: reference or {},
            "sigA": group or {},
        },
        totals={mining.REFERENCE: total_reference, "sigA": total_group},
    )


class TestCounts:
    def test_missing_itemset_counts_zero(self):
        # Upstream raises KeyError; 0 is the meaningful answer for an itemset that
        # never cleared MIN_COUNT.
        counts = counts_fixture()
        assert counts.count(item("a", "1"), "sigA") == 0

    def test_support_handles_zero_total(self):
        counts = pruning.Counts(counts={}, totals={mining.REFERENCE: 0, "sigA": 0})
        assert counts.support(item("a", "1"), "sigA") == 0.0


class TestShouldPruneLevel1:
    def test_keeps_a_strong_candidate(self):
        candidate = item("a", "1")
        counts = counts_fixture(
            reference={candidate: 100}, group={candidate: 80}
        )
        assert not pruning.should_prune(counts, "sigA", candidate, None, 0.15)

    def test_prunes_below_min_count_in_reference(self):
        candidate = item("a", "1")
        counts = counts_fixture(reference={candidate: 4}, group={candidate: 4})
        assert pruning.should_prune(counts, "sigA", candidate, None, 0.15)

    def test_prunes_below_min_count_in_group(self):
        candidate = item("a", "1")
        counts = counts_fixture(reference={candidate: 500}, group={candidate: 4})
        assert pruning.should_prune(counts, "sigA", candidate, None, 0.15)

    def test_prunes_when_both_supports_below_threshold(self):
        # 50/1000 and 5/100 are both under 0.15, so neither side is interesting.
        candidate = item("a", "1")
        counts = counts_fixture(reference={candidate: 50}, group={candidate: 5})
        assert pruning.should_prune(counts, "sigA", candidate, None, 0.15)

    def test_keeps_when_only_group_support_clears(self):
        # The whole point of the job: rare overall, common in this signature.
        candidate = item("a", "1")
        counts = counts_fixture(reference={candidate: 50}, group={candidate: 80})
        assert not pruning.should_prune(counts, "sigA", candidate, None, 0.15)


class TestShouldPruneLevel2:
    def test_prunes_when_a_parent_is_missing(self):
        # A parent below MIN_COUNT would divide by zero upstream.
        left, right = item("a", "1"), item("b", "2")
        candidate = left | right
        counts = counts_fixture(
            reference={candidate: 100}, group={candidate: 80}
        )
        assert pruning.should_prune(
            counts, "sigA", candidate, (left, right), 0.15
        )

    def test_keeps_a_candidate_that_always_co_occurs(self):
        """A pair holding in exactly its parents' rows is kept, not pruned.

        Worth pinning because the intuition points the other way. When the candidate
        count equals the parent count, support given the parent is 1.0, which is a
        large shift from the parent's own support, so the first check in should_prune
        keeps it. Only a candidate whose conditional support is close to its parent's
        unconditional support gets pruned.
        """
        left, right = item("a", "1"), item("b", "2")
        candidate = left | right
        counts = counts_fixture(
            reference={candidate: 300, left: 300, right: 300},
            group={candidate: 80, left: 80, right: 80},
        )
        assert not pruning.should_prune(
            counts, "sigA", candidate, (left, right), 0.15
        )

    def test_prunes_when_conditioning_barely_moves_support(self):
        """Pruned when the parents are near universal.

        Reaching the prune branch is narrower than it looks. The first check keeps the
        candidate when support-given-parent differs from the parent's own support by
        more than the threshold, and the second prunes when the candidate's support is
        close to the parent's. Satisfying the second without tripping the first
        requires the parent to cover almost every row, so that both the parent's
        support and the conditional support are near 1. A grid search over more
        ordinary values found no case that prunes here, which is worth knowing: for
        typical inputs this branch is unreachable and level 2 pruning is effectively
        the MIN_COUNT and support checks plus the chi-squared test.
        """
        left, right = item("a", "1"), item("b", "2")
        candidate = left | right
        counts = counts_fixture(
            reference={candidate: 985, left: 990, right: 990},
            group={candidate: 97, left: 99, right: 99},
        )
        assert pruning.should_prune(
            counts, "sigA", candidate, (left, right), 0.15
        )


class TestIgnoreRule:
    def test_drops_false_when_true_is_kept(self):
        candidate = item("addon_x", False)
        kept = {candidate, item("addon_x", True)}
        counts = counts_fixture()
        assert pruning.ignore_rule(candidate, kept, counts, "sigA", set())

    def test_keeps_false_when_true_is_absent(self):
        candidate = item("addon_x", False)
        counts = counts_fixture()
        assert not pruning.ignore_rule(candidate, {candidate}, counts, "sigA", set())

    def test_false_must_be_a_real_bool(self):
        """The string 'false' must not trigger the bool rule.

        This is the bug class that disabled this rule in the Spark job for years: a
        UDF declared StringType while returning a bool, so the value arrived as
        'false' and `is False` never matched.
        """
        candidate = item("addon_x", "false")
        kept = {candidate, item("addon_x", True)}
        counts = counts_fixture()
        assert not pruning.ignore_rule(candidate, kept, counts, "sigA", set())

    def test_drops_none_alongside_one(self):
        candidate = item("is_garbage_collecting", None)
        kept = {candidate, item("is_garbage_collecting", "1")}
        counts = counts_fixture()
        assert pruning.ignore_rule(candidate, kept, counts, "sigA", set())

    def test_drops_none_alongside_active(self):
        candidate = item("accessibility", None)
        kept = {candidate, item("accessibility", "Active")}
        counts = counts_fixture()
        assert pruning.ignore_rule(candidate, kept, counts, "sigA", set())

    def test_only_true_matters_for_submitted_from_infobar(self):
        counts = counts_fixture()
        for value in (False, None, "1"):
            candidate = item("submitted_from_infobar", value)
            assert pruning.ignore_rule(candidate, {candidate}, counts, "sigA", set())
        candidate = item("submitted_from_infobar", True)
        assert not pruning.ignore_rule(candidate, {candidate}, counts, "sigA", set())

    def test_drops_unavailable_addon_version(self):
        counts = counts_fixture()
        for value in (None, "Not installed"):
            candidate = item("ADDON0_VERSION", value)
            assert pruning.ignore_rule(
                candidate, {candidate}, counts, "sigA", ADDON_VERSIONS
            )

    def test_drops_addon_version_that_tracks_presence(self):
        # Version present in as many rows as the addon itself, so it's redundant.
        version = item("ADDON0_VERSION", "1.0")
        presence = item("ADDON0", True)
        counts = counts_fixture(group={version: 80, presence: 80})
        assert pruning.ignore_rule(
            version, {version, presence}, counts, "sigA", ADDON_VERSIONS
        )

    def test_keeps_addon_version_that_differs_from_presence(self):
        version = item("ADDON0_VERSION", "1.0")
        presence = item("ADDON0", True)
        counts = counts_fixture(group={version: 20, presence: 80})
        assert not pruning.ignore_rule(
            version, {version, presence}, counts, "sigA", ADDON_VERSIONS
        )

    def test_uses_the_real_generated_column_names(self):
        """The rule used to match a '-version' suffix, which is upstream's naming.

        The columns are ADDON<n>_VERSION here, so it never fired and every
        'Not installed' row reached the output. Build the mapping the way the job does
        rather than hand-writing it, so a rename breaks this test instead of the output.
        """
        from crash_correlations import queries

        features = queries.Features(
            pairs={"addon": [("ADDON0", "ublock@example.com")]}, labels={}, counts={}
        )
        addon_versions = features.addon_version_columns
        assert addon_versions == {"ADDON0_VERSION": "ADDON0"}

        candidate = item("ADDON0_VERSION", "Not installed")
        assert pruning.ignore_rule(
            candidate, {candidate}, counts_fixture(), "sigA", addon_versions
        )

    def test_unrelated_version_column_is_untouched(self):
        """Only addon version columns are covered, not anything ending in _VERSION."""
        candidate = item("cpu_microcode_version", "Not installed")
        assert not pruning.ignore_rule(
            candidate, {candidate}, counts_fixture(), "sigA", ADDON_VERSIONS
        )


class TestFilterLevel1:
    def test_orders_output(self):
        strong = [item("b", "1"), item("a", "1")]
        counts = counts_fixture(
            reference={i: 200 for i in strong},
            group={i: 80 for i in strong},
        )
        result = pruning.filter_level_1(counts, "sigA", strong, {}, 0.15)
        assert result == sorted(result, key=mining.itemset_sort_key)

    def test_applies_both_pruning_and_ignore_rules(self):
        true_item = item("addon_x", True)
        false_item = item("addon_x", False)
        weak = item("c", "1")
        counts = counts_fixture(
            reference={true_item: 200, false_item: 200, weak: 4},
            group={true_item: 80, false_item: 80, weak: 4},
        )
        result = pruning.filter_level_1(
            counts, "sigA", [true_item, false_item, weak], {}, 0.15
        )
        # weak pruned on MIN_COUNT, false dropped as redundant with true.
        assert result == [true_item]


class TestFilterLevel2:
    def test_preserves_input_order(self):
        """Output order must follow input order.

        filter_level_2 does not sort, because generate_candidates already returns its
        candidates in itemset_sort_key order and re-sorting 2.2 million of them cost 7.6s
        on release. That makes sorted input a precondition, so this pins that the function
        is order preserving: if it ever started sorting, or stopped preserving, the
        order-dependent filtering downstream would quietly change which rules survive.
        """
        pair_a = item("a", "1") | item("b", "1")
        pair_b = item("a", "1") | item("c", "1")
        pair_c = item("b", "1") | item("c", "1")
        ordered = sorted([pair_a, pair_b, pair_c], key=mining.itemset_sort_key)
        counts = counts_fixture(
            reference={p: 200 for p in ordered} | {item(c, "1"): 400 for c in "abc"},
            group={p: 80 for p in ordered} | {item(c, "1"): 90 for c in "abc"},
        )
        result = pruning.filter_level_2(counts, "sigA", ordered, {}, 0.15)
        assert result == [i for i in ordered if i in set(result)]

        # Reversed in, reversed out: the function itself imposes no order.
        reversed_result = pruning.filter_level_2(
            counts, "sigA", list(reversed(ordered)), {}, 0.15
        )
        assert reversed_result == list(reversed(result))

    def test_main_satisfies_the_sorted_precondition(self):
        # The precondition is only safe because generate_candidates guarantees it, which
        # is what main.py passes straight through.
        previous = {
            "sigA": [item("b", "2"), item("a", "1"), item("c", "3")],
        }
        candidates, _ = mining.generate_candidates(previous)
        assert candidates["sigA"] == sorted(
            candidates["sigA"], key=mining.itemset_sort_key
        )


class TestSignificance:
    def test_returns_p_and_phi(self):
        candidate = item("a", "1")
        counts = counts_fixture(reference={candidate: 100}, group={candidate: 80})
        p, phi = pruning.significance(counts, "sigA", candidate)
        assert 0.0 <= p <= 1.0
        # Strongly over-represented in the group, so this should be significant.
        assert p < 0.01
        assert phi > 0

    def test_independent_support_check_spots_independence(self):
        left, right = item("a", "1"), item("b", "2")
        candidate = left | right
        # 50% and 50% individually, 25% together is exactly independence.
        counts = counts_fixture(group={left: 50, right: 50, candidate: 25})
        assert pruning.independent_support_check(counts, "sigA", candidate)

    def test_independent_support_check_spots_dependence(self):
        left, right = item("a", "1"), item("b", "2")
        candidate = left | right
        # Co-occur far more than independence predicts.
        counts = counts_fixture(group={left: 50, right: 50, candidate: 50})
        assert not pruning.independent_support_check(counts, "sigA", candidate)
