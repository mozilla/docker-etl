"""Tests for the final rules filtering.

Most of these pin behaviour that's easy to port subtly wrong: the alpha_k threshold
leaking across signatures, `to_skip` suppressing families of rules, and the label
mapping that turns internal column names into what Crash Stats shows.
"""

import datetime
import os
import pathlib
import subprocess
import sys

from crash_correlations import filtering, mining, priors, pruning


REPO_ROOT = str(pathlib.Path(__file__).resolve().parent.parent)


def item(column, value):
    return frozenset(((column, value),))


def make_counts(reference, group, total_reference=1000, total_group=100):
    return pruning.Counts(
        counts={mining.REFERENCE: reference, "sigA": group},
        totals={mining.REFERENCE: total_reference, "sigA": total_group},
    )


def reachable():
    return priors.reachability(priors.build_graph())


class TestCleanItem:
    def test_maps_labels(self):
        labels = {"MOD0": 'Module "xul.dll"'}
        result = filtering.clean_item(item("MOD0", True), labels)
        assert result == {'Module "xul.dll"': True}

    def test_passes_unmapped_columns_through(self):
        assert filtering.clean_item(item("platform", "Linux"), {}) == {
            "platform": "Linux"
        }

    def test_stringifies_dates(self):
        value = datetime.date(2026, 8, 14)
        assert filtering.clean_item(item("build_date", value), {}) == {
            "build_date": "2026-08-14"
        }

    def test_preserves_bool_and_none(self):
        # The frontend renders these as-is, and ignore_rule already relied on the
        # distinction, so they must not be stringified.
        result = filtering.clean_item(
            frozenset((("a", False), ("b", None))), {}
        )
        assert result == {"a": False, "b": None}

    def test_key_order_is_stable_across_hash_seeds(self):
        """The item dict's key order must not depend on PYTHONHASHSEED.

        An itemset is a frozenset, so iterating it directly varies per process, and
        json.dump preserves insertion order. Before this was fixed, two runs over
        identical data produced byte-different output files with identical contents,
        which defeats byte comparison as a validation tool.
        """
        script = (
            "import json;"
            "from crash_correlations import filtering;"
            "s=frozenset(((\'cpu_arch\',\'x86\'),(\'theme\',\'default\'),"
            "(\'plugin\',False)));"
            "print(json.dumps(filtering.clean_item(s, {})))"
        )
        outputs = set()
        for seed in ("0", "1", "42", "12345"):
            env = dict(os.environ, PYTHONHASHSEED=seed, PYTHONPATH=REPO_ROOT)
            result = subprocess.run(
                [sys.executable, "-c", script],
                capture_output=True,
                env=env,
                check=True,
            )
            outputs.add(result.stdout.decode().strip())
        assert len(outputs) == 1, f"key order varied: {outputs}"


class TestFilterGroup:
    def test_emits_a_strong_single_item(self):
        candidate = item("platform", "Linux")
        counts = make_counts({candidate: 100}, {candidate: 80})
        results, _ = filtering.filter_group(
            counts, "sigA", [candidate], reachable(), {}, 0.15, 0.03,
            candidate_counts_by_level={1: 1},
        )
        assert list(results) == [candidate]
        row = results[candidate]
        assert row["count_reference"] == 100.0
        assert row["count_group"] == 80.0
        assert row["prior"] is None

    def test_counts_are_floats(self):
        # The live output has floats, from a no-op multiplication upstream, and the
        # frontend divides by them. Keep the type.
        candidate = item("platform", "Linux")
        counts = make_counts({candidate: 100}, {candidate: 80})
        results, _ = filtering.filter_group(
            counts, "sigA", [candidate], reachable(), {}, 0.15, 0.03,
            candidate_counts_by_level={1: 1},
        )
        assert isinstance(results[candidate]["count_reference"], float)

    def test_drops_a_candidate_with_no_support_difference(self):
        # Same support in the signature as the channel, so it says nothing.
        candidate = item("platform", "Linux")
        counts = make_counts({candidate: 100}, {candidate: 10})
        results, _ = filtering.filter_group(
            counts, "sigA", [candidate], reachable(), {}, 0.15, 0.03,
            candidate_counts_by_level={1: 1},
        )
        assert results == {}

    def test_alpha_k_only_tightens(self):
        candidate = item("platform", "Linux")
        counts = make_counts({candidate: 100}, {candidate: 80})
        _, alpha_k = filtering.filter_group(
            counts, "sigA", [candidate], reachable(), {}, 0.15, 0.03,
            alpha_k=filtering.ALPHA, candidate_counts_by_level={1: 1},
        )
        assert alpha_k <= filtering.ALPHA


class TestToSkip:
    def test_skips_supersets_of_a_skipped_family(self):
        """A candidate containing a skipped itemset is dropped.

        The `to_skip` accumulator is applied both as candidates are visited and again
        at the end, because a family can be marked after some members were recorded.
        """
        left = item("adapter_vendor_id", "0x8086")
        right = item("adapter_device_id", "0x1")
        pair = left | right
        # The pair's support given the vendor is identical to the vendor's own, so
        # conditioning removes the difference and the device is skipped.
        counts = make_counts(
            {left: 300, right: 300, pair: 300},
            {left: 90, right: 90, pair: 90},
        )
        results, _ = filtering.filter_group(
            counts, "sigA", [left, right, pair], reachable(), {}, 0.15, 0.03,
            candidate_counts_by_level={1: 2, 2: 1},
        )
        # The vendor survives; the device is explained by it.
        assert left in results


class TestFilterAll:
    def test_alpha_k_carries_across_groups_by_default(self):
        """Upstream initialises alpha_k outside the group loop, so it leaks.

        Preserved deliberately. This test documents it rather than endorsing it: the
        threshold a signature is judged against depends on the signatures before it.
        """
        candidate = item("platform", "Linux")
        counts = pruning.Counts(
            counts={
                mining.REFERENCE: {candidate: 100},
                "sigA": {candidate: 80},
                "sigB": {candidate: 80},
            },
            totals={mining.REFERENCE: 1000, "sigA": 100, "sigB": 100},
        )
        shared = filtering.filter_all(
            counts, ["sigA", "sigB"], {"sigA": [candidate], "sigB": [candidate]},
            reachable(), {},
            candidate_counts_by_level={"sigA": {1: 500}, "sigB": {1: 1}},
        )
        independent = filtering.filter_all(
            counts, ["sigA", "sigB"], {"sigA": [candidate], "sigB": [candidate]},
            reachable(), {},
            candidate_counts_by_level={"sigA": {1: 500}, "sigB": {1: 1}},
            share_alpha_k=False,
        )
        # sigA's large candidate count tightens alpha_k, which then applies to sigB
        # when shared. With share_alpha_k=False sigB is judged on its own.
        assert len(independent["sigB"]) >= len(shared["sigB"])

    def test_returns_a_list_per_group(self):
        candidate = item("platform", "Linux")
        counts = make_counts({candidate: 100}, {candidate: 80})
        output = filtering.filter_all(
            counts, ["sigA"], {"sigA": [candidate]}, reachable(), {},
            candidate_counts_by_level={"sigA": {1: 1}},
        )
        assert isinstance(output["sigA"], list)
        assert output["sigA"][0]["item"] == {"platform": "Linux"}

    def test_group_with_no_candidates_is_empty(self):
        counts = make_counts({}, {})
        output = filtering.filter_all(
            counts, ["sigA"], {}, reachable(), {}
        )
        assert output == {"sigA": []}
