"""Tests for the itemset counting.

The load-bearing test here is test_matches_naive_subset_counting: the fast inverted
loop has to produce exactly the counts the Spark implementation's
candidate-against-every-row approach would, because a difference wouldn't show up as
an error, only as slightly different correlations in the output.
"""

import itertools
import os
import pathlib
import subprocess
import sys

import pyarrow as pa

from crash_correlations import mining


REPO_ROOT = str(pathlib.Path(__file__).resolve().parent.parent)


COLUMNS = ["platform", "cpu_arch", "plugin"]


# Written row-wise because that's readable, then transposed. The table itself is
# columnar Arrow; see mining.FeatureTable.
ROWS = [
    ("sigA", "Windows NT", "x86", False),
    ("sigA", "Windows NT", "x86", False),
    ("sigA", "Windows NT", "amd64", True),
    ("sigB", "Linux", "amd64", False),
    ("sigB", None, "amd64", False),
]


def table_fixture(rows=None):
    """Three columns, deliberately including a bool and a None value.

    Goes through from_columns rather than pa.table so the fixtures stay literal Python.
    The bool column has to be pinned as boolean rather than inferred, which is the one
    thing that changed when the table moved to Arrow.
    """
    rows = ROWS if rows is None else rows
    return mining.FeatureTable.from_columns(
        signatures=[row[0] for row in rows],
        values={
            column: [row[i + 1] for row in rows]
            for i, column in enumerate(COLUMNS)
        },
    )


def rows_fixture(rows=None):
    """The same data as a list of row dicts, for the naive-counting comparison."""
    table = table_fixture(rows)
    return [
        dict(signature=table.signatures[i], **table.row(i))
        for i in range(len(table))
    ]


class TestFeatureTable:
    def test_len_is_the_row_count(self):
        assert len(table_fixture()) == 5

    def test_columns_excludes_signature(self):
        # signature is held separately, not as a feature column, so it can never be
        # counted as one.
        assert table_fixture().columns == COLUMNS
        assert "signature" not in table_fixture().columns

    def test_row_reconstructs_a_row(self):
        table = table_fixture()
        assert table.row(2) == {
            "platform": "Windows NT",
            "cpu_arch": "amd64",
            "plugin": True,
        }

    def test_values_keep_their_python_types(self):
        # The table is Arrow, but a value read out of it has to be a real Python bool or
        # None, because ignore_rule discriminates with `is False` and `is None`. This is
        # what the counting passes rely on when they build itemsets.
        table = table_fixture()
        assert table.column("plugin")[2] is True
        assert table.column("plugin")[0] is False
        assert table.column("platform")[4] is None

    def test_signature_is_not_a_feature_column(self):
        # It's in the Arrow table, so it has to be excluded by name rather than by
        # being held separately as it was when the table was a dict of lists.
        table = table_fixture()
        assert mining.SIGNATURE_COLUMN in table.arrow.column_names
        assert mining.SIGNATURE_COLUMN not in table.columns

    def test_from_columns_pins_a_boolean_column_as_boolean(self):
        # Inference would make an all-True column boolean and an all-None one null type,
        # and a null-typed column can't be compared against anything. Both have to end
        # up boolean so False and None are still countable items.
        table = mining.FeatureTable.from_columns(
            ["s"] * 2, {"flag": [True, None], "empty": [None, None]}
        )
        assert table.arrow.column("flag").type == pa.bool_()
        assert table.column("flag") == [True, None]
        assert table.column("empty") == [None, None]

    def test_chunked_input_is_combined(self):
        # A bitset is read straight out of one contiguous buffer, so a column arriving in
        # several chunks (which is what to_arrow gives, one per Storage API stream) has to
        # be combined first or only the first chunk would be seen.
        first = pa.table({"signature": ["a"], "flag": [True]})
        second = pa.table({"signature": ["b"], "flag": [False]})
        table = mining.FeatureTable(pa.concat_tables([first, second]))
        assert len(table.arrow.column("flag").chunks) == 1
        counts, _ = mining.count_level_1(table, ["flag"], ["a", "b"])
        assert counts[mining.REFERENCE][frozenset((("flag", True),))] == 1
        assert counts[mining.REFERENCE][frozenset((("flag", False),))] == 1

    def test_empty_table_works(self):
        # A channel with no crashes for its versions is skipped by main, but only after
        # the table is built, so constructing and counting over zero rows must not raise.
        table = mining.FeatureTable.from_columns([], {"flag": []})
        assert len(table) == 0
        counts, totals = mining.count_level_1(table, ["flag"], ["sigA"])
        assert totals[mining.REFERENCE] == 0
        assert not counts[mining.REFERENCE]


class TestBitsets:
    """The buffer arithmetic behind count_level_2.

    A boolean column's Arrow values buffer already *is* the bitset for True, which is what
    makes level 2 cheap, but it means reading raw bits and the edge cases are all in how
    Arrow lays them out: validity is separate, the values bits are undefined where
    validity is 0, and a sliced array starts partway into its parent's buffer.
    """

    def test_boolean_column_splits_into_three_values(self):
        array = pa.array([True, False, None, True], pa.bool_())
        bitsets = mining._bitsets(array, 4)
        assert bitsets[True] == 0b1001
        assert bitsets[False] == 0b0010
        assert bitsets[None] == 0b0100

    def test_no_null_column_has_no_none_item(self):
        array = pa.array([True, False], pa.bool_())
        assert set(mining._bitsets(array, 2)) == {True, False}

    def test_values_bits_are_masked_by_validity(self):
        # Arrow leaves the values bit undefined where validity is 0. If it happens to be
        # set, a null row would be counted as True; if clear, as False. Both have to be
        # excluded, which is what the & valid does.
        array = pa.array([None] * 8, pa.bool_())
        bitsets = mining._bitsets(array, 8)
        assert bitsets.get(True, 0) == 0
        assert bitsets.get(False, 0) == 0
        assert bitsets[None] == 0xFF

    def test_string_column_masks_each_value(self):
        array = pa.array(["a", "b", None, "a"])
        bitsets = mining._bitsets(array, 4)
        assert bitsets["a"] == 0b1001
        assert bitsets["b"] == 0b0010
        assert bitsets[None] == 0b0100

    def test_wanted_restricts_which_values_are_masked(self):
        # The optimisation that keeps the high cardinality columns cheap: address has
        # 73,804 distinct values on release and level 2 needs only the few in candidates.
        array = pa.array(["a", "b", None, "a"])
        assert set(mining._bitsets(array, 4, wanted={"a"})) == {"a"}
        assert set(mining._bitsets(array, 4, wanted={"a", None})) == {"a", None}
        assert mining._bitsets(array, 4, wanted={"a"})["a"] == 0b1001

    def test_sliced_array_reads_from_its_own_offset(self):
        # A slice shares its parent's buffers and starts `offset` bits in, so reading the
        # buffer without shifting would return the parent's leading rows instead.
        array = pa.array([True, True, False, True, False], pa.bool_())
        sliced = array.slice(2, 3)
        assert sliced.offset == 2
        bitsets = mining._bitsets(sliced, 3)
        assert bitsets[True] == 0b010
        assert bitsets[False] == 0b101

    def test_bits_beyond_the_length_are_not_counted(self):
        # Arrow pads the buffer to a byte, so a 3 row column has 5 spare bits. They must
        # not appear in a popcount.
        array = pa.array([True, True, True], pa.bool_())
        assert mining._bitsets(array, 3)[True].bit_count() == 3


class TestCountLevel1:
    def test_reference_counts_every_row(self):
        counts, totals = mining.count_level_1(table_fixture(), COLUMNS, ["sigA"])
        reference = counts[mining.REFERENCE]
        assert totals[mining.REFERENCE] == 5
        assert reference[frozenset((("platform", "Windows NT"),))] == 3
        assert reference[frozenset((("cpu_arch", "amd64"),))] == 3
        assert reference[frozenset((("plugin", False),))] == 4

    def test_group_counts_only_that_signature(self):
        counts, totals = mining.count_level_1(table_fixture(), COLUMNS, ["sigA"])
        assert totals["sigA"] == 3
        assert counts["sigA"][frozenset((("platform", "Windows NT"),))] == 3
        assert counts["sigA"][frozenset((("cpu_arch", "amd64"),))] == 1

    def test_signatures_not_asked_for_are_absent(self):
        counts, totals = mining.count_level_1(table_fixture(), COLUMNS, ["sigA"])
        assert "sigB" not in counts
        assert "sigB" not in totals

    def test_none_is_a_counted_value(self):
        # get_arch returns None when it can't classify and ignore_rule looks for that
        # itemset, so NULL is a value here rather than missing data.
        counts, _ = mining.count_level_1(table_fixture(), COLUMNS, ["sigB"])
        assert counts[mining.REFERENCE][frozenset((("platform", None),))] == 1

    def test_bool_values_stay_bool(self):
        counts, _ = mining.count_level_1(table_fixture(), COLUMNS, ["sigA"])
        item = frozenset((("plugin", False),))
        ((_, value),) = item
        assert value is False
        assert counts[mining.REFERENCE][item] == 4

    def test_reference_count_is_the_sum_over_groups(self):
        # The reference count comes from the same grouped aggregation as the group counts
        # rather than a second pass, so it has to include the rows of signatures nobody
        # asked about. Here sigB's two amd64 rows are only reachable that way.
        counts, _ = mining.count_level_1(table_fixture(), COLUMNS, ["sigA"])
        assert counts[mining.REFERENCE][frozenset((("cpu_arch", "amd64"),))] == 3
        assert counts["sigA"][frozenset((("cpu_arch", "amd64"),))] == 1

    def test_every_row_is_counted_for_every_column(self):
        # Arrow's count aggregation skips nulls by default, which would silently lose the
        # null rows; count_all is used instead. Checked as an invariant over all columns
        # rather than per value, since that's the property that breaks.
        table = table_fixture()
        counts, totals = mining.count_level_1(table, COLUMNS, ["sigA"])
        for column in COLUMNS:
            counted = sum(
                count
                for itemset, count in counts[mining.REFERENCE].items()
                if next(iter(itemset))[0] == column
            )
            assert counted == len(table), column


class TestGenerateCandidates:
    def test_pairs_items_within_a_group(self):
        previous = {
            "sigA": [
                frozenset((("platform", "Windows NT"),)),
                frozenset((("cpu_arch", "x86"),)),
            ]
        }
        candidates, _ = mining.generate_candidates(previous)
        assert candidates["sigA"] == [
            frozenset((("platform", "Windows NT"), ("cpu_arch", "x86")))
        ]

    def test_skips_pairs_on_the_same_column(self):
        # platform can't be both values for one row, so the pair can never be counted.
        previous = {
            "sigA": [
                frozenset((("platform", "Windows NT"),)),
                frozenset((("platform", "Linux"),)),
            ]
        }
        candidates, _ = mining.generate_candidates(previous)
        assert candidates == {"sigA": []}

    def test_handles_mixed_value_types(self):
        # str, bool and None don't sort against each other in Python 3; the sort key
        # uses repr to avoid a TypeError.
        previous = {
            "sigA": [
                frozenset((("platform", "Linux"),)),
                frozenset((("plugin", True),)),
                frozenset((("cpu_arch", None),)),
            ]
        }
        candidates, _ = mining.generate_candidates(previous)
        assert len(candidates["sigA"]) == 3

    def test_equal_candidates_are_one_object_across_groups(self):
        """A candidate in several groups must be a single shared object.

        `left | right` builds a new frozenset per group, so without interning the same
        value is allocated once per group. On release that's 2,212,768 allocations for
        93,415 distinct values and 0.45 GB of duplication. Checked with `is` rather than
        `==` because equality holds either way, which is exactly why the regression would
        be invisible.
        """
        items = [
            frozenset((("platform", "Linux"),)),
            frozenset((("cpu_arch", "x86"),)),
        ]
        groups = {name: list(items) for name in ("sigA", "sigB", "sigC")}
        candidates, parents = mining.generate_candidates(groups)

        first = candidates["sigA"][0]
        assert candidates["sigB"][0] is first
        assert candidates["sigC"][0] is first
        # The parent tuples are per candidate, not per group, for the same reason.
        assert parents["sigB"][first] is parents["sigA"][first]

    def test_interning_does_not_share_between_different_values(self):
        # The pool is keyed by value, so two genuinely different candidates must stay
        # distinct however similar they look.
        previous = {
            "sigA": [
                frozenset((("platform", "Linux"),)),
                frozenset((("cpu_arch", "x86"),)),
                frozenset((("plugin", True),)),
            ]
        }
        candidates, _ = mining.generate_candidates(previous)
        assert len({id(c) for c in candidates["sigA"]}) == len(candidates["sigA"])
        assert len(set(candidates["sigA"])) == len(candidates["sigA"])

    def test_interned_candidates_still_carry_their_own_parents(self):
        # Interning must not let one group's parents leak into another's when the groups
        # have different level 1 survivors: the shared candidate keeps the same parents
        # (there is only one decomposition), but each group's dict must still be keyed
        # only by its own candidates.
        shared = [
            frozenset((("platform", "Linux"),)),
            frozenset((("cpu_arch", "x86"),)),
        ]
        previous = {
            "sigA": shared,
            "sigB": shared + [frozenset((("plugin", True),))],
        }
        candidates, parents = mining.generate_candidates(previous)
        assert len(candidates["sigA"]) == 1
        assert len(candidates["sigB"]) == 3
        assert set(parents["sigA"]) == set(candidates["sigA"])
        assert set(parents["sigB"]) == set(candidates["sigB"])
        for group in ("sigA", "sigB"):
            for candidate, (left, right) in parents[group].items():
                assert left | right == candidate


class TestModulePairing:
    """A module pairs only with MODULE_PAIR_ALLOWED, and the rule must be symmetric.

    Upstream writes this asymmetrically, so (platform, MOD0) is kept and (MOD0, platform)
    dropped. Candidates are fed in sorted order here and every module column sorts before
    every allowed column, so transcribing the asymmetry dropped every module/platform
    pair: no module correlation could get a platform prior, and platform_pretty_version
    went missing from signatures where it was a real finding.
    """

    def module_and(self, column):
        return {
            "sigA": [
                frozenset((("MOD0", True),)),
                frozenset(((column, "x"),)),
            ]
        }

    def test_module_pairs_with_an_allowed_column_in_either_order(self):
        for column in sorted(mining.MODULE_PAIR_ALLOWED):
            candidates, _ = mining.generate_candidates(
                self.module_and(column), module_columns={"MOD0"}
            )
            assert candidates["sigA"] == [
                frozenset((("MOD0", True), (column, "x")))
            ], f"{column} should pair with a module"

    def test_sorted_order_puts_the_module_first(self):
        """The precondition for the bug: MOD* sorts before every allowed column, so an
        order-dependent rule always sees the module on the left."""
        for column in mining.MODULE_PAIR_ALLOWED:
            assert "MOD0" < column

    def test_module_does_not_pair_with_a_disallowed_column(self):
        for column in ("cpu_arch", "reason", "theme", "useragent_locale"):
            candidates, _ = mining.generate_candidates(
                self.module_and(column), module_columns={"MOD0"}
            )
            assert candidates["sigA"] == [], f"{column} should not pair with a module"

    def test_two_modules_never_pair(self):
        previous = {
            "sigA": [
                frozenset((("MOD0", True),)),
                frozenset((("MOD1", True),)),
            ]
        }
        candidates, _ = mining.generate_candidates(
            previous, module_columns={"MOD0", "MOD1"}
        )
        assert candidates["sigA"] == []

    def test_non_module_pairs_are_unrestricted(self):
        previous = {
            "sigA": [
                frozenset((("cpu_arch", "x86"),)),
                frozenset((("reason", "SIGSEGV"),)),
            ]
        }
        candidates, _ = mining.generate_candidates(previous, module_columns={"MOD0"})
        assert len(candidates["sigA"]) == 1

    def test_parents_are_recorded_for_a_module_pair(self):
        """filter_level_2 needs the parents, and priors are derived from them."""
        previous = self.module_and("platform_pretty_version")
        candidates, parents = mining.generate_candidates(
            previous, module_columns={"MOD0"}
        )
        candidate = candidates["sigA"][0]
        left, right = parents["sigA"][candidate]
        assert left | right == candidate


class TestCountLevel2:
    def test_counts_reference_and_group(self):
        candidate = frozenset((("platform", "Windows NT"), ("cpu_arch", "x86")))
        counts = mining.count_level_2(table_fixture(), COLUMNS, {"sigA": {candidate}})
        assert counts[mining.REFERENCE][candidate] == 2
        assert counts["sigA"][candidate] == 2

    def test_reference_counts_candidates_from_every_group(self):
        # count_candidates() broadcast the union of all groups' candidates for the
        # reference count, so a candidate from sigB is still counted channel-wide.
        candidate = frozenset((("platform", "Linux"), ("cpu_arch", "amd64")))
        counts = mining.count_level_2(table_fixture(), COLUMNS, {"sigB": {candidate}})
        assert counts[mining.REFERENCE][candidate] == 1

    def test_group_only_counts_its_own_candidates(self):
        # A candidate belonging to sigB must not accrue a count under sigA even
        # though sigA's rows are scanned for the reference total.
        b_candidate = frozenset((("cpu_arch", "amd64"), ("plugin", False)))
        counts = mining.count_level_2(
            table_fixture(), COLUMNS, {"sigA": set(), "sigB": {b_candidate}}
        )
        assert counts["sigB"][b_candidate] == 2
        assert b_candidate not in counts.get("sigA", {})

    def test_empty_candidates_returns_empty(self):
        counts = mining.count_level_2(table_fixture(), COLUMNS, {"sigA": set()})
        assert not counts

    def test_counts_a_null_valued_candidate(self):
        # 96% of production results contain a null-valued item, so this is the dominant
        # path rather than an edge case. In Arrow a null equals nothing, including itself,
        # so a null item can't come from an equality mask; it comes from the validity
        # bitmap. See _bitsets.
        candidate = frozenset((("platform", None), ("cpu_arch", "amd64")))
        counts = mining.count_level_2(table_fixture(), COLUMNS, {"sigB": {candidate}})
        assert counts[mining.REFERENCE][candidate] == 1
        assert counts["sigB"][candidate] == 1

    def test_candidate_on_a_value_no_row_holds(self):
        # generate_candidates pairs items from the level 1 survivors, so this shouldn't
        # arise, but a missing posting must count as zero rather than raise or skip the
        # candidate's other half.
        candidate = frozenset((("platform", "BeOS"), ("cpu_arch", "x86")))
        counts = mining.count_level_2(table_fixture(), COLUMNS, {"sigA": {candidate}})
        assert candidate not in counts[mining.REFERENCE]

    def test_group_with_no_rows_in_the_table(self):
        # The signature list comes from a live SuperSearch call over its own window, so it
        # can name a signature that isn't in the table at all.
        candidate = frozenset((("platform", "Windows NT"), ("cpu_arch", "x86")))
        counts = mining.count_level_2(
            table_fixture(), COLUMNS, {"sigGone": {candidate}}
        )
        assert counts[mining.REFERENCE][candidate] == 2
        assert candidate not in counts.get("sigGone", {})

    def test_matches_naive_subset_counting(self):
        """The inverted loop must agree with testing every candidate per row.

        This is the correctness guarantee for the optimisation that makes the whole
        rewrite viable, so it's checked against the slow approach the Spark job used
        rather than against hand-written expectations.
        """
        rows = rows_fixture()
        items = set()
        for row in rows:
            for column in COLUMNS:
                items.add(frozenset(((column, row[column]),)))
        # Every 2-item combination across different columns, so the candidate set is
        # far wider than pruning would leave and covers the pairs that never occur.
        candidates = set()
        for left, right in itertools.combinations(
            sorted(items, key=mining._item_sort_key), 2
        ):
            ((left_column, _),) = left
            ((right_column, _),) = right
            if left_column != right_column:
                candidates.add(left | right)

        fast = mining.count_level_2(table_fixture(), COLUMNS, {"sigA": candidates})

        naive = {}
        for candidate in candidates:
            count = 0
            for row in rows:
                row_items = {(column, row[column]) for column in COLUMNS}
                if candidate <= row_items:
                    count += 1
            if count:
                naive[candidate] = count

        assert dict(fast[mining.REFERENCE]) == naive


class TestOrdering:
    def test_shorter_itemsets_first(self):
        one = frozenset((("platform", "Linux"),))
        two = frozenset((("platform", "Linux"), ("cpu_arch", "x86")))
        ordered = sorted([two, one], key=mining.itemset_sort_key)
        assert ordered == [one, two]

    def test_orders_mixed_value_types(self):
        # str, bool and None don't compare against each other, so the key has to use
        # repr rather than the value itself.
        itemsets = [
            frozenset((("a", None),)),
            frozenset((("a", True),)),
            frozenset((("a", "x"),)),
        ]
        assert len(sorted(itemsets, key=mining.itemset_sort_key)) == 3

    def test_generate_candidates_returns_sorted_lists(self):
        previous = {
            "sigA": [
                frozenset((("platform", "Windows NT"),)),
                frozenset((("cpu_arch", "x86"),)),
                frozenset((("plugin", False),)),
            ]
        }
        candidates = mining.generate_candidates(previous)[0]["sigA"]
        assert isinstance(candidates, list)
        assert candidates == sorted(candidates, key=mining.itemset_sort_key)

    def test_order_candidates_is_stable_across_input_order(self):
        """Same itemsets in a different input order must come out the same.

        This is the property the Spark job lacked: its candidate lists came from
        Python sets, so iteration order varied between processes and the
        order-dependent final filter kept different equally-scoring candidates.
        """
        items = [
            frozenset((("platform", "Linux"), ("cpu_arch", "x86"))),
            frozenset((("platform", "Linux"),)),
            frozenset((("cpu_arch", None),)),
            frozenset((("plugin", True), ("theme", "default"))),
        ]
        first = mining.order_candidates({"sigA": items})["sigA"]
        second = mining.order_candidates({"sigA": list(reversed(items))})["sigA"]
        assert first == second

    def test_order_candidates_survives_set_iteration(self):
        # Passing a set is the realistic case, since that's what the counting
        # produces before ordering.
        items = {
            frozenset((("a", "1"),)),
            frozenset((("b", "2"),)),
            frozenset((("a", "1"), ("b", "2"))),
        }
        assert mining.order_candidates({"g": items})["g"] == mining.order_candidates(
            {"g": list(items)}
        )["g"]

    def test_ordering_is_stable_across_hash_seeds(self):
        """The ordering must not depend on PYTHONHASHSEED.

        Set iteration order varies per process, which is the mechanism behind the
        Spark job's non-determinism. Verified out of band that the unordered
        equivalent produces four different digests across four seeds while this
        produces one, so a subprocess is run here rather than trusting a single
        in-process ordering.
        """
        script = (
            "import json, hashlib;"
            "from crash_correlations import mining;"
            "cols=['platform','cpu_arch','plugin','theme'];"
            "vals={'platform':['Linux','Windows NT',None],"
            "'cpu_arch':['x86','amd64'],'plugin':[True,False],"
            "'theme':['default',None]};"
            "items={frozenset(((c,v),)) for c in cols for v in vals[c]};"
            "out=mining.order_candidates("
            "mining.generate_candidates({'g':items})[0])['g'];"
            "blob=json.dumps([sorted((k,repr(v)) for k,v in fs) for fs in out]);"
            "print(hashlib.sha256(blob.encode()).hexdigest())"
        )
        digests = set()
        for seed in ("0", "1", "42", "12345"):
            env = dict(os.environ, PYTHONHASHSEED=seed, PYTHONPATH=REPO_ROOT)
            result = subprocess.run(
                [sys.executable, "-c", script],
                capture_output=True,
                env=env,
                check=True,
            )
            digests.add(result.stdout.decode().strip())
        assert len(digests) == 1, f"ordering varied across hash seeds: {digests}"


class TestPruneByCount:
    def test_drops_below_minimum(self):
        counts = {
            mining.REFERENCE: {
                frozenset((("a", "1"),)): 10,
                frozenset((("a", "2"),)): 4,
            }
        }
        pruned = mining.prune_by_count(counts, minimum=5)
        assert pruned[mining.REFERENCE] == {frozenset((("a", "1"),)): 10}
