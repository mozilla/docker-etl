"""Frequent itemset counting over the feature table.

This replaces the RDD passes in crash_deviations.py: the level 1 flatMap/reduceByKey
and count_candidates(). The feature table fits in memory, so this is plain Python
rather than a cluster, and it measured faster than the Spark version.

Vocabulary, kept from the Spark job so the two can be diffed:

    item      a one element frozenset of (column, value)
    itemset   a frozenset of one or more (column, value) pairs, aka a candidate
    reference the whole channel
    group     the crashes for one signature

Counts are kept as {group_name: {itemset: count}}, which is what saved_counts held.

The table stays in Arrow, as a FeatureTable wrapping the pa.Table that comes back from
BigQuery. Three shapes were tried; this is the third. On release, 88,737 rows by 2,616
columns:

    row-major dicts      5.8 GB    one dict per row, ~52 KB of dict overhead each
    column-major lists   4.8 GB    {column: [values]}, but to_pylist adds 2.4 GB
    Arrow                0.2 GB    the buffers BigQuery already sent

Both Python shapes paid for converting Arrow into Python objects, which turned out to be
the whole memory ceiling: the data itself is 0.17 GB. Reading the Arrow arrays instead
removes that conversion, and both counting passes get faster as a side effect, since the
work moves out of the interpreter.

What does not change is the type of a *value* inside an itemset. Those stay real Python
bool/None/str, because pruning.ignore_rule discriminates on them with `is False`,
`is None` and equality against '1' and 'Active'. Values are pulled out of Arrow once per
distinct value per column rather than once per row, so there are thousands of them
instead of 232 million.
"""

import dataclasses
import itertools
from collections import defaultdict

import pyarrow as pa
import pyarrow.compute as pc


# 5 for the chi-squared test, same as MIN_COUNT in crash_deviations.py.
MIN_COUNT = 5

# Group name used for channel-wide counts. The Spark job used this string too.
REFERENCE = "reference"

# Columns a module feature is allowed to pair with at level 2.
# crash_deviations.py:637. Anything else a module would pair with is treated as
# explained by the priors graph rather than worth its own candidate.
MODULE_PAIR_ALLOWED = frozenset(
    {"platform", "platform_pretty_version", "platform_version", "startup_crash"}
)


SIGNATURE_COLUMN = "signature"


@dataclasses.dataclass
class FeatureTable:
    """The feature table, as an Arrow table.

    `arrow` holds one row per crash report: a `signature` column plus one column per
    feature. Row i is (signatures[i], {c: column(c)[i] for c in columns}), but nothing
    here materialises that dict and neither counting pass needs it.

    Chunked arrays are combined on construction. Both counting passes want one contiguous
    buffer per column so a bitset can be read straight out of it, and a chunked column
    would need the bits shifted and OR'd per chunk. BigQuery's to_arrow() returns one
    chunk per Storage API stream, so this is the normal case rather than an edge one.

    from_columns() builds one from Python lists, which is what the tests use: literal
    fixtures stay as readable as they were before the table went to Arrow.
    """

    arrow: pa.Table

    def __post_init__(self):
        # combine_chunks on a table with zero rows can leave a column with no chunks at
        # all, whose .buffers() then has nothing to read. Going through pa.array of the
        # empty list gives a real one-chunk array in that case.
        self.arrow = self.arrow.combine_chunks()
        self._arrays = {}
        for name in self.arrow.column_names:
            chunks = self.arrow.column(name).chunks
            self._arrays[name] = (
                chunks[0] if chunks else pa.array([], self.arrow.column(name).type)
            )

    @classmethod
    def from_columns(cls, signatures, values):
        """Build from {column: [values]} plus a signature list.

        The types have to be pinned rather than inferred. An all-None column infers as
        null type, which pc.equal can't compare against a string, and a column holding
        both bools and strings has no Arrow type at all. Feature columns are either
        boolean or string in the SQL, so the inference is: bool if every non-None value
        is a bool, string otherwise, with anything non-string stringified. That last
        case is for test fixtures; real tables come from BigQuery already typed.
        """
        columns = {SIGNATURE_COLUMN: pa.array(list(signatures), pa.string())}
        for name, column in values.items():
            present = [v for v in column if v is not None]
            if present and all(isinstance(v, bool) for v in present):
                columns[name] = pa.array(column, pa.bool_())
            else:
                columns[name] = pa.array(
                    [v if v is None or isinstance(v, str) else str(v) for v in column],
                    pa.string(),
                )
        return cls(pa.table(columns))

    def __len__(self):
        return self.arrow.num_rows

    @property
    def signatures(self):
        """One signature per row, as Python strings.

        A real list of 88,737 strings, but they're interned by Arrow's string array so
        this is one pointer each, and every caller wants to index it per row.
        """
        return self._arrays[SIGNATURE_COLUMN].to_pylist()

    @property
    def columns(self):
        return [n for n in self.arrow.column_names if n != SIGNATURE_COLUMN]

    def array(self, name):
        """The contiguous Arrow array for a column."""
        return self._arrays[name]

    def column(self, name):
        """A column as a Python list. Not used by the counting paths.

        Kept because it's the readable thing to assert against in a test, and because
        converting one column is cheap; it's converting all 2,616 that cost 2.4 GB.
        """
        return self._arrays[name].to_pylist()

    def row(self, index):
        """One row as a dict. For tests and debugging, not the counting paths."""
        return {name: self._arrays[name][index].as_py() for name in self.columns}


def _bitmap_to_int(buffer, length, offset):
    """An Arrow validity or boolean values buffer as a Python int.

    Arrow packs booleans one bit per row, little endian, which is exactly the layout of a
    Python int's magnitude. So this is a reinterpretation of bytes already in memory
    rather than a conversion: measured at 0.006 ms for an 88,737 row column against 4.2 ms
    to walk to_pylist() and set the bits one at a time.

    Arrow omits the buffer entirely when it would carry no information, which for a values
    buffer means every row is false. Callers only reach the validity buffer when
    null_count says there is one.

    The offset shift is what makes a sliced array safe, since a slice shares its parent's
    buffers and starts `offset` bits in, and the length mask drops the padding bits Arrow
    rounds the buffer up to.
    """
    if buffer is None:
        return 0
    return (int.from_bytes(buffer, "little") >> offset) & ((1 << length) - 1)


def _boolean_bitsets(array, length):
    """{value: bitset} for a boolean column, from its buffers alone.

    Bit i of the result for value v is set when row i holds v. Three values are possible
    and all three are counted, because False and None are items in their own right:
    `plugin=False` is a rule the output can contain, and pruning.ignore_rule tests
    `value is None` explicitly.

    Arrow stores this as a validity bitmap and a values bitmap, so True is
    values & validity, False is ~values & validity, and None is everything else. The
    values bitmap is undefined where validity is 0, hence the mask on both.
    """
    all_rows = (1 << length) - 1
    values = _bitmap_to_int(array.buffers()[1], length, array.offset)
    if array.null_count:
        valid = _bitmap_to_int(array.buffers()[0], length, array.offset)
    else:
        valid = all_rows

    bitsets = {True: values & valid, False: ~values & valid & all_rows}
    nulls = all_rows & ~valid
    if nulls:
        bitsets[None] = nulls
    return bitsets


def _bitsets(array, length, wanted=None):
    """{value: bitset} for any feature column.

    Boolean columns go straight through the buffers. Anything else is dictionary encoded
    first, which gives the distinct values once, then one equality mask per value, and
    each mask is itself a boolean array whose buffer is the bitset. So the per-row work is
    all inside Arrow either way and the only Python objects created are the distinct
    values.

    `wanted` restricts the string case to a set of values, which matters a lot on the wide
    columns: `address` has 73,804 distinct values over the release window, and building a
    mask for each costs ~0.6s per column. Level 2 only needs the values that appear in
    some candidate, a few thousand across the whole table, so passing `wanted` turns 11s
    of mask building into 0.2s. Pass None to get every value, which is what a caller
    counting the whole column wants.

    Empty bitsets are dropped. A value present in the dictionary but in no surviving row
    can happen after a slice, and an item no row holds cannot be counted.
    """
    if length == 0:
        return {}
    if pa.types.is_boolean(array.type):
        # Only three values are possible, so `wanted` would save nothing here.
        return {
            value: bits
            for value, bits in _boolean_bitsets(array, length).items()
            if bits
        }

    encoded = array.dictionary_encode()
    indices = encoded.indices
    bitsets = {}
    for index, value in enumerate(encoded.dictionary.to_pylist()):
        if wanted is not None and value not in wanted:
            continue
        # fill_null because equality against null is null in Arrow, not false, and a null
        # in the mask would read as a set bit in the values buffer where validity is 0.
        mask = pc.fill_null(pc.equal(indices, index), False)
        bits = _bitmap_to_int(mask.buffers()[1], length, mask.offset)
        if bits:
            bitsets[value] = bits

    if array.null_count and (wanted is None or None in wanted):
        nulls = _bitmap_to_int(array.buffers()[0], length, array.offset)
        nulls = ~nulls & ((1 << length) - 1)
        if nulls:
            bitsets[None] = nulls

    return bitsets


def count_level_1(table, columns, signatures):
    """Count every (column, value) across the channel and within each signature.

    One pass over the rows, incrementing the reference count always and the group
    count when the row's signature is one we care about. The Spark version emitted
    both from a single flatMap and split them afterwards by key type; doing it
    directly avoids needing the Row-versus-str discriminator at all.

    Returns (counts, totals) where counts is {group: {itemset: count}} and totals is
    {group: row count}. Both include REFERENCE.

    One grouped aggregation per column, `GROUP BY signature, column`, which gives every
    group's count for every value of that column in a single Arrow call. Both counts come
    out of the same pass: the reference count for a value is the sum over the groups, so
    it needs no second aggregation.

    The signature column is kept in the grouping rather than filtered to the wanted
    signatures first, because filtering copies the column and the aggregation has to walk
    every row for the reference count anyway. Rows whose signature isn't wanted are
    dropped when the results are read back.
    """
    wanted = set(signatures)
    counts = defaultdict(lambda: defaultdict(int))
    totals = defaultdict(int)

    signature_array = table.array(SIGNATURE_COLUMN)
    reference_counts = counts[REFERENCE]
    totals[REFERENCE] = len(table)

    for entry in pc.value_counts(signature_array).to_pylist():
        if entry["values"] in wanted:
            totals[entry["values"]] = entry["counts"]

    for column in columns:
        groups, values, column_counts = _grouped_value_counts(
            signature_array, table.array(column)
        )
        for signature, value, count in zip(groups, values, column_counts):
            item = frozenset(((column, value),))
            reference_counts[item] += count
            if signature in wanted:
                counts[signature][item] += count

    return counts, totals


def _grouped_value_counts(signature_array, value_array):
    """(signatures, values, counts) for GROUP BY signature, value.

    Arrow's group_by treats null as a group of its own, which is what's needed: a null
    feature value is an item the output can contain, not missing data. `count_all` rather
    than `count` for the same reason, since `count` skips nulls by default.
    """
    grouped = (
        pa.table({"signature": signature_array, "value": value_array})
        .group_by(["signature", "value"])
        .aggregate([([], "count_all")])
    )
    return (
        grouped.column("signature").to_pylist(),
        grouped.column("value").to_pylist(),
        grouped.column("count_all").to_pylist(),
    )


def count_level_2(table, columns, candidates_by_group):
    """Count 2-item candidates across the channel and within each signature.

    Five approaches were tried; the choice matters because this is the dominant stage.
    Measured on beta, 5,831 rows and 2,130 columns, and on release, 88,737 x 2,616:

      per-row pairs      enumerate every pair a row has, look each up.
                         O(rows x columns^2). 11 minutes for beta, ~3 hours for
                         release. Looks fine at 30 columns and collapses at 2,000.
      per-candidate      what Spark does: test every candidate against every row.
                         O(rows x candidates), ~1.2e9 subset tests for beta.
      indexed by item    index each candidate under its rarer item, then per row look
                         only at candidates for items that row has. Needs a set of the
                         row's items, which means a row-major table.
      per-candidate mask one Arrow mask per candidate, then filter and group by
                         signature. 0.24 ms per candidate, so ~18s on release. Simple,
                         but it walks all 88,737 rows once per candidate.
      postings bitsets   below. For each item, a Python int with bit i set when row i
                         holds it. A candidate's count is then the popcount of a
                         bitwise AND, with no per-row work at all.

    Bitsets win because the per-row work happens once per *item* rather than once per
    candidate, and there are far fewer items than candidates: on release, ~14,000 items
    against 72,688 candidates. After that a candidate costs two ANDs and two popcounts
    over machine words, which beats re-scanning the column.

    Building them out of Arrow is what makes them cheap. A boolean column's values buffer
    is already the bitset for True, bit for bit, so extracting it is
    `int.from_bytes(buffer)`: 0.006 ms for an 88,737 row column against 4.2 ms to walk a
    Python list and set the bits one at a time. See _bitsets.

    Memory matters here too. As sets of row indices the same postings cost 4.2 MB each for
    a common item and pushed peak RSS to 5.4 GB, worse than the row-major table they
    replaced. As ints the same thing is 12 KB, ~350x smaller, and the intersection is
    faster.

    Counts are identical to the per-candidate approach; that equivalence is what
    test_matches_naive_subset_counting checks.

    candidates_by_group is {group: iterable of 2-item frozensets}. The union is counted
    for the reference, matching count_candidates() broadcasting set.union of all
    candidate sets.
    """
    as_sets = {group: set(itemsets) for group, itemsets in candidates_by_group.items()}
    all_candidates = set()
    for group_candidates in as_sets.values():
        all_candidates |= group_candidates

    counts = defaultdict(lambda: defaultdict(int))
    if not all_candidates:
        return counts

    # Only items that appear in some candidate need a bitset. That's a small fraction of
    # all items, so restricting to them keeps this well under the cost of indexing
    # everything.
    wanted_items = set()
    for candidate in all_candidates:
        wanted_items.update(candidate)

    # Grouped by column so each column is visited once and only its wanted values get a
    # mask built. Without that restriction the high cardinality columns dominate: masking
    # every distinct value of all 102 scalar columns is 11s against 0.2s for the ~14,000
    # values candidates actually reference.
    wanted_by_column = defaultdict(set)
    for column, value in wanted_items:
        wanted_by_column[column].add(value)

    rows = len(table)
    postings = defaultdict(int)
    for column in columns:
        values = wanted_by_column.get(column)
        if not values:
            continue
        for value, bits in _bitsets(table.array(column), rows, values).items():
            postings[(column, value)] = bits

    # Which groups each candidate belongs to, so its matching rows are computed once and
    # then split by group. Looping groups on the outside instead, re-deriving the
    # intersection per group, costs candidates x groups of them: 72,688 x 196 on release.
    groups_by_candidate = defaultdict(list)
    for group, group_candidates in as_sets.items():
        for candidate in group_candidates:
            groups_by_candidate[candidate].append(group)

    # One bitset per group, so a group count is another AND rather than a scan. Same
    # trick as the feature columns: an equality mask over the signature column is a
    # boolean array whose buffer is the bitset. Restricted to the groups being counted,
    # since a channel has thousands of distinct signatures and only ~200 are groups.
    group_bits = _bitsets(table.array(SIGNATURE_COLUMN), rows, set(as_sets))

    reference_counts = counts[REFERENCE]

    for candidate in all_candidates:
        left, right = tuple(candidate)
        matching = postings[left] & postings[right]
        if not matching:
            continue
        reference_counts[candidate] = matching.bit_count()

        for group in groups_by_candidate.get(candidate, ()):
            # .get because a group can have candidates and no rows: the signature list
            # comes from a live SuperSearch call over its own window, so it can name a
            # signature the table doesn't contain.
            in_group = matching & group_bits.get(group, 0)
            if in_group:
                counts[group][candidate] = in_group.bit_count()

    return counts


def generate_candidates(previous, module_columns=()):
    """Build 2-item candidates from the level 1 itemsets that survived pruning.

    Mirrors generate_candidates() in crash_deviations.py for level 2: pair up
    surviving items within a group, skipping pairs that constrain the same column
    twice, which can never both hold for one row.

    module_columns is the set of module feature columns, needed for the pairing
    restriction at crash_deviations.py:636. A pair involving a module is only kept when
    one side is one of MODULE_PAIR_ALLOWED. Without that restriction, ~2,000 module
    features pair with everything: measured on beta, 1,479,861 candidates instead of the
    203,843 the Spark job reports, a 7x inflation that lands squarely on the most
    expensive stage.

    previous is {group: iterable of 1-item frozensets}. Returns
    (candidates, parents):

        candidates  {group: sorted list of 2-item frozensets}, sorted rather than a
                    set so the downstream order-dependent filtering is reproducible.
                    See itemset_sort_key.
        parents     {group: {2-item frozenset: (left, right)}}, the pair of 1-item
                    itemsets each candidate was built from.

    The parents mapping isn't bookkeeping, should_prune() needs it: most of that
    function compares a candidate's support against each parent's support, and
    without the parents it can only apply the three cheap MIN_COUNT and support
    checks. Upstream keeps the same dict, populated alongside the candidates.

    A pair can be reachable two ways, but only from the same two items, since a
    2-item set has exactly one decomposition into 1-item sets. So there's no
    ambiguity about which parents to record.

    Candidates and their parent tuples are interned across groups, which is worth 0.45 GB
    on release. Most candidates occur in many groups (2,212,768 of them for 93,415 distinct
    values, so ~24 each) and `left | right` builds a new frozenset every time, so without a
    pool the same value is allocated once per group at 216 bytes each. Interning is safe
    because itemsets are immutable and compared by value: nothing anywhere tests them with
    `is`. Measured with tracemalloc, 0.732 GB to 0.280 GB.
    """
    modules = set(module_columns)
    candidates = {}
    parents = {}
    # Shared across groups, so a candidate that survived in 24 signatures is one object
    # rather than 24 equal ones. Keyed by value, which is what dict lookup does for a
    # frozenset, so a freshly built duplicate is discarded on insert.
    pool = {}
    parent_pool = {}
    for group, items in previous.items():
        pairs = {}
        ordered = sorted(items, key=_item_sort_key)
        for left, right in itertools.combinations(ordered, 2):
            (left_column, _), = left
            (right_column, _), = right
            if left_column == right_column:
                continue
            if left_column in modules or right_column in modules:
                # A module only pairs with one of MODULE_PAIR_ALLOWED, and never with
                # another module. Without that restriction the ~2,000 module features pair
                # with everything: measured on beta, 1,479,861 candidates against the
                # 203,843 the Spark job reports, a 7x inflation on the most expensive
                # stage.
                #
                # crash_deviations.py:636 writes the same rule asymmetrically:
                #
                #   if i in module_ids or j in module_ids:
                #       if i not in ALLOWED or j in ALLOWED:
                #           continue
                #
                # which keeps (platform, MOD0) but drops (MOD0, platform), so whether a
                # module/platform pair exists depends on the order the two items happen to
                # be visited in. Upstream iterates a list built from a set, so it gets one
                # order or the other at random and produces the pair only sometimes.
                #
                # That is not reproduced. Candidates are fed in sorted order here, to keep
                # the output deterministic, and every module column sorts before every
                # allowed column, so the asymmetric form dropped module/platform pairs
                # *every* time. The visible effect was that no module correlation ever got
                # a platform prior, and platform_pretty_version went missing from
                # signatures where it is a real finding: on 2026-08-19 release it was
                # 7 of 8 crashes on one signature against 430 of 1,021 channel wide, a
                # support difference of +0.454, and absent from the output. Testing the
                # pair symmetrically restores both.
                if not _module_pair_allowed(left_column, right_column, modules):
                    continue
            candidate = left | right
            candidate = pool.setdefault(candidate, candidate)
            # The tuple too: it's two pointers plus an object header, and there are as
            # many of them as there are candidate slots. Keyed by the interned candidate,
            # since a 2-item set has exactly one decomposition into 1-item sets, so the
            # parents of a given candidate are always the same pair.
            pairs[candidate] = parent_pool.setdefault(candidate, (left, right))
        candidates[group] = sorted(pairs, key=itemset_sort_key)
        parents[group] = pairs
    return candidates, parents


def _module_pair_allowed(left_column, right_column, modules):
    """Whether a pair involving at least one module column may be generated.

    Symmetric, unlike the upstream expression it replaces: a module pairs with a column in
    MODULE_PAIR_ALLOWED whichever side each is on, and two modules never pair. See the note
    at the call site for why the asymmetry had to go rather than being transcribed.
    """
    left_is_module = left_column in modules
    right_is_module = right_column in modules
    if left_is_module and right_is_module:
        return False
    other = right_column if left_is_module else left_column
    return other in MODULE_PAIR_ALLOWED


def _item_sort_key(item):
    """Sort key for a 1-item frozenset.

    Values are mixed types (str, bool, None), which don't compare in Python 3, so
    sort on the repr of the value rather than the value.
    """
    (column, value), = item
    return (column, repr(value))


def itemset_sort_key(itemset):
    """Total order over itemsets of any size.

    Sorting on this is what makes the output reproducible. The Spark job built its
    candidate lists out of Python sets, so iteration order varied between processes,
    and the final filter is order dependent: `to_skip` accumulates as candidates are
    visited and `get_possible_priors` walks whatever is already in the results. Two
    runs over identical data therefore kept different members of a set of
    equally-scoring candidates. Measured on the 2026-08-14 run, where the esr bug
    made esr and release read the same crashes, the two agreed on every count but
    only produced the same surviving rule set for a quarter of signatures, median
    Jaccard 0.86.

    Shorter itemsets first, which matches how correlation.js sorts for display and
    means a 1-item rule is considered before the 2-item rules built from it. Then by
    (column, repr(value)) pairs. repr() rather than the value because a column can
    hold a str, a bool or None across rows and those don't compare in Python 3.
    """
    return (len(itemset), sorted((column, repr(value)) for column, value in itemset))


def order_candidates(candidates_by_group):
    """Put each group's candidates in a stable order.

    Takes {group: iterable of itemsets} and returns {group: sorted list}. Apply this
    before the pruning and significance filtering so a rerun on the same data gives
    the same output.
    """
    return {
        group: sorted(itemsets, key=itemset_sort_key)
        for group, itemsets in candidates_by_group.items()
    }


def prune_by_count(counts, minimum=MIN_COUNT):
    """Drop itemsets below the minimum count.

    The Spark passes did this with .filter(lambda k_v: k_v[1] >= MIN_COUNT) before
    collecting. Here it's applied after counting, which is equivalent and keeps the
    counting loops simple.
    """
    return {
        group: {
            itemset: count
            for itemset, count in group_counts.items()
            if count >= minimum
        }
        for group, group_counts in counts.items()
    }
