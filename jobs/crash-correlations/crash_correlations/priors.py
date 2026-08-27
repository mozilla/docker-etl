"""The priors graph, used to suppress correlations another one already explains.

Ported from crash_deviations.py:344-408. No Spark involved, this is a hand-built
directed graph over column names plus a reachability walk.

The idea: if a signature correlates with both `adapter_device_id` and
`adapter_driver_version`, the driver version is largely explained by the device, so
reporting both is noise. The graph encodes which columns can explain which, and the
final filtering stage uses it to attach one as a "prior" on the other rather than
emitting both as independent findings.

Upstream builds the graph inside find_deviations() because it splices in the
data-dependent feature lists (app notes, gfx errors, modules, addons). Same here,
via build_graph().
"""


# Static part of the graph. Values are the columns a key can act as a prior for.
# Data-dependent entries are spliced in by build_graph(). Keys not present here are
# leaves, which find_path treats as dead ends.
#
# Copied from crash_deviations.py:350. The android_* entries are retained even though
# this job only runs Firefox desktop, since they cost nothing and dropping them would
# be a silent behaviour change if the product filter ever widened.
BASE_GRAPH = {
    "platform": [
        "platform_pretty_version",
        "adapter_vendor_id",
        "bios_manufacturer",
        "CPU Info",
        "cpu_arch",
        "os_arch",
    ],
    "platform_pretty_version": ["platform_version"],
    "platform_version": [],
    "adapter_vendor_id": ["adapter_device_id"],
    "adapter_device_id": [
        "adapter_driver_version",
        "adapter_driver_version_clean",
        "adapter_subsys_id",
    ],
    "adapter_driver_version": [],
    "adapter_driver_version_clean": [],
    "cpu_arch": ["CPU Info"],
    "CPU Info": ["cpu_microcode_version"],
    "startup_crash": [
        "os_arch",
        "shutdown_progress",
        "safe_mode",
        "ipc_channel_error",
        "ipc_fatal_error_protocol",
        "gmp_plugin",
        "jit_category",
        "accessibility",
        "useragent_locale",
        "adapter_vendor_id",
        "adapter_device_id",
        "adapter_subsys_id",
        "theme",
        "e10s_enabled",
        "e10s_cohort",
        "bios_manufacturer",
        "process_type",
    ],
    "process_type": ["e10s_enabled", "startup_crash"],
    "android_hardware": [],
    "android_board": [],
    "android_manufacturer": [],
}

# Which base keys get the data-dependent feature lists appended, and which lists.
# From crash_deviations.py:350-365.
_SPLICES = {
    "platform_pretty_version": ("app_notes",),
    "platform_version": ("modules",),
    "adapter_driver_version": ("app_notes", "gfx_errors"),
    "adapter_driver_version_clean": ("app_notes", "gfx_errors"),
    "startup_crash": ("addons", "addon_versions", "modules", "app_notes"),
    "android_hardware": ("modules",),
    "android_board": ("modules",),
    "android_manufacturer": ("modules",),
}


def build_graph(
    app_notes=(), gfx_errors=(), modules=(), addons=(), addon_versions=None
):
    """Build the priors graph for one channel's feature set.

    The arguments are the feature column names that survived the support threshold,
    i.e. the same names used as columns in the feature table.

    `addon_versions` maps a presence column to its version column, i.e.
    {'ADDON3': 'ADDON3_VERSION'}; queries.Features.addon_version_columns is the inverse
    and gets flipped at the call site. Upstream derived it as `<addon>-version`, which is
    how it named the column (crash_deviations.py:367). Deriving it here instead produced
    edges for columns that don't exist, so an addon never explained its own version.
    Defaults to that derivation only so the base graph can be built without a feature
    set, which is all the callers that pass nothing need.
    """
    if addon_versions is None:
        addon_versions = {addon: f"{addon}-version" for addon in addons}

    lists = {
        "app_notes": list(app_notes),
        "gfx_errors": list(gfx_errors),
        "modules": list(modules),
        "addons": list(addons),
        "addon_versions": [addon_versions[a] for a in addons if a in addon_versions],
    }

    graph = {}
    for key, targets in BASE_GRAPH.items():
        extra = []
        for list_name in _SPLICES.get(key, ()):
            extra.extend(lists[list_name])
        graph[key] = list(targets) + extra

    # An addon explains its own version.
    for addon in addons:
        if addon in addon_versions:
            graph[addon] = [addon_versions[addon]]

    return graph


def find_path(graph, start, end):
    """Whether `end` is reachable from `start`, returning the path or None.

    crash_deviations.py:370. Depth first, skipping nodes already on the path so a
    cycle (process_type and startup_crash point at each other) terminates.

    Upstream rewrites dots to __DOT__ on the way in because Spark column names
    couldn't hold them. That escaping isn't used here, so names are compared as-is;
    see the generated column naming note in sql/feature_table.sql.
    """
    return _find_path(graph, start, end, ())


def _find_path(graph, start, end, path):
    path = path + (start,)

    if start == end:
        return list(path)

    if start not in graph:
        return None

    for node in graph[start]:
        if node in path:
            continue
        found = _find_path(graph, node, end, path)
        if found:
            return found

    return None


def possible_priors(candidate, reachable):
    """The items in a candidate that could explain all the others.

    crash_deviations.py:392. An item qualifies when every other item in the candidate
    is reachable from it, so it sits upstream of them in the explanation graph.

    `reachable` is the output of reachability(). Returns a list of 1-item frozensets,
    ordered by (column, repr(value)) so the caller's behaviour doesn't depend on set
    iteration order.
    """
    items = sorted(candidate, key=lambda pair: (pair[0], repr(pair[1])))
    elems = [frozenset((item,)) for item in items]

    found = []
    for prior in elems:
        ((prior_column, _),) = prior
        reachable_from_prior = reachable.get(prior_column, ())
        if all(
            other_column in reachable_from_prior
            for other in elems
            if other != prior
            for (other_column, _) in other
        ):
            found.append(prior)
    return found


def reachability(graph):
    """Reachable-column sets per column: {column: set of columns it can explain}.

    possible_priors runs for every multi-item candidate, and there are hundreds of
    thousands of those per channel, so walking the graph per call the way upstream
    does repeats the same traversal endlessly. The graph is fixed for a channel, so
    compute the transitive closure once and pass it in.

    A column is treated as reaching itself, matching find_path returning [start] when
    start == end, though possible_priors never relies on that since it skips the
    prior itself.
    """
    # Leaf columns appear only as targets, never as keys, but find_path still returns
    # a path from a leaf to itself, so they need entries too.
    nodes = set(graph)
    for targets in graph.values():
        nodes.update(targets)

    closure = {}
    for start in nodes:
        seen = set()
        stack = list(graph.get(start, ()))
        while stack:
            node = stack.pop()
            if node in seen:
                continue
            seen.add(node)
            stack.extend(graph.get(node, ()))
        seen.add(start)
        closure[start] = seen
    return closure
