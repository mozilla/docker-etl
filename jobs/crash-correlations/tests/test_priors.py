"""Tests for the priors graph.

The important one is test_reachability_matches_find_path: possible_priors uses a
precomputed transitive closure rather than walking the graph per call, which is an
optimisation over upstream, so the closure has to agree with the walk on every pair.
"""

from crash_correlations import priors


class TestBuildGraph:
    def test_static_edges_survive(self):
        graph = priors.build_graph()
        assert "adapter_device_id" in graph["adapter_vendor_id"]
        assert "cpu_microcode_version" in graph["CPU Info"]

    def test_splices_feature_lists(self):
        graph = priors.build_graph(
            app_notes=["AN0"],
            gfx_errors=["GFX0"],
            modules=["MOD0"],
            addons=["ADDON0"],
            addon_versions={"ADDON0": "ADDON0_VERSION"},
        )
        # platform_version explains modules.
        assert "MOD0" in graph["platform_version"]
        # adapter_driver_version explains app notes and gfx errors.
        assert "AN0" in graph["adapter_driver_version"]
        assert "GFX0" in graph["adapter_driver_version"]
        # startup_crash explains addons, their versions, modules and app notes.
        assert "ADDON0" in graph["startup_crash"]
        assert "ADDON0_VERSION" in graph["startup_crash"]
        assert "MOD0" in graph["startup_crash"]

    def test_addon_explains_its_own_version(self):
        """The version column has to be the one the feature table really generates.

        Deriving it as '<addon>-version' is upstream's naming and produced edges for
        columns that don't exist here, so an addon never explained its own version.
        """
        graph = priors.build_graph(
            addons=["ADDON0"], addon_versions={"ADDON0": "ADDON0_VERSION"}
        )
        assert graph["ADDON0"] == ["ADDON0_VERSION"]

    def test_addon_version_edges_match_the_real_column_names(self):
        """Build the mapping the way the job does, so a rename breaks this test."""
        from crash_correlations import queries

        features = queries.Features(
            pairs={"addon": [("ADDON0", "ublock@example.com")]}, labels={}, counts={}
        )
        graph = priors.build_graph(
            addons=features.addon_columns,
            addon_versions={
                presence: version
                for version, presence in features.addon_version_columns.items()
            },
        )
        assert graph["ADDON0"] == ["ADDON0_VERSION"]
        assert "ADDON0_VERSION" in graph["startup_crash"]

    def test_empty_feature_lists_leave_base_intact(self):
        assert priors.build_graph()["platform_version"] == []


class TestFindPath:
    def test_direct_edge(self):
        graph = priors.build_graph()
        assert priors.find_path(graph, "adapter_vendor_id", "adapter_device_id")

    def test_transitive(self):
        graph = priors.build_graph()
        # platform -> adapter_vendor_id -> adapter_device_id -> adapter_driver_version
        assert priors.find_path(graph, "platform", "adapter_driver_version")

    def test_no_path_backwards(self):
        graph = priors.build_graph()
        assert priors.find_path(graph, "adapter_device_id", "platform") is None

    def test_self_is_reachable(self):
        graph = priors.build_graph()
        assert priors.find_path(graph, "platform", "platform") == ["platform"]

    def test_terminates_on_a_cycle(self):
        # process_type and startup_crash point at each other.
        graph = priors.build_graph()
        assert "startup_crash" in graph["process_type"]
        assert "process_type" in graph["startup_crash"]
        assert priors.find_path(graph, "process_type", "nonexistent") is None

    def test_unknown_start_has_no_path(self):
        assert priors.find_path(priors.build_graph(), "nope", "platform") is None


class TestReachability:
    def test_matches_find_path(self):
        """The closure must agree with the walk on every pair of nodes.

        possible_priors uses the closure for speed, so a disagreement would silently
        change which correlations get a prior attached. Leaf columns are the subtle
        case: they only appear as edge targets, never as graph keys, but find_path
        still returns a path from a leaf to itself.
        """
        graph = priors.build_graph(
            app_notes=["AN0", "AN1"],
            gfx_errors=["GFX0"],
            modules=["MOD0", "MOD1"],
            addons=["AD0"],
        )
        reachable = priors.reachability(graph)
        nodes = set(graph)
        for targets in graph.values():
            nodes.update(targets)

        for start in sorted(nodes):
            for end in sorted(nodes):
                walked = priors.find_path(graph, start, end) is not None
                closed = end in reachable.get(start, ())
                assert walked == closed, f"{start} -> {end}"

    def test_leaf_nodes_get_entries(self):
        graph = priors.build_graph(modules=["MOD0"])
        reachable = priors.reachability(graph)
        assert "MOD0" in reachable
        assert reachable["MOD0"] == {"MOD0"}


class TestPossiblePriors:
    def test_finds_the_explaining_item(self):
        graph = priors.build_graph()
        reachable = priors.reachability(graph)
        candidate = frozenset(
            (("platform", "Linux"), ("adapter_device_id", "0x1"))
        )
        found = priors.possible_priors(candidate, reachable)
        assert found == [frozenset((("platform", "Linux"),))]

    def test_no_prior_when_neither_explains_the_other(self):
        graph = priors.build_graph()
        reachable = priors.reachability(graph)
        candidate = frozenset((("theme", "default"), ("safe_mode", "0")))
        assert priors.possible_priors(candidate, reachable) == []

    def test_both_when_mutually_reachable(self):
        # process_type and startup_crash explain each other, so either can be prior.
        graph = priors.build_graph()
        reachable = priors.reachability(graph)
        candidate = frozenset(
            (("process_type", "gpu"), ("startup_crash", "1"))
        )
        assert len(priors.possible_priors(candidate, reachable)) == 2

    def test_order_is_stable(self):
        graph = priors.build_graph()
        reachable = priors.reachability(graph)
        candidate = frozenset(
            (("process_type", "gpu"), ("startup_crash", "1"))
        )
        first = priors.possible_priors(candidate, reachable)
        second = priors.possible_priors(
            frozenset(reversed(list(candidate))), reachable
        )
        assert first == second

    def test_single_item_candidate_is_its_own_prior(self):
        # No "others" to explain, so the all() is vacuously true. Matches upstream,
        # though the caller only asks about multi-item candidates.
        graph = priors.build_graph()
        reachable = priors.reachability(graph)
        candidate = frozenset((("platform", "Linux"),))
        assert priors.possible_priors(candidate, reachable) == [candidate]
