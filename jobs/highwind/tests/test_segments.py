"""Resolving a run's segment names against metric-hub's segment_plan(), and its dark fallback."""

from highwind import segments
from highwind.discovery import Experiment, SegmentDisplay
from highwind import units


class FakePlan:
    def __init__(self, sources, segments, missing):
        self.sources = sources
        self.segments = segments
        self.missing = missing


class FakeSource:
    def __init__(
        self,
        data_source,
        from_expression="mozdata.telemetry.clients_last_seen",
        window_start=0,
        window_end=0,
        client_id_column="client_id",
        group_id_column="profile_group_id",
        submission_date_column="submission_date",
    ):
        self.data_source = data_source
        self.from_expression = from_expression
        self.window_start = window_start
        self.window_end = window_end
        self.client_id_column = client_id_column
        self.group_id_column = group_id_column
        self.submission_date_column = submission_date_column


class FakeSegmentMetric:
    def __init__(self, name, data_source, select_expression):
        self.name = name
        self.data_source = data_source
        self.select_expression = select_expression


def test_a_parser_without_segment_plan_resolves_nothing_and_reports_every_name_unresolved(
    monkeypatch,
):
    monkeypatch.setattr(segments, "_segment_plan", None)

    sources, resolved, unresolved = segments.build_segment_plan(
        configs=object(), segment_names=["regular_user", "new_profile"]
    )

    assert sources == {}
    assert resolved == {}
    assert unresolved == {
        "regular_user": segments.NOT_AVAILABLE_REASON,
        "new_profile": segments.NOT_AVAILABLE_REASON,
    }


def test_todays_pin_resolves_nothing_even_with_no_names_requested(monkeypatch):
    monkeypatch.setattr(segments, "_segment_plan", None)

    sources, resolved, unresolved = segments.build_segment_plan(object(), [])

    assert (sources, resolved, unresolved) == ({}, {}, {})


def test_a_resolving_parser_builds_sources_and_segments_from_the_plan(monkeypatch):
    def fake_segment_plan(configs, app_name, names):
        assert app_name == "firefox_desktop"
        assert names == ["new_profile", "regular_user"]
        return FakePlan(
            sources={
                "clients_last_seen": FakeSource(data_source="clients_last_seen"),
            },
            segments={
                "regular_user": FakeSegmentMetric(
                    "regular_user", "clients_last_seen",
                    "COALESCE(LOGICAL_OR(is_regular_user_v3), FALSE)",
                ),
                "new_profile": FakeSegmentMetric(
                    "new_profile", "clients_last_seen",
                    "COALESCE(LOGICAL_OR(is_new_profile), FALSE)",
                ),
            },
            missing=[],
        )

    monkeypatch.setattr(segments, "_segment_plan", fake_segment_plan)

    sources, resolved, unresolved = segments.build_segment_plan(
        configs=object(), segment_names=["regular_user", "new_profile", "regular_user"]
    )

    assert unresolved == {}
    assert set(sources) == {"clients_last_seen"}
    assert sources["clients_last_seen"].from_expression == "mozdata.telemetry.clients_last_seen"
    assert resolved["regular_user"].source == "clients_last_seen"
    assert resolved["regular_user"].select_expression == (
        "COALESCE(LOGICAL_OR(is_regular_user_v3), FALSE)"
    )


def test_an_inline_only_name_is_reported_missing_even_when_the_parser_resolves_others(
    monkeypatch,
):
    def fake_segment_plan(configs, app_name, names):
        return FakePlan(sources={}, segments={}, missing=list(names))

    monkeypatch.setattr(segments, "_segment_plan", fake_segment_plan)

    _, resolved, unresolved = segments.build_segment_plan(object(), ["an_inline_segment"])

    assert resolved == {}
    assert unresolved == {"an_inline_segment": segments.NOT_RESOLVED_REASON}


def experiment_with(segment_names, display=None):
    return Experiment(
        slug="a-slug",
        start_date=None,
        end_date=None,
        reference_branch="control",
        treatment_branches=("treatment",),
        unit=units.resolve("firefox_desktop", "normandy_id"),
        segments={
            name: (display or {}).get(name, SegmentDisplay(friendly_name=None, description=None))
            for name in segment_names
        },
    )


def test_for_experiment_keeps_only_the_segments_it_selected():
    resolved = {
        "regular_user": segments.Segment(
            name="regular_user", source="clients_last_seen",
            select_expression="COALESCE(LOGICAL_OR(is_regular_user_v3), FALSE)",
        ),
        "new_profile": segments.Segment(
            name="new_profile", source="clients_last_seen",
            select_expression="COALESCE(LOGICAL_OR(is_new_profile), FALSE)",
        ),
    }
    experiment = experiment_with(["regular_user"])

    chosen, problems = segments.segments_for_experiment(experiment, resolved, {})

    assert [segment.name for segment in chosen] == ["regular_user"]
    assert problems == []


def test_for_experiment_reports_a_name_it_selected_that_did_not_resolve():
    experiment = experiment_with(["an_inline_segment"])

    chosen, problems = segments.segments_for_experiment(
        experiment, {}, {"an_inline_segment": segments.NOT_RESOLVED_REASON}
    )

    assert chosen == []
    assert len(problems) == 1
    assert problems[0].name == "an_inline_segment"
    assert problems[0].reason == segments.NOT_RESOLVED_REASON


def test_for_experiment_never_resolves_a_segment_named_all_enrolled():
    # Even if metric-hub somehow resolved a segment under the reserved name, selecting it must
    # still be reported unresolved rather than silently standing in for the built-in all_enrolled.
    resolved = {
        "all_enrolled": segments.Segment(
            name="all_enrolled", source="clients_last_seen", select_expression="TRUE",
        ),
    }
    experiment = experiment_with(["all_enrolled"])

    chosen, problems = segments.segments_for_experiment(experiment, resolved, {})

    assert chosen == []
    assert len(problems) == 1
    assert problems[0].name == "all_enrolled"
    assert problems[0].reason == segments.ALL_ENROLLED_RESERVED_REASON


def test_for_experiment_defaults_an_unlisted_reason_to_not_resolved():
    # Reached if a name resolves through neither the plan's own segments nor its missing list,
    # which should not happen but must not raise if it ever does.
    experiment = experiment_with(["a_mystery_segment"])

    chosen, problems = segments.segments_for_experiment(experiment, {}, {})

    assert chosen == []
    assert problems[0].reason == segments.NOT_RESOLVED_REASON


def test_an_experiment_that_selected_nothing_resolves_nothing():
    experiment = experiment_with([])

    chosen, problems = segments.segments_for_experiment(experiment, {}, {})

    assert chosen == []
    assert problems == []


def test_group_by_source_groups_segments_sharing_one_source():
    regular_user = segments.Segment(
        name="regular_user", source="clients_last_seen", select_expression="r"
    )
    new_profile = segments.Segment(
        name="new_profile", source="clients_last_seen", select_expression="n"
    )
    suggest_user = segments.Segment(name="suggest_user", source="newtab", select_expression="s")

    by_source = segments.group_by_source([regular_user, new_profile, suggest_user])

    assert {segment.name for segment in by_source["clients_last_seen"]} == {
        "regular_user",
        "new_profile",
    }
    assert {segment.name for segment in by_source["newtab"]} == {"suggest_user"}
