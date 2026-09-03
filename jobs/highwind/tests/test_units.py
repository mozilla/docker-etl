"""Resolving a recipe's analysis unit, and refusing to invent one."""

from highwind import metrics, units


def test_a_group_randomized_desktop_recipe_is_analysed_at_the_profile_group():
    unit = units.resolve("firefox_desktop", "group_id")

    assert unit.kind == units.PROFILE_GROUP_ID
    assert unit.enrollment_column == "profile_group_id"
    assert unit.source_column == "profile_group_id"


def test_the_desktop_client_units_all_resolve_to_the_same_unit():
    normandy = units.resolve("firefox_desktop", "normandy_id")

    assert normandy.kind == units.CLIENT_ID
    assert normandy == units.resolve("firefox_desktop", "client_id")


def test_the_enrollment_and_source_columns_differ_for_a_client_randomized_recipe():
    # The enrollment scan and the metric tables name a client differently, and joining one against
    # the other matches nothing while still producing a cohort, so every metric would sum to zero.
    unit = units.resolve("firefox_desktop", "normandy_id")

    assert unit.enrollment_column == "legacy_telemetry_client_id"
    assert unit.source_column == "client_id"


def test_only_the_client_unit_matches_the_clustered_sample_column():
    # sample_id on the source tables is hashed from the client id, so it selects whole units only
    # when the unit IS the client.
    assert units.resolve("firefox_desktop", "normandy_id").clustered_sample_id is True
    assert units.resolve("firefox_desktop", "group_id").clustered_sample_id is False


def test_an_unknown_randomization_unit_resolves_to_nothing_rather_than_a_default():
    assert units.resolve("firefox_desktop", "nimbus_id") is None
    assert units.resolve("firefox_desktop", "user_id") is None
    assert units.resolve("firefox_desktop", None) is None


def test_a_unit_known_for_another_app_is_not_borrowed_for_this_one():
    # The pair is the key: the same name need not mean the same column on another app, so an app
    # with no entry resolves to nothing even for a unit desktop knows.
    assert units.resolve("fenix", "group_id") is None
    assert units.resolve("firefox_ios", "normandy_id") is None


def test_no_unit_column_is_also_a_metric_column():
    # The source CTE selects the unit column aliased to unit_id and then the metric columns, so a
    # column named by both would be selected twice under two names.
    for unit in set(units.ANALYSIS_UNITS.values()):
        assert unit.source_column
        assert unit.enrollment_column
        for spec in metrics.SOURCES.values():
            assert unit.source_column not in spec["columns"]
