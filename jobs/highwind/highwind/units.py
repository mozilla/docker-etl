"""The unit an experiment is analysed at, read from the recipe rather than assumed.

A recipe assigns whole units to arms, so the analysis has to aggregate to that same unit.
Aggregating finer than assignment leaves the rows inside one assigned unit correlated with each
other, which understates every standard error computed from them and reports intervals narrower than
the data supports. So the unit is a property of the recipe, and the recipe declares it.

What the declared name MEANS is a property of the application: the same name need not resolve to the
same column across apps, and the tables it has to be read from differ per app too. So resolution is
keyed on the (app, randomization unit) pair, and only Firefox Desktop is populated, matching the
rest of the job's scope. Adding an app is a row here rather than a change to the SQL generation.

A pair with no row is refused. A default would pick a grain, produce intervals from it, and give no
sign the grain was wrong, which is the failure this module exists to prevent.
"""

from dataclasses import dataclass

CLIENT_ID = "client_id"
PROFILE_GROUP_ID = "profile_group_id"


@dataclass(frozen=True)
class AnalysisUnit:
    """One analysis unit, and the column each side of the join records it in.

    `enrollment_column` and `source_column` are separate because the enrollment scan and the metric
    tables name the same unit differently: on desktop a client is `legacy_telemetry_client_id` in
    `events_stream` and `client_id` in the metric tables. Joining one name against the other matches
    nothing while still producing a full cohort, so every metric would silently sum to zero rather
    than fail. One `source_column` serves every source table because an app's tables agree on the
    name; a future app whose tables do not can carry the difference on the source instead.

    `clustered_sample_id` is whether the source tables' clustered `sample_id` column is hashed from
    THIS unit. It decides how a sampled run may be restricted, so it is per unit rather than assumed
    of every unit or of every app.
    """

    kind: str
    enrollment_column: str
    source_column: str
    clustered_sample_id: bool


# Keyed on the mirror's `app_name` and the recipe's `bucketConfig.randomizationUnit`, which is the
# pair the mirror reports for every recipe. Desktop randomizes on `normandy_id` by default and on
# `group_id` where a recipe opts into it; `client_id` is accepted as the same physical unit as
# `normandy_id`, since both identify one client and neither is recorded under its own name.
ANALYSIS_UNITS = {
    ("firefox_desktop", "group_id"): AnalysisUnit(
        kind=PROFILE_GROUP_ID,
        enrollment_column="profile_group_id",
        source_column="profile_group_id",
        clustered_sample_id=False,
    ),
    ("firefox_desktop", "normandy_id"): AnalysisUnit(
        kind=CLIENT_ID,
        enrollment_column="legacy_telemetry_client_id",
        source_column="client_id",
        clustered_sample_id=True,
    ),
    ("firefox_desktop", "client_id"): AnalysisUnit(
        kind=CLIENT_ID,
        enrollment_column="legacy_telemetry_client_id",
        source_column="client_id",
        clustered_sample_id=True,
    ),
}


def resolve(app_name, randomization_unit):
    """The analysis unit for one recipe, or None when the pair is not one this job knows."""
    return ANALYSIS_UNITS.get((app_name, randomization_unit))
