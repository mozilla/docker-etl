"""Version resolution. The rest of queries.py needs BigQuery or the network.

These cover the behaviour changes from upstream, which are the parts most likely to
regress: esr version strings keep their suffix, and versions resolve as of a date rather
than from whatever product-details says at the moment the job runs. The product-details
payloads are stubbed with the real shapes, including the detail that firefox.json's
per-release `version` field drops the suffix that its key carries.
"""

import datetime

import pytest

from crash_correlations import queries


MAJOR_RELEASES = {
    "152.0": "2026-06-16",
    "153.0": "2026-07-21",
    "154.0": "2026-08-18",
}

STABILITY_RELEASES = {
    "152.0.1": "2026-06-23",
    "153.0.1": "2026-07-28",
    "153.0.4": "2026-08-11",
    "153.1.0": "2026-08-18",
    # esr point releases share this file upstream, and must not leak into release.
    "140.14.0esr": "2026-08-18",
}

DEVELOPMENT_RELEASES = {
    "154.0b1": "2026-07-22",
    "154.0b10": "2026-08-12",
    "155.0b1": "2026-08-17",
}

FIREFOX_RELEASES = {
    "firefox-140.13.0esr": {"version": "140.13.0", "date": "2026-07-21"},
    "firefox-140.14.0esr": {"version": "140.14.0", "date": "2026-08-18"},
    "firefox-115.38.0esr": {"version": "115.38.0", "date": "2026-07-21"},
    "firefox-153.0esr": {"version": "153.0", "date": "2026-07-21"},
    # No date: has to be skipped rather than crash.
    "firefox-999.0esr": {"version": "999.0"},
    "firefox-153.0": {"version": "153.0", "date": "2026-07-21"},
}

VERSIONS = {
    "FIREFOX_NIGHTLY": "156.0a1",
    "LATEST_FIREFOX_VERSION": "154.0",
    "LATEST_FIREFOX_RELEASED_DEVEL_VERSION": "155.0b1",
    "FIREFOX_ESR": "140.14.0esr",
    "FIREFOX_ESR_NEXT": "153.1.0esr",
}


@pytest.fixture(autouse=True)
def stub_product_details(monkeypatch):
    payloads = {
        "versions": VERSIONS,
        "history_major_releases": MAJOR_RELEASES,
        "history_stability_releases": STABILITY_RELEASES,
        "history_development_releases": DEVELOPMENT_RELEASES,
    }
    monkeypatch.setattr(queries, "_product_details", payloads.__getitem__)
    monkeypatch.setattr(
        queries, "_product_details_releases", lambda: FIREFOX_RELEASES
    )


def test_release_resolves_as_of_a_past_date():
    """The day before 154.0 shipped, release is still 153."""
    versions = queries.channel_versions("release", as_of=datetime.date(2026, 8, 17))
    assert versions == ["153.0", "153.0.1", "153.0.4"]
    # 153.1.0 shipped on the 18th, so it must not appear on the 17th.
    assert "153.1.0" not in versions


def test_release_takes_the_newest_major_only():
    """Older lines are dropped however much traffic they still carry."""
    versions = queries.channel_versions("release", as_of=datetime.date(2026, 8, 18))
    assert versions == ["154.0"]


def test_release_excludes_esr_builds_from_the_shared_history_file():
    versions = queries.channel_versions("release", as_of=datetime.date(2026, 8, 18))
    assert not [v for v in versions if v.endswith("esr")]


def test_esr_keeps_the_suffix():
    """The bug this replaces: a stripped version matches no esr rows at all.

    Upstream sent esr down the release branch and matched unsuffixed builds, so its esr
    output is really release data. Showing the wrong channel's crashes is worse than
    showing fewer of them, so this is not reproduced.
    """
    versions = queries.channel_versions("esr", as_of=datetime.date(2026, 8, 18))
    assert versions == ["153.0esr"]
    assert all(v.endswith("esr") for v in versions)


def test_esr_skips_records_with_no_date():
    """An undated build can't be placed in time, so it can't be the newest."""
    versions = queries.channel_versions("esr", as_of=datetime.date(2026, 8, 18))
    assert "999.0esr" not in versions


def test_beta_takes_the_newest_major_across_a_transition():
    versions = queries.channel_versions("beta", as_of=datetime.date(2026, 8, 18))
    assert versions == ["155.0b1"]


def test_nightly_is_the_live_value():
    """product-details publishes no dated history for nightly."""
    assert queries.channel_versions(
        "nightly", as_of=datetime.date(2020, 1, 1)
    ) == ["156.0a1"]


def test_unknown_channel_raises():
    with pytest.raises(ValueError):
        queries.channel_versions("aurora", as_of=datetime.date(2026, 8, 18))


@pytest.mark.parametrize(
    "version,expected",
    [
        ("153.0", 153),
        ("140.14.0esr", 140),
        ("154.0b10", 154),
        ("156.0a1", 156),
        ("", None),
        ("garbage", None),
    ],
)
def test_major_parsing(version, expected):
    assert queries._major(version) == expected
