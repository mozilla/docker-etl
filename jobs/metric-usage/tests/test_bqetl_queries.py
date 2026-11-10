"""Real bigquery-etl queries, copied into tests/fixtures/bqetl, resolved against
small hand-built shapes of the tables they read."""

import pathlib

import pytest

from harness import events, resolve, table

FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "bqetl"


def fixture(name):
    return (FIXTURES / f"{name}.sql").read_text()


FENIX_EVENTS = table("org_mozilla_firefox.events", {
    "client_info.client_id": "client_id",
    "client_info.locale": "locale",
    "ping_info.reason": "reason",
    "metrics.string.region_home_region": "region.home_region",
    "metrics.boolean.policies_is_enterprise": "policies.is_enterprise",
    "metrics.labeled_counter.glean_error_invalid_label": "glean.error.invalid_label",
    "metrics.labeled_counter.wallpapers_new_wallpaper_applied": "wallpapers.new_wallpaper_applied",
    "metrics.url2.sample_page": "sample.page",
}, events_app="fenix")
FENIX_IDX = events("fenix", ("media.playback", "decode_error"),
                   ("media.playback", "first_frame_loaded"), ("media", "error"),
                   ("nav", "click"))


@pytest.fixture(scope="module")
def events_stream():
    return resolve(fixture("org_mozilla_firefox_derived.events_stream_v1"), FENIX_EVENTS,
                   events=FENIX_IDX)


def test_events_stream_keeps_metrics_at_their_json_paths(events_stream):
    r = events_stream
    assert set(r.carried("metrics.string.region_home_region")) == {"region.home_region"}
    assert set(r.carried("metrics.boolean.policies_is_enterprise")) == {"policies.is_enterprise"}
    # forwarded through * REPLACE, not named by this query
    assert not r.uses["region.home_region"].named


def test_events_stream_moves_url2_to_url(events_stream):
    assert set(events_stream.carried("metrics.url.sample_page")) == {"sample.page"}
    assert not any(p.startswith("metrics.url2") for p in events_stream.columns)


def test_events_stream_keeps_each_labeled_counter(events_stream):
    r = events_stream
    assert set(r.carried("metrics.labeled_counter.glean_error_invalid_label")) == \
        {"glean.error.invalid_label"}
    assert set(r.carried("metrics.labeled_counter.wallpapers_new_wallpaper_applied")) == \
        {"wallpapers.new_wallpaper_applied"}


def test_events_stream_client_columns(events_stream):
    r = events_stream
    assert set(r.carried("client_id")) == {"client_id"}
    assert r.uses["client_id"].named
    assert set(r.carried("client_info.locale")) == {"locale"}
    assert set(r.carried("reason")) == {"reason"}


def test_events_stream_identity_columns(events_stream):
    r = events_stream
    assert set(r.carried("event_category")) == {"fenix:(event category)"}
    assert set(r.carried("event_name")) == {"fenix:(event name)"}
    assert set(r.carried("event")) == {"fenix:(event category)", "fenix:(event name)"}


def test_events_stream_where_names_excluded_events(events_stream):
    r = events_stream
    for e in ("media.playback.decode_error", "media.playback.first_frame_loaded", "media.error"):
        assert r.uses[e].role == "filter"
        assert r.uses[e].named
    assert "nav.click" not in r.reached


def test_reading_events_stream_downstream(events_stream):
    t = events_stream.shape_for("org_mozilla_firefox_derived.events_stream_v1")
    r = resolve("""
        SELECT JSON_VALUE(metrics, '$.string.region_home_region') AS region, COUNT(*) AS n
        FROM `moz-fx-data-shared-prod.org_mozilla_firefox_derived.events_stream_v1`
        WHERE event = 'nav.click'
        GROUP BY region""", t, events=FENIX_IDX)
    assert set(r.carried("region")) == {"region.home_region"}
    assert r.uses["region.home_region"].named
    assert r.uses["nav.click"] == ("filter", True, True, r.uses["nav.click"].via)
    assert set(r.uses) == {"region.home_region", "nav.click"}
