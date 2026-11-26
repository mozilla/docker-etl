"""Legacy telemetry events: anonymous f1_ (category), f2_ (method) and f3_
(object) fields, matched against string literals by resolve_legacy()."""

import pathlib

import pytest

from harness import legacy_events, regex, resolve, table

EVENT = table(
    "telemetry_stable.event_v4",
    {"client_id": "client_id"},
    events_app="telemetry",
    legacy_paths=["payload.events.parent"],
)
PROBES = legacy_events(
    "telemetry",
    ("security.unexpectedload#load", "security", ["unexpectedload"], ["script", "stylesheet"]),
    ("address.detected#address_form", "address", ["detected"], ["address_form"]),
    ("address.detected#address_form_ext", "address", ["detected"], ["address_form_ext"]),
    ("normandy.enroll#enroll", "normandy", ["enroll", "unenroll"], ["preference_study"]),
    ("normandy.validationfailed#fail", "normandy", ["validationfailed"], ["nimbus_experiment"]),
    ("addonsmanager.install#install", "addonsmanager", ["install"], ["extension"]),
    ("addonsmanager.disable#disable", "addonsmanager", ["disable"], ["extension"]),
)
FROM = (
    "FROM `moz-fx-data-shared-prod.telemetry_stable.event_v4`, "
    "UNNEST(payload.events.parent) AS e"
)


def run(where):
    return resolve(f"SELECT client_id {FROM} WHERE {where}", EVENT, events=PROBES)


def test_category_with_method_names_the_probe():
    r = run("e.f1_ = 'security' AND e.f2_ = 'unexpectedload'")
    assert r.uses["security.unexpectedload#load"] == (
        "filter",
        True,
        True,
        r.uses["security.unexpectedload#load"].via,
    )
    assert set(r.reached) == {"client_id", "security.unexpectedload#load"}


def test_category_with_method_names_every_probe_that_fits():
    r = run("e.f1_ = 'address' AND e.f2_ = 'detected'")
    assert r.uses["address.detected#address_form"].named
    assert r.uses["address.detected#address_form_ext"].named


def test_object_narrows_the_fit():
    r = run("e.f1_ = 'address' AND e.f3_ = 'address_form_ext'")
    assert r.uses["address.detected#address_form_ext"].named
    assert "address.detected#address_form" not in r.reached


def test_method_alone_names_only_a_unique_probe():
    assert run("e.f2_ = 'unexpectedload'").uses["security.unexpectedload#load"].named
    r = run("e.f2_ = 'detected'")
    assert r.reached["address.detected#address_form"] is False
    assert r.reached["address.detected#address_form_ext"] is False


def test_category_alone_reaches_without_naming():
    r = run("e.f1_ = 'normandy'")
    assert r.reached == {
        "client_id": True,
        "normandy.enroll#enroll": False,
        "normandy.validationfailed#fail": False,
    }


def test_like_reaches_without_naming():
    r = run("e.f1_ LIKE 'addons%'")
    assert r.reached["addonsmanager.install#install"] is False
    assert r.reached["addonsmanager.disable#disable"] is False


def test_literal_case_is_ignored():
    r = run("e.f1_ = 'Normandy' AND e.f2_ = 'validationFailed'")
    assert r.uses["normandy.validationfailed#fail"].named


def test_full_name_on_a_combined_column():
    r = run("CONCAT(e.f1_, '.', e.f2_) = 'normandy.unenroll'")
    assert r.uses["normandy.enroll#enroll"].named


def test_deanonymize_event_names_the_fields():
    # udf.deanonymize_event is what turns f1_ / f2_ / f3_ into event_category,
    # event_method and event_object for everything downstream
    t = resolve(
        f"SELECT `moz-fx-data-shared-prod.udf.deanonymize_event`(e).* {FROM}", EVENT
    ).shape_for("telemetry_derived.events_v1")
    r = resolve(
        "SELECT COUNT(*) AS n FROM `moz-fx-data-shared-prod.telemetry_derived.events_v1` "
        "WHERE event_category = 'normandy' AND event_method = 'enroll'",
        t,
        events=PROBES,
    )
    assert r.uses["normandy.enroll#enroll"].named
    assert "normandy.validationfailed#fail" not in r.reached


def test_regex_fallback_needs_a_fitting_method():
    # quoted 'install' as a method rules out addonsManager.disable
    sql = (
        "SELECT f1_, f2_, FROM `moz-fx-data-shared-prod.telemetry_stable.event_v4`, "
        "UNNEST(payload.events.parent) WHERE f1_ = 'addonsmanager' AND f2_ = 'install'"
    )
    assert regex(sql, EVENT, events=PROBES) == {"addonsmanager.install#install": True}


def test_regex_fallback_category_alone_names_nothing():
    sql = (
        "SELECT f1_, FROM `moz-fx-data-shared-prod.telemetry_stable.event_v4`, "
        "UNNEST(payload.events.parent) WHERE f1_ = 'normandy'"
    )
    assert regex(sql, EVENT, events=PROBES) == {}


# telemetry_derived.event_events_v1, the real query: the per-process event
# arrays merged by an array literal of STRUCTs, then deanonymize_event(e).*

PROCESSES = ("content", "dynamic", "extension", "gpu", "parent")
TELEMETRY_EVENT = table(
    "telemetry.event",
    {"client_id": "client_id"},
    events_app="telemetry",
    legacy_paths=[f"payload.events.{p}" for p in PROCESSES],
)


@pytest.fixture(scope="module")
def event_events():
    sql = (
        pathlib.Path(__file__).parent
        / "fixtures"
        / "bqetl"
        / "telemetry_derived.event_events_v1.sql"
    ).read_text()
    return resolve(sql, TELEMETRY_EVENT, events=PROBES)


def test_event_events_identity_columns(event_events):
    assert set(event_events.carried("event_category")) == {"telemetry:(event category)"}
    assert set(event_events.carried("event_method")) == {"telemetry:(event method)"}
    assert set(event_events.carried("event_object")) == {"telemetry:(event object)"}


def test_event_events_where_names_excluded_probes(event_events):
    # it drops security / unexpectedload and normandy / validationFailed rows
    for probe in ("security.unexpectedload#load", "normandy.validationfailed#fail"):
        assert event_events.uses[probe].role == "filter"
        assert event_events.uses[probe].named
    assert "normandy.enroll#enroll" not in event_events.uses
