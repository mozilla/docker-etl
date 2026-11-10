"""Legacy environment fields: which paths of main_v5 are listed, and how
queries reading them resolve. A repeated field is one entry, so reading any
part of its elements is a read of it."""

from metric_usage import inventory

from harness import resolve, table

MAIN = "telemetry_stable.main_v5"
EVENT = "telemetry_stable.event_v4"
ENV_IDX = {
    "environment.system": [(MAIN, "STRUCT", None)],
    "environment.system.cpu": [(MAIN, "STRUCT", None)],
    "environment.system.cpu.cores": [(MAIN, "INT64", " The number of physical CPU cores. "),
                                     (EVENT, "INT64", "Something else")],
    "environment.system.cpu.extensions": [(MAIN, "ARRAY", None)],
    "environment.addons": [(MAIN, "STRUCT", None)],
    "environment.addons.active_addons": [(MAIN, "ARRAY", None)],
    "environment.addons.active_addons.key": [(MAIN, "STRING", None)],
    "environment.addons.active_addons.value": [(MAIN, "STRUCT", None)],
    "environment.addons.active_addons.value.version": [(MAIN, "STRING", None)],
    # only in another table, so not listed
    "environment.only_elsewhere": [(EVENT, "STRING", None)],
}


def test_inventory_lists_leaves_and_repeated_fields_whole():
    rows = {r["metric"]: r for r in inventory.environment_inventory(ENV_IDX)}
    assert set(rows) == {"environment.system.cpu.cores", "environment.system.cpu.extensions",
                         "environment.addons.active_addons"}
    assert rows["environment.system.cpu.cores"]["columns"] == [
        {"table": EVENT, "path": "environment.system.cpu.cores"},
        {"table": MAIN, "path": "environment.system.cpu.cores"},
    ]
    # the description is main_v5's
    assert rows["environment.system.cpu.cores"]["description"] == "The number of physical CPU cores."
    assert rows["environment.addons.active_addons"]["description"] == ""


MAIN_SHAPE = table(MAIN, {
    "environment.system.cpu.cores": "cpu.cores",
    "environment.system.cpu.vendor": "cpu.vendor",
    "environment.addons.active_addons": "active_addons",
})
TBL = "`moz-fx-data-shared-prod.telemetry_stable.main_v5`"


def test_field_is_named():
    r = resolve(f"SELECT environment.system.cpu.cores AS cpu_cores FROM {TBL}", MAIN_SHAPE)
    assert set(r.carried("cpu_cores")) == {"cpu.cores"}
    assert r.uses["cpu.cores"].named
    assert "cpu.vendor" not in r.uses


def test_whole_struct_carries_fields_without_naming():
    r = resolve(f"SELECT environment.system.cpu FROM {TBL}", MAIN_SHAPE)
    assert set(r.carried("cpu.cores")) == {"cpu.cores"}
    assert not r.uses["cpu.cores"].named


def test_element_of_repeated_field_reads_it():
    r = resolve(f"SELECT a.value.version AS v FROM {TBL}, "
                "UNNEST(environment.addons.active_addons) AS a", MAIN_SHAPE)
    assert set(r.carried("v")) == {"active_addons"}
    assert r.uses["active_addons"].named
