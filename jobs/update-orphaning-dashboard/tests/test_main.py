"""Tests for date math, report assembly, and JSON serialization in main."""

import datetime as dt
import json
from collections import Counter

from update_orphaning_dashboard import main


def test_report_dates_from_monday():
    # 2026-06-08 is a Monday. The legacy job runs on Mondays (ds_nodash).
    dates = main.ReportDates(dt.date(2026, 6, 8))
    # report_filename is the report week's Sunday (the day before the run).
    assert dates.report_filename == "20260607"
    # max_report_date is the previous Saturday.
    assert dates.max_report_date == dt.date(2026, 6, 6)
    # min_report_date is six days before that.
    assert dates.min_report_date == dt.date(2026, 5, 31)
    # SQL upper anchor is the day after the previous Saturday.
    assert dates.max_report_date_sql == dt.date(2026, 6, 7)
    # aggregation window is ~6 months back.
    assert dates.aggregation_to == dt.date(2026, 6, 6)
    assert dates.aggregation_from == dt.date(2026, 6, 6) - dt.timedelta(days=6 * 31)


def test_latest_version_on_date():
    releases = {
        "138.0": "2026-04-01",
        "139.0": "2026-05-01",
        "140.0": "2026-07-01",  # released after the cutoff
    }
    assert main.latest_version_on_date("2026-06-01", releases) == 139


def test_build_results_shape():
    dates = main.ReportDates(dt.date(2026, 6, 8))
    summary_row = {
        "versionUpToDate": 10,
        "versionOutOfDate": 5,
        "versionTooLow": 1,
        "versionTooHigh": 0,
        "versionMissing": 2,
    }
    counts = {
        "hasOutOfDateMaxVersion": Counter({True: 5, False: 3}),
        "ofConcernByVersion": Counter({"120.0": 2}),
        "checkCodeNotifyOfConcern": Counter({22: 1, -1: 1}),
    }
    results = main.build_results(dates, 140, summary_row, counts)
    assert results["reportDetails"]["latestVersion"] == 140
    assert results["reportDetails"]["minUpdatePingCount"] == 4
    assert results["summary"] == summary_row
    assert results["hasOutOfDateMaxVersion"][True] == 5


def test_to_json_capitalizes_boolean_keys():
    results = {
        "hasOutOfDateMaxVersion": {True: 5, False: 3},
        "ofConcernByVersion": {"120.0": 2},
    }
    payload = main.to_json(results)
    parsed = json.loads(payload)
    # Boolean dict keys are emitted capitalized, matching the legacy output the
    # dashboard frontend reads.
    assert parsed["hasOutOfDateMaxVersion"] == {"True": 5, "False": 3}
    assert '"true"' not in payload
    assert '"false"' not in payload
    # Non-boolean string keys are untouched.
    assert parsed["ofConcernByVersion"] == {"120.0": 2}
