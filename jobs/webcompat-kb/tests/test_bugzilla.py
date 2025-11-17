import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from unittest.mock import patch
from typing import Any, Iterable, Mapping

import pytest
import bugdantic.bugzilla

from webcompat_kb.bugzilla import (
    Bug,
    BugHistoryChange,
    BugHistoryEntry,
    BugHistoryUpdater,
    EXTERNAL_LINK_CONFIGS,
    PropertyHistory,
    add_datetime_limit,
    extract_int_from_field,
    get_etp_breakage_reports,
    get_kb_bug_core_bugs,
    get_kb_bug_site_report,
    get_recursive_dependencies,
    group_bugs,
    load_bugs,
    parse_user_story,
    write_bugs,
)


def to_bugs_by_id(data: Iterable[dict[str, Any]]) -> Mapping[int, Bug]:
    return {bug_data["id"]: Bug.from_json(bug_data) for bug_data in data}


def to_history(
    data: list[dict[str, Any]],
) -> Mapping[int, list[bugdantic.bugzilla.History]]:
    return {
        item["id"]: [
            bugdantic.bugzilla.History.model_validate(entry)
            for entry in item["history"]
        ]
        for item in data
    }


def to_history_entry(data: list[dict[str, Any]]) -> dict[int, BugHistoryEntry]:
    rv = defaultdict(list)
    for item in data:
        changes = [BugHistoryChange(**change) for change in item["changes"]]
        rv[item["number"]].append(
            BugHistoryEntry(
                number=item["number"],
                who=item["who"],
                change_time=item["change_time"],
                changes=changes,
            )
        )
    return rv


SAMPLE_KB_BUGS = to_bugs_by_id(
    [
        {
            "alias": None,
            "assigned_to": "test@example.org",
            "blocks": [],
            "component": "Knowledge Base",
            "creation_time": "2000-07-25T13:50:04Z",
            "creator": "nobody@mozilla.org",
            "depends_on": [903746],
            "id": 1835339,
            "keywords": [],
            "last_change_time": "2024-05-27T15:07:03Z",
            "last_resolved": None,
            "priority": None,
            "product": "Web Compatibility",
            "resolution": "",
            "see_also": [
                "https://github.com/webcompat/web-bugs/issues/13503",
                "https://github.com/webcompat/web-bugs/issues/91682",
                "https://bugzilla.mozilla.org/show_bug.cgi?id=1633399",
                "https://bugzilla.mozilla.org/show_bug.cgi?id=1735227",
                "https://bugzilla.mozilla.org/show_bug.cgi?id=1739489",
                "https://bugzilla.mozilla.org/show_bug.cgi?id=1739791",
                "https://github.com/webcompat/web-bugs/issues/109064",
                "https://github.com/mozilla-extensions/webcompat-addon/blob/5b391018e847a1eb30eba4784c86acd1c638ed26/src/injections/js/bug1739489-draftjs-beforeinput.js",
                "https://github.com/webcompat/web-bugs/issues/112848",
                "https://github.com/webcompat/web-bugs/issues/117039",
            ],
            "severity": None,
            "size_estimate": None,
            "status": "NEW",
            "summary": "Missing implementation of textinput event",
            "url": "",
            "user_story": "url:cmcreg.bancosantander.es/*\r\nurl:new.reddit.com/*\r\nurl:web.whatsapp.com/*\r\nurl:facebook.com/*\r\nurl:twitter.com/*\r\nurl:reddit.com/*\r\nurl:mobilevikings.be/*\r\nurl:book.ersthelfer.tv/*",  # noqa
            "webcompat_priority": None,
            "webcompat_score": None,
            "whiteboard": "",
        },
        {
            "alias": None,
            "assigned_to": "nobody@mozilla.org",
            "blocks": [],
            "component": "Knowledge Base",
            "creation_time": "2000-07-25T13:50:04Z",
            "creator": "nobody@mozilla.org",
            "depends_on": [],
            "id": 1835416,
            "keywords": [],
            "last_change_time": "2024-05-27T15:07:03Z",
            "last_resolved": None,
            "priority": None,
            "product": "Web Compatibility",
            "resolution": "",
            "see_also": [
                "https://github.com/webcompat/web-bugs/issues/100260",
                "https://github.com/webcompat/web-bugs/issues/22829",
                "https://github.com/webcompat/web-bugs/issues/62926",
                "https://github.com/webcompat/web-bugs/issues/66768",
                "https://github.com/webcompat/web-bugs/issues/112423",
                "https://mozilla.github.io/standards-positions/#webusb",
                "https://github.com/webcompat/web-bugs/issues/122436",
                "https://github.com/webcompat/web-bugs/issues/122127",
                "https://github.com/webcompat/web-bugs/issues/120886",
            ],
            "severity": None,
            "size_estimate": None,
            "status": "NEW",
            "summary": "Sites breaking due to the lack of WebUSB support",
            "url": "",
            "user_story": "url:webminidisc.com/*\r\nurl:app.webadb.com/*\r\nurl:www.numworks.com/*\r\nurl:webadb.github.io/*\r\nurl:www.stemplayer.com/*\r\nurl:wootility.io/*\r\nurl:python.microbit.org/*\r\nurl:flash.android.com/*",  # noqa
            "webcompat_priority": None,
            "webcompat_score": None,
            "whiteboard": "",
        },
        {
            "alias": None,
            "assigned_to": "nobody@mozilla.org",
            "blocks": [222222, 1734557],
            "component": "Knowledge Base",
            "creation_time": "2000-07-25T13:50:04Z",
            "creator": "nobody@mozilla.org",
            "depends_on": [555555],
            "id": 111111,
            "keywords": [],
            "last_change_time": "2024-05-27T15:07:03Z",
            "last_resolved": None,
            "priority": None,
            "product": "Web Compatibility",
            "resolution": "",
            "see_also": [
                "https://crbug.com/606208",
                "https://github.com/whatwg/html/issues/1896",
                "https://w3c.github.io/trusted-types/dist/spec/",
                "https://github.com/webcompat/web-bugs/issues/124877",
                "https://github.com/mozilla/standards-positions/issues/20",
                "https://github.com/WebKit/standards-positions/issues/186",
            ],
            "severity": None,
            "size_estimate": None,
            "status": "NEW",
            "summary": "Test bug",
            "url": "",
            "user_story": "",
            "webcompat_priority": None,
            "webcompat_score": None,
            "whiteboard": "",
        },
    ]
)

SAMPLE_CORE_BUGS = to_bugs_by_id(
    [
        {
            "alias": "core-bug-1",
            "assigned_to": "nobody@mozilla.org",
            "blocks": [1754236, 1835339],
            "component": "DOM: Events",
            "creation_time": "2000-07-25T13:50:04Z",
            "creator": "nobody@mozilla.org",
            "depends_on": [],
            "id": 903746,
            "keywords": [],
            "last_change_time": "2024-05-27T15:07:03Z",
            "last_resolved": None,
            "priority": None,
            "product": "Core",
            "resolution": "",
            "see_also": [
                "https://bugzilla.mozilla.org/show_bug.cgi?id=1739489",
                "https://bugzilla.mozilla.org/show_bug.cgi?id=1739791",
                "https://bugzilla.mozilla.org/show_bug.cgi?id=1735227",
                "https://bugzilla.mozilla.org/show_bug.cgi?id=1633399",
                "https://github.com/webcompat/web-bugs/issues/109064",
                "https://github.com/webcompat/web-bugs/issues/112848",
                "https://github.com/webcompat/web-bugs/issues/117039",
                "https://github.com/w3c/uievents/issues/353",
            ],
            "severity": None,
            "size_estimate": None,
            "status": "UNCONFIRMED",
            "summary": "Missing textinput event",
            "url": "",
            "user_story": "",
            "webcompat_priority": None,
            "webcompat_score": None,
            "whiteboard": "",
        },
        {
            "assigned_to": "nobody@mozilla.org",
            "alias": None,
            "blocks": [111111],
            "component": "Test",
            "creation_time": "2000-07-25T13:50:04Z",
            "creator": "nobody@mozilla.org",
            "depends_on": [],
            "id": 555555,
            "keywords": [],
            "last_change_time": "2024-05-27T15:07:03Z",
            "last_resolved": None,
            "priority": None,
            "product": "Core",
            "resolution": "",
            "see_also": ["https://mozilla.github.io/standards-positions/#testposition"],
            "severity": None,
            "size_estimate": None,
            "status": "UNCONFIRMED",
            "summary": "Test Core bug",
            "url": "",
            "user_story": "",
            "webcompat_priority": None,
            "webcompat_score": None,
            "whiteboard": "",
        },
        {
            "alias": None,
            "assigned_to": "nobody@mozilla.org",
            "blocks": [],
            "component": "Test",
            "creation_time": "2000-07-25T13:50:04Z",
            "creator": "nobody@mozilla.org",
            "depends_on": [],
            "id": 999999,
            "keywords": [],
            "last_change_time": "2024-05-27T15:07:03Z",
            "last_resolved": None,
            "priority": None,
            "product": "Core",
            "resolution": "",
            "see_also": [],
            "severity": None,
            "size_estimate": None,
            "status": "NEW",
            "summary": "Another Test Core bug",
            "url": "",
            "user_story": "",
            "webcompat_priority": None,
            "webcompat_score": None,
            "whiteboard": "",
        },
    ]
)

SAMPLE_BREAKAGE_BUGS = to_bugs_by_id(
    [
        {
            "alias": None,
            "assigned_to": "nobody@mozilla.org",
            "blocks": [],
            "component": "Site Reports",
            "creation_time": "2000-07-25T13:50:04Z",
            "creator": "nobody@mozilla.org",
            "depends_on": [111111],
            "id": 1734557,
            "keywords": [],
            "last_change_time": "2024-05-27T15:07:03Z",
            "last_resolved": None,
            "priority": None,
            "product": "Web Compatibility",
            "resolution": "",
            "see_also": [],
            "severity": None,
            "size_estimate": None,
            "status": "ASSIGNED",
            "summary": "Javascript causes infinite scroll because event.path is undefined",
            "url": "",
            "user_story": "url:angusnicneven.com/*",
            "webcompat_priority": None,
            "webcompat_score": None,
            "whiteboard": "",
        },
        {
            "alias": None,
            "assigned_to": "nobody@mozilla.org",
            "blocks": [],
            "component": "Site Reports",
            "creation_time": "2000-07-25T13:50:04Z",
            "creator": "nobody@mozilla.org",
            "depends_on": [111111],
            "id": 222222,
            "keywords": [],
            "last_change_time": "2024-05-27T15:07:03Z",
            "last_resolved": None,
            "priority": None,
            "product": "Web Compatibility",
            "resolution": "",
            "see_also": [],
            "severity": None,
            "size_estimate": None,
            "status": "ASSIGNED",
            "summary": "Test breakage bug",
            "url": "",
            "user_story": "url:example.com/*",
            "webcompat_priority": None,
            "webcompat_score": None,
            "whiteboard": "",
        },
    ]
)

SAMPLE_ETP_BUGS = to_bugs_by_id(
    [
        {
            "alias": None,
            "assigned_to": "nobody@mozilla.org",
            "blocks": [1101005],
            "component": "Privacy: Site Reports",
            "creation_time": "2024-07-30T07:37:28Z",
            "creator": "nobody@mozilla.org",
            "depends_on": [1875061],
            "id": 1910548,
            "keywords": ["priv-webcompat", "webcompat:site-report"],
            "last_change_time": "2024-05-27T15:07:03Z",
            "last_resolved": None,
            "priority": None,
            "product": "Web Compatibility",
            "resolution": "",
            "see_also": ["https://github.com/webcompat/web-bugs/issues/139647"],
            "severity": None,
            "size_estimate": None,
            "status": "NEW",
            "summary": "gothamist.com - The comments are not displayed with ETP set to Strict",
            "url": "https://gothamist.com/",
            "user_story": "",
            "webcompat_priority": None,
            "webcompat_score": None,
            "whiteboard": "",
        },
        {
            "alias": None,
            "assigned_to": "nobody@mozilla.org",
            "blocks": [],
            "component": "Privacy: Site Reports",
            "creation_time": "2024-10-01T08:50:58Z",
            "creator": "nobody@mozilla.org",
            "depends_on": [1101005, 1797458],
            "id": 1921943,
            "keywords": [
                "priv-webcompat",
                "webcompat:platform-bug",
                "webcompat:site-report",
            ],
            "last_change_time": "2024-05-27T15:07:03Z",
            "last_resolved": None,
            "priority": None,
            "product": "Web Compatibility",
            "resolution": "",
            "see_also": ["https://github.com/webcompat/web-bugs/issues/142250"],
            "severity": None,
            "size_estimate": None,
            "status": "NEW",
            "summary": "my.farys.be - Login option is missing with ETP set to STRICT",
            "url": "https://my.farys.be/myfarys/",
            "user_story": "",
            "webcompat_priority": None,
            "webcompat_score": None,
            "whiteboard": "",
        },
        {
            "alias": None,
            "assigned_to": "nobody@mozilla.org",
            "blocks": [],
            "component": "Privacy: Site Reports",
            "creation_time": "2024-10-30T15:04:41Z",
            "creator": "nobody@mozilla.org",
            "depends_on": [1101005, 1122334],
            "id": 1928102,
            "keywords": ["webcompat:site-report"],
            "last_change_time": "2024-05-27T15:07:03Z",
            "last_resolved": None,
            "priority": None,
            "product": "Web Compatibility",
            "resolution": "",
            "see_also": [],
            "severity": None,
            "size_estimate": None,
            "status": "NEW",
            "summary": "ryanair.com - The form to start a chat does not load with ETP set to STRICT",
            "url": "https://www.ryanair.com/gb/en/lp/chat",
            "user_story": "",
            "webcompat_priority": None,
            "webcompat_score": None,
            "whiteboard": "",
        },
    ]
)

SAMPLE_ETP_DEPENDENCIES_BUGS = to_bugs_by_id(
    [
        {
            "alias": None,
            "assigned_to": "nobody@mozilla.org",
            "blocks": [
                1526695,
                1903311,
                1903317,
                1903340,
                1903345,
            ],
            "component": "Privacy: Anti-Tracking",
            "creation_time": "2014-11-18T16:11:29Z",
            "creator": "nobody@mozilla.org",
            "depends_on": [
                1400025,
                1446243,
                1465962,
                1470298,
                1470301,
                1486425,
                1627322,
            ],
            "id": 1101005,
            "keywords": ["meta", "webcompat:platform-bug"],
            "last_change_time": "2024-05-27T15:07:03Z",
            "last_resolved": None,
            "priority": None,
            "product": "Core",
            "resolution": "",
            "see_also": [
                "https://webcompat.com/issues/2999",
                "https://webcompat.com/issues/10020",
                "https://github.com/webcompat/web-bugs/issues/23536",
                "https://github.com/webcompat/web-bugs/issues/23241",
                "https://github.com/webcompat/web-bugs/issues/23527",
                "https://github.com/webcompat/web-bugs/issues/23000",
                "https://github.com/webcompat/web-bugs/issues/21661",
                "https://github.com/webcompat/web-bugs/issues/22735",
                "https://webcompat.com/issues/23474",
                "https://github.com/webcompat/web-bugs/issues/23460",
                "https://github.com/webcompat/web-bugs/issues/24002",
                "https://webcompat.com/issues/23470",
                "https://webcompat.com/issues/24519",
                "https://github.com/webcompat/web-bugs/issues/25315",
                "https://github.com/webcompat/web-bugs/issues/26073",
                "https://github.com/webcompat/web-bugs/issues/27976",
                "https://github.com/webcompat/web-bugs/issues/28052#event-2248891646",
                "https://github.com/webcompat/web-bugs/issues/28425",
                "https://github.com/webcompat/web-bugs/issues/29218",
                "https://webcompat.com/issues/20266",
                "https://github.com/webcompat/web-bugs/issues/30320",
                "https://webcompat.com/issues/38315",
                "https://webcompat.com/issues/35647",
            ],
            "severity": None,
            "size_estimate": None,
            "status": "NEW",
            "summary": "[meta] ETP Strict mode or Private Browsing mode tracking protection breakage",
            "url": "",
            "user_story": "",
            "webcompat_priority": None,
            "webcompat_score": None,
            "whiteboard": "",
        },
        {
            "alias": None,
            "assigned_to": "nobody@mozilla.org",
            "blocks": [1101005, 1773684, 1921943],
            "component": "Privacy: Anti-Tracking",
            "creation_time": "2022-10-26T09:33:25Z",
            "creator": "nobody@mozilla.org",
            "depends_on": [
                1796560,
                1799094,
                1799618,
                1800007,
                1803127,
            ],
            "id": 1797458,
            "keywords": ["meta"],
            "last_change_time": "2024-05-27T15:07:03Z",
            "last_resolved": None,
            "priority": None,
            "product": "Core",
            "resolution": "",
            "see_also": [],
            "severity": None,
            "size_estimate": None,
            "status": "NEW",
            "summary": "[meta] Email Tracking Breakage",
            "url": "",
            "user_story": "",
            "webcompat_priority": None,
            "webcompat_score": None,
            "whiteboard": "",
        },
        {
            "alias": None,
            "assigned_to": "nobody@mozilla.org",
            "blocks": [
                1101005,
                1901474,
                1905920,
                1906053,
                1910548,
                1910855,
                1912261,
                1916183,
                1916443,
            ],
            "component": "Privacy: Anti-Tracking",
            "creation_time": "2024-01-17T13:40:16Z",
            "creator": "nobody@mozilla.org",
            "depends_on": [1884676, 1906418, 1894615],
            "id": 1875061,
            "keywords": ["meta"],
            "last_change_time": "2024-05-27T15:07:03Z",
            "last_resolved": None,
            "priority": None,
            "product": "Core",
            "resolution": "",
            "see_also": [
                "https://bugzilla.mozilla.org/show_bug.cgi?id=1869326",
                "https://bugzilla.mozilla.org/show_bug.cgi?id=1872855",
                "https://bugzilla.mozilla.org/show_bug.cgi?id=1874855",
                "https://bugzilla.mozilla.org/show_bug.cgi?id=1878855",
                "https://bugzilla.mozilla.org/show_bug.cgi?id=1428122",
                "https://bugzilla.mozilla.org/show_bug.cgi?id=1892176",
            ],
            "severity": None,
            "size_estimate": None,
            "status": "NEW",
            "summary": "[meta] ETP breakage for webpages that have Disqus comment section",
            "url": "",
            "user_story": "",
            "webcompat_priority": None,
            "webcompat_score": None,
            "whiteboard": "",
        },
        {
            "alias": None,
            "assigned_to": "nobody@mozilla.org",
            "blocks": [],
            "component": "Privacy: Anti-Tracking",
            "creation_time": "2024-01-17T13:40:16Z",
            "creator": "nobody@mozilla.org",
            "depends_on": [444444, 555555],
            "id": 1122334,
            "keywords": [],
            "last_change_time": "2024-05-27T15:07:03Z",
            "last_resolved": None,
            "priority": None,
            "product": "Core",
            "resolution": "",
            "see_also": [],
            "severity": None,
            "size_estimate": None,
            "status": "NEW",
            "summary": "Sample non meta ETP dependency",
            "url": "",
            "user_story": "",
            "webcompat_priority": None,
            "webcompat_score": None,
            "whiteboard": "",
        },
    ]
)

SAMPLE_CORE_AS_KB_BUGS = to_bugs_by_id(
    [
        {
            "alias": None,
            "assigned_to": "nobody@mozilla.org",
            "blocks": [1539848, 1729514, 1896383],
            "component": "JavaScript Engine",
            "creation_time": "2024-03-21T16:40:27Z",
            "creator": "nobody@mozilla.org",
            "depends_on": [],
            "id": 1886820,
            "keywords": ["parity-chrome", "parity-safari", "webcompat:platform-bug"],
            "last_change_time": "2024-05-27T15:07:03Z",
            "last_resolved": None,
            "priority": 3,
            "product": "Core",
            "resolution": "",
            "see_also": ["https://bugzilla.mozilla.org/show_bug.cgi?id=1740472"],
            "severity": 3,
            "size_estimate": None,
            "status": "NEW",
            "summary": "Consider adding support for Error.captureStackTrace",
            "url": "",
            "user_story": "",
            "webcompat_priority": None,
            "webcompat_score": None,
            "whiteboard": "",
        },
        {
            "alias": None,
            "assigned_to": "nobody@mozilla.org",
            "blocks": [1656444, 1835339, 222222],
            "component": "DOM: Window and Location",
            "creation_time": "2024-04-30T14:04:23Z",
            "creator": "nobody@mozilla.org",
            "depends_on": [1896672],
            "id": 1894244,
            "keywords": ["webcompat:platform-bug"],
            "last_change_time": "2024-05-14T15:19:21Z",
            "last_resolved": None,
            "priority": 3,
            "product": "Core",
            "resolution": "",
            "see_also": ["https://bugzilla.mozilla.org/show_bug.cgi?id=1863217"],
            "severity": 2,
            "size_estimate": None,
            "status": "NEW",
            "summary": "Popup blocker is too strict when opening new windows",
            "url": "",
            "user_story": "",
            "webcompat_priority": None,
            "webcompat_score": None,
            "whiteboard": "",
        },
        {
            "alias": None,
            "assigned_to": "nobody@mozilla.org",
            "blocks": [],
            "component": "JavaScript Engine",
            "creation_time": "2024-03-21T16:40:27Z",
            "creator": "nobody@mozilla.org",
            "depends_on": [999999],
            "id": 444444,
            "keywords": ["webcompat:platform-bug", "webcompat:site-report"],
            "last_change_time": "2024-05-27T15:07:03Z",
            "last_resolved": None,
            "priority": 3,
            "product": "Core",
            "resolution": "",
            "see_also": [],
            "severity": 3,
            "size_estimate": None,
            "status": "NEW",
            "summary": "Example core site report and platform bug",
            "url": "",
            "user_story": "",
            "webcompat_priority": None,
            "webcompat_score": None,
            "whiteboard": "",
        },
    ]
)

SAMPLE_ALL_BUGS = {**SAMPLE_KB_BUGS}
SAMPLE_ALL_BUGS.update(SAMPLE_CORE_BUGS)
SAMPLE_ALL_BUGS.update(SAMPLE_BREAKAGE_BUGS)
SAMPLE_ALL_BUGS.update(SAMPLE_CORE_AS_KB_BUGS)
SAMPLE_ALL_BUGS.update(SAMPLE_ETP_BUGS)
SAMPLE_ALL_BUGS.update(SAMPLE_ETP_DEPENDENCIES_BUGS)


SAMPLE_HISTORY = to_history(
    [
        {
            "id": 1536482,
            "history": [
                {
                    "changes": [
                        {"removed": "--", "field_name": "priority", "added": "P4"},
                        {
                            "added": "1464828, 1529973",
                            "removed": "",
                            "field_name": "depends_on",
                        },
                        {
                            "field_name": "cf_status_firefox68",
                            "removed": "affected",
                            "added": "---",
                        },
                        {
                            "field_name": "keywords",
                            "removed": "",
                            "added": "webcompat:needs-diagnosis",
                        },
                    ],
                    "when": "2023-05-01T17:41:18Z",
                    "who": "example",
                }
            ],
        },
        {
            "id": 1536483,
            "history": [
                {
                    "changes": [
                        {
                            "field_name": "cf_user_story",
                            "added": "@@ -0,0 +1,3 @@\n+platform:linux\r\n+impact:feature-broken\r\n+affects:some\n\\ No newline at end of file\n",  # noqa
                            "removed": "",
                        },
                        {"field_name": "priority", "removed": "--", "added": "P3"},
                        {"removed": "--", "added": "S4", "field_name": "severity"},
                    ],
                    "who": "example",
                    "when": "2023-03-18T16:58:27Z",
                },
                {
                    "changes": [
                        {
                            "field_name": "status",
                            "added": "ASSIGNED",
                            "removed": "UNCONFIRMED",
                        },
                        {
                            "field_name": "cc",
                            "removed": "",
                            "added": "example@example.com",
                        },
                    ],
                    "when": "2023-06-01T10:00:00Z",
                    "who": "example",
                },
            ],
        },
        {
            "id": 1536484,
            "alias": None,
            "history": [
                {
                    "changes": [],
                    "when": "2023-07-01T12:00:00Z",
                    "who": "example",
                }
            ],
        },
        {
            "id": 1536485,
            "alias": None,
            "history": [
                {
                    "changes": [
                        {
                            "removed": "",
                            "field_name": "cc",
                            "added": "someone@example.com",
                        },
                        {
                            "removed": "",
                            "field_name": "keywords",
                            "added": "webcompat:platform-bug",
                        },
                    ],
                    "when": "2023-05-01T14:00:00Z",
                    "who": "example",
                },
                {
                    "changes": [
                        {
                            "removed": "ASSIGNED",
                            "field_name": "status",
                            "added": "RESOLVED",
                        }
                    ],
                    "when": "2023-08-01T14:00:00Z",
                    "who": "example",
                },
            ],
        },
    ]
)

MISSING_KEYWORDS_HISTORY = to_history(
    [
        {
            "id": 1898563,
            "alias": None,
            "history": [
                {
                    "when": "2024-05-27T15:10:10Z",
                    "changes": [
                        {
                            "added": "@@ -1 +1,4 @@\n-\n+platform:windows,mac,linux,android\r\n+impact:blocked\r\n+configuration:general\r\n+affects:all\n",  # noqa
                            "field_name": "cf_user_story",
                            "removed": "",
                        },
                        {"removed": "--", "field_name": "severity", "added": "S2"},
                        {
                            "removed": "",
                            "added": "name@example.com",
                            "field_name": "cc",
                        },
                        {"added": "P2", "field_name": "priority", "removed": "P1"},
                        {"removed": "", "added": "1886128", "field_name": "depends_on"},
                    ],
                    "who": "name@example.com",
                }
            ],
        },
        {
            "history": [
                {
                    "who": "someone@example.com",
                    "when": "2024-05-13T16:03:18Z",
                    "changes": [
                        {
                            "field_name": "cf_user_story",
                            "added": "@@ -1 +1,4 @@\n-\n+platform:windows,mac,linux\r\n+impact:site-broken\r\n+configuration:general\r\n+affects:all\n",  # noqa
                            "removed": "",
                        },
                        {"removed": "P3", "added": "P1", "field_name": "priority"},
                        {
                            "removed": "",
                            "field_name": "keywords",
                            "added": "webcompat:needs-diagnosis",
                        },
                        {"added": "S2", "field_name": "severity", "removed": "--"},
                        {
                            "removed": "",
                            "field_name": "cc",
                            "added": "someone@example.com",
                        },
                    ],
                },
                {
                    "who": "someone@example.com",
                    "when": "2024-05-21T17:17:52Z",
                    "changes": [
                        {
                            "removed": "",
                            "field_name": "cc",
                            "added": "someone@example.com",
                        }
                    ],
                },
                {
                    "when": "2024-05-21T17:22:20Z",
                    "changes": [
                        {"field_name": "depends_on", "added": "1886820", "removed": ""}
                    ],
                    "who": "someone@example.com",
                },
                {
                    "changes": [
                        {
                            "removed": "webcompat:needs-diagnosis",
                            "field_name": "keywords",
                            "added": "webcompat:needs-sitepatch",
                        },
                        {
                            "added": "someone@example.com",
                            "field_name": "cc",
                            "removed": "",
                        },
                    ],
                    "when": "2024-05-27T15:07:33Z",
                    "who": "someone@example.com",
                },
                {
                    "who": "someone@example.com",
                    "changes": [
                        {"field_name": "depends_on", "added": "1876368", "removed": ""}
                    ],
                    "when": "2024-06-05T19:25:37Z",
                },
                {
                    "changes": [
                        {
                            "added": "someone@example.com",
                            "field_name": "cc",
                            "removed": "",
                        }
                    ],
                    "when": "2024-06-09T02:49:27Z",
                    "who": "someone@example.com",
                },
                {
                    "who": "someone@example.com",
                    "changes": [
                        {
                            "field_name": "keywords",
                            "added": "webcompat:sitepatch-applied",
                            "removed": "webcompat:needs-sitepatch",
                        }
                    ],
                    "when": "2024-06-11T16:34:22Z",
                },
            ],
            "alias": None,
            "id": 1896383,
        },
        {
            "history": [
                {
                    "who": "someone@example.com",
                    "changes": [
                        {
                            "field_name": "keywords",
                            "added": "",
                            "removed": "webcompat:needs-diagnosis",
                        }
                    ],
                    "when": "2024-06-11T16:34:22Z",
                },
            ],
            "alias": None,
            "id": 222222,
        },
    ]
)

MISSING_KEYWORDS_INITIAL = to_history_entry(
    [
        {
            "number": 1898563,
            "who": "name@example.com",
            "change_time": datetime.fromisoformat("2024-05-23T16:40:29Z"),
            "changes": [
                {
                    "added": "webcompat:needs-diagnosis, webcompat:needs-sitepatch",
                    "field_name": "keywords",
                    "removed": "",
                }
            ],
        },
        {
            "number": 222222,
            "who": "name@example.com",
            "change_time": datetime.fromisoformat("2024-05-13T13:02:11Z"),
            "changes": [
                {
                    "added": "webcompat:needs-diagnosis",
                    "field_name": "keywords",
                    "removed": "",
                }
            ],
        },
    ]
)

MISSING_KEYWORDS_BUGS = to_bugs_by_id(
    [
        {
            "alias": None,
            "assigned_to": "nobody@mozilla.org",
            "blocks": [],
            "component": "Site Reports",
            "creation_time": "2024-05-23T16:40:29Z",
            "creator": "name@example.com",
            "depends_on": [1886128],
            "id": 1898563,
            "keywords": ["webcompat:needs-diagnosis", "webcompat:needs-sitepatch"],
            "last_change_time": "2024-05-27T15:07:03Z",
            "last_resolved": None,
            "priority": None,
            "product": "Web Compatibility",
            "resolution": "",
            "see_also": ["https://github.com/webcompat/web-bugs/issues/135636"],
            "severity": None,
            "size_estimate": None,
            "status": "NEW",
            "summary": "mylotto.co.nz - Website not supported on Firefox",
            "url": "",
            "user_story": "",
            "webcompat_priority": None,
            "webcompat_score": None,
            "whiteboard": "",
        },
        {
            "alias": None,
            "assigned_to": "nobody@mozilla.org",
            "blocks": [],
            "component": "Site Reports",
            "creation_time": "2024-05-13T13:02:11Z",
            "creator": "name@example.com",
            "depends_on": [1886820, 1876368],
            "id": 1896383,
            "keywords": ["webcompat:sitepatch-applied"],
            "last_change_time": "2024-06-11T16:34:22Z",
            "last_resolved": None,
            "priority": None,
            "product": "Web Compatibility",
            "resolution": "",
            "see_also": ["https://github.com/webcompat/web-bugs/issues/136865"],
            "severity": None,
            "size_estimate": None,
            "status": "NEW",
            "summary": "www.unimarc.cl - Buttons not working",
            "url": "",
            "user_story": "",
            "webcompat_priority": None,
            "webcompat_score": None,
            "whiteboard": "",
        },
        {
            "alias": None,
            "assigned_to": "nobody@mozilla.org",
            "blocks": [],
            "component": "Desktop",
            "creation_time": "2024-05-13T13:02:11Z",
            "creator": "name@example.com",
            "depends_on": [111111],
            "id": 222222,
            "keywords": [],
            "last_change_time": "2024-06-11T16:34:22Z",
            "last_resolved": None,
            "priority": None,
            "product": "Web Compatibility",
            "resolution": "",
            "see_also": [],
            "severity": None,
            "size_estimate": None,
            "status": "ASSIGNED",
            "summary": "Test breakage bug",
            "url": "",
            "user_story": "",
            "webcompat_priority": None,
            "webcompat_score": None,
            "whiteboard": "",
        },
    ]
)

REMOVED_READDED_BUGS = to_bugs_by_id(
    [
        {
            "alias": None,
            "assigned_to": "nobody@mozilla.org",
            "blocks": [],
            "component": "Desktop",
            "creation_time": "2024-05-13T13:02:11Z",
            "creator": "name@example.com",
            "depends_on": [111111],
            "id": 333333,
            "keywords": ["webcompat:needs-diagnosis"],
            "last_change_time": "2024-05-27T15:07:03Z",
            "last_resolved": None,
            "priority": None,
            "product": "Web Compatibility",
            "resolution": "",
            "see_also": [],
            "severity": None,
            "size_estimate": None,
            "status": "ASSIGNED",
            "summary": "Test breakage bug",
            "url": "",
            "user_story": "",
            "webcompat_priority": None,
            "webcompat_score": None,
            "whiteboard": "",
        }
    ]
)

REMOVED_READDED_HISTORY = to_history(
    [
        {
            "history": [
                {
                    "who": "someone@example.com",
                    "changes": [
                        {
                            "field_name": "keywords",
                            "added": "",
                            "removed": "webcompat:needs-diagnosis",
                        }
                    ],
                    "when": "2024-06-11T16:34:22Z",
                },
                {
                    "who": "someone@example.com",
                    "changes": [
                        {
                            "field_name": "keywords",
                            "added": "webcompat:needs-sitepatch",
                            "removed": "",
                        }
                    ],
                    "when": "2024-06-15T16:34:22Z",
                },
                {
                    "who": "someone@example.com",
                    "changes": [
                        {
                            "field_name": "keywords",
                            "added": "webcompat:needs-diagnosis",
                            "removed": "",
                        }
                    ],
                    "when": "2024-07-11T16:34:22Z",
                },
                {
                    "who": "someone@example.com",
                    "changes": [
                        {
                            "field_name": "keywords",
                            "added": "",
                            "removed": "webcompat:needs-sitepatch",
                        }
                    ],
                    "when": "2024-07-14T16:34:22Z",
                },
                {
                    "who": "someone@example.com",
                    "changes": [
                        {
                            "field_name": "keywords",
                            "added": "",
                            "removed": "webcompat:needs-diagnosis",
                        }
                    ],
                    "when": "2024-09-11T16:34:22Z",
                },
                {
                    "who": "someone@example.com",
                    "changes": [
                        {
                            "field_name": "keywords",
                            "added": "webcompat:needs-diagnosis",
                            "removed": "",
                        }
                    ],
                    "when": "2024-12-11T16:34:22Z",
                },
            ],
            "alias": None,
            "id": 333333,
        },
    ]
)

KEYWORDS_AND_STATUS = to_history(
    [
        {
            "history": [
                {
                    "changes": [
                        {
                            "added": "parity-chrome, parity-edge, parity-ie",
                            "field_name": "keywords",
                            "removed": "",
                        },
                    ],
                    "who": "someone@example.com",
                    "when": "2018-05-02T18:25:47Z",
                },
                {
                    "changes": [
                        {"added": "RESOLVED", "removed": "NEW", "field_name": "status"}
                    ],
                    "when": "2024-05-16T10:58:15Z",
                    "who": "someone@example.com",
                },
                {
                    "who": "someone@example.com",
                    "when": "2024-06-03T14:44:48Z",
                    "changes": [
                        {
                            "removed": "RESOLVED",
                            "field_name": "status",
                            "added": "REOPENED",
                        },
                        {
                            "field_name": "keywords",
                            "removed": "",
                            "added": "webcompat:platform-bug",
                        },
                    ],
                },
                {
                    "when": "2016-01-14T14:01:36Z",
                    "who": "someone@example.com",
                    "changes": [
                        {
                            "added": "NEW",
                            "removed": "UNCONFIRMED",
                            "field_name": "status",
                        }
                    ],
                },
            ],
            "alias": None,
            "id": 1239595,
        },
    ]
)


@pytest.fixture()
def history_updater(bq_client):
    return BugHistoryUpdater(bq_client, None)


def test_extract_int_from_field():
    field = extract_int_from_field("P3")
    assert field == 3

    field = extract_int_from_field("critical", value_map={"critical": 1})
    assert field == 1

    field = extract_int_from_field("--")
    assert field is None

    field = extract_int_from_field("N/A", value_map={"n/a": None})
    assert field is None

    field = extract_int_from_field("")
    assert field is None

    field = extract_int_from_field(None)
    assert field is None


def test_group_bugs():
    site_reports, etp_reports, kb_bugs, platform_bugs = group_bugs(SAMPLE_ALL_BUGS)
    assert site_reports == {222222, 444444, 1734557}
    assert etp_reports == set(SAMPLE_ETP_BUGS.keys())
    assert kb_bugs == set(SAMPLE_KB_BUGS.keys()) | set(
        SAMPLE_CORE_AS_KB_BUGS.keys()
    ) | {444444, 999999, 1101005}
    assert platform_bugs == set(SAMPLE_CORE_BUGS.keys()) | set(
        SAMPLE_CORE_AS_KB_BUGS.keys()
    ) | set(SAMPLE_ETP_DEPENDENCIES_BUGS.keys()) | {444444}


def test_get_kb_bug_site_report():
    site_reports, _, kb_bugs, _ = group_bugs(SAMPLE_ALL_BUGS)

    kb_bugs_site_reports = get_kb_bug_site_report(
        SAMPLE_ALL_BUGS, kb_bugs, site_reports
    )
    assert kb_bugs_site_reports == {
        111111: {1734557, 222222},
        444444: {444444},
        1894244: {222222},
    }


def test_get_kb_bug_core_bug():
    _, _, kb_bugs, platform_bugs = group_bugs(SAMPLE_ALL_BUGS)

    kb_bugs_core_bugs = get_kb_bug_core_bugs(SAMPLE_ALL_BUGS, kb_bugs, platform_bugs)
    assert kb_bugs_core_bugs == {111111: {555555}, 1835339: {903746}}


def test_get_etp_breakage_reports():
    _, etp_bugs, _, _ = group_bugs(SAMPLE_ALL_BUGS)

    etp_links = get_etp_breakage_reports(SAMPLE_ALL_BUGS, etp_bugs)

    assert etp_links == {
        1910548: {1101005, 1875061},
        1921943: {1101005, 1797458},
        1928102: {1101005},
    }


def test_get_external_links():
    _, _, kb_bugs, _ = group_bugs(SAMPLE_ALL_BUGS)

    assert EXTERNAL_LINK_CONFIGS["interventions"].get_links(
        SAMPLE_ALL_BUGS, kb_bugs
    ) == {
        1835339: {
            "https://github.com/mozilla-extensions/webcompat-addon/blob/5b391018e847a1eb30eba4784c86acd1c638ed26/src/injections/js/bug1739489-draftjs-beforeinput.js"
        },
    }

    assert EXTERNAL_LINK_CONFIGS["other_browser_issues"].get_links(
        SAMPLE_ALL_BUGS, kb_bugs
    ) == {111111: {"https://crbug.com/606208"}}

    assert EXTERNAL_LINK_CONFIGS["standards_issues"].get_links(
        SAMPLE_ALL_BUGS, kb_bugs
    ) == {
        111111: {"https://github.com/whatwg/html/issues/1896"},
        1835339: {"https://github.com/w3c/uievents/issues/353"},
    }

    assert EXTERNAL_LINK_CONFIGS["standards_positions"].get_links(
        SAMPLE_ALL_BUGS, kb_bugs
    ) == {
        111111: {
            "https://github.com/mozilla/standards-positions/issues/20",
            "https://github.com/WebKit/standards-positions/issues/186",
            "https://mozilla.github.io/standards-positions/#testposition",
        },
        1835416: {"https://mozilla.github.io/standards-positions/#webusb"},
    }


def test_bugzilla_to_history_entry(history_updater):
    expected_result = {bug_id: [] for bug_id in SAMPLE_HISTORY}

    expected_result.update(
        to_history_entry(
            [
                {
                    "number": 1536482,
                    "who": "example",
                    "change_time": datetime.fromisoformat("2023-05-01T17:41:18Z"),
                    "changes": [
                        {
                            "field_name": "keywords",
                            "removed": "",
                            "added": "webcompat:needs-diagnosis",
                        }
                    ],
                },
                {
                    "number": 1536483,
                    "who": "example",
                    "change_time": datetime.fromisoformat("2023-03-18T16:58:27Z"),
                    "changes": [
                        {
                            "field_name": "cf_user_story",
                            "added": "@@ -0,0 +1,3 @@\n+platform:linux\r\n+impact:feature-broken\r\n+affects:some\n\\ No newline at end of file\n",  # noqa
                            "removed": "",
                        }
                    ],
                },
                {
                    "number": 1536483,
                    "who": "example",
                    "change_time": datetime.fromisoformat("2023-06-01T10:00:00Z"),
                    "changes": [
                        {
                            "field_name": "status",
                            "added": "ASSIGNED",
                            "removed": "UNCONFIRMED",
                        }
                    ],
                },
                {
                    "number": 1536485,
                    "who": "example",
                    "change_time": datetime.fromisoformat("2023-05-01T14:00:00Z"),
                    "changes": [
                        {
                            "removed": "",
                            "field_name": "keywords",
                            "added": "webcompat:platform-bug",
                        }
                    ],
                },
                {
                    "number": 1536485,
                    "who": "example",
                    "change_time": datetime.fromisoformat("2023-08-01T14:00:00Z"),
                    "changes": [
                        {
                            "removed": "ASSIGNED",
                            "field_name": "status",
                            "added": "RESOLVED",
                        }
                    ],
                },
            ]
        )
    )

    entries = history_updater.bugzilla_to_history_entry(SAMPLE_HISTORY)
    assert entries == expected_result


def test_create_initial_history(history_updater):
    history = history_updater.bugzilla_to_history_entry(MISSING_KEYWORDS_HISTORY)
    result = history_updater.create_initial_history_entry(
        MISSING_KEYWORDS_BUGS, history
    )

    assert result == MISSING_KEYWORDS_INITIAL


def test_create_initial_history_removed_readded(history_updater):
    history = history_updater.bugzilla_to_history_entry(REMOVED_READDED_HISTORY)
    result = history_updater.create_initial_history_entry(REMOVED_READDED_BUGS, history)

    expected = to_history_entry(
        [
            {
                "number": 333333,
                "who": "name@example.com",
                "change_time": datetime.fromisoformat("2024-05-13T13:02:11Z"),
                "changes": [
                    {
                        "added": "webcompat:needs-diagnosis",
                        "field_name": "keywords",
                        "removed": "",
                    }
                ],
            }
        ]
    )

    assert result == expected


@patch("webcompat_kb.bugzilla.BugHistoryUpdater.bugzilla_fetch_history")
def test_create_new_bugs_history(mock_bugzilla_fetch_history, history_updater):
    mock_bugzilla_fetch_history.return_value = (
        history_updater.bugzilla_to_history_entry(MISSING_KEYWORDS_HISTORY)
    )

    expected = history_updater.bugzilla_to_history_entry(MISSING_KEYWORDS_HISTORY)
    for bug_id, update in MISSING_KEYWORDS_INITIAL.items():
        expected[bug_id].extend(update)

    result = history_updater.new_bugs_history(MISSING_KEYWORDS_BUGS)

    assert result == expected


def test_missing_initial_add():
    keyword_map = {
        "added": {
            "webcompat:needs-sitepatch": [
                datetime(2024, 6, 15, 16, 34, 22, tzinfo=timezone.utc)
            ],
            "webcompat:needs-diagnosis": [
                datetime(2024, 7, 11, 16, 34, 22, tzinfo=timezone.utc),
                datetime(2024, 12, 11, 16, 34, 22, tzinfo=timezone.utc),
            ],
        },
        "removed": {
            "webcompat:needs-diagnosis": [
                datetime(2024, 6, 11, 16, 34, 22, tzinfo=timezone.utc),
                datetime(2024, 9, 11, 16, 34, 22, tzinfo=timezone.utc),
            ],
            "webcompat:needs-sitepatch": [
                datetime(2024, 7, 14, 16, 34, 22, tzinfo=timezone.utc)
            ],
        },
    }

    property_histories = defaultdict(PropertyHistory)
    for action, items in keyword_map.items():
        for keyword, change_times in items.items():
            for change_time in change_times:
                property_histories[keyword].add(change_time, action)

    assert property_histories["webcompat:needs-diagnosis"].missing_initial_add()
    assert not property_histories["webcompat:needs-sitepatch"].missing_initial_add()
    removed_first = PropertyHistory()
    removed_first.add(datetime(2024, 7, 14, 16, 34, 22, tzinfo=timezone.utc), "removed")
    assert removed_first.missing_initial_add()
    added_first = PropertyHistory()
    added_first.add(datetime(2024, 7, 14, 16, 34, 22, tzinfo=timezone.utc), "added")
    assert not added_first.missing_initial_add()
    empty_history = PropertyHistory()
    assert empty_history.missing_initial_add()


@patch("webcompat_kb.bugzilla.BugHistoryUpdater.bugzilla_fetch_history")
def test_existing_bugs_history(mock_bugzilla_fetch_history, history_updater):
    mock_bugzilla_fetch_history.return_value = (
        history_updater.bugzilla_to_history_entry(MISSING_KEYWORDS_HISTORY)
    )

    result = history_updater.existing_bugs_history(
        MISSING_KEYWORDS_BUGS, datetime(2020, 1, 1, tzinfo=timezone.utc)
    )

    expected = to_history_entry(
        [
            {
                "number": 1898563,
                "who": "name@example.com",
                "change_time": datetime.fromisoformat("2024-05-27T15:10:10Z"),
                "changes": [
                    {
                        "added": "@@ -1 +1,4 @@\n-\n+platform:windows,mac,linux,android\r\n+impact:blocked\r\n+configuration:general\r\n+affects:all\n",  # noqa
                        "field_name": "cf_user_story",
                        "removed": "",
                    },
                ],
            },
            {
                "number": 1896383,
                "who": "someone@example.com",
                "change_time": datetime.fromisoformat("2024-05-13T16:03:18Z"),
                "changes": [
                    {
                        "field_name": "cf_user_story",
                        "added": "@@ -1 +1,4 @@\n-\n+platform:windows,mac,linux\r\n+impact:site-broken\r\n+configuration:general\r\n+affects:all\n",  # noqa
                        "removed": "",
                    },
                    {
                        "removed": "",
                        "field_name": "keywords",
                        "added": "webcompat:needs-diagnosis",
                    },
                ],
            },
            {
                "number": 1896383,
                "who": "someone@example.com",
                "change_time": datetime.fromisoformat("2024-05-27T15:07:33Z"),
                "changes": [
                    {
                        "removed": "webcompat:needs-diagnosis",
                        "field_name": "keywords",
                        "added": "webcompat:needs-sitepatch",
                    },
                ],
            },
            {
                "number": 1896383,
                "who": "someone@example.com",
                "change_time": datetime.fromisoformat("2024-06-11T16:34:22Z"),
                "changes": [
                    {
                        "field_name": "keywords",
                        "added": "webcompat:sitepatch-applied",
                        "removed": "webcompat:needs-sitepatch",
                    }
                ],
            },
            {
                "number": 222222,
                "who": "someone@example.com",
                "change_time": datetime.fromisoformat("2024-06-11T16:34:22Z"),
                "changes": [
                    {
                        "field_name": "keywords",
                        "added": "",
                        "removed": "webcompat:needs-diagnosis",
                    }
                ],
            },
        ]
    )

    assert result == expected


@patch("webcompat_kb.bugzilla.BugHistoryUpdater.bugzilla_fetch_history")
def test_existing_bugs_history_filter_updated(
    mock_bugzilla_fetch_history, history_updater
):
    mock_bugzilla_fetch_history.return_value = (
        history_updater.bugzilla_to_history_entry(MISSING_KEYWORDS_HISTORY)
    )

    result = history_updater.existing_bugs_history(
        MISSING_KEYWORDS_BUGS, datetime(2024, 5, 28, tzinfo=timezone.utc)
    )

    expected = to_history_entry(
        [
            {
                "number": 1896383,
                "who": "someone@example.com",
                "change_time": datetime.fromisoformat("2024-06-11T16:34:22Z"),
                "changes": [
                    {
                        "field_name": "keywords",
                        "added": "webcompat:sitepatch-applied",
                        "removed": "webcompat:needs-sitepatch",
                    }
                ],
            },
            {
                "number": 222222,
                "who": "someone@example.com",
                "change_time": datetime.fromisoformat("2024-06-11T16:34:22Z"),
                "changes": [
                    {
                        "field_name": "keywords",
                        "added": "",
                        "removed": "webcompat:needs-diagnosis",
                    }
                ],
            },
        ]
    )

    assert result == expected


def test_missing_records(history_updater):
    initial_history = history_updater.bugzilla_to_history_entry(
        MISSING_KEYWORDS_HISTORY
    )
    new_history = {key: value[:] for key, value in initial_history.items()}
    for bug_id, update in MISSING_KEYWORDS_INITIAL.items():
        new_history[bug_id].extend(update)

    expected = MISSING_KEYWORDS_INITIAL

    result = history_updater.missing_records(initial_history, new_history)

    assert result == expected


@pytest.mark.parametrize(
    "input,expected",
    [
        ("", {}),
        (None, {}),
        ("key:value", {"key": "value"}),
        ("key1:value1\nkey2:value2", {"key1": "value1", "key2": "value2"}),
        ("key:value1\r\nkey:value2", {"key": ["value1", "value2"]}),
        (
            "key1:value1\r\nkey2:value2\nkey3:value3",
            {"key1": "value1", "key2": "value2", "key3": "value3"},
        ),
        ("\n\n", {}),
        (
            "platform:linux\r\nimpact:feature-broken\r\naffects:some",
            {
                "platform": "linux",
                "impact": "feature-broken",
                "affects": "some",
            },
        ),
        (
            "url:http://chatgpt-tokenizer.com/*\r\nurl:excalidraw.com/*\r\nurl:godbolt.org/*\r\nurl:youwouldntsteala.website/*\r\nurl:yandex.ru/images/*",
            {
                "url": [
                    "http://chatgpt-tokenizer.com/*",
                    "excalidraw.com/*",
                    "godbolt.org/*",
                    "youwouldntsteala.website/*",
                    "yandex.ru/images/*",
                ]
            },
        ),
    ],
)
def test_parse_user_story(input, expected):
    assert parse_user_story(input) == expected


@pytest.mark.parametrize(
    "input_data, test_fields",
    [
        (
            {},
            {
                "severity": None,
                "priority": None,
                "creation_time": datetime.fromisoformat("2024-03-21T16:40:27Z"),
                "assigned_to": None,
                "last_change_time": datetime.fromisoformat("2024-03-22T16:40:27Z"),
                "webcompat_priority": None,
                "webcompat_score": None,
            },
        ),
        ({"assigned_to": "test@example.org"}, {"assigned_to": "test@example.org"}),
        ({"priority": "P1"}, {"priority": 1}),
        ({"severity": "blocker"}, {"severity": 1}),
        ({"severity": "minor"}, {"severity": 4}),
        ({"severity": "S1"}, {"severity": 1}),
        ({"severity": "N/A"}, {"severity": None}),
        ({"cf_webcompat_priority": "P1"}, {"webcompat_priority": "P1"}),
        ({"cf_webcompat_priority": "?"}, {"webcompat_priority": "?"}),
        ({"cf_webcompat_score": "10"}, {"webcompat_score": 10}),
    ],
)
def test_from_bugzilla(input_data, test_fields):
    bug_data = {
        "id": 1,
        "alias": None,
        "summary": "Example",
        "status": "NEW",
        "resolution": "",
        "product": "Web Compatibility",
        "component": "Test",
        "see_also": [],
        "depends_on": [],
        "blocks": [],
        "priority": "--",
        "severity": "--",
        "creation_time": "2024-03-21T16:40:27Z",
        "assigned_to": "nobody@mozilla.org",
        "keywords": [],
        "url": "https://example.test",
        "cf_user_story": "",
        "last_resolved": None,
        "last_change_time": "2024-03-22T16:40:27Z",
        "whiteboard": "",
        "creator": "nobody@mozilla.org",
        "cf_webcompat_priority": "---",
        "cf_webcompat_score": "---",
        "cf_size_estimate": "---",
    }
    bug_data.update(input_data)
    bugzilla_bug = bugdantic.bugzilla.Bug.model_validate(bug_data)
    bug = Bug.from_bugzilla(bugzilla_bug)

    for attr, expected in test_fields.items():
        assert getattr(bug, attr) == expected


def test_read_write_data():
    site_reports, etp_reports, kb_bugs, platform_bugs = group_bugs(SAMPLE_ALL_BUGS)
    with tempfile.NamedTemporaryFile("w") as f:
        write_bugs(
            f.name,
            SAMPLE_ALL_BUGS,
            site_reports,
            etp_reports,
            kb_bugs,
            platform_bugs,
            [],
            [],
        )

        assert load_bugs(None, None, f.name, None) == SAMPLE_ALL_BUGS


@pytest.mark.parametrize(
    "input_query, expected_query",
    [
        ({}, {"f1": "delta_ts", "o1": "greaterthaneq", "v1": "2025-05-10 00:30"}),
        (
            {"product": "Web Compatibility"},
            {
                "product": "Web Compatibility",
                "f1": "delta_ts",
                "o1": "greaterthaneq",
                "v1": "2025-05-10 00:30",
            },
        ),
        (
            {"f1": "product", "o1": "equals", "v1": "Web Compatibility"},
            {
                "f1": "product",
                "o1": "equals",
                "v1": "Web Compatibility",
                "f2": "delta_ts",
                "o2": "greaterthaneq",
                "v2": "2025-05-10 00:30",
            },
        ),
        (
            {
                "f1": "OP",
                "j1": "OR",
                "f2": "product",
                "o2": "equals",
                "v2": "Web Compatibility",
                "f3": "keywords",
                "o3": "substring",
                "v3": "webcompat:site-report",
                "f4": "CP",
            },
            {
                "f1": "OP",
                "j1": "OR",
                "f2": "product",
                "o2": "equals",
                "v2": "Web Compatibility",
                "f3": "keywords",
                "o3": "substring",
                "v3": "webcompat:site-report",
                "f4": "CP",
                "f5": "delta_ts",
                "o5": "greaterthaneq",
                "v5": "2025-05-10 00:30",
            },
        ),
    ],
)
def test_add_datetime_limit(input_query, expected_query):
    assert (
        add_datetime_limit(input_query, datetime(2025, 5, 10, 0, 30)) == expected_query
    )


def test_add_datetime_limit_error():
    with pytest.raises(ValueError):
        add_datetime_limit(
            {
                "test": "Example",
                "j_top": "OR",
                "f1": "product",
                "o1": "equals",
                "v1": "Web Compatibility",
            },
            datetime(2025, 5, 10, 0, 30),
        )


def _bug_defaults():
    return {
        "alias": None,
        "summary": "Test bug",
        "status": "NEW",
        "resolution": "",
        "product": "Web Compatibility",
        "component": "Site Reports",
        "creator": "nobody@mozilla.org",
        "see_also": [],
        "priority": None,
        "severity": None,
        "creation_time": datetime(2025, 1, 1),
        "assigned_to": None,
        "keywords": [],
        "url": "",
        "user_story": "",
        "last_resolved": None,
        "last_change_time": datetime(2025, 10, 1),
        "size_estimate": None,
        "whiteboard": "",
        "webcompat_priority": None,
        "webcompat_score": None,
    }


def test_get_recursive_dependencies_simple():
    """Test simple one-level dependency chain"""
    bugs = {
        1000: Bug(id=1000, depends_on=[2000], blocks=[], **_bug_defaults()),
        2000: Bug(id=2000, depends_on=[], blocks=[], **_bug_defaults()),
        3000: Bug(id=3000, depends_on=[], blocks=[], **_bug_defaults()),
    }

    result = get_recursive_dependencies({1000}, bugs)

    assert result == {2000}


def test_get_recursive_dependencies_multiple():
    """Test multiple starting bugs"""
    bugs = {
        1000: Bug(id=1000, depends_on=[3000], blocks=[], **_bug_defaults()),
        2000: Bug(id=2000, depends_on=[3000], blocks=[], **_bug_defaults()),
        3000: Bug(id=3000, depends_on=[4000], blocks=[], **_bug_defaults()),
        4000: Bug(id=4000, depends_on=[], blocks=[], **_bug_defaults()),
    }

    result = get_recursive_dependencies({1000, 2000}, bugs)

    assert result == {3000, 4000}


def test_get_recursive_dependencies_blocks():
    """Test that blocks relationships are also traversed"""
    bugs = {
        1000: Bug(id=1000, depends_on=[], blocks=[2000], **_bug_defaults()),
        2000: Bug(id=2000, depends_on=[], blocks=[3000], **_bug_defaults()),
        3000: Bug(id=3000, depends_on=[], blocks=[], **_bug_defaults()),
    }

    result = get_recursive_dependencies({1000}, bugs)

    assert result == {2000, 3000}


def test_get_recursive_dependencies_mixed():
    """Test mix of depends_on and blocks relationships"""
    bugs = {
        1000: Bug(id=1000, depends_on=[2000], blocks=[3000], **_bug_defaults()),
        2000: Bug(id=2000, depends_on=[4000], blocks=[], **_bug_defaults()),
        3000: Bug(id=3000, depends_on=[], blocks=[5000], **_bug_defaults()),
        4000: Bug(id=4000, depends_on=[], blocks=[], **_bug_defaults()),
        5000: Bug(id=5000, depends_on=[], blocks=[], **_bug_defaults()),
    }

    result = get_recursive_dependencies({1000}, bugs)

    assert result == {2000, 3000, 4000, 5000}


def test_get_recursive_dependencies_circular():
    """Test circular dependencies don't cause infinite loop"""
    bugs = {
        1000: Bug(id=1000, depends_on=[2000], blocks=[], **_bug_defaults()),
        2000: Bug(id=2000, depends_on=[3000], blocks=[], **_bug_defaults()),
        3000: Bug(id=3000, depends_on=[1000], blocks=[], **_bug_defaults()),
    }

    result = get_recursive_dependencies({1000}, bugs)

    assert result == {2000, 3000}


def test_get_recursive_dependencies_missing_bug():
    """Test that missing bugs in dependency chain are handled"""
    bugs = {
        1000: Bug(id=1000, depends_on=[2000, 9999], blocks=[], **_bug_defaults()),
        2000: Bug(id=2000, depends_on=[], blocks=[], **_bug_defaults()),
    }

    result = get_recursive_dependencies({1000}, bugs)

    assert result == {2000, 9999}


def test_get_recursive_dependencies_empty():
    """Test with no dependencies"""
    bugs = {
        1000: Bug(id=1000, depends_on=[], blocks=[], **_bug_defaults()),
    }

    result = get_recursive_dependencies({1000}, bugs)

    assert result == set()
