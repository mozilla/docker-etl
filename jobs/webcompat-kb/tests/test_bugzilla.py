from datetime import datetime, timezone
from unittest.mock import Mock, patch
from google.cloud.bigquery import Row

import pytest

from webcompat_kb.base import get_client
from webcompat_kb.bugzilla import BugzillaToBigQuery
from webcompat_kb.bugzilla import extract_int_from_field
from webcompat_kb.bugzilla import parse_string_to_json
from webcompat_kb.bugzilla import parse_datetime_str
from webcompat_kb.bugzilla import RELATION_CONFIG, LINK_FIELDS, ETP_RELATION_CONFIG

SAMPLE_BUGS = {
    item["id"]: item
    for item in [
        {
            "see_also": [
                "https://github.com/webcompat/web-bugs/issues/13503",
                "https://github.com/webcompat/web-bugs/issues/91682",
                "https://bugzilla.mozilla.org/show_bug.cgi?id=1633399",
                "https://bugzilla.mozilla.org/show_bug.cgi?id=1735227",
                "https://bugzilla.mozilla.org/show_bug.cgi?id=1739489",
                "https://bugzilla.mozilla.org/show_bug.cgi?id=1739791",
                "https://github.com/webcompat/web-bugs/issues/109064",
                "https://github.com/mozilla-extensions/webcompat-addon/blob/5b391018e847a1eb30eba4784c86acd1c638ed26/src/injections/js/bug1739489-draftjs-beforeinput.js",  # noqa
                "https://github.com/webcompat/web-bugs/issues/112848",
                "https://github.com/webcompat/web-bugs/issues/117039",
            ],
            "cf_user_story": "url:cmcreg.bancosantander.es/*\r\nurl:new.reddit.com/*\r\nurl:web.whatsapp.com/*\r\nurl:facebook.com/*\r\nurl:twitter.com/*\r\nurl:reddit.com/*\r\nurl:mobilevikings.be/*\r\nurl:book.ersthelfer.tv/*",  # noqa
            "severity": "--",
            "priority": "--",
            "depends_on": [903746],
            "component": "Knowledge Base",
            "product": "Web Compatibility",
            "resolution": "",
            "status": "NEW",
            "blocks": [],
            "id": 1835339,
            "summary": "Missing implementation of textinput event",
            "assigned_to": "test@example.org",
            "creation_time": "2000-07-25T13:50:04Z",
            "keywords": [],
            "url": "",
            "whiteboard": "",
            "cf_webcompat_priority": "---",
            "cf_webcompat_score": "---",
        },
        {
            "component": "Knowledge Base",
            "product": "Web Compatibility",
            "depends_on": [],
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
            "summary": "Sites breaking due to the lack of WebUSB support",
            "id": 1835416,
            "blocks": [],
            "resolution": "",
            "priority": "--",
            "severity": "--",
            "cf_user_story": "url:webminidisc.com/*\r\nurl:app.webadb.com/*\r\nurl:www.numworks.com/*\r\nurl:webadb.github.io/*\r\nurl:www.stemplayer.com/*\r\nurl:wootility.io/*\r\nurl:python.microbit.org/*\r\nurl:flash.android.com/*",  # noqa
            "status": "NEW",
            "assigned_to": "nobody@mozilla.org",
            "creation_time": "2000-07-25T13:50:04Z",
            "keywords": [],
            "url": "",
            "whiteboard": "",
            "cf_webcompat_priority": "---",
            "cf_webcompat_score": "---",
        },
        {
            "component": "Knowledge Base",
            "product": "Web Compatibility",
            "depends_on": [555555],
            "see_also": [
                "https://crbug.com/606208",
                "https://github.com/whatwg/html/issues/1896",
                "https://w3c.github.io/trusted-types/dist/spec/",
                "https://github.com/webcompat/web-bugs/issues/124877",
                "https://github.com/mozilla/standards-positions/issues/20",
                "https://github.com/WebKit/standards-positions/issues/186",
            ],
            "summary": "Test bug",
            "id": 111111,
            "blocks": [222222, 1734557],
            "resolution": "",
            "priority": "--",
            "severity": "--",
            "cf_user_story": "",
            "status": "NEW",
            "assigned_to": "nobody@mozilla.org",
            "creation_time": "2000-07-25T13:50:04Z",
            "keywords": [],
            "url": "",
            "whiteboard": "",
            "cf_webcompat_priority": "---",
            "cf_webcompat_score": "---",
        },
    ]
}

SAMPLE_CORE_BUGS = {
    item["id"]: item
    for item in [
        {
            "id": 903746,
            "severity": "--",
            "priority": "--",
            "cf_user_story": "",
            "depends_on": [],
            "status": "UNCONFIRMED",
            "product": "Core",
            "blocks": [1754236, 1835339],
            "component": "DOM: Events",
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
            "resolution": "",
            "summary": "Missing textinput event",
            "assigned_to": "nobody@mozilla.org",
        },
        {
            "id": 555555,
            "severity": "--",
            "priority": "--",
            "cf_user_story": "",
            "depends_on": [],
            "status": "UNCONFIRMED",
            "product": "Core",
            "blocks": [],
            "component": "Test",
            "see_also": ["https://mozilla.github.io/standards-positions/#testposition"],
            "resolution": "",
            "summary": "Test Core bug",
            "assigned_to": "nobody@mozilla.org",
        },
    ]
}

SAMPLE_BREAKAGE_BUGS = {
    item["id"]: item
    for item in [
        {
            "id": 1734557,
            "product": "Web Compatibility",
            "cf_user_story": "url:angusnicneven.com/*",
            "blocks": [],
            "status": "ASSIGNED",
            "summary": "Javascript causes infinite scroll because event.path is undefined",
            "resolution": "",
            "depends_on": [111111],
            "see_also": [],
            "component": "Desktop",
            "severity": "--",
            "priority": "--",
            "assigned_to": "nobody@mozilla.org",
            "cf_webcompat_priority": "---",
            "cf_webcompat_score": "---",
        },
        {
            "id": 222222,
            "product": "Web Compatibility",
            "cf_user_story": "url:example.com/*",
            "blocks": [],
            "status": "ASSIGNED",
            "summary": "Test breakage bug",
            "resolution": "",
            "depends_on": [111111],
            "see_also": [],
            "component": "Desktop",
            "severity": "--",
            "priority": "--",
            "assigned_to": "nobody@mozilla.org",
            "cf_webcompat_priority": "---",
            "cf_webcompat_score": "---",
        },
        {
            "whiteboard": "",
            "see_also": [],
            "severity": "S3",
            "product": "Core",
            "depends_on": [999999],
            "summary": "Example core site report and platform bug",
            "resolution": "",
            "last_change_time": "2024-05-27T15:07:03Z",
            "keywords": ["webcompat:platform-bug", "webcompat:site-report"],
            "priority": "P3",
            "creation_time": "2024-03-21T16:40:27Z",
            "cf_user_story": "",
            "status": "NEW",
            "blocks": [],
            "url": "",
            "cf_last_resolved": None,
            "component": "JavaScript Engine",
            "id": 444444,
            "assigned_to": "nobody@mozilla.org",
            "cf_webcompat_priority": "P3",
            "cf_webcompat_score": "2",
        },
    ]
}

SAMPLE_ETP_BUGS = {
    item["id"]: item
    for item in [
        {
            "url": "https://gothamist.com/",
            "summary": "gothamist.com - The comments are not displayed with ETP set to Strict",
            "id": 1910548,
            "keywords": ["priv-webcompat", "webcompat:site-report"],
            "component": "Privacy: Site Reports",
            "resolution": "",
            "blocks": [1101005],
            "depends_on": [1875061],
            "creation_time": "2024-07-30T07:37:28Z",
            "see_also": ["https://github.com/webcompat/web-bugs/issues/139647"],
            "product": "Web Compatibility",
            "status": "NEW",
            "cf_webcompat_priority": "---",
            "cf_webcompat_score": "---",
        },
        {
            "see_also": ["https://github.com/webcompat/web-bugs/issues/142250"],
            "id": 1921943,
            "summary": "my.farys.be - Login option is missing with ETP set to STRICT",
            "product": "Web Compatibility",
            "keywords": [
                "priv-webcompat",
                "webcompat:platform-bug",
                "webcompat:site-report",
            ],
            "status": "NEW",
            "resolution": "",
            "component": "Privacy: Site Reports",
            "blocks": [],
            "depends_on": [1101005, 1797458],
            "creation_time": "2024-10-01T08:50:58Z",
            "url": "https://my.farys.be/myfarys/",
            "cf_webcompat_priority": "---",
            "cf_webcompat_score": "---",
        },
        {
            "see_also": [],
            "summary": "ryanair.com - The form to start a chat does not load with ETP set to STRICT",
            "id": 1928102,
            "product": "Web Compatibility",
            "status": "NEW",
            "keywords": ["webcompat:site-report"],
            "blocks": [],
            "component": "Privacy: Site Reports",
            "resolution": "",
            "depends_on": [1101005, 1122334],
            "url": "https://www.ryanair.com/gb/en/lp/chat",
            "creation_time": "2024-10-30T15:04:41Z",
            "cf_webcompat_priority": "---",
            "cf_webcompat_score": "---",
        },
    ]
}

SAMPLE_ETP_DEPENDENCIES_BUGS = {
    item["id"]: item
    for item in [
        {
            "blocks": [
                1526695,
                1903311,
                1903317,
                1903340,
                1903345,
            ],
            "resolution": "",
            "status": "NEW",
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
            "creation_time": "2014-11-18T16:11:29Z",
            "summary": "[meta] ETP Strict mode or Private Browsing mode tracking protection breakage",
            "url": "",
            "id": 1101005,
            "component": "Privacy: Anti-Tracking",
            "depends_on": [
                1400025,
                1446243,
                1465962,
                1470298,
                1470301,
                1486425,
                1627322,
            ],
            "keywords": ["meta", "webcompat:platform-bug"],
            "product": "Core",
        },
        {
            "status": "NEW",
            "resolution": "",
            "blocks": [1101005, 1773684, 1921943],
            "summary": "[meta] Email Tracking Breakage",
            "creation_time": "2022-10-26T09:33:25Z",
            "see_also": [],
            "component": "Privacy: Anti-Tracking",
            "url": "",
            "id": 1797458,
            "product": "Core",
            "keywords": ["meta"],
            "depends_on": [
                1796560,
                1799094,
                1799618,
                1800007,
                1803127,
            ],
        },
        {
            "status": "NEW",
            "resolution": "",
            "creation_time": "2024-01-17T13:40:16Z",
            "see_also": [
                "https://bugzilla.mozilla.org/show_bug.cgi?id=1869326",
                "https://bugzilla.mozilla.org/show_bug.cgi?id=1872855",
                "https://bugzilla.mozilla.org/show_bug.cgi?id=1874855",
                "https://bugzilla.mozilla.org/show_bug.cgi?id=1878855",
                "https://bugzilla.mozilla.org/show_bug.cgi?id=1428122",
                "https://bugzilla.mozilla.org/show_bug.cgi?id=1892176",
            ],
            "url": "",
            "keywords": ["meta"],
            "product": "Core",
            "depends_on": [1884676, 1906418, 1894615],
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
            "summary": "[meta] ETP breakage for webpages that have Disqus comment section",
            "component": "Privacy: Anti-Tracking",
            "id": 1875061,
        },
        {
            "status": "NEW",
            "resolution": "",
            "creation_time": "2024-01-17T13:40:16Z",
            "see_also": [],
            "url": "",
            "keywords": [],
            "product": "Core",
            "depends_on": [444444, 555555],
            "blocks": [],
            "summary": "Sample non meta ETP dependency",
            "component": "Privacy: Anti-Tracking",
            "id": 1122334,
        },
    ]
}

SAMPLE_CORE_AS_KB_BUGS = {
    item["id"]: item
    for item in [
        {
            "whiteboard": "",
            "see_also": ["https://bugzilla.mozilla.org/show_bug.cgi?id=1740472"],
            "severity": "S3",
            "product": "Core",
            "depends_on": [],
            "summary": "Consider adding support for Error.captureStackTrace",
            "resolution": "",
            "last_change_time": "2024-05-27T15:07:03Z",
            "keywords": ["parity-chrome", "parity-safari", "webcompat:platform-bug"],
            "priority": "P3",
            "creation_time": "2024-03-21T16:40:27Z",
            "cf_user_story": "",
            "status": "NEW",
            "blocks": [1539848, 1729514, 1896383],
            "url": "",
            "cf_last_resolved": None,
            "component": "JavaScript Engine",
            "id": 1886820,
            "assigned_to": "nobody@mozilla.org",
        },
        {
            "depends_on": [1896672],
            "product": "Core",
            "severity": "S2",
            "see_also": ["https://bugzilla.mozilla.org/show_bug.cgi?id=1863217"],
            "whiteboard": "",
            "resolution": "",
            "summary": "Popup blocker is too strict when opening new windows",
            "status": "NEW",
            "cf_user_story": "",
            "priority": "P3",
            "creation_time": "2024-04-30T14:04:23Z",
            "keywords": ["webcompat:platform-bug"],
            "last_change_time": "2024-05-14T15:19:21Z",
            "id": 1894244,
            "component": "DOM: Window and Location",
            "cf_last_resolved": None,
            "url": "",
            "blocks": [1656444, 1835339, 222222],
            "assigned_to": "nobody@mozilla.org",
        },
        {
            "whiteboard": "",
            "see_also": [],
            "severity": "S3",
            "product": "Core",
            "depends_on": [999999],
            "summary": "Example core site report and platform bug",
            "resolution": "",
            "last_change_time": "2024-05-27T15:07:03Z",
            "keywords": ["webcompat:platform-bug", "webcompat:site-report"],
            "priority": "P3",
            "creation_time": "2024-03-21T16:40:27Z",
            "cf_user_story": "",
            "status": "NEW",
            "blocks": [],
            "url": "",
            "cf_last_resolved": None,
            "component": "JavaScript Engine",
            "id": 444444,
            "assigned_to": "nobody@mozilla.org",
        },
    ]
}

SAMPLE_HISTORY = [
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
        "history": [{"changes": [], "when": "2023-07-01T12:00:00Z", "who": "example"}],
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

MISSING_KEYWORDS_HISTORY = [
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
                    {"removed": "", "field_name": "cc", "added": "someone@example.com"}
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
                    {"added": "someone@example.com", "field_name": "cc", "removed": ""}
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

MISSING_KEYWORDS_BUGS = {
    item["id"]: item
    for item in [
        {
            "creator": "name@example.com",
            "see_also": ["https://github.com/webcompat/web-bugs/issues/135636"],
            "id": 1898563,
            "component": "Site Reports",
            "keywords": ["webcompat:needs-diagnosis", "webcompat:needs-sitepatch"],
            "resolution": "",
            "summary": "mylotto.co.nz - Website not supported on Firefox",
            "product": "Web Compatibility",
            "creator_detail": {
                "real_name": "Sample",
                "id": 111111,
                "nick": "sample",
                "email": "name@example.com",
                "name": "name@example.com",
            },
            "status": "NEW",
            "depends_on": [1886128],
            "creation_time": "2024-05-23T16:40:29Z",
        },
        {
            "component": "Site Reports",
            "keywords": ["webcompat:sitepatch-applied"],
            "see_also": ["https://github.com/webcompat/web-bugs/issues/136865"],
            "id": 1896383,
            "creator": "name@example.com",
            "depends_on": [1886820, 1876368],
            "status": "NEW",
            "product": "Web Compatibility",
            "creator_detail": {
                "name": "name@example.com",
                "id": 111111,
                "email": "name@example.com",
                "nick": "sample",
                "real_name": "Sample",
            },
            "resolution": "",
            "summary": "www.unimarc.cl - Buttons not working",
            "creation_time": "2024-05-13T13:02:11Z",
        },
        {
            "id": 222222,
            "product": "Web Compatibility",
            "blocks": [],
            "status": "ASSIGNED",
            "summary": "Test breakage bug",
            "resolution": "",
            "depends_on": [111111],
            "see_also": [],
            "component": "Desktop",
            "severity": "--",
            "priority": "--",
            "creator_detail": {
                "name": "name@example.com",
                "id": 111111,
                "email": "name@example.com",
                "nick": "sample",
                "real_name": "Sample",
            },
            "creator": "name@example.com",
            "creation_time": "2024-05-13T13:02:11Z",
            "keywords": [],
        },
    ]
}

REMOVED_READDED_BUGS = {
    item["id"]: item
    for item in [
        {
            "id": 333333,
            "product": "Web Compatibility",
            "blocks": [],
            "status": "ASSIGNED",
            "summary": "Test breakage bug",
            "resolution": "",
            "depends_on": [111111],
            "see_also": [],
            "component": "Desktop",
            "severity": "--",
            "priority": "--",
            "creator_detail": {
                "name": "name@example.com",
                "id": 111111,
                "email": "name@example.com",
                "nick": "sample",
                "real_name": "Sample",
            },
            "creator": "name@example.com",
            "creation_time": "2024-05-13T13:02:11Z",
            "keywords": ["webcompat:needs-diagnosis"],
        }
    ]
}

REMOVED_READDED_HISTORY = [
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

KEYWORDS_AND_STATUS = [
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


@pytest.fixture(scope="module")
@patch("webcompat_kb.base.google.auth.default")
@patch("webcompat_kb.base.bigquery.Client")
def bz(mock_bq, mock_auth_default):
    mock_credentials = Mock()
    mock_project_id = "placeholder_id"
    mock_auth_default.return_value = (mock_credentials, mock_project_id)
    mock_bq.return_value = Mock()

    mock_bq.return_value = Mock()
    client = get_client(mock_project_id)
    return BugzillaToBigQuery(
        client=client,
        bq_dataset_id="placeholder_dataset",
        bugzilla_api_key="placeholder_key",
        write=False,
        include_history=True,
    )


def test_extract_int_from_field():
    field = extract_int_from_field("P3")
    assert field == 3

    field = extract_int_from_field("critical")
    assert field == 1

    field = extract_int_from_field("--")
    assert field is None

    field = extract_int_from_field("N/A")
    assert field is None

    field = extract_int_from_field("")
    assert field is None

    field = extract_int_from_field(None)
    assert field is None


def test_process_relations_with_no_bugs(bz):
    result = bz.process_relations({}, RELATION_CONFIG)
    expected = ({}, {"core": set(), "breakage": set()})
    assert result == expected


def test_process_relations(bz):
    bugs, ids = bz.process_relations(SAMPLE_BUGS, RELATION_CONFIG)
    expected_processed_bugs = {
        1835339: {
            "core_bugs": [903746],
            "breakage_reports": [],
            "interventions": [
                "https://github.com/mozilla-extensions/webcompat-addon/blob/5b391018e847a1eb30eba4784c86acd1c638ed26/src/injections/js/bug1739489-draftjs-beforeinput.js"  # noqa
            ],
            "other_browser_issues": [],
            "standards_issues": [],
            "standards_positions": [],
        },
        1835416: {
            "core_bugs": [],
            "breakage_reports": [],
            "interventions": [],
            "other_browser_issues": [],
            "standards_issues": [],
            "standards_positions": [
                "https://mozilla.github.io/standards-positions/#webusb"
            ],
        },
        111111: {
            "core_bugs": [555555],
            "breakage_reports": [222222, 1734557],
            "interventions": [],
            "other_browser_issues": ["https://crbug.com/606208"],
            "standards_issues": ["https://github.com/whatwg/html/issues/1896"],
            "standards_positions": [
                "https://github.com/mozilla/standards-positions/issues/20",
                "https://github.com/WebKit/standards-positions/issues/186",
            ],
        },
    }

    expected_bug_ids = {
        "core": {903746, 555555},
        "breakage": {222222, 1734557},
    }

    assert bugs == expected_processed_bugs
    assert ids == expected_bug_ids


def test_add_breakage_kb_entries(bz):
    kb_bugs = {
        bug_id: bug
        for bug_id, bug in SAMPLE_BREAKAGE_BUGS.items()
        if bug["product"] != "Web Compatibility"
    }
    kb_data, kb_dep_ids = bz.process_relations(kb_bugs, RELATION_CONFIG)
    assert set(kb_data.keys()) == set(kb_bugs.keys())
    assert kb_dep_ids["breakage"] == set()

    bz.add_kb_entry_breakage(kb_data, kb_dep_ids, SAMPLE_BREAKAGE_BUGS)
    assert kb_data[444444]["breakage_reports"] == [444444]
    assert kb_dep_ids["breakage"] == set(kb_bugs.keys())


def test_relations(bz):
    bugs, _ = bz.process_relations(SAMPLE_BUGS, RELATION_CONFIG)
    relations = bz.build_relations(bugs, RELATION_CONFIG)

    assert relations["core_bugs"] == [
        {"knowledge_base_bug": 1835339, "core_bug": 903746},
        {"knowledge_base_bug": 111111, "core_bug": 555555},
    ]

    assert relations["breakage_reports"] == [
        {"knowledge_base_bug": 111111, "breakage_bug": 222222},
        {"knowledge_base_bug": 111111, "breakage_bug": 1734557},
    ]

    assert relations["interventions"] == [
        {
            "knowledge_base_bug": 1835339,
            "code_url": "https://github.com/mozilla-extensions/webcompat-addon/blob/5b391018e847a1eb30eba4784c86acd1c638ed26/src/injections/js/bug1739489-draftjs-beforeinput.js",  # noqa
        }
    ]
    assert relations["other_browser_issues"] == [
        {"knowledge_base_bug": 111111, "issue_url": "https://crbug.com/606208"}
    ]
    assert relations["standards_issues"] == [
        {
            "knowledge_base_bug": 111111,
            "issue_url": "https://github.com/whatwg/html/issues/1896",
        }
    ]
    assert relations["standards_positions"] == [
        {
            "knowledge_base_bug": 1835416,
            "discussion_url": "https://mozilla.github.io/standards-positions/#webusb",  # noqa
        },
        {
            "knowledge_base_bug": 111111,
            "discussion_url": "https://github.com/mozilla/standards-positions/issues/20",  # noqa
        },
        {
            "knowledge_base_bug": 111111,
            "discussion_url": "https://github.com/WebKit/standards-positions/issues/186",  # noqa
        },
    ]


def test_add_links(bz):
    bugs, _ = bz.process_relations(SAMPLE_BUGS, RELATION_CONFIG)
    core_bugs, _ = bz.process_relations(
        SAMPLE_CORE_BUGS, {key: RELATION_CONFIG[key] for key in LINK_FIELDS}
    )

    result = bz.add_links(bugs, core_bugs)

    assert result[1835339]["standards_issues"] == [
        "https://github.com/w3c/uievents/issues/353"
    ]
    assert result[111111]["standards_positions"] == [
        "https://github.com/mozilla/standards-positions/issues/20",
        "https://github.com/WebKit/standards-positions/issues/186",
        "https://mozilla.github.io/standards-positions/#testposition",
    ]


def test_add_links_no_core(bz):
    bugs, _ = bz.process_relations(SAMPLE_BUGS, RELATION_CONFIG)
    core_bugs, _ = bz.process_relations(SAMPLE_CORE_BUGS, RELATION_CONFIG)

    result = bz.add_links(bugs, {})

    assert result[1835339]["standards_issues"] == []
    assert result[111111]["standards_positions"] == [
        "https://github.com/mozilla/standards-positions/issues/20",
        "https://github.com/WebKit/standards-positions/issues/186",
    ]


def test_get_bugs_updated_since_last_import(bz):
    all_bugs = {
        item["id"]: item
        for item in [
            {"id": 1, "last_change_time": "2023-04-01T10:00:00Z"},
            {"id": 2, "last_change_time": "2023-04-02T11:30:00Z"},
            {"id": 3, "last_change_time": "2023-04-03T09:45:00Z"},
        ]
    }

    last_import_time = datetime(2023, 4, 2, 10, 0, tzinfo=timezone.utc)
    expected_result = {2, 3}
    result = bz.get_bugs_updated_since_last_import(all_bugs, last_import_time)
    assert result == expected_result


def test_filter_bug_history_changes(bz):
    expected_result = [
        {
            "number": 1536482,
            "who": "example",
            "change_time": "2023-05-01T17:41:18Z",
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
            "change_time": "2023-06-01T10:00:00Z",
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
            "change_time": "2023-05-01T14:00:00Z",
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
            "change_time": "2023-08-01T14:00:00Z",
            "changes": [
                {"removed": "ASSIGNED", "field_name": "status", "added": "RESOLVED"}
            ],
        },
    ]

    result, bug_ids = bz.extract_history_fields(SAMPLE_HISTORY)
    assert result == expected_result
    assert bug_ids == {1536482, 1536483, 1536485}


def test_create_synthetic_history(bz):
    history, bug_ids = bz.extract_history_fields(MISSING_KEYWORDS_HISTORY)
    result = bz.create_synthetic_history(MISSING_KEYWORDS_BUGS, history)

    expected = [
        {
            "number": 1898563,
            "who": "name@example.com",
            "change_time": "2024-05-23T16:40:29Z",
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
            "change_time": "2024-05-13T13:02:11Z",
            "changes": [
                {
                    "added": "webcompat:needs-diagnosis",
                    "field_name": "keywords",
                    "removed": "",
                }
            ],
        },
    ]

    assert result == expected


def test_create_synthetic_history_removed_readded(bz):
    history, bug_ids = bz.extract_history_fields(REMOVED_READDED_HISTORY)
    result = bz.create_synthetic_history(REMOVED_READDED_BUGS, history)

    expected = [
        {
            "number": 333333,
            "who": "name@example.com",
            "change_time": "2024-05-13T13:02:11Z",
            "changes": [
                {
                    "added": "webcompat:needs-diagnosis",
                    "field_name": "keywords",
                    "removed": "",
                }
            ],
        }
    ]

    assert result == expected


def test_is_removed_earliest(bz):
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

    is_removed_first_diagnosis = bz.is_removed_earliest(
        keyword_map["added"]["webcompat:needs-diagnosis"],
        keyword_map["removed"]["webcompat:needs-diagnosis"],
    )

    is_removed_first_sitepatch = bz.is_removed_earliest(
        keyword_map["added"]["webcompat:needs-sitepatch"],
        keyword_map["removed"]["webcompat:needs-sitepatch"],
    )

    is_removed_first_empty_added = bz.is_removed_earliest(
        [],
        [datetime(2024, 7, 14, 16, 34, 22, tzinfo=timezone.utc)],
    )

    is_removed_first_empty_removed = bz.is_removed_earliest(
        [datetime(2024, 7, 14, 16, 34, 22, tzinfo=timezone.utc)],
        [],
    )

    is_removed_first_empty = bz.is_removed_earliest(
        [],
        [],
    )

    assert is_removed_first_diagnosis
    assert not is_removed_first_sitepatch
    assert is_removed_first_empty_added
    assert not is_removed_first_empty_removed
    assert not is_removed_first_empty


@patch("webcompat_kb.bugzilla.BugzillaToBigQuery.get_existing_history_records_by_ids")
def test_filter_only_unsaved_changes(mock_get_existing, bz):
    schema = {"number": 0, "who": 1, "change_time": 2, "changes": 3}

    mock_get_existing.return_value = [
        Row(
            (
                1896383,
                "someone@example.com",
                datetime(2024, 5, 27, 15, 7, 33, tzinfo=timezone.utc),
                [
                    {
                        "field_name": "keywords",
                        "added": "webcompat:needs-sitepatch",
                        "removed": "webcompat:needs-diagnosis",
                    }
                ],
            ),
            schema,
        ),
        Row(
            (
                1896383,
                "someone@example.com",
                datetime(2024, 6, 11, 16, 34, 22, tzinfo=timezone.utc),
                [
                    {
                        "field_name": "keywords",
                        "added": "webcompat:sitepatch-applied",
                        "removed": "webcompat:needs-sitepatch",
                    }
                ],
            ),
            schema,
        ),
    ]

    history, bug_ids = bz.extract_history_fields(MISSING_KEYWORDS_HISTORY)
    result = bz.filter_only_unsaved_changes(history, bug_ids)

    expected = [
        {
            "number": 1896383,
            "who": "someone@example.com",
            "change_time": "2024-05-13T16:03:18Z",
            "changes": [
                {
                    "removed": "",
                    "field_name": "keywords",
                    "added": "webcompat:needs-diagnosis",
                }
            ],
        },
        {
            "number": 222222,
            "who": "someone@example.com",
            "change_time": "2024-06-11T16:34:22Z",
            "changes": [
                {
                    "field_name": "keywords",
                    "added": "",
                    "removed": "webcompat:needs-diagnosis",
                }
            ],
        },
    ]

    result.sort(key=lambda item: item["number"])
    expected.sort(key=lambda item: item["number"])

    assert result == expected


@patch("webcompat_kb.bugzilla.BugzillaToBigQuery.get_existing_history_records_by_ids")
def test_filter_only_unsaved_changes_multiple_changes(mock_get_existing, bz):
    schema = {"number": 0, "who": 1, "change_time": 2, "changes": 3}

    mock_get_existing.return_value = [
        Row(
            (
                1239595,
                "someone@example.com",
                datetime(2018, 5, 2, 18, 25, 47, tzinfo=timezone.utc),
                [
                    {
                        "field_name": "keywords",
                        "added": "parity-chrome, parity-edge, parity-ie",
                        "removed": "",
                    }
                ],
            ),
            schema,
        ),
        Row(
            (
                1239595,
                "someone@example.com",
                datetime(2016, 1, 14, 14, 1, 36, tzinfo=timezone.utc),
                [{"field_name": "status", "added": "NEW", "removed": "UNCONFIRMED"}],
            ),
            schema,
        ),
        Row(
            (
                1239595,
                "someone@example.com",
                datetime(2024, 5, 16, 10, 58, 15, tzinfo=timezone.utc),
                [{"field_name": "status", "added": "RESOLVED", "removed": "NEW"}],
            ),
            schema,
        ),
    ]

    history, bug_ids = bz.extract_history_fields(KEYWORDS_AND_STATUS)
    result = bz.filter_only_unsaved_changes(history, bug_ids)
    changes = result[0]["changes"]

    expected_changes = [
        {
            "field_name": "keywords",
            "added": "webcompat:platform-bug",
            "removed": "",
        },
        {"field_name": "status", "added": "REOPENED", "removed": "RESOLVED"},
    ]

    changes.sort(key=lambda item: item["field_name"])
    expected_changes.sort(key=lambda item: item["field_name"])

    assert len(result) == 1
    assert changes == expected_changes


@patch("webcompat_kb.bugzilla.BugzillaToBigQuery.get_existing_history_records_by_ids")
def test_filter_only_unsaved_changes_empty(mock_get_existing, bz):
    mock_get_existing.return_value = []

    history, bug_ids = bz.extract_history_fields(MISSING_KEYWORDS_HISTORY)
    result = bz.filter_only_unsaved_changes(history, bug_ids)

    expected = [
        {
            "number": 1896383,
            "who": "someone@example.com",
            "change_time": "2024-05-13T16:03:18Z",
            "changes": [
                {
                    "removed": "",
                    "field_name": "keywords",
                    "added": "webcompat:needs-diagnosis",
                }
            ],
        },
        {
            "number": 1896383,
            "who": "someone@example.com",
            "change_time": "2024-05-27T15:07:33Z",
            "changes": [
                {
                    "removed": "webcompat:needs-diagnosis",
                    "field_name": "keywords",
                    "added": "webcompat:needs-sitepatch",
                }
            ],
        },
        {
            "number": 1896383,
            "who": "someone@example.com",
            "change_time": "2024-06-11T16:34:22Z",
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
            "change_time": "2024-06-11T16:34:22Z",
            "changes": [
                {
                    "field_name": "keywords",
                    "added": "",
                    "removed": "webcompat:needs-diagnosis",
                }
            ],
        },
    ]

    assert result == expected


@patch("webcompat_kb.bugzilla.BugzillaToBigQuery.get_existing_history_records_by_ids")
def test_filter_only_unsaved_changes_synthetic(mock_get_existing, bz):
    history, bug_ids = bz.extract_history_fields(MISSING_KEYWORDS_HISTORY)
    s_history = bz.create_synthetic_history(MISSING_KEYWORDS_BUGS, history)

    schema = {"number": 0, "who": 1, "change_time": 2, "changes": 3}

    mock_get_existing.return_value = [
        Row(
            (
                1898563,
                "name@example.com",
                datetime(2024, 5, 23, 16, 40, 29, tzinfo=timezone.utc),
                [
                    {
                        "field_name": "keywords",
                        "added": "webcompat:needs-diagnosis, webcompat:needs-sitepatch",  # noqa
                        "removed": "",
                    }
                ],
            ),
            schema,
        )
    ]

    result = bz.filter_only_unsaved_changes(s_history, bug_ids)

    expected = [
        {
            "number": 222222,
            "who": "name@example.com",
            "change_time": "2024-05-13T13:02:11Z",
            "changes": [
                {
                    "added": "webcompat:needs-diagnosis",
                    "field_name": "keywords",
                    "removed": "",
                }
            ],
        }
    ]

    assert result == expected


def test_empty_input():
    assert parse_string_to_json("") == ""


def test_null_input():
    assert parse_string_to_json(None) == ""


def test_single_key_value_pair():
    input_str = "key:value"
    expected = {"key": "value"}
    assert parse_string_to_json(input_str) == expected


def test_multiple_key_value_pairs():
    input_str = "key1:value1\nkey2:value2"
    expected = {"key1": "value1", "key2": "value2"}
    assert parse_string_to_json(input_str) == expected


def test_multiple_values_for_same_key():
    input_str = "key:value1\r\nkey:value2"
    expected = {"key": ["value1", "value2"]}
    assert parse_string_to_json(input_str) == expected


def test_mixed_line_breaks():
    input_str = "key1:value1\r\nkey2:value2\nkey3:value3"
    expected = {"key1": "value1", "key2": "value2", "key3": "value3"}
    assert parse_string_to_json(input_str) == expected


def test_empty_result():
    input_str = "\n\n"
    assert parse_string_to_json(input_str) == ""


def test_severity_string():
    input_str = "platform:linux\r\nimpact:feature-broken\r\naffects:some"
    expected = {
        "platform": "linux",
        "impact": "feature-broken",
        "affects": "some",
    }
    assert parse_string_to_json(input_str) == expected


def test_values_with_colon():
    input_str = "url:http://chatgpt-tokenizer.com/*\r\nurl:excalidraw.com/*\r\nurl:godbolt.org/*\r\nurl:youwouldntsteala.website/*\r\nurl:yandex.ru/images/*"  # noqa
    expected = {
        "url": [
            "http://chatgpt-tokenizer.com/*",
            "excalidraw.com/*",
            "godbolt.org/*",
            "youwouldntsteala.website/*",
            "yandex.ru/images/*",
        ]
    }
    assert parse_string_to_json(input_str) == expected


def test_kb_bugs_from_platform_bugs(bz):
    core_as_kb_bugs = bz.kb_bugs_from_platform_bugs(
        SAMPLE_CORE_AS_KB_BUGS, {1835339}, {1896383, 222222}
    )

    assert core_as_kb_bugs == {
        item["id"]: item
        for item in [
            {
                "assigned_to": "nobody@mozilla.org",
                "whiteboard": "",
                "see_also": ["https://bugzilla.mozilla.org/show_bug.cgi?id=1740472"],
                "severity": "S3",
                "product": "Core",
                "depends_on": [],
                "summary": "Consider adding support for Error.captureStackTrace",
                "resolution": "",
                "last_change_time": "2024-05-27T15:07:03Z",
                "keywords": [
                    "parity-chrome",
                    "parity-safari",
                    "webcompat:platform-bug",
                ],
                "priority": "P3",
                "creation_time": "2024-03-21T16:40:27Z",
                "cf_user_story": "",
                "status": "NEW",
                "blocks": [1896383],
                "url": "",
                "cf_last_resolved": None,
                "component": "JavaScript Engine",
                "id": 1886820,
            },
            {
                "assigned_to": "nobody@mozilla.org",
                "whiteboard": "",
                "see_also": [],
                "severity": "S3",
                "product": "Core",
                "depends_on": [],
                "summary": "Example core site report and platform bug",
                "resolution": "",
                "last_change_time": "2024-05-27T15:07:03Z",
                "keywords": ["webcompat:platform-bug", "webcompat:site-report"],
                "priority": "P3",
                "creation_time": "2024-03-21T16:40:27Z",
                "cf_user_story": "",
                "status": "NEW",
                "blocks": [],
                "url": "",
                "cf_last_resolved": None,
                "component": "JavaScript Engine",
                "id": 444444,
            },
        ]
    }


def test_convert_bug_data(bz):
    expected_data = [
        {
            "assigned_to": "test@example.org",
            "component": "Knowledge Base",
            "creation_time": "2000-07-25T13:50:04Z",
            "keywords": [],
            "number": 1835339,
            "priority": None,
            "product": "Web Compatibility",
            "resolution": "",
            "resolved_time": None,
            "severity": None,
            "status": "NEW",
            "title": "Missing implementation of textinput event",
            "url": "",
            "user_story": {
                "url": [
                    "cmcreg.bancosantander.es/*",
                    "new.reddit.com/*",
                    "web.whatsapp.com/*",
                    "facebook.com/*",
                    "twitter.com/*",
                    "reddit.com/*",
                    "mobilevikings.be/*",
                    "book.ersthelfer.tv/*",
                ],
            },
            "whiteboard": "",
            "webcompat_priority": None,
            "webcompat_score": None,
        },
        {
            "assigned_to": None,
            "component": "Knowledge Base",
            "creation_time": "2000-07-25T13:50:04Z",
            "keywords": [],
            "number": 1835416,
            "priority": None,
            "product": "Web Compatibility",
            "resolution": "",
            "resolved_time": None,
            "severity": None,
            "status": "NEW",
            "title": "Sites breaking due to the lack of WebUSB support",
            "url": "",
            "user_story": {
                "url": [
                    "webminidisc.com/*",
                    "app.webadb.com/*",
                    "www.numworks.com/*",
                    "webadb.github.io/*",
                    "www.stemplayer.com/*",
                    "wootility.io/*",
                    "python.microbit.org/*",
                    "flash.android.com/*",
                ],
            },
            "whiteboard": "",
            "webcompat_priority": None,
            "webcompat_score": None,
        },
        {
            "assigned_to": None,
            "component": "Knowledge Base",
            "creation_time": "2000-07-25T13:50:04Z",
            "keywords": [],
            "number": 111111,
            "priority": None,
            "product": "Web Compatibility",
            "resolution": "",
            "resolved_time": None,
            "severity": None,
            "status": "NEW",
            "title": "Test bug",
            "url": "",
            "user_story": "",
            "whiteboard": "",
            "webcompat_priority": None,
            "webcompat_score": None,
        },
    ]
    for bug, expected in zip(SAMPLE_BUGS.values(), expected_data):
        assert bz.convert_bug_data(bug) == expected


def test_parse_datetime():
    result = parse_datetime_str("2024-06-11T16:35:50Z")
    assert result == datetime(2024, 6, 11, 16, 35, 50, tzinfo=timezone.utc)


def test_unify_etp_dependencies(bz):
    unified_etp_bugs = bz.unify_etp_dependencies(
        SAMPLE_ETP_BUGS, SAMPLE_ETP_DEPENDENCIES_BUGS
    )

    assert unified_etp_bugs == {
        item["id"]: item
        for item in [
            {
                "url": "https://gothamist.com/",
                "summary": "gothamist.com - The comments are not displayed with ETP set to Strict",
                "id": 1910548,
                "keywords": ["priv-webcompat", "webcompat:site-report"],
                "component": "Privacy: Site Reports",
                "resolution": "",
                "blocks": [],
                "depends_on": [1101005, 1875061],
                "creation_time": "2024-07-30T07:37:28Z",
                "see_also": ["https://github.com/webcompat/web-bugs/issues/139647"],
                "product": "Web Compatibility",
                "status": "NEW",
                "cf_webcompat_priority": "---",
                "cf_webcompat_score": "---",
            },
            {
                "see_also": ["https://github.com/webcompat/web-bugs/issues/142250"],
                "id": 1921943,
                "summary": "my.farys.be - Login option is missing with ETP set to STRICT",
                "product": "Web Compatibility",
                "keywords": [
                    "priv-webcompat",
                    "webcompat:platform-bug",
                    "webcompat:site-report",
                ],
                "status": "NEW",
                "resolution": "",
                "component": "Privacy: Site Reports",
                "blocks": [],
                "depends_on": [1101005, 1797458],
                "creation_time": "2024-10-01T08:50:58Z",
                "url": "https://my.farys.be/myfarys/",
                "cf_webcompat_priority": "---",
                "cf_webcompat_score": "---",
            },
            {
                "see_also": [],
                "summary": "ryanair.com - The form to start a chat does not load with ETP set to STRICT",
                "id": 1928102,
                "product": "Web Compatibility",
                "status": "NEW",
                "keywords": ["webcompat:site-report"],
                "blocks": [],
                "component": "Privacy: Site Reports",
                "resolution": "",
                "depends_on": [1101005],
                "url": "https://www.ryanair.com/gb/en/lp/chat",
                "creation_time": "2024-10-30T15:04:41Z",
                "cf_webcompat_priority": "---",
                "cf_webcompat_score": "---",
            },
        ]
    }


def test_build_etp_relations(bz):
    unified_etp_bugs = bz.unify_etp_dependencies(
        SAMPLE_ETP_BUGS, SAMPLE_ETP_DEPENDENCIES_BUGS
    )
    etp_data, _ = bz.process_relations(unified_etp_bugs, ETP_RELATION_CONFIG)
    etp_rels = bz.build_relations(etp_data, ETP_RELATION_CONFIG)

    assert etp_rels == {
        "etp_breakage_reports": [
            {"breakage_bug": 1910548, "etp_meta_bug": 1101005},
            {"breakage_bug": 1910548, "etp_meta_bug": 1875061},
            {"breakage_bug": 1921943, "etp_meta_bug": 1101005},
            {"breakage_bug": 1921943, "etp_meta_bug": 1797458},
            {"breakage_bug": 1928102, "etp_meta_bug": 1101005},
        ]
    }
