"""Module name lists fetched from the missing_symbols repo.

https://github.com/marco-c/missing_symbols is where people go to suppress false
positives in this report, so it stays the source of truth. The old job did a
`git clone` at runtime; we fetch the three things we need over HTTPS instead,
which avoids needing git in the image.

Three lists:
  known_modules    - directory of <module name>.txt files. Modules we expect not
                     to have symbols for; filtered out of the report entirely.
  firefox_modules  - Firefox's own modules. We should have symbols for these, so
                     they're highlighted (red with a debug ID, orange without).
  windows_modules  - OS modules. Also highlighted, in blue.

All three are compared case-insensitively, so everything is lowercased here.
"""

import requests

RAW_BASE = "https://raw.githubusercontent.com/marco-c/missing_symbols/master/"
API_BASE = "https://api.github.com/repos/marco-c/missing_symbols/contents/"

REQUEST_TIMEOUT = 60


def _fetch_name_list(session, filename):
    """Read one newline separated module list, lowercased, blanks dropped."""
    response = session.get(RAW_BASE + filename, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return {
        name.strip().lower()
        for name in response.text.split("\n")
        if name.strip() != ""
    }


def _fetch_known_modules(session):
    """Names of the known_modules/*.txt files, minus the .txt, lowercased.

    The old job listed the cloned directory and did `module[:-4]` to drop the
    extension. Every entry is a .txt file, so that was fine; we're explicit
    about it here rather than slicing blindly.
    """
    response = session.get(API_BASE + "known_modules", timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return {
        entry["name"][: -len(".txt")].lower()
        for entry in response.json()
        if entry["type"] == "file" and entry["name"].endswith(".txt")
    }


def fetch_module_lists(session=None):
    """Fetch all three lists. Returns (known, firefox, windows) sets."""
    session = session or requests.Session()
    return (
        _fetch_known_modules(session),
        _fetch_name_list(session, "firefox_modules.txt"),
        _fetch_name_list(session, "windows_modules.txt"),
    )
