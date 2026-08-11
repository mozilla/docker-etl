"""Checks against the symbols server and the Firefox release history."""

import datetime
from urllib.parse import quote, urljoin

import requests

SYMBOLS_BASE = "https://symbols.mozilla.org/"
RELEASE_HISTORY_URL = (
    "https://product-details.mozilla.org/1.0/firefox_history_major_releases.json"
)

# Our symbols server expires debug information after two years.
SYMBOL_EXPIRY_DAYS = 730

REQUEST_TIMEOUT = 30


def symbol_url(debug_file, debug_id):
    """URL a symbol file would live at on the symbols server.

    Layout is <debug_file>/<debug_id>/<symbol file>, where the symbol file is
    the debug file with a .pdb extension swapped for .sym. Non-Windows modules
    have no extension to swap and use the debug file name as is.
    """
    if debug_file.endswith(".pdb"):
        symbol_file = debug_file[: -len("pdb")] + "sym"
    else:
        symbol_file = debug_file
    return urljoin(SYMBOLS_BASE, quote(f"{debug_file}/{debug_id}/{symbol_file}"))


def are_symbols_available(session, debug_file, debug_id, fix_arg_order=False):
    """Whether the symbols server has symbols for this module now.

    A module can show up in the report and then have its symbols uploaded before
    the report goes out. Those are worth reprocessing, so the report marks them.

    The Spark job called this with (debug_id, debug_file) into parameters
    declared (debug_file, debug_id), so every request asked for a path like
    <debug_id>/<debug_file>/<debug_id> and got a 404. No module was ever marked
    available. With fix_arg_order false the transposition is reproduced for
    parity; pass true to look up the URL that actually exists.
    """
    if not debug_file or not debug_id:
        return False
    if not fix_arg_order:
        debug_file, debug_id = debug_id, debug_file
    try:
        response = session.head(
            symbol_url(debug_file, debug_id), timeout=REQUEST_TIMEOUT
        )
    except requests.RequestException:
        # A flaky lookup shouldn't sink the whole report.
        return False
    return response.ok


def fetch_old_firefox_versions(session=None, today=None):
    """Major Firefox versions released longer ago than symbols are kept.

    Returns major version numbers as strings, e.g. {"115", "116"}.
    """
    session = session or requests.Session()
    today = today or datetime.datetime.utcnow()

    response = session.get(RELEASE_HISTORY_URL, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    old_versions = set()
    for version, release_date in response.json().items():
        released = datetime.datetime.strptime(release_date, "%Y-%m-%d")
        if abs((today - released).days) > SYMBOL_EXPIRY_DAYS:
            old_versions.add(version.split(".")[0])
    return old_versions


def is_old_firefox_module(module, firefox_modules, old_firefox_versions):
    """Whether this is a Firefox module old enough that symbols have expired.

    We don't want to be notified about these, since it's expected that they've
    aged out of the symbols server. Modules that aren't Firefox's own, or that
    carry no version information, are never considered old.
    """
    if module.name.lower() not in firefox_modules or not module.version:
        return False
    return any(
        module.version.startswith(major + ".") for major in old_firefox_versions
    )
