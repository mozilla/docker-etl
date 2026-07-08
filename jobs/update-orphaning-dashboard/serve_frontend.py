#!/usr/bin/env python3
"""Fetch the dashboard frontend, point it at local data, and serve it.

The Firefox Application Update Out Of Date dashboard's static site lives in
https://github.com/mozilla/telemetry-dashboard/tree/gh-pages/update-orphaning and
normally reads its JSON from the live analysis-output GCS endpoint. This script
pulls a fresh copy at run time, flips it to read from a local `data/` directory
instead, drops in the JSON from a dry run, makes the dry-run report dates
selectable in the dropdown, and starts an HTTP server -- so you can eyeball this
job's output in the real UI without committing a (quickly stale) copy of the site.

    python serve_frontend.py                 # serve test_output/ (the default dry-run dir)
    python serve_frontend.py --data-dir foo  # serve JSON from foo/ instead
    python serve_frontend.py --port 9000

A file:// URL will not work; the page fetches JSON over AJAX, which browsers
block for local files, so this serves over HTTP.

First produce some data to view:

    python -m update_orphaning_dashboard.main --run-date 2026-06-08 --dry-run
"""

import functools
import http.server
import io
import json
import pathlib
import shutil
import tarfile
import tempfile
import urllib.request

import click

# Upstream source of the static site (the gh-pages branch serves it directly).
# The tarball gives us the whole update-orphaning/ tree in one request, no git.
TARBALL_URL = (
    "https://github.com/mozilla/telemetry-dashboard/archive/refs/heads/gh-pages.tar.gz"
)
SITE_SUBDIR = "update-orphaning"

# The site's data source. index.js fetches `DATA_URL + "<YYYYMMDD>.json"`; we
# point it at the local data/ directory instead.
JS_FILE = "index.js"
DATA_URL_FROM = (
    "const DATA_URL = "
    '"https://analysis-output.telemetry.mozilla.org/app-update/data/out-of-date/";'
)
DATA_URL_TO = 'const DATA_URL = "data/";'

# A marker line in initDashboard() after which we inject the available dry-run
# files as the first (and selected) dropdown options, so the dashboard loads our
# data regardless of which report week the dry run used.
DROPDOWN_ANCHOR = '  dateDropdown.textContent = "";'
INJECT_TEMPLATE = """
  // --- injected by serve_frontend.py: prepend local dry-run files ---
  var LOCAL_DATA_FILES = {files};
  LOCAL_DATA_FILES.forEach(function(fname) {{
    var d = fname.replace(".json", "");
    var opt = document.createElement("option");
    opt.value = fname;
    opt.textContent = d.slice(4, 6) + "/" + d.slice(6, 8) + "/" + d.slice(0, 4)
      + " (local)";
    dateDropdown.appendChild(opt);
  }});
  // --- end injected ---
"""


def fetch_site(dest):
    """Download the upstream tarball and extract the update-orphaning/ tree."""
    print(f"Fetching frontend from {TARBALL_URL}")
    with urllib.request.urlopen(TARBALL_URL) as resp:
        raw = resp.read()
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as tar:
        # Members look like "telemetry-dashboard-gh-pages/update-orphaning/...".
        root = tar.getnames()[0].split("/")[0]
        prefix = f"{root}/{SITE_SUBDIR}/"
        members = [m for m in tar.getmembers() if m.name.startswith(prefix)]
        if not members:
            raise RuntimeError(f"No {SITE_SUBDIR}/ found in tarball")
        for m in members:
            rel = m.name.removeprefix(prefix)
            if not rel:
                continue
            target = dest / rel
            if m.isdir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                extracted = tar.extractfile(m)
                if extracted is not None:
                    target.write_bytes(extracted.read())
    print(f"Extracted {SITE_SUBDIR}/ to {dest}")


def patch_local_mode(site, data_files):
    """Point the site at local data/ and make the dry-run dates selectable."""
    js_path = site / JS_FILE
    text = js_path.read_text()

    if DATA_URL_FROM not in text:
        raise RuntimeError(
            f"Could not find the DATA_URL line in {JS_FILE}; the upstream site "
            f"may have changed. Looked for: {DATA_URL_FROM!r}"
        )
    text = text.replace(DATA_URL_FROM, DATA_URL_TO)

    if DROPDOWN_ANCHOR not in text:
        raise RuntimeError(
            f"Could not find the dropdown anchor in {JS_FILE}; the upstream site "
            f"may have changed. Looked for: {DROPDOWN_ANCHOR!r}"
        )
    inject = INJECT_TEMPLATE.format(files=json.dumps(data_files))
    text = text.replace(DROPDOWN_ANCHOR, DROPDOWN_ANCHOR + inject)

    js_path.write_text(text)
    print(
        f"Patched {JS_FILE} to read from local data/ and list {len(data_files)} file(s)"
    )


def populate_data(site, data_dir):
    """Copy the dry-run JSON from `data_dir` into the site's data/ directory.

    Returns the sorted list of JSON filenames copied (newest first).
    """
    src = pathlib.Path(data_dir)
    json_files = sorted(src.glob("*.json"), reverse=True) if src.is_dir() else []
    if not json_files:
        print(
            f"WARNING: no *.json files in {src}/. Run a dry run first, e.g.\n"
            f"  python -m update_orphaning_dashboard.main --run-date 2026-06-08 "
            f"--dry-run\n"
            f"(writes to test_output/ by default), then re-run this script."
        )
        return []
    dest = site / "data"
    dest.mkdir(parents=True, exist_ok=True)
    for f in json_files:
        shutil.copy(f, dest / f.name)
    names = [f.name for f in json_files]
    print(
        f"Copied {len(names)} JSON file(s) from {src}/ into data/: {', '.join(names)}"
    )
    return names


def serve(site, port):
    handler = functools.partial(
        http.server.SimpleHTTPRequestHandler, directory=str(site)
    )
    httpd = http.server.HTTPServer(("", port), handler)
    print(f"\nServing {site} at http://localhost:{port}/  (Ctrl-C to stop)")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")


@click.command(help=__doc__)
@click.option(
    "--data-dir",
    default="test_output",
    show_default=True,
    help="Directory of dry-run JSON to render (the job's --test-output-dir).",
)
@click.option("--port", type=int, default=8000, show_default=True)
@click.option(
    "--work-dir",
    default=None,
    help="Where to extract the frontend. Defaults to a temp dir cleaned up on exit.",
)
def main(data_dir, port, work_dir):
    if work_dir:
        site = pathlib.Path(work_dir)
        site.mkdir(parents=True, exist_ok=True)
        _run(site, data_dir, port)
    else:
        with tempfile.TemporaryDirectory(prefix="orphaning-frontend-") as tmp:
            _run(pathlib.Path(tmp), data_dir, port)


def _run(site, data_dir, port):
    fetch_site(site)
    data_files = populate_data(site, data_dir)
    patch_local_mode(site, data_files)
    serve(site, port)


if __name__ == "__main__":
    main()
