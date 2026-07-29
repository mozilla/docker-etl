#!/usr/bin/env python3
"""Fetch the dashboard frontend, point it at local data, and serve it.

The Firefox Graphics Telemetry dashboard's static site lives in
https://github.com/FirefoxGraphics/telemetry/tree/master/www and normally reads
its chart data from the live GCS endpoint. This script pulls a fresh copy at run
time, flips it to read from a local `data/` directory instead, drops in the JSON
from a dry run, and starts an HTTP server, so you can eyeball this job's output
in the real UI without committing a (quickly stale) copy of the site.

    python serve_frontend.py                 # serve test_output/ (the default dry-run dir)
    python serve_frontend.py --data-dir foo  # serve JSON from foo/ instead
    python serve_frontend.py --port 9000

A file:// URL will not work; the page fetches JSON over AJAX, which browsers
block for local files, so this serves over HTTP.
"""

import functools
import http.server
import io
import pathlib
import shutil
import tarfile
import tempfile
import urllib.request

import click

# Upstream source of the static site. The tarball gives us the whole www/ tree
# (chart JS, flot, jquery, css) in one request, no git required.
TARBALL_URL = (
    "https://github.com/FirefoxGraphics/telemetry/archive/refs/heads/master.tar.gz"
)
WWW_SUBDIR = "www"

# The site's data-source toggle. When false it loads chart data from a relative
# `data/` directory instead of the live GCS endpoint.
FLAG_FILE = "chart-impl.js"
FLAG_FROM = "var USE_S3_FOR_CHART_DATA = true;"
FLAG_TO = "var USE_S3_FOR_CHART_DATA = false;"


def fetch_www(dest):
    """Download the upstream tarball and extract its www/ tree into `dest`."""
    print(f"Fetching frontend from {TARBALL_URL}")
    with urllib.request.urlopen(TARBALL_URL) as resp:
        raw = resp.read()
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:gz") as tar:
        # Members look like "telemetry-master/www/...". Pull out the www/ subtree.
        root = tar.getnames()[0].split("/")[0]
        prefix = f"{root}/{WWW_SUBDIR}/"
        members = [m for m in tar.getmembers() if m.name.startswith(prefix)]
        if not members:
            raise RuntimeError(f"No {WWW_SUBDIR}/ found in tarball")
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
    print(f"Extracted {WWW_SUBDIR}/ to {dest}")


def patch_local_mode(www):
    """Flip the site to read chart data from the local data/ directory."""
    flag_path = www / FLAG_FILE
    text = flag_path.read_text()
    if FLAG_FROM not in text:
        raise RuntimeError(
            f"Could not find expected flag in {FLAG_FILE}; the upstream site "
            f"may have changed. Looked for: {FLAG_FROM!r}"
        )
    flag_path.write_text(text.replace(FLAG_FROM, FLAG_TO))
    print(f"Patched {FLAG_FILE} to read from local data/")


def populate_data(www, data_dir):
    """Copy the dry-run JSON from `data_dir` into the site's data/ directory."""
    src = pathlib.Path(data_dir)
    json_files = sorted(src.glob("*.json")) if src.is_dir() else []
    if not json_files:
        print(
            f"WARNING: no *.json files in {src}/. Run a dry run first, e.g.\n"
            f"  python -m graphics_dashboard.dashboard --dry-run\n"
            f"  python -m graphics_dashboard.trends --dry-run\n"
            f"(writes to test_output/ by default), then re-run this script."
        )
        return
    dest = www / "data"
    dest.mkdir(parents=True, exist_ok=True)
    for f in json_files:
        shutil.copy(f, dest / f.name)
    print(f"Copied {len(json_files)} JSON file(s) from {src}/ into data/")


def serve(www, port):
    handler = functools.partial(
        http.server.SimpleHTTPRequestHandler, directory=str(www)
    )
    httpd = http.server.HTTPServer(("", port), handler)
    print(f"\nServing {www} at http://localhost:{port}/  (Ctrl-C to stop)")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")


@click.command(help=__doc__)
@click.option(
    "--data-dir",
    default="test_output",
    show_default=True,
    help="Directory of dry-run JSON to render (the dashboard --test-output-dir).",
)
@click.option("--port", type=int, default=8000, show_default=True)
@click.option(
    "--work-dir",
    default=None,
    help="Where to extract the frontend. Defaults to a temp dir cleaned up on exit.",
)
def main(data_dir, port, work_dir):
    if work_dir:
        www = pathlib.Path(work_dir)
        www.mkdir(parents=True, exist_ok=True)
        _run(www, data_dir, port)
    else:
        with tempfile.TemporaryDirectory(prefix="gfx-frontend-") as tmp:
            _run(pathlib.Path(tmp), data_dir, port)


def _run(www, data_dir, port):
    fetch_www(www)
    patch_local_mode(www)
    populate_data(www, data_dir)
    serve(www, port)


if __name__ == "__main__":
    main()
