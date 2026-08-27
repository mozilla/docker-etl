#!/usr/bin/env python3
"""Render this job's output through Crash Stats' own correlation.js.

The Correlations tabs on Crash Stats are drawn by
webapp/crashstats/crashstats/static/crashstats/js/socorro/correlation.js in
https://github.com/mozilla-services/socorro, which fetches the JSON this job
writes and turns it into the lines of text on the tab. That file is small and
almost entirely pure functions: getCorrelations() returns an array of strings,
not DOM. So it can be run against a scratch bucket or a local directory with no
Socorro checkout, no bundler and no browser, which makes it a cheap way to see
what the tab would actually say about your data.

Same idea as ../update-orphaning-dashboard/serve_frontend.py, which patches that
dashboard's static site to read local files. There's no static site to fetch
here, so this patches the one script and drives it from node.

    # the scratch bucket a dev run wrote to
    python render_frontend.py --bucket benwu-correlations-output \\
        --signature "IPCError-browser | ShutDownKill"

    # production, to compare
    python render_frontend.py --production --signature "..."

    # a local directory laid out like the bucket
    python render_frontend.py --data-dir ./out --signature "..."

Four patches are applied to the upstream file, and each fails loudly if its
anchor moves, since a patch that silently no-ops would leave this testing the
wrong thing:

  1. getDataURL() is repointed at the local data directory.
  2. The jssha import is dropped for node's built in crypto, so there's no npm
     install.
  3. The jsSHA call becomes that crypto call.
  4. $('#mainbody').data('channels') becomes the channel being rendered, since
     there's no jQuery and no page.

What this does not cover: CSP, CORS and gzip content negotiation as a browser
does them. It validates the data contract, not the deployment.
"""

import gzip
import hashlib
import json
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.request

import click


CORRELATION_JS_URL = (
    "https://raw.githubusercontent.com/mozilla-services/socorro/main/webapp/"
    "crashstats/crashstats/static/crashstats/js/socorro/correlation.js"
)

PRODUCTION_BASE = (
    "https://analysis-output.telemetry.mozilla.org/top-signatures-correlations/data/"
)

CHANNELS = ("release", "beta", "nightly", "esr")

# node: prefixed imports in the generated module need 16; top level await needs
# 14.8. No npm install is required.
MINIMUM_NODE_MAJOR = 16

# Anchors in the upstream file. Exact strings, so a change upstream is an error
# rather than a patch that quietly does nothing.
ANCHOR_IMPORT = "import jsSHA from 'jssha';"
ANCHOR_DATA_URL = (
    "      return 'https://analysis-output.telemetry.mozilla.org/"
    "top-signatures-correlations/data/';"
)
ANCHOR_SHA = """        var shaObj = new jsSHA('SHA-1', 'TEXT');
        shaObj.update(signature);
        var sha1signature = shaObj.getHash('HEX');"""
ANCHOR_CHANNELS = """          var channels = $('#mainbody').data('channels');
          if (!channels) {
            channels = [$('#mainbody').data('channel')];
          }"""

# correlation.js is an ES module (it has a top level import), so the harness is
# too and the patched copy is written as .mjs.
REPLACEMENT_SHA = """        var sha1signature = SHA1_HEX(signature);"""

REPLACEMENT_CHANNELS = """          var channels = [HARNESS_CHANNEL];"""

# Driver appended after the patched module. correlation.js assigns to
# window.correlations, so the harness supplies window and a disk-backed fetch.
DRIVER_TEMPLATE = """
const RESULTS = await window.correlations.getCorrelations(
  SIGNATURE, HARNESS_CHANNEL, PRODUCT
);
if (typeof RESULTS === 'string') {
  console.log(JSON.stringify({kind: 'message', lines: [RESULTS]}));
} else if (Array.isArray(RESULTS)) {
  console.log(JSON.stringify({kind: 'results', lines: RESULTS}));
} else {
  console.log(JSON.stringify({kind: 'empty', lines: []}));
}
"""

PRELUDE_TEMPLATE = """// Written by render_frontend.py. Do not edit.
import {{ readFileSync }} from 'node:fs';
import {{ gunzipSync }} from 'node:zlib';
import {{ createHash }} from 'node:crypto';

const DATA_DIR = {data_dir};
const SIGNATURE = {signature};
const HARNESS_CHANNEL = {channel};
const PRODUCT = {product};

// Upstream uses jssha; node has this built in and the digest is the same.
function SHA1_HEX(text) {{
  return createHash('sha1').update(text, 'utf8').digest('hex');
}}

// Minimal stand-ins for the browser globals correlation.js expects. fetch()
// reads the file the URL points at and gunzips it, because the real objects are
// stored gzipped with Content-Encoding: gzip and the browser decompresses them
// before the JS sees them.
globalThis.window = globalThis;
globalThis.fetch = async function (url) {{
  const path = DATA_DIR + '/' + url;
  let buf;
  try {{
    buf = readFileSync(path);
  }} catch (e) {{
    return {{
      ok: false,
      status: 404,
      json: async () => {{ throw new Error('not found: ' + path); }},
    }};
  }}
  if (buf[0] === 0x1f && buf[1] === 0x8b) {{
    buf = gunzipSync(buf);
  }}
  const text = buf.toString('utf8');
  return {{ ok: true, status: 200, json: async () => JSON.parse(text) }};
}};
"""


def fetch_correlation_js():
    click.echo(f"Fetching correlation.js from {CORRELATION_JS_URL}", err=True)
    with urllib.request.urlopen(CORRELATION_JS_URL) as response:
        return response.read().decode("utf-8")


def patch(source, channel):
    """Apply the three patches, erroring if any anchor has moved upstream."""
    for name, anchor in (
        ("jssha import", ANCHOR_IMPORT),
        ("getDataURL return", ANCHOR_DATA_URL),
        ("jsSHA usage", ANCHOR_SHA),
        ("channels lookup", ANCHOR_CHANNELS),
    ):
        if anchor not in source:
            raise click.ClickException(
                f"Could not find the {name} in correlation.js. Upstream has "
                f"changed and this harness needs updating. Looked "
                f"for:\n{anchor}"
            )

    source = source.replace(ANCHOR_IMPORT, "")
    # getDataURL's return value is truthiness-checked before use, so it can't be
    # ''. './' is resolved against DATA_DIR by the harness fetch().
    source = source.replace(ANCHOR_DATA_URL, "      return './';")
    source = source.replace(ANCHOR_SHA, REPLACEMENT_SHA)
    source = source.replace(
        ANCHOR_CHANNELS, REPLACEMENT_CHANNELS.replace("HARNESS_CHANNEL", repr(channel))
    )
    return source


def download_from_bucket(bucket, channel, signature, dest):
    """Pull all.json.gz and the one signature file out of a GCS bucket."""
    sha1 = hashlib.sha1(signature.encode("utf-8")).hexdigest()
    prefix = f"gs://{bucket}/top-signatures-correlations/data"
    (dest / channel).mkdir(parents=True, exist_ok=True)

    for remote, local in (
        (f"{prefix}/all.json.gz", dest / "all.json.gz"),
        (f"{prefix}/{channel}/{sha1}.json.gz", dest / channel / f"{sha1}.json.gz"),
    ):
        # gsutil cp honours Content-Encoding and would decompress on the way
        # down; cat gives the stored bytes, which is what a browser receives.
        result = subprocess.run(
            ["gsutil", "cat", remote], capture_output=True
        )
        if result.returncode != 0:
            raise click.ClickException(
                f"Could not read {remote}:\n{result.stderr.decode('utf-8', 'replace')}"
            )
        local.write_bytes(result.stdout)
    return sha1


def download_from_production(channel, signature, dest):
    sha1 = hashlib.sha1(signature.encode("utf-8")).hexdigest()
    (dest / channel).mkdir(parents=True, exist_ok=True)
    for url, local in (
        (PRODUCTION_BASE + "all.json.gz", dest / "all.json.gz"),
        (
            f"{PRODUCTION_BASE}{channel}/{sha1}.json.gz",
            dest / channel / f"{sha1}.json.gz",
        ),
    ):
        try:
            with urllib.request.urlopen(url) as response:
                local.write_bytes(response.read())
        except Exception as exc:  # noqa: BLE001 - reported to the user as-is
            raise click.ClickException(f"Could not fetch {url}: {exc}")
    return sha1


def copy_from_dir(data_dir, channel, signature, dest):
    sha1 = hashlib.sha1(signature.encode("utf-8")).hexdigest()
    src = pathlib.Path(data_dir)
    (dest / channel).mkdir(parents=True, exist_ok=True)
    pairs = [
        (src / "all.json.gz", dest / "all.json.gz"),
        (src / channel / f"{sha1}.json.gz", dest / channel / f"{sha1}.json.gz"),
    ]
    for source_path, local in pairs:
        if not source_path.exists():
            # Accept uncompressed files too, since a local dry run may not gzip.
            plain = source_path.with_suffix("")
            if plain.exists():
                source_path = plain
            else:
                raise click.ClickException(f"Missing {source_path}")
        shutil.copy(source_path, local)
    return sha1


def describe_data(dest, channel, sha1):
    """Print what the frontend is about to be given, so failures are interpretable."""
    def load(path):
        raw = path.read_bytes()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw)

    totals = load(dest / "all.json.gz")
    signature_data = load(dest / channel / f"{sha1}.json.gz")
    click.echo(
        f"all.json date={totals.get('date')} "
        f"{channel}_total={totals.get(channel)}",
        err=True,
    )
    click.echo(
        f"signature total={signature_data.get('total')} "
        f"results={len(signature_data.get('results', []))}",
        err=True,
    )
    if "top_words" in signature_data:
        click.echo(
            "NOTE: this file has top_words, which the 2.2 port no longer produces. "
            "correlation.js still renders it if present.",
            err=True,
        )
    return totals, signature_data


def list_signatures(channel, bucket, production, data_dir, product):
    """Report which of Crash Stats' top signatures the run wrote a file for.

    The output files are named sha1(signature), so a bucket listing tells you
    nothing about which signatures are in it. This hashes the current top
    signatures and checks which hashes exist, which also shows up signatures that
    fell below MIN_COUNT and so produced no file.
    """
    url = (
        "https://crash-stats.mozilla.com/api/SuperSearch/"
        f"?product={product}&_results_number=0&_facets_size=200&_facets=signature"
    )
    click.echo(f"Fetching top signatures from {url}", err=True)
    with urllib.request.urlopen(url) as response:
        facets = json.load(response)["facets"]["signature"]
    signatures = [facet["term"] for facet in facets]

    if bucket:
        prefix = f"gs://{bucket}/top-signatures-correlations/data/{channel}/"
        result = subprocess.run(["gsutil", "ls", prefix], capture_output=True)
        present = {
            line.rsplit("/", 1)[-1]
            for line in result.stdout.decode("utf-8", "replace").splitlines()
            if line.strip()
        }
    elif data_dir:
        directory = pathlib.Path(data_dir) / channel
        present = {path.name for path in directory.glob("*.json.gz")}
    else:
        raise click.UsageError(
            "--top-signatures needs --bucket or --data-dir; there's no listing API "
            "for the production endpoint."
        )

    found = []
    missing = []
    for candidate in signatures:
        name = hashlib.sha1(candidate.encode("utf-8")).hexdigest() + ".json.gz"
        (found if name in present else missing).append(candidate)

    click.echo(
        f"{len(found)} of the top {len(signatures)} signatures have a file in "
        f"{channel}, {len(missing)} do not.",
        err=True,
    )
    click.echo(err=True)
    for candidate in found:
        click.echo(candidate)
    if missing:
        click.echo(err=True)
        click.echo(
            f"--- no file, likely below MIN_COUNT in this channel ({len(missing)}) ---",
            err=True,
        )
        for candidate in missing:
            click.echo(candidate, err=True)


def check_node():
    """Fail with something readable if node is missing or too old.

    The generated module needs node: prefixed imports, which is the highest bar at
    node 16. Top level await (14.8) and ES modules (12) are lower. Nothing needs
    npm: the jssha dependency is replaced with node's own crypto.
    """
    try:
        result = subprocess.run(
            ["node", "--version"], capture_output=True, check=False
        )
    except FileNotFoundError:
        raise click.ClickException(
            "node was not found on PATH. This harness runs Crash Stats' "
            "correlation.js, which is JavaScript, so node 16 or newer is required. "
            "No npm install is needed."
        )
    version = result.stdout.decode("utf-8", "replace").strip()
    match = re.match(r"v(\d+)\.", version)
    if match and int(match.group(1)) < MINIMUM_NODE_MAJOR:
        raise click.ClickException(
            f"node {version} is too old. The generated module uses node: prefixed "
            f"imports and top level await, so {MINIMUM_NODE_MAJOR} or newer is "
            f"required."
        )
    return version


def run_node(work, module_source, prelude, driver):
    module_path = work / "correlation.mjs"
    module_path.write_text(prelude + module_source + driver)
    result = subprocess.run(
        ["node", str(module_path)], capture_output=True, cwd=str(work)
    )
    stderr = result.stderr.decode("utf-8", "replace").strip()
    if result.returncode != 0:
        raise click.ClickException(
            f"node exited {result.returncode}:\n{stderr}\n"
            "Keep the work directory with --work-dir to inspect the generated module."
        )
    if stderr:
        # correlation.js logs load failures through handleError rather than throwing.
        click.echo(f"stderr from correlation.js:\n{stderr}", err=True)
    return result.stdout.decode("utf-8", "replace").strip()


@click.command(help=__doc__)
@click.option(
    "--signature",
    default=None,
    help=(
        "Crash signature to render. Quote it, they contain spaces and pipes. "
        "Filenames are sha1 of the signature, so the exact text matters."
    ),
)
@click.option(
    "--top-signatures",
    is_flag=True,
    help=(
        "Instead of rendering, list the signatures in the channel by looking up the "
        "top 200 from Crash Stats and reporting which ones the run actually wrote. "
        "Useful because the files are named by hash, so you can't read a signature "
        "off the bucket."
    ),
)
@click.option(
    "--channel",
    type=click.Choice(CHANNELS),
    default="release",
    show_default=True,
)
@click.option(
    "--bucket",
    default=None,
    help="GCS bucket a run wrote to, without the gs:// prefix, e.g. a scratch bucket.",
)
@click.option(
    "--production",
    is_flag=True,
    help=f"Read from {PRODUCTION_BASE} instead of a bucket.",
)
@click.option(
    "--data-dir",
    default=None,
    help=(
        "Local directory laid out like the bucket, i.e. all.json.gz and "
        "<channel>/<sha1>.json.gz."
    ),
)
@click.option("--product", default="Firefox", show_default=True)
@click.option(
    "--work-dir",
    default=None,
    help=(
        "Where to put the patched module and data. Defaults to a temp directory "
        "removed on exit."
    ),
)
def main(
    signature,
    top_signatures,
    channel,
    bucket,
    production,
    data_dir,
    product,
    work_dir,
):
    sources = [bool(bucket), production, bool(data_dir)]
    if sum(sources) != 1:
        raise click.UsageError(
            "Pass exactly one of --bucket, --production or --data-dir."
        )

    if top_signatures:
        list_signatures(channel, bucket, production, data_dir, product)
        return

    if not signature:
        raise click.UsageError("Pass --signature, or --top-signatures to list them.")

    if work_dir:
        work = pathlib.Path(work_dir)
        work.mkdir(parents=True, exist_ok=True)
        _run(work, signature, channel, bucket, production, data_dir, product)
    else:
        with tempfile.TemporaryDirectory(prefix="crash-correlations-") as tmp:
            _run(
                pathlib.Path(tmp),
                signature,
                channel,
                bucket,
                production,
                data_dir,
                product,
            )


def _run(work, signature, channel, bucket, production, data_dir, product):
    data = work / "data"
    data.mkdir(parents=True, exist_ok=True)

    if bucket:
        sha1 = download_from_bucket(bucket, channel, signature, data)
        origin = f"gs://{bucket}"
    elif production:
        sha1 = download_from_production(channel, signature, data)
        origin = PRODUCTION_BASE
    else:
        sha1 = copy_from_dir(data_dir, channel, signature, data)
        origin = data_dir

    click.echo(f"signature: {signature}", err=True)
    click.echo(f"sha1     : {sha1}", err=True)
    click.echo(f"source   : {origin}", err=True)
    describe_data(data, channel, sha1)

    click.echo(f"node     : {check_node()}", err=True)
    module_source = patch(fetch_correlation_js(), channel)
    prelude = PRELUDE_TEMPLATE.format(
        data_dir=json.dumps(str(data)),
        signature=json.dumps(signature),
        channel=json.dumps(channel),
        product=json.dumps(product),
    )
    driver = (
        DRIVER_TEMPLATE.replace("HARNESS_CHANNEL", json.dumps(channel))
        .replace("SIGNATURE", json.dumps(signature))
        .replace("PRODUCT", json.dumps(product))
    )
    output = run_node(work, module_source, prelude, driver)

    if not output:
        raise click.ClickException("correlation.js produced no output.")

    payload = json.loads(output)
    click.echo(err=True)
    if payload["kind"] == "message":
        click.echo("correlation.js reported:", err=True)
    else:
        click.echo(
            f"--- Correlations tab, {channel}, {len(payload['lines'])} line(s) ---",
            err=True,
        )
    for line in payload["lines"]:
        click.echo(line)


if __name__ == "__main__":
    sys.exit(main())
