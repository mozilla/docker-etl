"""How often each Glean metric is opened in GLAM, as a band relative to the others.

    python -m metric_usage.glam              # fetch, classify, write tables
    python -m metric_usage.glam --dry-run    # everything but writing

Writes refs_glam and artifacts_glam, in the shared schemas the refs and
artifacts views read: one artifact per metric page in GLAM, and one ref from
the metric to it carrying observed_days, last_observed and top_percent.

GLAM logs a row each time its data endpoint answers (_log_probe_query in
glam/api/views.py), and serves the log at /api/v1/usage/. That log has the
metric, a timestamp and the request parameters, but no user, IP or user agent,
and a row is a data fetch rather than a view: the page fetches once per filter
combination. It does include moving between metrics inside GLAM, which GLAM's
own Glean telemetry (glam_derived.events_stream_v1, automatic page_load and
element_click events) misses. Crawlers run GLAM's JavaScript and call the data
endpoint too, so the telemetry is used only to remove them: a fetch is dropped
when a crawler loaded the same metric within a minute, or when the only GLAM
telemetry in the hour before it came from crawlers. Crawlers are told apart by
network (cloud providers), since is_bot_generated is false on every row.

What remains is counted as distinct days with a fetch, which damps filter
changes and repeated fetches in one sitting, and turned into the percent of a
product's opened metrics used on at least as many days. A count would read as
more precise than this is; a rank holds up when every metric is over- or
under-counted by about the same factor.

Legacy desktop GLAM (product firefox) is decommissioned and left out. Requests
not shaped like the web page's (a script sweeping every metric, which sends
app_id and channel and no versions) are left out too.
"""

import argparse
import bisect
import collections
import datetime
import json
import urllib.parse

from .common import (DEST, PREFIX, artifact_schema, days_within, get_json, log, query,
                     ref_schema, snakecase, union_views, utc_today, window_end, write_table)

GLAM = "https://glam.telemetry.mozilla.org"
EVENTS = "moz-fx-data-shared-prod.glam_derived.events_stream_v1"
WINDOW_DAYS = 365
# GLAM's telemetry starts here; before it, crawlers cannot be removed
EVENTS_START = datetime.date(2025, 9, 11)

# GLAM product to inventory app, and how the page names it
PRODUCTS = {"fog": ("firefox_desktop", "Firefox Desktop"), "fenix": ("fenix", "Fenix")}

# Networks the crawlers come from. Mozilla staff on a cloud VPN would be lost
# too, but almost all of this traffic is one event per client id.
CLOUD_RE = (r"(?i)alibaba|amazon|tencent|google|digitale suisse|microsoft|ovh|hetzner|"
            r"digitalocean|oracle|linode|akamai|choopa|vultr")
SAME_METRIC_SECONDS = 60
SESSION_SECONDS = 3600


def months(start, end):
    """First days of every month overlapping [start, end)."""
    d = start.replace(day=1)
    while d < end:
        yield d
        d = (d.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)


def fetches(start, end):
    """Usage log rows in [start, end), one request per month.

    The log only grows, so a whole month is fetched once, after it ends. The
    month still going is asked up to end, a URL that changes every day, so a
    partial month is never cached under the whole month's URL.
    """
    seen, rows = set(), []
    for m in months(start, end):
        nxt = (m.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
        # toDate is inclusive at midnight, so a row exactly on it comes back
        # twice; seen drops the copy
        url = f"{GLAM}/api/v1/usage/?" + urllib.parse.urlencode({
            "fields": "timestamp,probe_name,context",
            "fromDate": m.isoformat(), "toDate": min(nxt, end).isoformat()})
        for r in get_json(url):
            key = (r["timestamp"], r["probe_name"], json.dumps(r["context"], sort_keys=True))
            if key in seen:
                continue
            seen.add(key)
            t = datetime.datetime.fromisoformat(r["timestamp"])
            if start <= t.date() < end:
                rows.append((t, r["probe_name"], r["context"] or {}))
    return rows


def telemetry(start, end):
    """(page loads of a metric by (product, probe, crawler), every event by crawler).

    Both are sorted lists of epoch seconds. Only glam.telemetry.mozilla.org
    counts; clicks with no URL are kept as evidence of somebody being there.
    """
    rows = query(f"""
SELECT
  UNIX_SECONDS(submission_timestamp) AS t,
  event_name = 'page_load' AS is_load,
  REGEXP_EXTRACT(url, r'^https://glam\\.telemetry\\.mozilla\\.org/([a-z]+)/probe/') AS product,
  REGEXP_EXTRACT(url, r'^https://glam\\.telemetry\\.mozilla\\.org/[a-z]+/probe/([a-z0-9_]+)') AS probe,
  REGEXP_CONTAINS(IFNULL(metadata.isp.name, ''), r'{CLOUD_RE}') AS crawler,
FROM (
  SELECT *, JSON_VALUE(event_extra, '$.url') AS url
  FROM `{EVENTS}`
  WHERE DATE(submission_timestamp) >= '{start - datetime.timedelta(days=1)}'
    AND DATE(submission_timestamp) <= '{end}'
)
WHERE IFNULL(NET.HOST(url), 'glam.telemetry.mozilla.org') = 'glam.telemetry.mozilla.org'
""", name="GLAM telemetry")
    loads, anything = collections.defaultdict(list), collections.defaultdict(list)
    for r in rows:
        t = int(r["t"])
        anything[r["crawler"]].append(t)
        if r["is_load"] and r["probe"]:
            loads[(r["product"], r["probe"], r["crawler"])].append(t)
    for v in (*loads.values(), *anything.values()):
        v.sort()
    return loads, anything


def within(times, lo, hi):
    i = bisect.bisect_left(times, lo)
    return i < len(times) and times[i] <= hi


def classify(t, product, probe, loads, anything):
    """'crawler', 'person' (next to a person's page load or session) or 'unseen'
    (no GLAM telemetry near it, which looks like a browser blocking Glean's
    upload: spread evenly over days and hours, few bursts)."""
    s = int(t.timestamp())
    lo, hi = s - SAME_METRIC_SECONDS, s + SAME_METRIC_SECONDS
    if within(loads.get((product, probe, True), []), lo, hi):
        return "crawler"
    if within(loads.get((product, probe, False), []), lo, hi):
        return "person"
    lo = s - SESSION_SECONDS
    person = within(anything.get(False, []), lo, hi)
    crawler = within(anything.get(True, []), lo, hi)
    if crawler and not person:
        return "crawler"
    return "person" if person else "unseen"


def run():
    end = utc_today()
    start = end - datetime.timedelta(days=WINDOW_DAYS)
    if start < EVENTS_START:
        log(f"  warning: the window starts {start}, before GLAM's telemetry ({EVENTS_START}); "
            "crawlers before then are counted")
    log(f"  window {start} to {end}")

    rows = fetches(start, end)
    log(f"  usage log: {len(rows):,} fetches")
    shapes = collections.Counter()
    kept = []
    for t, probe, ctx in rows:
        product = ctx.get("product")
        if product not in PRODUCTS:
            shapes[f"product {product}"] += 1
        elif "versions" not in ctx or "channel" in ctx:
            shapes["not the web page's request"] += 1
        else:
            shapes["web page"] += 1
            kept.append((t, product, probe))
    for k, n in shapes.most_common():
        log(f"    {n:7,}  {k}")

    loads, anything = telemetry(start, end)
    days = collections.defaultdict(set)
    outcome = collections.Counter()
    for t, product, probe in kept:
        c = classify(t, product, probe, loads, anything)
        outcome[c] += 1
        if c != "crawler":
            days[(product, probe)].add(t.date())
    log("  web page fetches by who sent them:")
    for k, n in outcome.most_common():
        log(f"    {n:7,}  {k}")

    inventory = query(f"SELECT app, metric FROM `{DEST}.{PREFIX}metrics` "
                      f"WHERE app IN ({', '.join(repr(a) for a, _ in PRODUCTS.values())})",
                      tables=["metrics"], name="inventory")
    by_key = collections.defaultdict(list)
    for r in inventory:
        by_key[(r["app"], snakecase(r["metric"]))].append(r["metric"])

    ref_rows, art_rows = [], []
    unmatched = collections.Counter()
    for product, (app, label) in PRODUCTS.items():
        used = {p: d for (pr, p), d in days.items() if pr == product}
        n_days = sorted(len(d) for d in used.values())
        for probe, d in sorted(used.items()):
            metrics = by_key.get((app, probe))
            if not metrics:
                unmatched[product] += 1
                continue
            # percent of this product's opened metrics used on at least as many days
            at_least = len(n_days) - bisect.bisect_left(n_days, len(d))
            top = round(100 * at_least / len(n_days), 1)
            aid = f"glam:{product}:{probe}"
            for metric in metrics:
                ref_rows.append({
                    "app": app, "metric": metric, "artifact_id": aid,
                    "artifact_kind": "glam_metric", "role": "viewed", "hops": None,
                    "named": True, "transformed": None, "via": None,
                    "output_columns": [], "n_output_columns": 0, "evidence": "observed",
                    "observed_days": len(d), "last_observed": max(d).isoformat(),
                    # the page's windows, for whether it was opened in one; the
                    # band stays a year's
                    **days_within(d, window_end()),
                    "top_percent": top,
                })
            art_rows.append({
                "artifact_id": aid, "kind": "glam_metric", "name": f"GLAM: {label}",
                "url": f"{GLAM}/{product}/probe/{probe}/explore", "owners": [],
                "note": f"opened on {len(d)} of the last {WINDOW_DAYS} days, among "
                        f"{len(used):,} {label} metrics opened in GLAM",
                "n_metrics_named": len(metrics), "n_metrics_total": len(metrics),
                "last_run": max(d).isoformat(),
            })
        log(f"  {label}: {len(used):,} metrics opened, "
            f"{unmatched[product]:,} not in the inventory; median "
            f"{n_days[len(n_days) // 2] if n_days else 0} days, most {n_days[-1] if n_days else 0}")
    log(f"\n{len(ref_rows):,} refs over {len(art_rows):,} GLAM pages")
    return ref_rows, art_rows


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    refs, arts = run()
    if args.dry_run:
        return
    suffix = ("Prototype table maintained by the docker-etl metric-usage job, metric_usage/glam.py; "
              "replaced on each run.")
    write_table("refs_glam", refs, ref_schema(),
                f"Glean metrics opened in GLAM over {WINDOW_DAYS} days, with days used and "
                "a percent rank among the product's metrics. " + suffix,
                clustering=["app", "metric"])
    write_table("artifacts_glam", arts, artifact_schema(),
                "GLAM metric pages with a fetch by a person (crawlers removed) over "
                f"{WINDOW_DAYS} days. " + suffix, clustering=["artifact_id"])
    union_views()


if __name__ == "__main__":
    main()
