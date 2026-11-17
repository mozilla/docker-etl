"""Score the parser against the reviewed disagreements.

    python -m metric_usage.check

Reads .cache/accuracy/summary.csv and labels/disagreements.csv, as written by
accuracy.py and then labelled: `verdict` is `ref` if the artifact really uses
the metric and `not_ref` if it does not. Unlabelled rows are left out of the
rates and counted.

For each metric, what both methods found is taken as correct. Parser-only
artifacts are real at the rate the reviewed sample of them was, and text-only
artifacts are misses at the rate theirs was. That gives

  precision = real parser refs / all parser refs
  recall    = real parser refs / (real parser refs + real text-only refs)

Recall is an upper bound: a reference both methods miss is invisible here.

Then the home page's list of metrics with little or no use is checked the same
way: a sampled metric listed as having no use seen in the longest window is
wrongly listed if a text-only hit the parser missed (labelled ref, or not yet
labelled) is on something used in that window. A false "no use seen" is the
costly mistake there, since the list is read as candidates for removal.

Each source is scored on its own, the warehouse SQL of bigquery-etl and Redash
("sql"), Looker, ad hoc queries from BigQuery job history, and metric-hub,
then all together.
"""

import collections
import csv

from .common import CACHE, DEST, LABELS, LONGEST, PREFIX, log, query

OUT = CACHE / "accuracy"
SOURCE_NAMES = {"sql": "bigquery-etl and Redash", "looker": "Looker",
                "adhoc": "ad hoc queries", "metrichub": "metric-hub"}


def source_of(artifact_id):
    if artifact_id.startswith("looker:"):
        return "looker"
    if artifact_id.startswith("metric-hub:"):
        return "metrichub"
    if artifact_id.startswith(("adhoc:", "jetstream:")):
        return "adhoc"
    return "sql"


def rate(rows):
    """Share labelled `ref`, or None unless every row in the group is labelled.

    A group with only some rows labelled would otherwise be scored on those
    alone, so partial groups count as unreviewed.
    """
    if not rows or any(r["verdict"] not in ("ref", "not_ref") for r in rows):
        return None
    return sum(r["verdict"] == "ref" for r in rows) / len(rows)


def bounds(acc):
    """(low, high) precision and recall. Unreviewed parser-only refs count as
    wrong for the low bound and right for the high one; unreviewed text-only
    hits count as real misses for the low recall and as noise for the high."""
    tp, fp, fn, u_po, u_to = acc
    def p(t, f):
        return t / (t + f) if t + f else float("nan")
    return (p(tp, fp + u_po), p(tp + u_po, fp)), (p(tp, fn + u_to), p(tp + u_po, fn))


def main():
    summary = list(csv.DictReader(open(OUT / "summary.csv")))
    dis = list(csv.DictReader(open(LABELS / "disagreements.csv")))
    groups = collections.defaultdict(list)
    for r in dis:
        groups[(r["app"], r["metric"], source_of(r["artifact_id"]), r["side"])].append(r)

    # [true positives, false positives, misses, unreviewed parser-only,
    #  unreviewed text-only], overall, per source and per source and stratum
    total = [0.0] * 5
    by_source = collections.defaultdict(lambda: [0.0] * 5)
    by_stratum = collections.defaultdict(lambda: [0.0] * 5)
    for s in summary:
        source = s["source"]
        k = (s["app"], s["metric"], source)
        both, po, to = int(s["both"]), int(s["parser_only"]), int(s["text_only"])
        p_po = rate(groups[k + ("parser_only",)])
        p_to = rate(groups[k + ("text_only",)])
        row = [float(both), 0.0, 0.0, 0.0, 0.0]
        if p_po is None:
            row[3] += po
        else:
            row[0] += po * p_po
            row[1] += po * (1 - p_po)
        if p_to is None:
            row[4] += to
        else:
            row[2] += to * p_to
        for acc in (total, by_source[source], by_stratum[(source, s["stratum"])]):
            for i, v in enumerate(row):
                acc[i] += v

    def line(name, acc):
        (pl, ph), (rl, rh) = bounds(acc)
        rng = lambda lo, hi: f"{lo:6.1%}" if abs(hi - lo) < 0.0005 else f"{lo:6.1%} to {hi:6.1%}"
        n = acc[0] + acc[1] + acc[3]
        log(f"{name:<26} precision {rng(pl, ph):<18} recall {rng(rl, rh):<18} parser refs {n:,.0f}")

    for source, title in SOURCE_NAMES.items():
        if not sum(by_source[source]):
            continue
        line(title, by_source[source])
        for (src, stratum), acc in sorted(by_stratum.items()):
            if src == source and sum(acc):
                line(f"  {stratum}", acc)
        log("")
    line("all sources", total)
    labelled = sum(1 for r in dis if r["verdict"] in ("ref", "not_ref"))
    log(f"\n{labelled} of {len(dis)} review rows labelled; unreviewed disagreements: "
        f"{total[3]:,.0f} parser-only, {total[4]:,.0f} text-only")
    little_use(summary, dis)


# What counts as use in definitions (summary.py), for one artifact, by kind
USED_SQL = f"""
SELECT artifact_id
FROM `{DEST}.{PREFIX}artifacts`
WHERE kind IN ('person', 'service_account', 'jetstream_experiment')
   OR (kind IN ('looker_dashboard', 'looker_look', 'looker_explore', 'redash_query')
       AND views_{LONGEST}d > 0)
"""

LISTED_SQL = f"""
SELECT m.app, m.metric, d.n_consumers
FROM `{DEST}.{PREFIX}metrics` AS m
JOIN `{DEST}.{PREFIX}definitions` AS d
  ON d.window_days = {LONGEST}
  AND d.origin = IF(m.source = 'legacy', 'telemetry', IFNULL(m.origin, m.app))
  AND d.metric = m.metric
"""


def little_use(summary, dis):
    keys = {(s["app"], s["metric"]) for s in summary}
    listed = {(r["app"], r["metric"]) for r in query(LISTED_SQL, name="definitions",
                                                    tables=["metrics", "definitions"])
              if (r["app"], r["metric"]) in keys and r["n_consumers"] == 0}
    used = {r["artifact_id"] for r in query(USED_SQL, name="used artifacts", tables=["artifacts"])}
    wrong = collections.defaultdict(list)
    for r in dis:
        if (r["side"] == "text_only" and r["verdict"] != "not_ref"
                and (r["app"], r["metric"]) in listed and r["artifact_id"] in used):
            wrong[(r["app"], r["metric"])].append(r)
    log(f"\nlittle or no use: {len(listed)} of {len(keys)} sampled metrics listed with no use seen "
        f"in {LONGEST} days; {len(wrong)} of them have a missed use")
    for (app, metric), rows in sorted(wrong.items()):
        log(f"  {app}:{metric}  " + ", ".join(
            f"{r['artifact_id']} ({r['verdict'] or 'unlabelled'})" for r in rows))


if __name__ == "__main__":
    main()
