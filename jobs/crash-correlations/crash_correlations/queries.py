"""Everything that talks to something outside this process.

Three external sources, plus the two SQL files:

    product-details.mozilla.org   which versions are on each channel
    crash-stats.mozilla.com       the top signatures (SuperSearch)
    searchfox.org                 the app note and gfx error string literals

The searchfox scraping looks odd for an ETL job but it's how upstream works and there
isn't a better source: the app note and graphics critical error candidates are string
literals in mozilla-central, passed to ScopedGfxFeatureReporter and gfxCriticalError,
and the crash reports contain them as substrings. Only the counting of which ones
actually appear is data driven.

Ported from download_data.py, versions.py, app_notes.py, gfx_critical_errors.py and
utils.py in the vendored crashcorrelations.
"""

import dataclasses
import datetime
import functools
import pathlib
import re

import requests
from google.cloud import bigquery

from crash_correlations import mining


PRODUCT_DETAILS = "https://product-details.mozilla.org/1.0/firefox_{}.json"
# The only place esr builds are dated. firefox_history_*.json omit them entirely.
PRODUCT_DETAILS_FIREFOX = "https://product-details.mozilla.org/1.0/firefox.json"
SUPERSEARCH = "https://crash-stats.mozilla.com/api/SuperSearch/"
SEARCHFOX = "https://searchfox.org/mozilla-central/search"

TIMEOUT = 120

_SQL_DIR = pathlib.Path(__file__).resolve().parent / "sql"

# Prefixes for the generated feature columns. BigQuery column names are restricted to
# letters, digits and underscores, and the feature values contain quotes, parentheses,
# at signs and non-ASCII text, so the columns are numbered and the real names are kept
# in the labels mapping. See the note in sql/feature_table.sql.
MODULE_PREFIX = "MOD"
ADDON_PREFIX = "ADDON"
APP_NOTE_PREFIX = "APPNOTE"
GFX_PREFIX = "GFX"


@dataclasses.dataclass
class Features:
    """The feature columns for one channel, and their level 1 counts.

    pairs is {kind: [(column_name, feature_value), ...]} for the four generated kinds
    (module, addon, app_note, gfx_error). Column names are synthetic (MOD0, ADDON3,
    ...) because BigQuery won't accept the feature values as identifiers, so the column
    and the value it tests have to travel together: generated_columns() needs both to
    emit the SQL.

    labels maps a column name to what Crash Stats displays, e.g.
    MOD0 -> 'Module "xul.dll"'. That's what filtering.clean_item consumes.

    counts is {group: {itemset: count}} for the module and addon features, which
    frequent_values.sql already counted. Those columns are in counted_columns so the
    level 1 pass skips them rather than double counting.
    """

    pairs: dict
    labels: dict
    counts: dict

    def columns(self, kind):
        return [column for column, _ in self.pairs.get(kind, ())]

    @property
    def module_columns(self):
        return self.columns("module")

    @property
    def addon_columns(self):
        return self.columns("addon")

    @property
    def addon_version_columns(self):
        """{version column: presence column} for the addon features.

        Upstream inferred this from the column name, since it aliased the version column
        as `<guid>-version` and could strip the suffix back off. The columns here are
        synthetic (ADDON3, ADDON3_VERSION), so the pairing is stated explicitly instead
        of being recovered from a string: pruning.ignore_rule needs to find the presence
        column for a version column, and a rule keyed on a naming convention silently
        stops firing when the convention changes.
        """
        return {f"{column}_VERSION": column for column in self.addon_columns}

    @property
    def app_note_columns(self):
        return self.columns("app_note")

    @property
    def gfx_error_columns(self):
        return self.columns("gfx_error")

    @property
    def counted_columns(self):
        """Features the SQL already counted, so the level 1 pass must skip them.

        All four generated kinds are counted in SQL: modules and addons by
        frequent_values.sql, app notes and gfx errors by frequent_substrings.sql.
        Counting them again at level 1 would overwrite the same itemsets with the same
        numbers here, but it would also mean scanning ~1,900 columns per row for no
        reason, and it's the sort of duplication that silently diverges later.

        The addon _VERSION columns are NOT counted in SQL, so they stay in the level 1
        pass. They're a value per row rather than a boolean, so they can't be counted
        the same way.
        """
        return (
            set(self.module_columns)
            | set(self.addon_columns)
            | set(self.app_note_columns)
            | set(self.gfx_error_columns)
        )


def _get(url, params=None, headers=None):
    """GET with the retry policy upstream used, which matters for searchfox."""
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        max_retries=requests.adapters.Retry(
            total=16, backoff_factor=1, status_forcelist=[429]
        )
    )
    session.mount("https://", adapter)
    response = session.get(url, params=params, headers=headers, timeout=TIMEOUT)
    response.raise_for_status()
    return response


@functools.lru_cache(maxsize=None)
def _product_details(kind):
    return _get(PRODUCT_DETAILS.format(kind)).json()


def _major(version):
    """Major version as an int, tolerating an 'esr' suffix and odd shapes."""
    head = version.removesuffix("esr").split(".")[0]
    try:
        return int(head)
    except ValueError:
        return None


def _dated_builds(channel):
    """{version string as it appears in crash data: release date} for a channel.

    The version strings have to match socorro_crash_v2.version exactly, which for esr
    means keeping the 'esr' suffix. Upstream dropped it, which is why its esr filter
    matched nothing; see "The esr channel" in the README.
    """
    if channel == "release":
        # Majors are dated in one file and their point releases in another. esr point
        # releases live in the stability file too, so filter those out by suffix.
        builds = {
            version: date
            for version, date in _product_details("history_major_releases").items()
            if not version.endswith("esr")
        }
        builds.update({
            version: date
            for version, date in _product_details(
                "history_stability_releases"
            ).items()
            if not version.endswith("esr")
        })
        return builds

    if channel == "beta":
        return dict(_product_details("history_development_releases"))

    if channel == "esr":
        # firefox.json is the only place esr builds are dated. Its keys are release
        # names like 'firefox-140.14.0esr'; the record's own 'version' field drops the
        # suffix, so rebuild it from the key.
        builds = {}
        for name, record in _product_details_releases().items():
            if "esr" not in name or not record.get("date"):
                continue
            version = record.get("version")
            if version:
                builds[f"{version}esr"] = record["date"]
        return builds

    raise ValueError(f"unknown channel {channel!r}")


@functools.lru_cache(maxsize=None)
def _product_details_releases():
    return _get(PRODUCT_DETAILS_FIREFOX).json()["releases"]


def channel_versions(channel, as_of=None):
    """Version strings to filter on for a channel: the newest shipped major.

    Replaces versions.py and download_data.get_versions, which resolved versions from
    whatever product-details said at the moment the job ran and so could not reproduce
    an earlier run. Everything here is resolved as of `as_of`, from dated history, so a
    rerun for a past date gets that date's versions.

    esr version strings keep their 'esr' suffix, so they match crash data. Upstream
    stripped it and routed esr down the release branch, whose history file holds no
    suffixed builds, so its filter matched zero esr rows and collided with release. That
    is not reproduced, because it makes the esr tab show release data rather than merely
    showing less; see "Which versions get analysed" in the README.

    nightly has no dated history in product-details, only a current value, and one live
    version at a time. It ships daily, so the current value is nearly always right for a
    recent window.
    """
    if channel == "nightly":
        return [_product_details("versions")["FIREFOX_NIGHTLY"]]

    as_of = str(as_of) if as_of else None
    builds = _dated_builds(channel)
    if as_of:
        builds = {v: d for v, d in builds.items() if d <= as_of}
    if not builds:
        return []

    known = {_major(v) for v in builds} - {None}
    newest = max(known)
    return sorted(v for v in builds if _major(v) == newest)


def top_signatures(number, versions, end_date, window_days):
    """The most frequent signatures over the window, via SuperSearch.

    Upstream passes only a lower bound, computed as today - days + 1, and no upper
    bound, so its runs can't be reproduced for a past date. Here the window is bounded
    on both sides and matches the SQL, which is what makes --date meaningful.

    The +1 in upstream's lower bound is dropped deliberately: it made the signature
    window one day shorter than the window the counting used.
    """
    start = end_date - datetime.timedelta(days=window_days)
    response = _get(
        SUPERSEARCH,
        params={
            "product": "Firefox",
            "date": [f">={start}", f"<{end_date}"],
            "version": list(versions),
            "_results_number": 0,
            "_facets_size": number,
            "_facets": "signature",
        },
    )
    facets = response.json()["facets"]["signature"]
    return [facet["term"] for facet in facets]


def _searchfox_literals(query):
    """String literals on the lines matching a searchfox query."""
    response = _get(
        SEARCHFOX,
        params={"q": query, "limit": 1000},
        headers={"Accept": "application/json"},
    )
    results = sum(response.json()["normal"].values(), [])
    matches = (
        re.search(r'"(.*?)"', line["line"])
        for result in results
        for line in result["lines"]
    )
    return [match.group(1) for match in matches if match is not None]


@functools.lru_cache(maxsize=None)
def app_note_candidates():
    """Graphics feature names that appear in app_notes, from app_notes.py.

    Each feature is reported with a ?, - or + suffix depending on its status, so all
    three are candidates.
    """
    literals = set(_searchfox_literals("ScopedGfxFeatureReporter "))
    literals.discard("gfxCrashReporterUtils.h")
    return sorted(
        suffixed
        for literal in literals
        for suffixed in (literal + "?", literal + "-", literal + "+")
    )


@functools.lru_cache(maxsize=None)
def gfx_error_candidates():
    """Graphics critical error strings, from gfx_critical_errors.py."""
    literals = set()
    for query in (
        "gfxCriticalError(",
        "gfxCriticalNote <<",
        "gfxCriticalErrorOnce(",
    ):
        literals.update(_searchfox_literals(query))
    literals.discard(", ")
    return sorted(literals)


def _read_sql(name):
    return (_SQL_DIR / name).read_text()


def _query(client, sql, parameters):
    job_config = bigquery.QueryJobConfig(query_parameters=parameters)
    return list(client.query(sql, job_config=job_config).result())


def _window_parameters(end_date, window_days, versions):
    return [
        bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        bigquery.ScalarQueryParameter("window_days", "INT64", window_days),
        bigquery.ScalarQueryParameter("product", "STRING", "Firefox"),
        bigquery.ArrayQueryParameter("versions", "STRING", list(versions)),
    ]


def frequent_values(
    billing_project, end_date, window_days, versions, signatures,
    min_support_diff=0.15,
):
    """Feature columns for a channel, plus the level 1 counts SQL already produced.

    Runs frequent_values.sql for the data-derived features (modules and addons) and
    pairs them with the searchfox-scraped candidates, which are only turned into
    columns; their counting happens in the level 1 pass over the feature table because
    they're substring tests rather than exploded arrays.
    """
    client = bigquery.Client(project=billing_project)
    rows = _query(
        client,
        _read_sql("frequent_values.sql"),
        _window_parameters(end_date, window_days, versions)
        + [
            bigquery.ArrayQueryParameter("signatures", "STRING", list(signatures)),
            bigquery.ScalarQueryParameter("min_count", "INT64", mining.MIN_COUNT),
            bigquery.ScalarQueryParameter(
                "min_support_diff", "FLOAT64", min_support_diff
            ),
        ],
    )

    # Assign a numbered column name per value, sorted so a rerun on the same data gives
    # the same names. Nothing persists the names, but stable names make two runs
    # comparable.
    #
    # The (column, value) pairs are kept as an explicit list per kind, because
    # generated_columns() has to emit the SQL for a column next to the value it tests.
    # Deriving the two orderings separately and trusting them to line up would work
    # today and break the first time someone reorders one of them.
    values = {"module": set(), "addon": set()}
    for row in rows:
        values[row["kind"]].add(row["value"])

    columns = {}
    labels = {}
    pairs = {}
    for kind, prefix, label in (
        ("module", MODULE_PREFIX, 'Module "{}"'),
        ("addon", ADDON_PREFIX, 'Addon "{}"'),
    ):
        pairs[kind] = []
        for index, value in enumerate(sorted(values[kind])):
            name = f"{prefix}{index}"
            columns[(kind, value)] = name
            labels[name] = label.format(value)
            pairs[kind].append((name, value))
            if kind == "addon":
                labels[f"{name}_VERSION"] = f'Addon "{value}" Version'

    # The counts SQL already produced, keyed the way mining does.
    counts = {}
    for row in rows:
        column = columns[(row["kind"], row["value"])]
        group = row["signature"] or mining.REFERENCE
        itemset = frozenset(((column, True),))
        counts.setdefault(group, {})[itemset] = row["count"]

    # The searchfox candidates need the same support filter before becoming columns.
    # augment() builds columns from all_app_notes and all_gfx_critical_errors, which are
    # count_substrings' filtered output, not the raw candidate lists. Skipping this
    # would carry 511 columns instead of ~26; see frequent_substrings.sql.
    candidate_values = app_note_candidates() + gfx_error_candidates()
    candidate_kinds = ["app_note"] * len(app_note_candidates()) + ["gfx_error"] * len(
        gfx_error_candidates()
    )
    substring_rows = _query(
        client,
        _read_sql("frequent_substrings.sql"),
        _window_parameters(end_date, window_days, versions)
        + [
            bigquery.ArrayQueryParameter("signatures", "STRING", list(signatures)),
            bigquery.ArrayQueryParameter("substrings", "STRING", candidate_values),
            bigquery.ArrayQueryParameter("kinds", "STRING", candidate_kinds),
            bigquery.ScalarQueryParameter("min_count", "INT64", mining.MIN_COUNT),
            bigquery.ScalarQueryParameter(
                "min_support_diff", "FLOAT64", min_support_diff
            ),
        ],
    )
    seen = {"app_note": set(), "gfx_error": set()}
    for row in substring_rows:
        seen[row["kind"]].add(row["value"])

    substring_columns = {}
    for kind, prefix, template in (
        ("app_note", APP_NOTE_PREFIX, '"{}" in app_notes'),
        ("gfx_error", GFX_PREFIX, 'GFX_ERROR "{}"'),
    ):
        pairs[kind] = []
        for index, value in enumerate(sorted(seen[kind])):
            name = f"{prefix}{index}"
            labels[name] = template.format(value)
            pairs[kind].append((name, value))
            substring_columns[(kind, value)] = name

    for row in substring_rows:
        column = substring_columns[(row["kind"], row["value"])]
        group = row["signature"] or mining.REFERENCE
        counts.setdefault(group, {})[frozenset(((column, True),))] = row["count"]

    return Features(pairs=pairs, labels=labels, counts=counts)


def _quote(value):
    """A BigQuery string literal. These come from the crash data, so escape them."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def generated_columns(features):
    """The {generated_feature_columns} substitution for feature_table.sql."""
    lines = []

    for name, value in features.pairs.get("app_note", ()):
        lines.append(f"  STRPOS(app_notes, {_quote(value)}) != 0 AS {name}")

    for name, value in features.pairs.get("gfx_error", ()):
        lines.append(
            f"  STRPOS(graphics_critical_error, {_quote(value)}) != 0 AS {name}"
        )

    for name, value in features.pairs.get("module", ()):
        lines.append(f"  {_quote(value)} IN UNNEST(module_files) AS {name}")

    for name, value in features.pairs.get("addon", ()):
        quoted = _quote(value)
        lines.append(f"  {quoted} IN UNNEST(addon_names) AS {name}")
        # Three distinct cases, which ignore_rule treats differently: NULL addons,
        # addon absent, addon present. See sql/feature_table.sql.
        lines.append(
            "  CASE WHEN addons IS NULL THEN NULL ELSE COALESCE("
            "(SELECT SPLIT(e, ':')[SAFE_OFFSET(1)] FROM UNNEST(addon_entries) e "
            f"WHERE SPLIT(e, ':')[OFFSET(0)] = {quoted} LIMIT 1), "
            f"'Not installed') END AS {name}_VERSION"
        )

    return ",\n".join(lines)


def feature_table(billing_project, end_date, window_days, versions, features):
    """The feature table for one channel, as Arrow.

    Returns (table, columns) where table is a mining.FeatureTable. Item values keep their
    Python types where mining produces them: booleans stay bool and NULLs stay None,
    which ignore_rule and the filtering both discriminate on.

    Fetched via Arrow rather than iterating the RowIterator. This result is unusually
    wide, around 2,100 columns, and the row-at-a-time path deserialises each field
    individually: measured at 97 ms per row, which is 9 minutes for beta and would be
    hours for release. to_arrow() uses the BigQuery Storage API and measured 1.1 ms per
    row on the same data, an ~88x difference. It needs google-cloud-bigquery-storage
    and pyarrow, both pinned in requirements.txt.

    The Arrow table is then handed to the miner as-is. Converting it to Python was the
    job's memory ceiling: the buffers are 0.17 GB on release and to_pylist over all 2,616
    columns added 2.4 GB on top, most of the 4.8 GB peak. See mining.FeatureTable.
    """
    sql = _read_sql("feature_table.sql").replace(
        "{generated_feature_columns}", generated_columns(features)
    )
    client = bigquery.Client(project=billing_project)
    result = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=_window_parameters(end_date, window_days, versions)
        ),
    ).result()

    table = mining.FeatureTable(result.to_arrow())
    return table, table.columns
