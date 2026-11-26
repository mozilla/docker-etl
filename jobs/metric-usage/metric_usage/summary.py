"""Per-metric counts the page needs before anything is selected.

    python -m metric_usage.summary

Writes mozdata.analysis.benwu_metrics_usage_summary, one row per (window, app,
metric): the search index and the numbers behind each metric's one-line verdict.
Run it after the source scripts, since it reads the refs and artifacts views
over them. A ref counts toward a window when it holds in every window
(seen_within is null) or was seen within it.

The page loads one window's 68k rows once for search, so the row is kept
narrow; anything per-reference is fetched when a metric is opened.

Also writes benwu_metrics_usage_definitions, a row per (window, metric
definition) for the home page's list of metrics with little or no use; see
DEFINITIONS_SQL. And benwu_metrics_usage_windows, a row per window with its
first and last day, which the page reads for its window list and for what "not
written in the window" means, and the first day of the ad hoc job history. The
sources each end their windows yesterday, so this holds when they ran on the
same day as this script, as run_all.sh does.

Both tables carry each metric's size, from monitoring_derived.column_size_v1:
the bytes its columns hold in a day's partition, as a dry run of a query
reading only them counts them, summed over every table it is in and averaged
over the last SIZE_DAYS days. A Glean metric's size covers all its channels and
pings. A dry run counts a few bytes a row for a column even where it is empty,
which would make every legacy probe look large, so each column is measured
above the smallest column of its table that day. A table with no empty column
is undercounted by its smallest one.
Events have none, since an event is a row of the events column rather than a
column of its own. Among the same app's metrics with data, size_top_percent is
the percent at least as large, so 1 is the top 1%, and size_percent is its share
of their combined size. In definitions a row's size is every live copy's, and
both are among the definitions of the same origin, so a gecko metric is placed
among gecko's metrics rather than among one app's.
"""

from .common import DEST, LONGEST, PREFIX, WINDOWS, client, log, window_end, window_start

COLUMN_SIZE = "moz-fx-data-shared-prod.monitoring_derived.column_size_v1"
# a week, so a metric sent mostly on weekdays is not judged by a Sunday
SIZE_DAYS = 7

# Each metric's size; shared by both tables below, which have their own ranks.
SIZES = f"""
sizes AS (
  SELECT
    m.app,
    m.metric,
    CAST(ROUND(SUM(s.bytes)) AS INT64) AS bytes_per_day,
  FROM `{DEST}.{PREFIX}metrics` AS m
  CROSS JOIN UNNEST(m.columns) AS c
  JOIN (
    -- a column missing on some days (new, or a ping not sent every day) is
    -- averaged over the days it has
    SELECT tbl, column_name, AVG(byte_size - empty) AS bytes
    FROM (
      SELECT
        CONCAT(dataset_id, '.', table_id) AS tbl,
        column_name,
        byte_size,
        MIN(byte_size) OVER (PARTITION BY dataset_id, table_id, submission_date) AS empty,
      FROM `{COLUMN_SIZE}`
      WHERE submission_date BETWEEN '{window_start(SIZE_DAYS, window_end())}' AND '{window_end()}'
    )
    GROUP BY 1, 2
  ) AS s ON s.tbl = c.table AND s.column_name = c.path
  GROUP BY 1, 2
)"""


def size_rank(table, keys, within):
    """size_top_percent and size_percent for the rows of table with data, by
    keys, among the rows with the same value of within."""
    return f"""
  SELECT
    {keys},
    ROUND(100 * CUME_DIST() OVER (PARTITION BY {within} ORDER BY bytes_per_day DESC), 2)
      AS size_top_percent,
    100 * bytes_per_day / SUM(bytes_per_day) OVER (PARTITION BY {within}) AS size_percent,
  FROM {table}
  WHERE bytes_per_day > 0"""


WINDOWS_SQL = f"""
CREATE OR REPLACE TABLE `{DEST}.{PREFIX}windows`
OPTIONS (
  description = "The windows of use the metric usage app offers, in days, each ending on "
    || "end_date. adhoc_start_date is the first day of BigQuery job history kept, "
    || "so ad hoc use in a window starting before it covers only the days since. "
    || "Prototype table maintained by the docker-etl metric-usage job, "
    || "metric_usage/summary.py; replaced on each run."
)
AS
SELECT w.*, h.adhoc_start_date
FROM UNNEST([
  {", ".join(f"STRUCT({w} AS days, DATE '{window_start(w, window_end())}' AS start_date, "
             f"DATE '{window_end()}' AS end_date)" for w in WINDOWS)}
]) AS w
CROSS JOIN (SELECT MIN(day) AS adhoc_start_date FROM `{DEST}.{PREFIX}adhoc_jobs`) AS h
"""

SQL = f"""
CREATE OR REPLACE TABLE `{DEST}.{PREFIX}summary`
CLUSTER BY window_days, app, metric
OPTIONS (
  description = "Per (window, app, metric) reference counts for the metric usage app's search "
    || "and verdict line; filter on window_days. Prototype table maintained by "
    || "the docker-etl metric-usage job, metric_usage/summary.py; replaced on each run."
)
AS
WITH {SIZES},
size_ranks AS ({size_rank("sizes", "app, metric", "app")}
),
refs AS (
  SELECT
    w AS window_days,
    r.app,
    r.metric,
    r.artifact_kind,
    r.named,
    a.is_draft,
    a.is_scheduled,
    a.generated_by,
    a.parent_ids,
    a.last_run,
  FROM `{DEST}.{PREFIX}refs` AS r
  LEFT JOIN `{DEST}.{PREFIX}artifacts` AS a USING (artifact_id)
  CROSS JOIN UNNEST([{", ".join(map(str, WINDOWS))}]) AS w
  WHERE r.seen_within IS NULL OR r.seen_within <= w
),
counts AS (
  SELECT
    window_days,
    app,
    metric,
    COUNTIF(named AND artifact_kind = 'bqetl_table') AS n_tables,
    -- generated ping views are plumbing even where they name a metric
    COUNTIF(named AND artifact_kind = 'bqetl_view'
      AND IFNULL(generated_by, '') != 'stable_views') AS n_views,
    COUNTIF(NOT named AND artifact_kind IN ('bqetl_table', 'bqetl_view', 'glean_ping_view'))
      AS n_bulk,
    COUNTIF(artifact_kind = 'redash_dashboard' AND NOT IFNULL(is_draft, FALSE)) AS n_dashboards,
    -- a saved query somebody relies on: on a dashboard, or refreshed on a schedule
    COUNTIF(artifact_kind = 'redash_query'
      AND (ARRAY_LENGTH(parent_ids) > 0 OR IFNULL(is_scheduled, FALSE))) AS n_redash_kept,
    COUNTIF(artifact_kind = 'redash_query') AS n_redash_all,
    MAX(IF(artifact_kind = 'redash_query', last_run, NULL)) AS last_redash_run,
    COUNTIF(named AND artifact_kind IN ('looker_dashboard', 'looker_look', 'looker_explore'))
      AS n_looker,
    -- accounts whose own queries named it, from BigQuery's job history (adhoc.py)
    COUNTIF(artifact_kind = 'person') AS n_people,
    COUNTIF(artifact_kind = 'service_account') AS n_service_accounts,
    COUNTIF(named AND artifact_kind = 'jetstream_experiment') AS n_experiments,
    COUNTIF(NOT named AND artifact_kind = 'jetstream_experiment') AS n_experiments_default,
    COUNTIF(named AND artifact_kind = 'metric_hub_metric') AS n_metric_hub,
    COUNTIF(named AND artifact_kind = 'opmon_project') AS n_opmon,
  FROM refs
  GROUP BY window_days, app, metric
)
SELECT
  w AS window_days,
  m.app,
  m.metric,
  m.type,
  m.source,
  m.column_status,
  m.in_source,
  m.app_deprecated,
  IFNULL(c.n_tables, 0) AS n_tables,
  IFNULL(c.n_views, 0) AS n_views,
  IFNULL(c.n_bulk, 0) AS n_bulk,
  IFNULL(c.n_dashboards, 0) AS n_dashboards,
  IFNULL(c.n_redash_kept, 0) AS n_redash_kept,
  IFNULL(c.n_redash_all, 0) AS n_redash_all,
  c.last_redash_run,
  IFNULL(c.n_looker, 0) AS n_looker,
  IFNULL(c.n_people, 0) AS n_people,
  IFNULL(c.n_service_accounts, 0) AS n_service_accounts,
  IFNULL(c.n_experiments, 0) AS n_experiments,
  IFNULL(c.n_experiments_default, 0) AS n_experiments_default,
  IFNULL(c.n_metric_hub, 0) AS n_metric_hub,
  IFNULL(c.n_opmon, 0) AS n_opmon,
  sz.bytes_per_day,
  sr.size_top_percent,
  sr.size_percent,
FROM `{DEST}.{PREFIX}metrics` AS m
CROSS JOIN UNNEST([{", ".join(map(str, WINDOWS))}]) AS w
LEFT JOIN counts AS c ON c.window_days = w AND c.app = m.app AND c.metric = m.metric
LEFT JOIN sizes AS sz ON sz.app = m.app AND sz.metric = m.metric
LEFT JOIN size_ranks AS sr ON sr.app = m.app AND sr.metric = m.metric
"""


def per_window(column):
    """The window's copy of a per-window column, for a row joined to windows as w."""
    return "CASE w.days " + " ".join(f"WHEN {d} THEN {column}_{d}d" for d in WINDOWS) + " END"


# Glean SDK metrics, which every app carries and no app team owns
LIBRARIES = ("glean-core", "glean-js", "glean-android")

# What the home page lists metrics with little or no use from. A row is a
# metric definition, not an app's copy: a gecko metric is in seven apps and
# can only go if all seven copies are unused, so its use is every copy's, and
# a Glean metric's also its legacy mirror's (removing the metric stops the
# mirror). Only definitions with a live copy (in the code, in a live app,
# with a column) are listed.
#
# Use is somebody or something reading the metric in the window, not a
# reference: a generated view that names every column is not use, and a query
# on a derived column already counts toward the metric it comes from, through
# lineage. Counted: ad hoc queries by people and service accounts, Jetstream
# experiments and OpMon projects that chose it, Looker dashboards, Looks and
# explore queries naming it with runs in the window, saved Redash queries
# naming it with views (which include their dashboards'), and GLAM opens.
# Saved content naming it that would need fixing is counted apart, whether or
# not it was used: bigquery-etl tables and hand-written views, Redash queries,
# Looker dashboards and Looks, and metric-hub metrics.
DEFINITIONS_SQL = f"""
CREATE OR REPLACE TABLE `{DEST}.{PREFIX}definitions`
CLUSTER BY window_days, origin
OPTIONS (
  description = "Per (window, metric definition) use, for the metric usage app's list of metrics "
    || "with little or no use; filter on window_days. Prototype table maintained by "
    || "the docker-etl metric-usage job, metric_usage/summary.py; replaced on each run."
)
AS
WITH {SIZES},
m AS (
  SELECT metrics.*, sizes.bytes_per_day,
    IF(source = 'legacy', 'telemetry', IFNULL(origin, app)) AS def_origin
  FROM `{DEST}.{PREFIX}metrics` AS metrics
  LEFT JOIN sizes USING (app, metric)
),
w AS (
  SELECT days, start_date FROM `{DEST}.{PREFIX}windows`
),
r AS (
  SELECT
    w.days AS window_days,
    r.app,
    r.metric,
    r.artifact_id,
    r.artifact_kind,
    r.named,
    a.generated_by,
    COALESCE(a.name, r.artifact_id) AS name,
    {per_window("r.observed_days")} AS days,
    r.artifact_kind IN ('person', 'service_account')
      OR (r.named AND r.artifact_kind IN ('jetstream_experiment', 'opmon_project'))
      OR (r.named AND r.artifact_kind IN ('looker_dashboard', 'looker_look', 'looker_explore',
                                          'redash_query')
          AND IFNULL({per_window("a.views")}, 0) > 0)
      OR (r.artifact_kind = 'glam_metric' AND IFNULL({per_window("r.observed_days")}, 0) > 0)
      AS used,
    COALESCE(r.last_observed, a.last_used) AS last_day,
  FROM `{DEST}.{PREFIX}refs` AS r
  LEFT JOIN `{DEST}.{PREFIX}artifacts` AS a USING (artifact_id)
  CROSS JOIN w
  WHERE r.seen_within IS NULL OR r.seen_within <= w.days
),
-- the (app, metric) copies whose use counts toward each definition
members AS (
  SELECT def_origin, metric AS def_metric, app, metric, FALSE AS mirror FROM m
  UNION DISTINCT
  SELECT def_origin, metric, 'telemetry', telemetry_mirror, TRUE FROM m
  WHERE telemetry_mirror IS NOT NULL
),
uses AS (
  SELECT
    r.window_days,
    mb.def_origin,
    mb.def_metric,
    r.artifact_id,
    ANY_VALUE(r.artifact_kind) AS kind,
    ANY_VALUE(r.name) AS name,
    MAX(r.days) AS days,
    MAX(r.last_day) AS last_day,
    LOGICAL_AND(mb.mirror) AS mirror,
  FROM members AS mb
  JOIN r ON r.app = mb.app AND r.metric = mb.metric
  WHERE r.used
  GROUP BY 1, 2, 3, 4
),
use AS (
  SELECT
    window_days,
    def_origin,
    def_metric,
    COUNT(*) AS n_consumers,
    -- kind, id, name, days in the window, and m when it is the mirror's use
    ARRAY_AGG(CONCAT(kind, '\\t', artifact_id, '\\t', name, '\\t',
                     IFNULL(CAST(days AS STRING), ''), '\\t', IF(mirror, 'm', ''))
              ORDER BY days DESC, artifact_id LIMIT 5) AS consumers,
  FROM uses
  GROUP BY 1, 2, 3
),
-- the last day of use in the longest window, shown in every window
last_use AS (
  SELECT def_origin, def_metric, MAX(last_day) AS last_used
  FROM uses
  WHERE window_days = {LONGEST}
  GROUP BY 1, 2
),
fix AS (
  SELECT
    m.def_origin,
    m.metric AS def_metric,
    COUNT(DISTINCT IF(r.named AND r.artifact_kind = 'bqetl_table', r.artifact_id, NULL))
      AS n_tables,
    COUNT(DISTINCT IF(r.named AND r.artifact_kind = 'bqetl_view'
      AND IFNULL(r.generated_by, '') NOT IN ('stable_views', 'glean_usage'), r.artifact_id, NULL))
      AS n_views,
    COUNT(DISTINCT IF(r.named AND r.artifact_kind = 'redash_query', r.artifact_id, NULL))
      AS n_redash,
    COUNT(DISTINCT IF(r.named AND r.artifact_kind IN ('looker_dashboard', 'looker_look'),
      r.artifact_id, NULL)) AS n_looker,
    COUNT(DISTINCT IF(r.named AND r.artifact_kind = 'metric_hub_metric', r.artifact_id, NULL))
      AS n_metric_hub,
  FROM m
  JOIN r USING (app, metric)
  WHERE r.window_days = {LONGEST}
  GROUP BY 1, 2
),
defs AS (
  SELECT
    def_origin AS origin,
    metric,
    -- the copy the list links to: the defining app's, else desktop's, else any
    IF(LOGICAL_OR(app = def_origin), def_origin,
       IF(LOGICAL_OR(app = 'firefox_desktop'), 'firefox_desktop', MIN(app))) AS app,
    ARRAY_AGG(app ORDER BY app) AS apps,
    ANY_VALUE(source) AS source,
    ANY_VALUE(type) AS type,
    ANY_VALUE(owners) AS owners,
    ANY_VALUE(expires) AS expires,
    MIN(first_seen) AS first_seen,
    ANY_VALUE(telemetry_mirror) AS telemetry_mirror,
    -- every live copy's, which is what removing the definition saves
    SUM(bytes_per_day) AS bytes_per_day,
  FROM m
  WHERE in_source AND NOT app_deprecated AND column_status != 'missing'
  GROUP BY 1, 2
),
def_ranks AS ({size_rank("defs", "origin, metric", "origin")}
)
SELECT
  w.days AS window_days,
  d.*,
  IFNULL(SAFE.PARSE_DATE('%Y-%m-%d', d.expires) < CURRENT_DATE(), FALSE) AS expired,
  IFNULL(d.first_seen >= w.start_date, FALSE) AS is_new,
  d.origin IN UNNEST({list(LIBRARIES)}) AS is_library,
  IFNULL(u.n_consumers, 0) AS n_consumers,
  IFNULL(u.consumers, []) AS consumers,
  l.last_used,
  IFNULL(f.n_tables, 0) AS n_tables,
  IFNULL(f.n_views, 0) AS n_views,
  IFNULL(f.n_redash, 0) AS n_redash,
  IFNULL(f.n_looker, 0) AS n_looker,
  IFNULL(f.n_metric_hub, 0) AS n_metric_hub,
  dr.size_top_percent,
  dr.size_percent,
FROM defs AS d
CROSS JOIN w
LEFT JOIN use AS u ON u.window_days = w.days AND u.def_origin = d.origin AND u.def_metric = d.metric
LEFT JOIN last_use AS l ON l.def_origin = d.origin AND l.def_metric = d.metric
LEFT JOIN fix AS f ON f.def_origin = d.origin AND f.def_metric = d.metric
LEFT JOIN def_ranks AS dr ON dr.origin = d.origin AND dr.metric = d.metric
"""


def main():
    client().query(WINDOWS_SQL).result()
    client().query(SQL).result()
    client().query(DEFINITIONS_SQL).result()
    for name in ("summary", "definitions"):
        t = client().get_table(f"{DEST}.{PREFIX}{name}")
        log(f"  wrote {DEST}.{PREFIX}{name}: {t.num_rows:,} rows")


if __name__ == "__main__":
    main()
