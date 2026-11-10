#!/bin/sh
# Rebuild every table the page reads, in dependency order. PY defaults to the
# job's .venv; FETCH=0 skips refreshing the generated-sql and metric-hub copies.
set -e
cd "$(dirname "$0")/.."
PY="${PY:-.venv/bin/python}"
[ "${FETCH:-1}" = 0 ] || { script/fetch_generated_sql.sh && script/fetch_metric_hub.sh; }
$PY -m metric_usage.inventory
$PY -m metric_usage.lineage
# before looker and adhoc, which use its OpMon projects and experiment configs
$PY -m metric_usage.metric_hub
$PY -m metric_usage.redash
# needs the Looker API key in the keyring or environment; see metric_usage/looker_api.py
$PY -m metric_usage.looker
$PY -m metric_usage.glam
# reads BigQuery job history; needs bigquery.jobs.listAll in the projects it reads
$PY -m metric_usage.adhoc
$PY -m metric_usage.summary
