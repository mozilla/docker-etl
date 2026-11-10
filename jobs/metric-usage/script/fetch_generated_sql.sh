#!/bin/sh
# A shallow copy of bigquery-etl's generated-sql branch, which lineage.py reads:
# every deployed query and view, including what `bqetl generate` produces. The
# branch is rewritten regularly, so it is fetched rather than pinned as a
# submodule, and the commit used is printed and kept in .cache/generated_sql.
set -e
cd "$(dirname "$0")/.."
D=.cache/generated_sql
if [ -d "$D/.git" ]; then
  git -C "$D" fetch -q --depth 1 origin generated-sql
  git -C "$D" reset -q --hard FETCH_HEAD
else
  rm -rf "$D"
  git clone -q --depth 1 --branch generated-sql https://github.com/mozilla/bigquery-etl.git "$D"
fi
git -C "$D" log -1 --format='generated-sql at %h, %cd' --date=short
