#!/bin/sh
# A shallow copy of metric-hub, which metric_hub.py reads: metric definitions
# and the Jetstream and OpMon configs that use them. Fetched like generated-sql
# rather than pinned, and the commit used is printed.
set -e
cd "$(dirname "$0")/.."
D=.cache/metric_hub
if [ -d "$D/.git" ]; then
  git -C "$D" fetch -q --depth 1 origin main
  git -C "$D" reset -q --hard FETCH_HEAD
else
  rm -rf "$D"
  git clone -q --depth 1 https://github.com/mozilla/metric-hub.git "$D"
fi
git -C "$D" log -1 --format='metric-hub at %h, %cd' --date=short
