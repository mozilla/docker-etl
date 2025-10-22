#!/usr/bin/env bash

set -ex

uv sync --extra=test
uv run mypy webcompat_kb
uv run pytest --ruff --ruff-format .
uv run webcompat-check-templates --bq-project-id="moz-fx-dev-dschubert-wckb"
