#!/usr/bin/env bash

set -ex

uv sync --extra=test
uv run mypy webcompat_kb
uv run pytest --ruff --ruff-format .
