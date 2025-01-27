#!/usr/bin/env bash

set -ex

if [ -x "$(command -v uv)" ]; then
    uv sync --extra=test
    uv run mypy -m webcompat_kb
    uv run pytest --ruff --ruff-format .
else
    if [ ! -d "_venv" ]; then
        python3 -m _venv
    fi
    source _venv/bin/activate

    pip install -e .
    pip install .[test]

    mypy -m webcompat_kb
    pytest --ruff --ruff-format .
fi



