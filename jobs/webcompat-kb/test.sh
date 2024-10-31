#! /usr/bin/bash

set -ex

if [ ! -d "_venv" ]; then
  python3 -m _venv
fi
source _venv/bin/activate

pip install -e .
pip install .[test]

mypy -m webcompat_kb
pytest --ruff --ruff-format .

