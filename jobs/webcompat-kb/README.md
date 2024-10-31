# Web compatibility Knowledge Base bugs import from bugzilla 

This job fetches bugzilla bugs from Web Compatibility > Knowledge Base component, 
as well as their core bugs dependencies and breakage reports and puts them into BQ.  

## Usage

This script is intended to be run in a docker container.
Build the docker image with:

```sh
docker build -t webcompat-kb .
```

To run locally, first install dependencies in `jobs/webcompat-kb`:

```sh
python3 -m venv  _venv
source _venv/bin/activate
pip install -e .
```

And then run the script after authentication with gcloud:

```sh
gcloud auth application-default login
webcompat-etl --bq-project=<your_project_id> --no-write
```

## Development

Run tests with:

```sh
pytest
```

`flake8` and `black` are included for code linting and formatting:

```sh
pytest --black --flake8
```

or

```sh
flake8 webcompat_kb/ tests/
black --diff webcompat_kb/ tests/
```
