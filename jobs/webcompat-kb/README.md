# Web compatibility Knowledge Base ETL

This job fetches bugzilla bugs from Web Compatibility > Knowledge Base
component, as well as their core bugs dependencies and breakage
reports and puts them into BQ. It also has additional sub-jobs to
record the webcompat metric score, and changes to the score, and to
ensure that we have fresh data from external sources such as CrUX.

## Usage

This script is intended to be run in a docker container.
Build the docker image with:

```sh
docker build -t webcompat-kb .
```

### Running locally

First authenticate with gcloud. You also need to ensure Google Drive
access is enabled:

```sh
gcloud auth login --enable-gdrive-access --update-adc
```

This will open an authentication flow in your web browser. It has to
be rerun when the access token expires.

It is highly recommended to use [uv](https://docs.astral.sh/uv/) to
run the project. Assuming uv is installed starting the ETL locally
should be as simple as:

And then run the script after authentication with gcloud:

```sh
uv run webcompat-etl --bq-project=<your_project_id> --bq-kb-dataset=<your_dataset_id> --no-write
```

By default all the jobs that run in production are run. Specific jobs
can be specified by name; see `webcompat-etl --help` for more details.

## Development

Run tests with:

```sh
./test.sh
```

Ruff is used for code formatting:
```sh
uv run ruff format .
```
