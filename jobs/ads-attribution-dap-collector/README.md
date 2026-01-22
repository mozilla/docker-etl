# Ads Attribution DAP Collection Job

This job collects metrics from DAP and write the results to BigQuery.

## Overview
This job is driven by a config file from a GCS bucket. Use `job_config_gcp_project` 
and `job_config_bucket` to specify the file.  The config file must be named
`attribution-conf.json` and a sample is available [here](https://github.com/mozilla-services/mars/tree/main/internal/gcp/storage/testdata/mars-attribution-config).

## Usage

This script is intended to be run in a docker container.
Build the docker image with:

It requires setup of some environment variables that hold DAP credentials, and the job will look for those when it
starts up. A dev script, `dev_run_docker.sh`, is included for convenience to build and run the job locally, and it
also documents those variables.

Once the environment variables are set up, run the job with:


```sh
./dev_run_docker.sh
```
To just build the docker image, use:
```
docker build -t ads-attribution-dap-collector .
```

## Testing

First create the job venv using
```
python -m venv ./venv
source ./venv/bin/activat
pip install -r requirements.txt
```
Run tests from `/jobs/ads-attribution-dap-collector` using: 
`python -m pytest`

## Linting and Formatting
```
black .
```
```
flake8 .
```