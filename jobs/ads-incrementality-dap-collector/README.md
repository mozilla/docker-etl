# Ads Incrementality DAP collector

## Background

This job collects metrics from DAP for incrementality experiments, and write the results them to BQ.

## Overview

This job is driven by a config file from a GCS bucket. Inform the job of the config file location by passing the
`gcp_project` and `gcs_config_bucket` parameters. See `example_config.json` for how to structure this file.

The config file specifies the incrementality experiments that are currently running, some config and credentials from DAP,
and where in BQ to write the incrementality results.

The job will go out to Nimbus and read data for each of the experiments, then go out to DAP and read experiment branch results,
then put it all together into results rows and write metrics to BQ.

## Configuration

The three recognized top-level keys here are `bq`, `dap`, and `nimbus`

#### bq

Everything the job needs to connect to BigQuery.

- `project`:         GCP project
- `namespace`:       BQ namespace for ads incrementality
- `table`:           BQ table where incrementality results go

#### dap

Everything the job needs to connect to DAP.

- `auth_token`:           Token defined in the collector credentials, used to authenticate to the leader
- `hpke_private_key`:     Private key defined in the collector credentials, used to decrypt shares from the leader
                          and helper
- `hpke_config`:          base64 url-encoded version of public key defined in the collector credentials
- `batch_start`:          Start of the collection interval, as the number of seconds since the Unix epoch


#### nimbus

Everything the job needs to connect to Nimbus.

- `api_url`:        API URL for fetching the Nimbus experiment info
- `experiments`:    List of incrementality experiments configs

##### experiment config list

The experiments that the job should collect results for.

- `slug`:               Experiment slug
- `batch_duration`:     Optional. Duration of the collection batch interval, in seconds.
                        This will default to 7 days if not specified

## Usage

This script is intended to be run in a docker container.

It requires setup of some environment variables that hold DAP credentials, and the job will look for those when it
starts up. A dev script, `dev_run_docker.sh`, is included for convenience to build and run the job locally, and it
also documents those variables.

Once the environment variables are set up, run the job with:


```sh
./dev_run_docker.sh
```

To just build the docker image, use:

```sh
docker build -t ads_incrementality_dap_collector .
```

To run outside of docker, install dependencies with:

```sh
pip install -r requirements.txt
```

Run the script with:

```sh
python3 -m python_template_job.main
```

## Testing

Run tests with:

```sh
python3 -m pytest
```

## Linting and format

`flake8` and `black` are included for code linting and formatting:

```sh
pytest --black --flake8
```

or

```sh
flake8 .
```

or

```sh
black .
```

or

```sh
black --diff .
```
