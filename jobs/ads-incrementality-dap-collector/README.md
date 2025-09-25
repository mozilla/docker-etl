# Ads Incrementality DAP collector

## Background

Incrementality is a way to measure the effectiveness of our ads in a general, agreggated, privacy-preserving way --
without knowing anything about specific users.

Incrementality works by dividing clients into various Nimbus experiment branches that vary how/whether an ad is shown.
Separately, a [DAP](https://docs.divviup.org/) task is configured to store the metrics for each experiment branch in a
different DAP bucket.

Firefox is instrumented with [DAP telemetry functionality](https://github.com/mozilla-firefox/firefox/tree/main/toolkit/components/telemetry/dap), which allows it to send metrics and reports into the correct DAP buckets as configured in the experiment.

Then this job can go out and collect metrics from DAP (using bucket info from the experiment's data), and write them
to BQ.

Some examples of existing metrics are
- "url visit counting", which increments counters in DAP when a firefox client visits an ad landing page.

Great care is taken to preserve privacy and anonymity of these metrics. The DAP technology itself agreggates counts
in separate systems and adds noise. The DAP telemetry feature will only submit a count to DAP once per week per client.
All DAP reports are deleted after 2 weeks.

## Overview

This job is driven by a config file from a GCS bucket. Inform the job of the config file location by passing the
`gcp_project` and `gcs_config_bucket` parameters. See `example_config.json` for how to structure this file.

The config file specifies the incrementality experiments that are currently running, some config and credentials from DAP,
and where in BQ to write the incrementality results.

The job will go out to Nimbus and read data for each of the experiments, then go out to DAP and read experiment branch results,
then put it all together into results rows and write metrics to BQ.

## Usage

This script is intended to be run in a docker container.

It requires setup of some environment variables that hold DAP credentials, and the job will look for those when it
starts up. A dev script, `dev_run_docker.sh`, is included for convenience to build and run the job locally, and it
also documents those variables.

There is also a `dev_runbook.md` doc that walks through what is required to set up a DAP account, create some DAP
tasks for testing, and the DAP credentials setup and management. The `public_key_to_hpke_config.py` utility will help
with encoding the DAP credentials for consumption by this job.

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
