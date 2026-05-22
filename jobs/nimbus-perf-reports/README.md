# Nimbus Perf Reports

## Overview

Nimbus Perf Reports is a python project imported from https://github.com/dpalmeiro/telemetry-perf-reports that is designed for analyzing and generating performance reports based on telemetry data.  This job will check for any recently finished Nimbus experiments, and automatically generate and publish a performance report for that experiment which covers some basic performance coverage.

## Dependencies

You can install the necessary python dependencies with:

`pip install -r requirements.txt`

This project also requires the [gcloud sdk](https://cloud.google.com/sdk/docs/install) and expects that authentication has already been established.

## Usage

To generate a report locally:

1.  Define a config for your experiment.  See https://github.com/dpalmeiro/telemetry-perf-reports/tree/main/configs for some examples.
2.  `./generate-perf-report --config <path to config>`  


To update the existing perf-reports list on protosaur, you can run this through docker:

1.  `docker build -t nimbusperf-app .`
2.  `docker run -it --rm -v ~/.config/gcloud:/root/.config/gcloud:ro nimbusperf-app`
