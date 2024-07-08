# Firefox-CI Taskcluster Export Job

This job builds an image that includes a tool for exporting data related to the
[Firefox-CI Taskcluster instance] to BigQuery.

There are two main commands that can be run within this image:

1. `fxci-etl metric export`

   This command uses the Google Cloud Python client to retrieve Taskcluster worker
   metrics from Google Cloud Monitoring, then exports them to BigQuery.

2. `fxci-etl pulse drain`

   This command connects to a [Taskcluster pulse queue], receives all pending messages,
   then exports the data to BigQuery.

[Firefox-CI Taskcluster instance]: https://firefox-ci-tc.services.mozilla.com/
[Taskcluster pulse queue]: https://docs.taskcluster.net/docs/manual/design/apis/pulse#pulse

## Usage

This script is intended to be run in a docker container.
Build the docker image with:

```sh
podman build -t fxci-taskcluster-export .
```

To run locally, install dependencies with (in jobs/fxci-taskcluster-export):

```sh
pip install -r requirements.txt
pip install --no-deps .
```

### Running Commands

Once in the container or dependencies are installed locally, you can run the
`fxci-etl` command.

To run the Google Cloud Monitoring export:

```sh
fxci-etl metric export
```

To process messages from the pulse queues:
```sh
fxci-etl pulse drain
```

### Configuration

The `fxci-etl` binary looks for configuration in the environment. The following
configuration variables are supported:

#### bigquery

This configuration is always required.

* `FXCI_ETL_BIGQUERY_PROJECT` (str): Name of the GCP project where the dataset resides (required).
* `FXCI_ETL_BIGQUERY_DATASET` (str): Name of the dataset where the tables will be created (required).
* `FXCI_ETL_BIGQUERY_CREDENTIALS` (str): Base64 encoded contents of a service account credentials
    file. The credentials require the DataWriter role for the configured dataset
    (default: uses credentials from environment).

#### storage

This configuration is always required.

* `FXCI_ETL_STORAGE_PROJECT` (str): Name of the GCP project where the storage bucket resides (required).
* `FXCI_ETL_STORAGE_BUCKET` (str): Name of the GCS bucket to store state between ETL invocations (required).
* `FXCI_ETL_STROAGE_CREDENTIALS` (str): Base64 encoded contents of a service
    account credentials file. The credentials require Read/Write access to the
    configured bucket (default: uses credentials from environment).

#### pulse

This configuraiton is required for the `fxci-etl pulse` command.

* `FXCI_ETL_PULSE_USER` (str): Name of the pulse user which owns the queues (required).
* `FXCI_ETL_PULSE_PASSWORD` (str): Password of the pulse user which owns the queues (required).
* `FXCI_ETL_PULSE_HOST` (str): Host name for the pulse instance (default: "pulse.mozilla.org").
* `FXCI_ETL_PULSE_PORT` (int): Port for the pulse instance (default: 5671)

#### monitoring

This configuraiton is required for the `fxci-etl metric` command.

* `FXCI_ETL_MONITORING_CREDENTIALS` (str): Base64 encoded contents of a service
    account credentials file. The credentials require the `MonitoringViewer` on
    the Firefox-CI worker projects defined in config.py (default: uses
    credentials from environment).
