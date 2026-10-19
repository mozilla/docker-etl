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

   It drains the Taskcluster queue's task exchanges (`task-completed`, `task-failed`,
   `task-exception`, `task-defined`) into the `tasks_v2`, `task_runs_v1` and
   `task_definitions_v1` tables, and the worker-manager exchanges (`worker-requested`,
   `worker-running`, `worker-stopped`, `worker-removed`, `worker-pool-error`) into the
   `worker_events_v1` table.

[Firefox-CI Taskcluster instance]: https://firefox-ci-tc.services.mozilla.com/
[Taskcluster pulse queue]: https://docs.taskcluster.net/docs/manual/design/apis/pulse#pulse

## Usage

### Building the Image

This script is intended to be run in a docker container.
Build the docker image with:

```sh
podman build -t fxci-taskcluster-export .
```

To run locally, install dependencies with (in jobs/fxci-taskcluster-export):

```sh
pip install -r requirements/base.txt
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

Configuration can live in a toml file under the standard XDG configuration
directory for your system. E.g, on Linux this would be
`~/.config/fxci-etl/config.toml`. Alternatively, it can be configured via
environment variables prefixed with `FXCI_ETL` and with subsections separated
by an underscore. E.g, to define the config `pulse.user`, you could set the
`FXCI_ETL_PULSE_USER` environment variable.

#### bigquery

This configuration is always required.

* `bigquery.project` (str): Name of the GCP project where the dataset resides (required).
* `bigquery.dataset` (str): Name of the dataset where the tables will be created (required).
* `bigquery.credentials` (str): Base64 encoded contents of a service account credentials
    file. The credentials require the DataWriter role for the configured dataset
    (default: uses credentials from environment).

#### storage

This configuration is always required.

* `storage.project` (str): Name of the GCP project where the storage bucket resides (required).
* `storage.bucket` (str): Name of the GCS bucket to store state between ETL invocations (required).
* `storage.credentials` (str): Base64 encoded contents of a service
    account credentials file. The credentials require Read/Write access to the
    configured bucket (default: uses credentials from environment).

#### pulse

This configuraiton is required for the `fxci-etl pulse` command.

* `pulse.user` (str): Name of the pulse user which owns the queues (required).
* `pulse.password` (str): Password of the pulse user which owns the queues (required).
* `pulse.host` (str): Host name for the pulse instance (default: "pulse.mozilla.org").
* `pulse.port` (int): Port for the pulse instance (default: 5671)
* `pulse.durable` (bool): Whether the queues are durable. Set to `false` for local
    development so the queues are automatically deleted when the connection closes
    (default: `true`).

For local development, pass a config file with `--config`, e.g.
`fxci-etl pulse drain --config ~/.cache/fxci-etl-dev/config.toml`, with a TOML file
like:

```toml
[bigquery]
project = "unused"
dataset = "unused"
[storage]
project = "unused"
bucket = "unused"
[pulse]
user = "your-pulse-user"
password = "..."
durable = false
```

#### monitoring

This configuraiton is required for the `fxci-etl metric` command.

* `monitoring.credentials` (str): Base64 encoded contents of a service
    account credentials file. The credentials require the `MonitoringViewer` on
    the Firefox-CI worker projects defined in config.py (default: uses
    credentials from environment).
