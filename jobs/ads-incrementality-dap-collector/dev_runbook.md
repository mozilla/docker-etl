# Divviup Setup

Follow the [DAP Developer setup guide](https://mozilla-hub.atlassian.net/wiki/spaces/MA1/pages/981336333/DAP+Developer+Setup) to create an account with Divviup, to download the CLI tool, and to set up crendetials. (Divviup's own [command line tutorial](https://docs.divviup.org/command-line-tutorial) is helpful, too, but the Confluence page is a concise and effective summary.)

Then set up some environment variables to use the CLI tool:

```sh
export DIVVIUP_API_URL="https://api.divviup.org/"
# This value is the API token you created during developer setup, and goes with the 'divviup' CLI tool
# Not to be confused with the "token" value from the collector credentials json file, which is used by the python code DAP collector
export DIVVIUP_TOKEN="shhhh"
export DIVVIUP_ACCOUNT_ID="ddbbdb16-32d6-482b-bae5-5027d5e01c1f"
export LEADER_ID="059d9e5e-e10c-47b0-bd1f-260bcdcaf430"  # Divvi Up (DAP-09)
export HELPER_ID="27a1094a-d4eb-4a63-bae7-e1e4c318a0e7"  # Mozilla DAP-09 Dev
```

# Create your own DAP task

```sh
export COLLECTOR_CREDENTIAL_ID="5aa88f31-5c10-480b-b7e0-de1ca8e5400c"
# The path to your collector credentials json file
export COLLECTOR_CREDENTIAL_PATH="/Users/mlifshin/.divviup/mlifshin-dev.json"

divviup task create --name "mlifshin-incrementality-dev" \
    --leader-aggregator-id "$LEADER_ID"  \
    --helper-aggregator-id "$HELPER_ID"  \
    --vdaf histogram \
    --length 3  \
    --min-batch-size 100 \
    --time-precision 1m \
    --collector-credential-id "$COLLECTOR_CREDENTIAL_ID" \
    --differential-privacy-strategy pure-dp-discrete-laplace \
    --differential-privacy-epsilon 1
```

## Sample Output

```json
{
  "id": "8zcQLXLFfGfyoWidkNIbaZNVjfal0ghwjDhiUH0M2j4",
  "account_id": "ddbbdb16-32d6-482b-bae5-5027d5e01c1f",
  "name": "mlifshin-incrementality-dev",
  "vdaf": {
    "type": "histogram",
    "length": 3,
    "chunk_length": 1,
    "dp_strategy": {
      "dp_strategy": "PureDpDiscreteLaplace",
      "budget": {
        "epsilon": [
          [
            1
          ],
          [
            1
          ]
        ]
      }
    }
  },
  "min_batch_size": 100,
  "max_batch_size": null,
  "batch_time_window_size_seconds": null,
  "created_at": "2025-07-11T01:12:55.063534Z",
  "updated_at": "2025-07-11T01:12:55.063535Z",
  "deleted_at": null,
  "time_precision_seconds": 60,
  "report_count": 0,
  "aggregate_collection_count": 0,
  "expiration": "2026-07-11T01:12:54.768234Z",
  "leader_aggregator_id": "059d9e5e-e10c-47b0-bd1f-260bcdcaf430",
  "helper_aggregator_id": "27a1094a-d4eb-4a63-bae7-e1e4c318a0e7",
  "collector_credential_id": "5aa88f31-5c10-480b-b7e0-de1ca8e5400c",
  "report_counter_interval_collected": 0,
  "report_counter_decode_failure": 0,
  "report_counter_decrypt_failure": 0,
  "report_counter_expired": 0,
  "report_counter_outdated_key": 0,
  "report_counter_success": 0,
  "report_counter_too_early": 0,
  "report_counter_task_expired": 0
}
```

The task ID is an important value to keep around, for sending reports and for running the collector.
Stash it somewhere like an env var.

Note that only the credentials that created the task will be able to run the collector for that task. However, any other user with Divviup credentials will be able to get task metadata via the `divviup task get ${taskId}` CLI command.

```sh
export TASK_ID="8zcQLXLFfGfyoWidkNIbaZNVjfal0ghwjDhiUH0M2j4"
```

# Send some test reports to the task

```sh
export TASK_ID="8zcQLXLFfGfyoWidkNIbaZNVjfal0ghwjDhiUH0M2j4"
export BATCH_START=$(( $(date +%s) / 3600 * 3600 ))
for i in {1..150}; do
  measurement=$(( $RANDOM % 3 ))
  divviup dap-client upload --task-id "$TASK_ID" --measurement $measurement;
  echo $i
done
```

# Create a staging experiment with the task id in the metadata

Here's an [example experiment](https://stage.experimenter.nonprod.webservices.mozgcp.net/nimbus/incrementality-etl-testing/summary
) that you can clone.

# Run the collector job locally via run_docker.sh

## Encode the the hpke config param

Take the public key value from your credentials json file, and paste it into `generate_hpke_config.py` utility, on line 34, to base64 encode it.

Then run the utility:

```sh
python3 generate_hpke_config.py
```

The code will expect to find this value in a `config.json` file in the GCS bucket defined by the param `--job_config_bucket`. See `example_config.json` for how that file should be structured.

## Set up credentials and run the collector
```sh
# This value is the "token" value from the collector credentials json file, which is used by the python code DAP collector
# Not to be confused with the API token you created during developer setup, and goes with the 'divviup' CLI tool
export DAP_HPKE_TOKEN="shhhh"
export DAP_PRIVATE_KEY="shhh"
./dev_run_docker.sh
```
