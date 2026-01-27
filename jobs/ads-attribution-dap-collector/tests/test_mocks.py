from google.cloud import bigquery
from collections.abc import Mapping, Sequence
from subprocess import CompletedProcess
from typing import Any
from uuid import uuid4
from datetime import date

JAN_1_2026 = date(2026, 1, 1)
JAN_7_2026 = date(2026, 1, 7)
JAN_15_2026 = date(2026, 1, 5)

DURATION_3_DAYS = 259200
DURATION_7_DAYS = 604800
DURATION_1_DAY = 86400

MOCK_PARTNER_ID_1 = uuid4()
MOCK_PARTNER_ID_2 = uuid4()

MOCK_TASK_ID_1 = "0QqFBHvuEk1_y4v4GIa9bTaa3vXXtLjsK64QeifzHp1"
MOCK_TASK_ID_2 = "0QqFBHvuEk1_y4v4GIa9bTaa3vXXtLjsK64QeifzHp2"


def mock_get_valid_config() -> dict[str, Any]:
    return {
        "collection_config": {
            "hpke_config": "AQAgAAEAAQAgAjnUwz-F_tIm85OQd5dlfGqm0VhycGn2D1rkQCB4Lyk"
        },
        "advertisers": [
            {
                "name": "advertiser_1",
                "partner_id": "295beef7-1e3b-4128-b8f8-858e12aa660a",
                "start_date": "2026-01-01",
                "collector_duration": 604800,
                "conversion_type": "view",
                "lookback_window": 7,
            },
            {
                "name": "advertiser_2",
                "partner_id": "c8d55857-ab7a-470a-9853-23e303e4594d",
                "start_date": "2026-01-08",
                "collector_duration": 259200,
                "conversion_type": "click",
                "lookback_window": 14,
            },
        ],
        "partners": {
            "295beef7-1e3b-4128-b8f8-858e12aa660a": {
                "task_id": "ix_ucynIiL-tUOPDqLTI2KrhOy4j4vpnIGZKq6jlSeA",
                "vdaf": "histogram",
                "bits": 0,
                "length": 40,
                "time_precision": 60,
                "default_measurement": 0,
            },
            "c8d55857-ab7a-470a-9853-23e303e4594d": {
                "task_id": "0QqFBHvuEk1_y4v4GIa9bTaa3vXXtLjsK64QeifzHp2",
                "vdaf": "histogram",
                "bits": 0,
                "length": 101,
                "time_precision": 3600,
                "default_measurement": 100,
            },
        },
        "ads": {
            "provider_a:1234": {
                "partner_id": "295beef7-1e3b-4128-b8f8-858e12aa660a",
                "index": 9,
            },
            "provider_a:5678": {
                "partner_id": "295beef7-1e3b-4128-b8f8-858e12aa660a",
                "index": 2,
            },
            "provider_a:9876": {
                "partner_id": "295beef7-1e3b-4128-b8f8-858e12aa660a",
                "index": 3,
            },
            "provider_b:1234": {
                "partner_id": "c8d55857-ab7a-470a-9853-23e303e4594d",
                "index": 1,
            },
            "provider_b:5678": {
                "partner_id": "c8d55857-ab7a-470a-9853-23e303e4594d",
                "index": 2,
            },
        },
    }


def mock_get_config_invalid_conversion() -> dict[str, Any]:
    return {
        "collection_config": {
            "hpke_config": "AQAgAAEAAQAgAjnUwz-F_tIm85OQd5dlfGqm0VhycGn2D1rkQCB4Lyk"
        },
        "advertisers": [
            {
                "name": "advertiser_1",
                "partner_id": "295beef7-1e3b-4128-b8f8-858e12aa660a",
                "start_date": "2026-01-01",
                "collector_duration": 604800,
                "conversion_type": "viewandclick",
                "lookback_window": 7,
            }
        ],
        "partners": {
            "295beef7-1e3b-4128-b8f8-858e12aa660a": {
                "task_id": "ix_ucynIiL-tUOPDqLTI2KrhOy4j4vpnIGZKq6jlSeA",
                "vdaf": "histogram",
                "bits": 0,
                "length": 40,
                "time_precision": 60,
                "default_measurement": 0,
            }
        },
        "ads": {
            "provider_a:1234": {
                "partner_id": "295beef7-1e3b-4128-b8f8-858e12aa660a",
                "index": 9,
            }
        },
    }


def mock_get_config_invalid_duration_value() -> dict[str, Any]:
    return {
        "collection_config": {
            "hpke_config": "AQAgAAEAAQAgAjnUwz-F_tIm85OQd5dlfGqm0VhycGn2D1rkQCB4Lyk"
        },
        "advertisers": [
            {
                "name": "advertiser_1",
                "partner_id": "295beef7-1e3b-4128-b8f8-858e12aa660a",
                "start_date": "2026-01-01",
                "collector_duration": 7,
                "conversion_type": "view",
                "lookback_window": 7,
            }
        ],
        "partners": {
            "295beef7-1e3b-4128-b8f8-858e12aa660a": {
                "task_id": "ix_ucynIiL-tUOPDqLTI2KrhOy4j4vpnIGZKq6jlSeA",
                "vdaf": "histogram",
                "bits": 0,
                "length": 40,
                "time_precision": 60,
                "default_measurement": 0,
            }
        },
        "ads": {
            "provider_a:1234": {
                "partner_id": "295beef7-1e3b-4128-b8f8-858e12aa660a",
                "index": 9,
            }
        },
    }


def mock_dap_subprocess_success(
    args: list[str], capture_output: bool, text: bool, check: bool, timeout: int
) -> CompletedProcess:
    return CompletedProcess(
        args=[
            "./collect",
            "--task-id",
            MOCK_TASK_ID_1,
            "--leader",
            "https://dap-leader-url",
            "--vdaf",
            "histogram",
            "--length",
            "4",
            "--authorization-bearer-token",
            "ssh_secret_token",
            "--batch-interval-start",
            "1768780800",
            "--batch-interval-duration",
            "604800",
            "--hpke-config",
            "AQAgAAEAAQAgpdceoGiuWvIiogA8SPCdprkhWMNtLq_y0GSePI7EhXE",
            "--hpke-private-key",
            "ssh-secret-private-key",
        ],
        returncode=0,
        stdout="Number of reports: 150\nInterval start: 2026-01-19 00:00:00 UTC\nInterval end: 2026-01-25 00:00:00 UTC\nInterval length: 120s\nAggregation result: [50, 11, 22, 33]\n",  # noqa: E501
        stderr="",
    )


def mock_create_dataset(data_set: str, exists_ok: bool):
    pass


def mock_create_table(table: bigquery.Table, exists_ok: bool):
    pass


def mock_insert_rows_json(table: str, json_rows: dict) -> Sequence[Mapping]:
    return []


def mock_insert_rows_json_fail(table: str, json_rows: dict) -> Sequence[Mapping]:
    return [
        {"key": 0, "errors": "Problem writing bucket 1 results"},
    ]


def mock_bq_table() -> bigquery.Table:
    return bigquery.Table(
        "some-gcp-project-id.ads_dap_derived.newtab_attribution_v1",
        schema=[
            bigquery.SchemaField(
                "collection_start",
                "DATE",
                mode="REQUIRED",
                description="Start date of the collected time window, inclusive.",
            ),
            bigquery.SchemaField(
                "collection_end",
                "DATE",
                mode="REQUIRED",
                description="End date of the collected time window, inclusive.",
            ),
            bigquery.SchemaField(
                "provider",
                "STRING",
                mode="REQUIRED",
                description="The external service providing the ad.",
            ),
            bigquery.SchemaField(
                "ad_id",
                "INT64",
                mode="REQUIRED",
                description="Id of ad, unique by provider.",
            ),
            bigquery.SchemaField(
                "lookback_window",
                "INT64",
                mode="REQUIRED",
                description="Maximum number of days to attribute an ad.",
            ),
            bigquery.SchemaField(
                "conversion_type",
                "STRING",
                mode="REQUIRED",
                description="Indicates the type of conversion [view, click, default]",
            ),
            bigquery.SchemaField(
                "conversion_count",
                "INT64",
                mode="REQUIRED",
                description="Aggregated number of conversions attributed to the ad_id.",
            ),
            bigquery.SchemaField(
                "created_timestamp",
                "TIMESTAMP",
                mode="REQUIRED",
                description="Timestamp for when this row was created.",
            ),
        ],
    )
