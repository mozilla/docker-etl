from google.cloud import bigquery
from collections.abc import Mapping, Sequence
from subprocess import CompletedProcess
from models import (
    IncrementalityBranchResultsRow,
    NimbusExperiment,
    BQConfig,
    DAPConfig,
    ExperimentConfig,
)
from tests.test_mock_responses import (
    NIMBUS_SUCCESS,
    NIMBUS_NOT_AN_INCREMENTALITY_EXPERIMENT,
)


class MockResponse:
    """Mock for returning a response, used in functions that mock requests."""

    def __init__(
        self, json_data: object, status_code: int, headers_data: object = None
    ):
        self.json_data = json_data
        self.status_code = status_code
        self.headers = headers_data or {}
        self.ok = 200 <= self.status_code < 400

    def json(self):
        """Mock json data."""
        return self.json_data


def mock_nimbus_success(*args, **kwargs) -> MockResponse:
    """Mock successful POST requests to Nimbus."""

    return MockResponse(NIMBUS_SUCCESS, 200)


def mock_nimbus_fail(*args, **kwargs) -> MockResponse:
    """Mock failing POST requests to Nimbus."""

    return MockResponse({}, 404)


def mock_nimbus_experiment() -> NimbusExperiment:
    nimbus_success_json = NIMBUS_SUCCESS
    nimbus_success_json["batchDuration"] = mock_experiment_config().batch_duration
    return NimbusExperiment.from_dict(nimbus_success_json)


def mock_task_id() -> str:
    return "mubArkO3So8Co1X98CBo62-lSCM4tB-NZPOUGJ83N1o"


def mock_control_row(experiment) -> IncrementalityBranchResultsRow:
    return IncrementalityBranchResultsRow(
        experiment,
        "control",
        {
            "name": "1841986",
            "bucket": 1,
            "task_id": "mubArkO3So8Co1X98CBo62-lSCM4tB-NZPOUGJ83N1o",
            "task_veclen": 4,
            "urls": ["*://*.glamazon.com/"],
        },
    )


def mock_treatment_a_row(experiment) -> IncrementalityBranchResultsRow:
    return IncrementalityBranchResultsRow(
        experiment,
        "treatment-a",
        {
            "name": "1841986",
            "bucket": 2,
            "task_id": "mubArkO3So8Co1X98CBo62-lSCM4tB-NZPOUGJ83N1o",
            "task_veclen": 4,
            "urls": ["*://*.glamazon.com/"],
        },
    )


def mock_treatment_b_row(experiment) -> IncrementalityBranchResultsRow:
    return IncrementalityBranchResultsRow(
        experiment,
        "treatment-b",
        {
            "name": "1841986",
            "bucket": 3,
            "task_id": "mubArkO3So8Co1X98CBo62-lSCM4tB-NZPOUGJ83N1o",
            "task_veclen": 4,
            "urls": [
                "*://*.glamazon.com/",
                "*://*.glamazon.com/*tag=admarketus*ref=*mfadid=adm",
            ],
        },
    )


def mock_nimbus_unparseable_experiment() -> NimbusExperiment:
    nimbus_unparseable_json = NIMBUS_NOT_AN_INCREMENTALITY_EXPERIMENT
    nimbus_unparseable_json["batchDuration"] = mock_experiment_config().batch_duration
    return NimbusExperiment.from_dict(nimbus_unparseable_json)


def mock_tasks_to_collect() -> dict[str, dict[int, IncrementalityBranchResultsRow]]:
    experiment = mock_nimbus_experiment()
    return {
        "mubArkO3So8Co1X98CBo62-lSCM4tB-NZPOUGJ83N1o": {
            1: mock_control_row(experiment),
            2: mock_treatment_b_row(experiment),
            3: mock_treatment_a_row(experiment),
        }
    }


def mock_collected_tasks() -> dict[str, dict[int, IncrementalityBranchResultsRow]]:
    experiment = mock_nimbus_experiment()
    tasks_to_collect = {
        "mubArkO3So8Co1X98CBo62-lSCM4tB-NZPOUGJ83N1o": {
            1: mock_control_row(experiment),
            2: mock_treatment_b_row(experiment),
            3: mock_treatment_a_row(experiment),
        }
    }
    tasks_to_collect["mubArkO3So8Co1X98CBo62-lSCM4tB-NZPOUGJ83N1o"][
        1
    ].value_count = 13645
    tasks_to_collect["mubArkO3So8Co1X98CBo62-lSCM4tB-NZPOUGJ83N1o"][
        2
    ].value_count = 18645
    tasks_to_collect["mubArkO3So8Co1X98CBo62-lSCM4tB-NZPOUGJ83N1o"][
        3
    ].value_count = 9645
    return tasks_to_collect


def mock_dap_config() -> DAPConfig:
    return DAPConfig(
        hpke_config="AQAgAAEAAQAgpdceoGiuWvIiogA8SPCdprkhWMNtLq_y0GSePI7EhXE",
        auth_token="shh-secret-token",
        hpke_private_key="ssh-private-key",
        batch_start="1755291600",
    )


def mock_experiment_config() -> ExperimentConfig:
    return ExperimentConfig(slug="traffic-impact-study-5", batch_duration=604800)


def mock_bq_config() -> BQConfig:
    return BQConfig(
        project="some-gcp-project-id", namespace="ads_dap", table="incrementality"
    )


def mock_dap_subprocess_success(
    args: list[str], capture_output: bool, text: bool, check: bool, timeout: int
) -> CompletedProcess:
    return CompletedProcess(
        args=[
            "./collect",
            "--task-id",
            "mubArkO3So8Co1X98CBo62-lSCM4tB-NZPOUGJ83N1o",
            "--leader",
            "https://dap-09-3.api.divviup.org",
            "--vdaf",
            "histogram",
            "--length",
            "3",
            "--authorization-bearer-token",
            "ssh_secret_token",
            "--batch-interval-start",
            "1756335600",
            "--batch-interval-duration",
            "3600",
            "--hpke-config",
            "AQAgAAEAAQAgpdceoGiuWvIiogA8SPCdprkhWMNtLq_y0GSePI7EhXE",
            "--hpke-private-key",
            "ssh-secret-private-key",
        ],
        returncode=0,
        stdout="Number of reports: 150\nInterval start: 2025-08-27 23:32:00 UTC\nInterval end: 2025-08-27 23:34:00 UTC\nInterval length: 120s\nAggregation result: [53, 48, 56]\n",  # noqa: E501
        stderr="",
    )


def mock_dap_subprocess_fail(
    args: list[str], capture_output: bool, text: bool, check: bool, timeout: int
) -> CompletedProcess:
    return CompletedProcess(
        args=[
            "./collect",
            "--task-id",
            "mubArkO3So8Co1X98CBo62-lSCM4tB-NZPOUGJ83N1o",
            "--leader",
            "https://dap-09-3.api.divviup.org",
            "--vdaf",
            "histogram",
            "--length",
            "3",
            "--authorization-bearer-token",
            "ssh_secret_token",
            "--batch-interval-start",
            "1756335600",
            "--batch-interval-duration",
            "3600",
            "--hpke-config",
            "AQAgAAEAAQAgpdceoGiuWvIiogA8SPCdprkhWMNtLq_y0GSePI7EhXE",
            "--hpke-private-key",
            "ssh-secret-private-key",
        ],
        returncode=0,
        stdout="Derp",
        stderr="Uh-oh stuff went wrong actually",
    )


def mock_dap_subprocess_raise(
    args: list[str], capture_output: bool, text: bool, check: bool, timeout: int
) -> CompletedProcess:
    raise Exception(
        "Collection failed for mubArkO3So8Co1X98CBo62-lSCM4tB-NZPOUGJ83N1o, 1, stderr: Uh-oh"
    ) from None

def mock_create_dataset(data_set: str, exists_ok: bool):
    pass

def mock_create_dataset_fail(data_set: str, exists_ok: bool):
    raise Exception("BQ create dataset Uh-oh")

def mock_create_table(table: bigquery.Table, exists_ok: bool):
    pass

def mock_create_table_fail(table: bigquery.Table, exists_ok: bool):
    raise Exception("BQ create table Uh-oh")

def mock_insert_rows_json(table: str, json_rows: dict) -> Sequence[Mapping]:
    return []

def mock_insert_rows_json_fail(table: str, json_rows: dict) -> Sequence[Mapping]:
    return [
        {"key": 0, "errors": "Problem writing bucket 1 results"},
        {"key": 1, "errors": "Problem writing bucket 2 results"},
        {"key": 2, "errors": "Problem writing bucket 3 results"},
    ]

def mock_bq_table() -> bigquery.Table:
    return bigquery.Table('some-gcp-project-id.ads_dap.incrementality',
        schema=[
            bigquery.SchemaField('collection_start', 'DATE', 'REQUIRED', None, 'Start date of the collected time window, inclusive.', (), None),
            bigquery.SchemaField('collection_end', 'DATE', 'REQUIRED', None, 'End date of the collected time window, inclusive.', (), None),
            bigquery.SchemaField('country_codes', 'JSON', 'NULLABLE', None, 'List of 2-char country codes for the experiment', (), None),
            bigquery.SchemaField('experiment_slug', 'STRING', 'REQUIRED', None, 'Slug indicating the experiment.', (), None),
            bigquery.SchemaField('experiment_branch', 'STRING', 'REQUIRED', None, 'The experiment branch this data is associated with.', (), None),
            bigquery.SchemaField('advertiser', 'STRING', 'REQUIRED', None, 'Advertiser associated with this experiment.', (), None),
            bigquery.SchemaField('metric', 'STRING', 'REQUIRED', None, 'Metric collected for this experiment.', (), None),
            bigquery.SchemaField('value', 'RECORD', 'REQUIRED', None, None, (
                bigquery.SchemaField('count', 'INT64', 'NULLABLE', None, None, (), None),
                bigquery.SchemaField('histogram', 'JSON', 'NULLABLE', None, None, (), None)), None),
            bigquery.SchemaField('created_at', 'TIMESTAMP', 'REQUIRED', None, 'Timestamp for when this row was written.', (), None)])
