from models import IncrementalityBranchResultsRow, NimbusExperiment
from tests.test_mock_responses import NIMBUS_SUCCESS, NIMBUS_NOT_AN_INCREMENTALITY_EXPERIMENT

class MockResponse:
    """Mock for returning a response, used in functions that mock requests."""

    def __init__(self, json_data: object, status_code: int, headers_data: object = None):
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
    return NimbusExperiment.from_dict(NIMBUS_SUCCESS)

def mock_task_id() -> str:
    return "mubArkO3So8Co1X98CBo62-lSCM4tB-NZPOUGJ83N1o"

def mock_control_row(experiment) -> IncrementalityBranchResultsRow:
    return IncrementalityBranchResultsRow(
        experiment,
        "control",
        {
            'name': '1841986',
            'bucket': 1,
            'task_id': 'mubArkO3So8Co1X98CBo62-lSCM4tB-NZPOUGJ83N1o',
            'task_veclen': 4,
            'urls': ['*://*.amazon.com/']
        }
    )

def mock_treatment_a_row(experiment) -> IncrementalityBranchResultsRow:
    return IncrementalityBranchResultsRow(
        experiment,
        "treatment-a",
        {
            'name': '1841986',
            'bucket': 2,
            'task_id': 'mubArkO3So8Co1X98CBo62-lSCM4tB-NZPOUGJ83N1o',
            'task_veclen': 4,
            'urls': ['*://*.amazon.com/']
        }
    )

def mock_treatment_b_row(experiment) -> IncrementalityBranchResultsRow:
    return IncrementalityBranchResultsRow(
        experiment,
        "treatment-b",
        {
            'name': '1841986',
            'bucket': 3,
            'task_id': 'mubArkO3So8Co1X98CBo62-lSCM4tB-NZPOUGJ83N1o',
            'task_veclen': 4,
            'urls': ['*://*.amazon.com/', '*://*.amazon.com/*tag=admarketus*ref=*mfadid=adm']
        }
    )

def mock_nimbus_unparseable_experiment() -> NimbusExperiment:
    return NimbusExperiment.from_dict(NIMBUS_NOT_AN_INCREMENTALITY_EXPERIMENT)
