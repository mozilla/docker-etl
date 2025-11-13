from unittest.mock import Mock, patch

import pytest

from webcompat_kb.bqhelpers import get_client, BigQuery


@pytest.fixture(scope="module")
@patch("webcompat_kb.bqhelpers.google.auth.default")
@patch("webcompat_kb.bqhelpers.bigquery.Client")
def bq_client(mock_bq, mock_auth_default):
    mock_credentials = Mock()
    mock_project_id = "placeholder_id"
    mock_auth_default.return_value = (mock_credentials, mock_project_id)
    mock_bq.return_value = Mock()

    mock_bq.return_value = Mock()
    return BigQuery(get_client(mock_project_id), "test_dataset", True)
