import os
import sys
## Append the source code directory to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../ads_incrementality_dap_collector')))

import pytest

from unittest import TestCase
from unittest.mock import patch

from tests.test_mocks import mock_nimbus_success, mock_nimbus_fail, mock_nimbus_experiment, mock_control_row, mock_treatment_a_row, mock_treatment_b_row, mock_task_id, mock_nimbus_unparseable_experiment
from ads_incrementality_dap_collector.helpers import get_experiment, prepare_results_rows
from ads_incrementality_dap_collector.models import IncrementalityBranchResultsRow


class TestHelpers(TestCase):
    @patch("requests.get", side_effect=mock_nimbus_success)
    def test_get_experiment_success(self, fetch_mock):
        experiment = get_experiment("traffic-impact-study-5", "nimbus_api_url")
        self.assertEqual("traffic-impact-study-5", experiment.slug, )
        self.assertEqual(1, fetch_mock.call_count)

    @patch("requests.get", side_effect=mock_nimbus_fail)
    def test_get_experiment_fal(self, fetch_mock):
        with pytest.raises(Exception, match='Failed fetching experiment: traffic-impact-study-5 from: nimbus_api_url'):
            _ = get_experiment("traffic-impact-study-5", "nimbus_api_url")
            self.assertEqual(1, fetch_mock.call_count)

    def test_prepare_results_rows_success(self):
        experiment = mock_nimbus_experiment()
        results_rows = prepare_results_rows(experiment)
        task_id = mock_task_id()
        self.assertEqual([task_id], list(results_rows.keys()))
        self.assertEqual(mock_control_row(experiment), results_rows[task_id][1])
        self.assertEqual(mock_treatment_a_row(experiment), results_rows[task_id][2])
        self.assertEqual(mock_treatment_b_row(experiment), results_rows[task_id][3])

    def test_prepare_results_row_unparseable_experiment(self):
        experiment = mock_nimbus_unparseable_experiment()
        results_rows = prepare_results_rows(experiment)
        self.assertEqual({}, results_rows)
        self.assertEqual([], list(results_rows.keys()))

    # def test_collect_dap_results(self):
