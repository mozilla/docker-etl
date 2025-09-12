import os
import sys
## Append the source code directory to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../ads_incrementality_dap_collector')))

import pytest
import re
from unittest import TestCase
from unittest.mock import patch

from tests.test_mocks import (
    mock_nimbus_success, mock_nimbus_fail,
    mock_nimbus_experiment, mock_control_row, mock_treatment_a_row, mock_treatment_b_row,
    mock_task_id, mock_nimbus_unparseable_experiment,
    mock_tasks_to_collect, mock_dap_config, mock_experiment_config,
    mock_dap_subprocess_success, mock_dap_subprocess_fail, mock_dap_subprocess_raise,
    mock_collected_tasks, mock_bq_config,
    mock_create_dataset_success, mock_create_table_success, mock_insert_rows_json_success,
    mock_create_dataset_fail, mock_create_table_fail, mock_insert_rows_json_fail
)
from ads_incrementality_dap_collector.helpers import (
    get_experiment, prepare_results_rows, collect_dap_results, write_results_to_bq
)

class TestHelpers(TestCase):
    @patch("requests.get", side_effect=mock_nimbus_success)
    def test_get_experiment_success(self, mock_fetch):
        experiment = get_experiment(mock_experiment_config(), "nimbus_api_url")
        self.assertEqual("traffic-impact-study-5", experiment.slug)
        self.assertEqual(1, mock_fetch.call_count)

    @patch("requests.get", side_effect=mock_nimbus_fail)
    def test_get_experiment_fail(self, mock_fetch):
        with pytest.raises(Exception, match='Failed getting experiment: traffic-impact-study-5 from: nimbus_api_url'):
            _ = get_experiment(mock_experiment_config(), "nimbus_api_url")
            self.assertEqual(1, mock_fetch.call_count)

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

    @patch("subprocess.run", side_effect=mock_dap_subprocess_success)
    def test_collect_dap_results_success(self, mock_dap_subprocess_success):
        tasks_to_collect = mock_tasks_to_collect()
        task_id = list(tasks_to_collect.keys())[0]
        collect_dap_results(tasks_to_collect, mock_dap_config(), mock_experiment_config())
        self.assertEqual(1, mock_dap_subprocess_success.call_count)
        self.assertEqual(tasks_to_collect[task_id][1].value_count, 53)
        self.assertEqual(tasks_to_collect[task_id][2].value_count, 48)
        self.assertEqual(tasks_to_collect[task_id][3].value_count, 56)

    @patch("subprocess.run", side_effect=mock_dap_subprocess_fail)
    def test_collect_dap_results_fail(self, mock_dap_subprocess_fail):
        tasks_to_collect = mock_tasks_to_collect()
        with pytest.raises(Exception, match='Failed to parse collected DAP results: None'):
            collect_dap_results(tasks_to_collect, mock_dap_config(), mock_experiment_config())
            self.assertEqual(1, mock_dap_subprocess_success.call_count)

    @patch("subprocess.run", side_effect=mock_dap_subprocess_raise)
    def test_collect_dap_results_raise(self, mock_dap_subprocess_raise):
        tasks_to_collect = mock_tasks_to_collect()
        task_id = list(tasks_to_collect.keys())[0]
        with pytest.raises(Exception, match=f'Collection failed for {task_id}, 1, stderr: Uh-oh'):
            collect_dap_results(tasks_to_collect, mock_dap_config(), mock_experiment_config())
            self.assertEqual(1, mock_dap_subprocess_success.call_count)

    @patch("google.cloud.bigquery.Client.create_dataset", side_effect=mock_create_dataset_success)
    @patch("google.cloud.bigquery.Client.create_table", side_effect=mock_create_table_success)
    @patch("google.cloud.bigquery.Client.insert_rows_json", side_effect=mock_insert_rows_json_success)
    def test_write_results_to_bq_success(self, mock_insert_rows_json_success, mock_create_table_success, mock_create_dataset_success):
        collected_tasks = mock_collected_tasks()
        write_results_to_bq(collected_tasks, mock_bq_config())
        self.assertEqual(1, mock_create_dataset_success.call_count)
        self.assertEqual(1, mock_create_table_success.call_count)
        self.assertEqual(len(collected_tasks["mubArkO3So8Co1X98CBo62-lSCM4tB-NZPOUGJ83N1o"]), mock_insert_rows_json_success.call_count)

    @patch("google.cloud.bigquery.Client.create_dataset", side_effect=mock_create_dataset_fail)
    @patch("google.cloud.bigquery.Client.create_table", side_effect=mock_create_table_success)
    @patch("google.cloud.bigquery.Client.insert_rows_json", side_effect=mock_insert_rows_json_success)
    def test_write_results_to_bq_create_dataset_fail(self, mock_insert_rows_json_success, mock_create_table_success, mock_create_dataset_fail):
        with pytest.raises(Exception, match='BQ create dataset Uh-oh'):
            write_results_to_bq(mock_collected_tasks(), mock_bq_config())
            self.assertEqual(1, mock_create_dataset_fail.call_count)
            self.assertEqual(0, mock_create_table_success.call_count)
            self.assertEqual(0, mock_insert_rows_json_success.call_count)

    @patch("google.cloud.bigquery.Client.create_dataset", side_effect=mock_create_dataset_success)
    @patch("google.cloud.bigquery.Client.create_table", side_effect=mock_create_table_fail)
    @patch("google.cloud.bigquery.Client.insert_rows_json", side_effect=mock_insert_rows_json_success)
    def test_write_results_to_bq_create_table_fail(self, mock_insert_rows_json_success, mock_create_table_fail, mock_create_dataset_success):
        with pytest.raises(Exception, match='Failed to create BQ table: some-gcp-project-id.ads_dap.incrementality'):
            write_results_to_bq(mock_collected_tasks(), mock_bq_config())
            self.assertEqual(1, mock_create_dataset_success.call_count)
            self.assertEqual(1, mock_create_dataset_fail.call_count)
            self.assertEqual(0, mock_insert_rows_json_success.call_count)

    @patch("google.cloud.bigquery.Client.create_dataset", side_effect=mock_create_dataset_success)
    @patch("google.cloud.bigquery.Client.create_table", side_effect=mock_create_table_success)
    @patch("google.cloud.bigquery.Client.insert_rows_json", side_effect=mock_insert_rows_json_fail)
    def test_write_results_to_bq_insert_rows_fail(self, mock_insert_rows_json_fail, mock_create_table_success, mock_create_dataset_success):
        with pytest.raises(Exception, match=re.escape("Error inserting rows into some-gcp-project-id.ads_dap.incrementality: [{'key': 0, 'errors': 'Problem writing bucket 1 results'}, {'key': 1, 'errors': 'Problem writing bucket 2 results'}, {'key': 2, 'errors': 'Problem writing bucket 3 results'}]")):
            write_results_to_bq(mock_collected_tasks(), mock_bq_config())
            self.assertEqual(1, mock_create_dataset_success.call_count)
            self.assertEqual(1, mock_create_table_success.call_count)
            self.assertEqual(1, mock_insert_rows_json_fail.call_count)
