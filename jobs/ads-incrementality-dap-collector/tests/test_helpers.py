from datetime import date, datetime
import os
import pytest
import re
import sys
from unittest import TestCase
from unittest.mock import call, patch


# Append the source code directory to the path
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../ads_incrementality_dap_collector")
    )
)

from tests.test_mocks import (  # noqa: E402
    mock_nimbus_success,
    mock_nimbus_fail,
    mock_nimbus_experiment,
    mock_control_row,
    mock_treatment_a_row,
    mock_treatment_b_row,
    mock_task_id,
    mock_nimbus_unparseable_experiment,
    mock_tasks_to_collect,
    mock_dap_config,
    mock_experiment_config,
    mock_experiment_config_with_default_duration,
    mock_dap_subprocess_success,
    mock_dap_subprocess_fail,
    mock_dap_subprocess_raise,
    mock_collected_tasks,
    mock_bq_config,
    mock_bq_table,
    mock_create_dataset,
    mock_create_dataset_fail,
    mock_create_table,
    mock_create_table_fail,
    mock_insert_rows_json,
    mock_insert_rows_json_fail,
)
from ads_incrementality_dap_collector.helpers import (  # noqa: E402
    get_experiment,
    prepare_results_rows,
    collect_dap_results,
    write_results_to_bq,
)
from ads_incrementality_dap_collector.constants import (  # noqa: E402
    COLLECTOR_RESULTS_SCHEMA,
    DEFAULT_BATCH_DURATION,
)


class TestHelpers(TestCase):
    @patch("requests.get", side_effect=mock_nimbus_success)
    def test_get_experiment_success(self, mock_fetch):
        experiment = get_experiment(mock_experiment_config(), "nimbus_api_url")
        self.assertEqual("traffic-impact-study-5", experiment.slug)
        self.assertEqual(
            mock_experiment_config().batch_duration, experiment.batchDuration
        )
        self.assertEqual(1, mock_fetch.call_count)

    @patch("requests.get", side_effect=mock_nimbus_success)
    def test_get_experiment_with_default_duration_success(self, mock_fetch):
        experiment = get_experiment(
            mock_experiment_config_with_default_duration(), "nimbus_api_url"
        )
        self.assertEqual("traffic-impact-study-5", experiment.slug)
        self.assertEqual(DEFAULT_BATCH_DURATION, experiment.batchDuration)
        self.assertEqual(1, mock_fetch.call_count)

    @patch("requests.get", side_effect=mock_nimbus_fail)
    def test_get_experiment_fail(self, mock_fetch):
        with pytest.raises(
            Exception,
            match="Failed getting experiment: traffic-impact-study-5 from: nimbus_api_url",
        ):
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
        collect_dap_results(
            tasks_to_collect, mock_dap_config(), mock_experiment_config()
        )
        self.assertEqual(1, mock_dap_subprocess_success.call_count)
        self.assertEqual(tasks_to_collect[task_id][1].value_count, 53)
        self.assertEqual(tasks_to_collect[task_id][2].value_count, 48)
        self.assertEqual(tasks_to_collect[task_id][3].value_count, 56)

    @patch("subprocess.run", side_effect=mock_dap_subprocess_fail)
    def test_collect_dap_results_fail(self, mock_dap_subprocess_fail):
        tasks_to_collect = mock_tasks_to_collect()
        with pytest.raises(
            Exception, match="Failed to parse collected DAP results: None"
        ):
            collect_dap_results(
                tasks_to_collect, mock_dap_config(), mock_experiment_config()
            )
            self.assertEqual(1, mock_dap_subprocess_success.call_count)

    @patch("subprocess.run", side_effect=mock_dap_subprocess_raise)
    def test_collect_dap_results_raise(self, mock_dap_subprocess_raise):
        tasks_to_collect = mock_tasks_to_collect()
        task_id = list(tasks_to_collect.keys())[0]
        with pytest.raises(
            Exception, match=f"Collection failed for {task_id}, 1, stderr: Uh-oh"
        ):
            collect_dap_results(
                tasks_to_collect, mock_dap_config(), mock_experiment_config()
            )
            self.assertEqual(1, mock_dap_subprocess_success.call_count)

    @patch("google.cloud.bigquery.Table")
    @patch("google.cloud.bigquery.Client")
    @patch("ads_incrementality_dap_collector.helpers.datetime")
    @patch("ads_incrementality_dap_collector.models.NimbusExperiment.todays_date")
    def test_write_results_to_bq_success(
        self,
        mock_todays_date,
        datetime_in_helpers,
        bq_client,
        bq_table,
    ):
        bq_client.return_value.create_dataset.side_effect = mock_create_dataset
        bq_client.return_value.create_table.side_effect = mock_create_table
        bq_client.return_value.insert_rows_json.side_effect = mock_insert_rows_json

        mock_datetime = datetime(2025, 9, 19, 16, 54, 34, 366228)
        datetime_in_helpers.now.return_value = mock_datetime
        datetime_in_helpers.side_effect = lambda *args, **kw: datetime(*args, **kw)

        mock_todays_date.return_value = date(2025, 9, 19)

        bq_config = mock_bq_config()
        collected_tasks = mock_collected_tasks()
        write_results_to_bq(collected_tasks, bq_config)

        bq_client.assert_called_once_with(project=bq_config.project)
        bq_table.assert_called_once_with(
            f"{bq_config.project}.{bq_config.namespace}.{bq_config.table}",
            schema=COLLECTOR_RESULTS_SCHEMA,
        )
        bq_client.return_value.create_dataset.assert_called_once_with(
            f"{bq_config.project}.{bq_config.namespace}", exists_ok=True
        )
        bq_client.return_value.create_table.assert_called_once_with(
            mock_bq_table(), exists_ok=True
        )

        calls = [
            call(
                table=f"{bq_config.project}.{bq_config.namespace}.{bq_config.table}",
                json_rows=[
                    {
                        "collection_start": "2025-09-08",
                        "collection_end": "2025-09-14",
                        "country_codes": '["US"]',
                        "experiment_slug": "traffic-impact-study-5",
                        "experiment_branch": "control",
                        "advertiser": "glamazon",
                        "metric": "unique_client_organic_visits",
                        "value": {"count": 13645, "histogram": None},
                        "created_at": mock_datetime.isoformat(),
                    }
                ],
            ),
            call(
                table=f"{bq_config.project}.{bq_config.namespace}.{bq_config.table}",
                json_rows=[
                    {
                        "collection_start": "2025-09-08",
                        "collection_end": "2025-09-14",
                        "country_codes": '["US"]',
                        "experiment_slug": "traffic-impact-study-5",
                        "experiment_branch": "treatment-b",
                        "advertiser": "glamazon",
                        "metric": "unique_client_organic_visits",
                        "value": {"count": 18645, "histogram": None},
                        "created_at": mock_datetime.isoformat(),
                    }
                ],
            ),
            call(
                table=f"{bq_config.project}.{bq_config.namespace}.{bq_config.table}",
                json_rows=[
                    {
                        "collection_start": "2025-09-08",
                        "collection_end": "2025-09-14",
                        "country_codes": '["US"]',
                        "experiment_slug": "traffic-impact-study-5",
                        "experiment_branch": "treatment-a",
                        "advertiser": "glamazon",
                        "metric": "unique_client_organic_visits",
                        "value": {"count": 9645, "histogram": None},
                        "created_at": mock_datetime.isoformat(),
                    }
                ],
            ),
        ]
        bq_client.return_value.insert_rows_json.assert_has_calls(calls)

    @patch("google.cloud.bigquery.Client")
    def test_write_results_to_bq_create_dataset_fail(
        self,
        bq_client,
    ):
        bq_client.return_value.create_dataset.side_effect = mock_create_dataset_fail
        bq_client.return_value.create_table.side_effect = mock_create_table
        bq_client.return_value.insert_rows_json.side_effect = mock_insert_rows_json
        bq_config = mock_bq_config()

        with pytest.raises(Exception, match="BQ create dataset Uh-oh"):
            write_results_to_bq(mock_collected_tasks(), bq_config)

            bq_client.return_value.create_dataset.assert_called_once_with(
                f"{bq_config.project}.{bq_config.namespace}", exists_ok=True
            )
            bq_client.return_value.create_table.assert_not_called()
            bq_client.return_value.insert_rows_json.assert_not_called()

    @patch("google.cloud.bigquery.Client")
    def test_write_results_to_bq_create_table_fail(
        self,
        bq_client,
    ):
        bq_client.return_value.create_dataset.side_effect = mock_create_dataset
        bq_client.return_value.create_table.side_effect = mock_create_table_fail
        bq_client.return_value.insert_rows_json.side_effect = mock_insert_rows_json
        bq_config = mock_bq_config()

        with pytest.raises(
            Exception,
            match=f"Failed to create BQ table: {bq_config.project}.{bq_config.namespace}.{bq_config.table}",
        ):

            write_results_to_bq(mock_collected_tasks(), bq_config)
            bq_client.return_value.create_dataset.assert_called_once_with(
                f"{bq_config.project}.{bq_config.namespace}", exists_ok=True
            )
            bq_client.return_value.create_table.assert_called_once_with(
                mock_bq_table(), exists_ok=True
            )
            bq_client.return_value.insert_rows_json.assert_not_called()

    @patch("google.cloud.bigquery.Client")
    def test_write_results_to_bq_insert_rows_fail(
        self,
        bq_client,
    ):
        bq_client.return_value.create_dataset.side_effect = mock_create_dataset
        bq_client.return_value.create_table.side_effect = mock_create_table
        bq_client.return_value.insert_rows_json.side_effect = mock_insert_rows_json_fail
        bq_config = mock_bq_config()
        mock_datetime = datetime(2025, 9, 19, 16, 54, 34, 366228)

        with pytest.raises(
            Exception,
            match=re.escape(
                "Error inserting rows into some-gcp-project-id.ads_dap.incrementality: [{'key': 0, 'errors': 'Problem writing bucket 1 results'}, {'key': 1, 'errors': 'Problem writing bucket 2 results'}, {'key': 2, 'errors': 'Problem writing bucket 3 results'}]"  # noqa: E501
            ),
        ):
            write_results_to_bq(mock_collected_tasks(), bq_config)
            bq_client.return_value.create_dataset.assert_called_once_with(
                f"{bq_config.project}.{bq_config.namespace}", exists_ok=True
            )
            bq_client.return_value.create_table.assert_called_once_with(
                mock_bq_table(), exists_ok=True
            )
            calls = [
                call(
                    table="some-gcp-project-id.ads_dap.incrementality",
                    json_rows=[
                        {
                            "collection_start": "2025-09-08",
                            "collection_end": "2025-09-15",
                            "country_codes": '["US"]',
                            "experiment_slug": "traffic-impact-study-5",
                            "experiment_branch": "control",
                            "advertiser": "glamazon",
                            "metric": "unique_client_organic_visits",
                            "value": {"count": 13645, "histogram": None},
                            "created_at": mock_datetime.isoformat(),
                        }
                    ],
                ),
                call(
                    table="some-gcp-project-id.ads_dap.incrementality",
                    json_rows=[
                        {
                            "collection_start": "2025-09-08",
                            "collection_end": "2025-09-15",
                            "country_codes": '["US"]',
                            "experiment_slug": "traffic-impact-study-5",
                            "experiment_branch": "treatment-b",
                            "advertiser": "glamazon",
                            "metric": "unique_client_organic_visits",
                            "value": {"count": 18645, "histogram": None},
                            "created_at": mock_datetime.isoformat(),
                        }
                    ],
                ),
                call(
                    table="some-gcp-project-id.ads_dap.incrementality",
                    json_rows=[
                        {
                            "collection_start": "2025-09-08",
                            "collection_end": "2025-09-15",
                            "country_codes": '["US"]',
                            "experiment_slug": "traffic-impact-study-5",
                            "experiment_branch": "treatment-a",
                            "advertiser": "glamazon",
                            "metric": "unique_client_organic_visits",
                            "value": {"count": 9645, "histogram": None},
                            "created_at": mock_datetime.isoformat(),
                        }
                    ],
                ),
            ]
            bq_client.return_value.insert_rows_json.assert_has_calls(calls)
