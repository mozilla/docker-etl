import pytest
import re
from unittest import TestCase
from unittest.mock import call, patch
from tests.test_mocks import (
    JAN_1_2026,
    JAN_7_2026,
    mock_create_dataset,
    mock_create_table,
    mock_insert_rows_json,
    mock_insert_rows_json_fail,
    mock_bq_table,
)

from ads_attribution_dap_collector.persist import (
    NAMESPACE,
    COLLECTOR_RESULTS_SCHEMA,
    create_bq_table_if_not_exists,
    create_bq_row,
    insert_into_bq,
)

from google.cloud import bigquery

CREATED_TIMESTAMP = "2026-01-20T15:56:39.003071"


class TestHelpers(TestCase):
    @patch("google.cloud.bigquery.Table")
    @patch("google.cloud.bigquery.Client")
    def test_write_record_to_bigquery_success(
        self,
        bq_client,
        bq_table,
    ):
        bq_client.return_value.create_dataset.side_effect = mock_create_dataset
        bq_client.return_value.create_table.side_effect = mock_create_table
        bq_client.return_value.insert_rows_json.side_effect = mock_insert_rows_json

        project_id = "test-project-id"
        bq_test_client = bigquery.Client(project=project_id)

        full_table_id = create_bq_table_if_not_exists(project_id, bq_test_client)

        self.assertIn("ads_dap_derived", full_table_id)
        self.assertIn("newtab_attribution_v1", full_table_id)

        bq_client.return_value.create_dataset.assert_called_once_with(
            f"{project_id}.{NAMESPACE}", exists_ok=True
        )

        bq_client.assert_called_once_with(project=project_id)
        bq_table.assert_called_once_with(
            full_table_id,
            schema=COLLECTOR_RESULTS_SCHEMA,
        )

        bq_client.return_value.create_table.assert_called_once_with(
            mock_bq_table(), exists_ok=True
        )
        row = create_bq_row(
            collection_start=JAN_1_2026,
            collection_end=JAN_7_2026,
            provider="test",
            ad_id=1234,
            lookback_window=7,
            conversion_type="default",
            conversion_count=150,
        )

        # overwrite created_timestamp for test stability
        row["created_timestamp"] = CREATED_TIMESTAMP
        insert_into_bq(row, bq_test_client, full_table_id)

        calls = [
            call(
                table=full_table_id,
                json_rows=[
                    {
                        "collection_start": "2026-01-01",
                        "collection_end": "2026-01-07",
                        "provider": "test",
                        "ad_id": 1234,
                        "lookback_window": 7,
                        "conversion_type": "default",
                        "conversion_count": 150,
                        "created_timestamp": CREATED_TIMESTAMP,
                    }
                ],
            )
        ]
        bq_client.return_value.insert_rows_json.assert_has_calls(calls)

    # what can cause it to fail
    @patch("google.cloud.bigquery.Table")
    @patch("google.cloud.bigquery.Client")
    def test_write_record_to_bigquery_fail_insert_row(
        self,
        bq_client,
        bq_table,
    ):
        bq_client.return_value.create_dataset.side_effect = mock_create_dataset
        bq_client.return_value.create_table.side_effect = mock_create_table
        bq_client.return_value.insert_rows_json.side_effect = mock_insert_rows_json_fail

        project_id = "test-project-id"
        bq_test_client = bigquery.Client(project=project_id)

        full_table_id = create_bq_table_if_not_exists(project_id, bq_test_client)

        row = create_bq_row(
            collection_start=JAN_1_2026,
            collection_end=JAN_7_2026,
            provider="test",
            ad_id=1234,
            lookback_window=7,
            conversion_type="default",
            conversion_count=150,
        )

        with pytest.raises(
            Exception,
            match=re.escape(
                "test-project-id.ads_dap_derived.newtab_attribution_v1: "
                "[{'key': 0, 'errors': 'Problem writing bucket 1 results'}]"
            ),
        ):
            insert_into_bq(row, bq_test_client, full_table_id)
