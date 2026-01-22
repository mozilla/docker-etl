from datetime import date

from unittest import TestCase
from unittest.mock import patch

from ads_attribution_dap_collector.collect import (
    current_batch_start,
    current_batch_end,
    _should_collect_batch,
    _parse_http_error,
    _correct_wraparound,
    _parse_histogram,
    collect_dap_result,
    get_aggregated_results,
)

from tests.test_mocks import (
    JAN_1_2026,
    DURATION_3_DAYS,
    DURATION_7_DAYS,
    DURATION_1_DAY,
    mock_dap_subprocess_success,
    MOCK_TASK_ID_1,
)


class TestHelpers(TestCase):
    def test_current_batch_start_3_day(self):
        """
        batches
        [2026-01-01 : 2026-01-03]
        [2026-01-04 : 2026-01-06]
        [2026-01-07 : 2026-01-09]
        [2026-01-10 : 2026-01-12]
        [2026-01-13 : 2026-01-15]
        [2026-01-16 : 2026-01-18]
        [2026-01-19 : 2026-01-21]
        """

        # process date at start of first batch
        batch_start = current_batch_start(
            process_date=date(2026, 1, 1),
            partner_start_date=JAN_1_2026,
            duration=DURATION_3_DAYS,
        )
        self.assertEqual(batch_start, date(2026, 1, 1))

        # process date middle of first batch
        batch_start = current_batch_start(
            process_date=date(2026, 1, 2),
            partner_start_date=JAN_1_2026,
            duration=DURATION_3_DAYS,
        )
        self.assertEqual(batch_start, date(2026, 1, 1))

        # process date end of first batch
        batch_start = current_batch_start(
            process_date=date(2026, 1, 3),
            partner_start_date=JAN_1_2026,
            duration=DURATION_3_DAYS,
        )
        self.assertEqual(batch_start, date(2026, 1, 1))

        # process date at start of subsequent batch
        batch_start = current_batch_start(
            process_date=date(2026, 1, 10),
            partner_start_date=JAN_1_2026,
            duration=DURATION_3_DAYS,
        )
        self.assertEqual(batch_start, date(2026, 1, 7))

        # process date middle of subsequent batch
        batch_start = current_batch_start(
            process_date=date(2026, 1, 11),
            partner_start_date=JAN_1_2026,
            duration=DURATION_3_DAYS,
        )
        self.assertEqual(batch_start, date(2026, 1, 7))

        # process date end of subsequent batch
        batch_start = current_batch_start(
            process_date=date(2026, 1, 12),
            partner_start_date=JAN_1_2026,
            duration=DURATION_3_DAYS,
        )
        self.assertEqual(batch_start, date(2026, 1, 10))

    def test_current_batch_start_1_day(self):
        """
        batches
        [2026-01-01 : 2026-01-01]
        [2026-01-02 : 2026-01-02]
        [2026-01-03 : 2026-01-03]
        [2026-01-04 : 2026-01-04]
        """

        # process date before start of first batch
        batch_start = current_batch_start(
            process_date=date(2025, 12, 30),
            partner_start_date=JAN_1_2026,
            duration=DURATION_1_DAY,
        )
        self.assertIsNone(batch_start)

        # process date at start of first batch
        batch_start = current_batch_start(
            process_date=date(2026, 1, 1),
            partner_start_date=JAN_1_2026,
            duration=DURATION_1_DAY,
        )
        self.assertEqual(batch_start, date(2026, 1, 1))

        # process date at start of subsequent batch
        batch_start = current_batch_start(
            process_date=date(2026, 1, 15),
            partner_start_date=JAN_1_2026,
            duration=DURATION_1_DAY,
        )
        self.assertEqual(batch_start, date(2026, 1, 15))

    def test_current_batch_end_(self):
        batch_end = current_batch_end(
            batch_start=date(2026, 1, 1), duration=DURATION_3_DAYS
        )
        self.assertEqual(batch_end, date(2026, 1, 3))

        batch_end = current_batch_end(
            batch_start=date(2026, 1, 10), duration=DURATION_7_DAYS
        )
        self.assertEqual(batch_end, date(2026, 1, 16))

        batch_end = current_batch_end(
            batch_start=date(2026, 1, 12), duration=DURATION_1_DAY
        )
        self.assertEqual(batch_end, date(2026, 1, 12))

    def test_should_collect_batch(self):
        # process_date is end of batch
        batch_end = current_batch_end(
            batch_start=date(2026, 1, 1), duration=DURATION_3_DAYS
        )
        process_date = date(2026, 1, 3)
        self.assertTrue(_should_collect_batch(process_date, batch_end))

        # process date is not end of batch
        batch_end = current_batch_end(
            batch_start=date(2026, 1, 1), duration=DURATION_3_DAYS
        )
        process_date = date(2026, 1, 2)
        self.assertFalse(_should_collect_batch(process_date, batch_end))

        # process date is start of batch
        batch_end = current_batch_end(
            batch_start=date(2026, 1, 1), duration=DURATION_3_DAYS
        )
        process_date = date(2026, 1, 1)
        self.assertFalse(_should_collect_batch(process_date, batch_end))

    def test_parse_http_error_400(self):
        msg = (
            "HTTP response status 400 Bad Request - "
            "The number of reports included in the batch is invalid."
        )

        status_code, status_text, error_message = _parse_http_error(msg)

        self.assertEqual(status_code, 400)
        self.assertEqual(status_text, "Bad Request")
        self.assertEqual(
            error_message, "The number of reports included in the batch is invalid."
        )

    def test_parse_http_error_404(self):
        msg = "HTTP response status 404 Not Found"

        status_code, status_text, error_message = _parse_http_error(msg)

        self.assertEqual(status_code, 404)
        self.assertEqual(status_text, "Not Found")
        self.assertIsNone(error_message)

    def test_correct_wraparound(self):
        wrapped = _correct_wraparound(340282366920938462946865773367900766210)
        self.assertEqual(wrapped, 1)

    def test_parse_histogram(self):
        histogram_string = "5,3, 6,0, 8"
        parse_dict = _parse_histogram(histogram_string)
        self.assertEqual(parse_dict[0], 5)
        self.assertEqual(parse_dict[1], 3)
        self.assertEqual(parse_dict[2], 6)
        self.assertEqual(parse_dict[3], 0)
        self.assertEqual(parse_dict[4], 8)

    @patch("subprocess.run", side_effect=mock_dap_subprocess_success)
    def test_collect_dap_result_success(self, mock_dap_subprocess_success):
        task_id = MOCK_TASK_ID_1
        collected_tasks = collect_dap_result(
            task_id=task_id,
            vdaf_length=4,
            batch_start=date(2026, 1, 1),
            duration=123,
            bearer_token="token",
            hpke_config="config",
            hpke_private_key="private_key",
        )
        self.assertEqual(1, mock_dap_subprocess_success.call_count)
        self.assertEqual(len(collected_tasks), 4)
        self.assertEqual(collected_tasks[1], 11)
        self.assertEqual(collected_tasks[2], 22)
        self.assertEqual(collected_tasks[3], 33)

    @patch("subprocess.run", side_effect=mock_dap_subprocess_success)
    def test_get_aggregated_results(self, mock_dap_subprocess_success):
        task_id = MOCK_TASK_ID_1
        process_date = date(2026, 1, 7)
        batch_end = current_batch_end(batch_start=JAN_1_2026, duration=DURATION_7_DAYS)

        aggregated_results = get_aggregated_results(
            process_date=process_date,
            task_id=task_id,
            vdaf_length=4,
            batch_start=JAN_1_2026,
            batch_end=batch_end,
            collector_duration=DURATION_7_DAYS,
            bearer_token="token",
            hpke_config="config",
            hpke_private_key="private_key",
        )
        self.assertIsNotNone(aggregated_results)

        process_date = date(2026, 1, 8)
        aggregated_results = get_aggregated_results(
            process_date=process_date,
            task_id=task_id,
            vdaf_length=4,
            batch_start=JAN_1_2026,
            batch_end=batch_end,
            collector_duration=DURATION_7_DAYS,
            bearer_token="token",
            hpke_config="config",
            hpke_private_key="private_key",
        )
        self.assertIsNone(aggregated_results)
