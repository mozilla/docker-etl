from datetime import date
from unittest import TestCase

from tests.test_mocks import mock_visit_experiment  # noqa: E402


class TestHelpers(TestCase):

    # Tests for default batch duration of 7 days
    def test_batch_interval_for_future_experiment_start_date(self):
        # Mock experiment starts on 2025-08-18 and has batch duration 7 days
        # Process date is 2025-08-01
        experiment = mock_visit_experiment("2025-08-01")

        self.assertEqual(False, experiment.should_collect_batch())
        self.assertEqual(date(2025, 8, 18), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 8, 24), experiment.latest_collectible_batch_end())

    def test_batch_interval_for_process_date_is_the_experiment_start_date(self):
        # Mock experiment starts on 2025-08-18 and has batch duration 7 days
        # Process date is 2025-08-18
        experiment = mock_visit_experiment("2025-08-18")

        self.assertEqual(False, experiment.should_collect_batch())
        self.assertEqual(date(2025, 8, 18), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 8, 24), experiment.latest_collectible_batch_end())

    def test_batch_interval_for_process_date_in_middle_of_first_batch(self):
        # Mock experiment starts on 2025-08-18 and has batch duration 7 days
        # Process date is 2025-08-22
        experiment = mock_visit_experiment("2025-08-22")

        self.assertEqual(False, experiment.should_collect_batch())
        self.assertEqual(date(2025, 8, 18), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 8, 24), experiment.latest_collectible_batch_end())

    def test_batch_interval_for_process_date_is_the_end_date_of_first_batch(self):
        # Mock experiment starts on 2025-08-18 and has batch duration 7 days
        # Process date is 2025-08-24
        experiment = mock_visit_experiment("2025-08-24")

        self.assertEqual(True, experiment.should_collect_batch())
        self.assertEqual(date(2025, 8, 18), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 8, 24), experiment.latest_collectible_batch_end())

    def test_batch_interval_for_process_date_is_start_date_of_subsequent_batch(self):
        # Mock experiment starts on 2025-08-18 and has batch duration 7 days
        # Process date is 2025-09-08
        experiment = mock_visit_experiment("2025-09-08")

        self.assertEqual(False, experiment.should_collect_batch())
        self.assertEqual(date(2025, 9, 1), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 9, 7), experiment.latest_collectible_batch_end())

    def test_batch_interval_for_process_date_in_middle_of_subsequent_batch(self):
        # Mock experiment starts on 2025-08-18 and has batch duration 7 days
        # Process date 2025-10-07
        experiment = mock_visit_experiment("2025-10-07")

        self.assertEqual(False, experiment.should_collect_batch())
        self.assertEqual(date(2025, 9, 29), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 10, 5), experiment.latest_collectible_batch_end())

    def test_batch_interval_for_process_date_is_end_date_of_subsequent_batch(self):
        # Mock experiment starts on 2025-08-18 and has batch duration 7 days
        # Process date is 2025-09-14
        experiment = mock_visit_experiment("2025-09-14")

        self.assertEqual(True, experiment.should_collect_batch())
        self.assertEqual(date(2025, 9, 8), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 9, 14), experiment.latest_collectible_batch_end())

    # Tests for non-default batch durations
    def test_non_default_batch_interval_for_process_date_is_in_the_middle_of_first_batch(
        self,
    ):
        # Mock experiment starts on 2025-08-18 and has batch duration 5 days
        # Process date is 2025-08-20
        experiment = mock_visit_experiment("2025-08-20")
        experiment.batchDuration = 432000

        self.assertEqual(False, experiment.should_collect_batch())
        self.assertEqual(date(2025, 8, 18), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 8, 22), experiment.latest_collectible_batch_end())

    def test_non_default_batch_interval_for_process_date_is_the_end_date_of_first_batch(
        self,
    ):
        # Mock experiment starts on 2025-08-18 and has batch duration 5 days
        # Process date is 2025-08-22
        experiment = mock_visit_experiment("2025-08-22")
        experiment.batchDuration = 432000

        self.assertEqual(True, experiment.should_collect_batch())
        self.assertEqual(date(2025, 8, 18), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 8, 22), experiment.latest_collectible_batch_end())

    def test_non_default_batch_interval_for_process_date_is_start_date_of_subsequent_batch(
        self,
    ):
        # Mock experiment starts on 2025-08-18 and has batch duration 5 days
        # Process date is 2025-08-28
        experiment = mock_visit_experiment("2025-08-28")
        experiment.batchDuration = 432000

        self.assertEqual(False, experiment.should_collect_batch())
        self.assertEqual(date(2025, 8, 23), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 8, 27), experiment.latest_collectible_batch_end())

    def test_non_default_batch_interval_for_process_date_in_middle_of_subsequent_batch(
        self,
    ):
        # Mock experiment starts on 2025-08-18 and has batch duration 5 days
        # Process date is 2025-08-31
        experiment = mock_visit_experiment("2025-08-31")
        experiment.batchDuration = 432000

        self.assertEqual(False, experiment.should_collect_batch())
        self.assertEqual(date(2025, 8, 23), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 8, 27), experiment.latest_collectible_batch_end())

    def test_non_default_batch_interval_for_process_date_is_end_date_of_subsequent_batch(
        self,
    ):
        # Mock experiment starts on 2025-08-18 and has batch duration 5 days
        # Process date is 2025-09-01
        experiment = mock_visit_experiment("2025-09-01")
        experiment.batchDuration = 432000

        self.assertEqual(True, experiment.should_collect_batch())
        self.assertEqual(date(2025, 8, 28), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 9, 1), experiment.latest_collectible_batch_end())
