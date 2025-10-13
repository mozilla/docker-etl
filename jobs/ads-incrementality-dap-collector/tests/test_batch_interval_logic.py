from datetime import date
from unittest import TestCase

from tests.test_mocks import mock_nimbus_experiment  # noqa: E402


class TestHelpers(TestCase):

    # Tests for default batch duration of 7 days
    def test_batch_interval_for_future_experiment_start_date(self):  # , todays_date):
        # Mock experiment starts on 2025-08-18 and has batch duration 7 days
        experiment = mock_nimbus_experiment("2025-08-01")

        self.assertEqual(False, experiment.collect_today())
        self.assertEqual(date(2025, 8, 18), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 8, 24), experiment.latest_collectible_batch_end())

    def test_batch_interval_for_todays_date_is_the_experiment_start_date(self):
        # Mock experiment starts on 2025-08-18 and has batch duration 7 days
        experiment = mock_nimbus_experiment("2025-08-18")

        self.assertEqual(False, experiment.collect_today())
        self.assertEqual(date(2025, 8, 18), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 8, 24), experiment.latest_collectible_batch_end())

    def test_batch_interval_for_todays_date_in_middle_of_first_batch(self):
        # Mock experiment starts on 2025-08-18 and has batch duration 7 days
        experiment = mock_nimbus_experiment("2025-08-18")

        self.assertEqual(False, experiment.collect_today())
        self.assertEqual(date(2025, 8, 18), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 8, 24), experiment.latest_collectible_batch_end())

    def test_batch_interval_for_todays_date_is_the_end_date_of_first_batch(self):
        # Mock experiment starts on 2025-08-18 and has batch duration 7 days
        experiment = mock_nimbus_experiment("2025-08-24")

        self.assertEqual(False, experiment.collect_today())
        self.assertEqual(date(2025, 8, 18), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 8, 24), experiment.latest_collectible_batch_end())

    def test_batch_interval_for_todays_date_is_start_date_of_subsequent_batch(self):
        # Mock experiment starts on 2025-08-18 and has batch duration 7 days
        experiment = mock_nimbus_experiment("2025-09-08")

        self.assertEqual(True, experiment.collect_today())
        self.assertEqual(date(2025, 9, 1), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 9, 7), experiment.latest_collectible_batch_end())

    def test_batch_interval_for_todays_date_in_middle_of_subsequent_batch(self):
        # Mock experiment starts on 2025-08-18 and has batch duration 7 days
        experiment = mock_nimbus_experiment("2025-10-07")

        self.assertEqual(False, experiment.collect_today())
        self.assertEqual(date(2025, 9, 29), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 10, 5), experiment.latest_collectible_batch_end())

    def test_batch_interval_for_todays_date_is_end_date_of_subsequent_batch(self):
        # Mock experiment starts on 2025-08-18 and has batch duration 7 days
        experiment = mock_nimbus_experiment("2025-09-14")

        self.assertEqual(False, experiment.collect_today())
        self.assertEqual(date(2025, 9, 1), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 9, 7), experiment.latest_collectible_batch_end())

    # Tests for non-default batch durations
    def test_non_default_batch_interval_for_todays_date_is_in_the_middle_of_first_batch(
        self,
    ):
        experiment = mock_nimbus_experiment("2025-08-20")
        # Mock experiment starts on 2025-08-18 and has batch duration 5 days
        experiment.batchDuration = 432000

        self.assertEqual(False, experiment.collect_today())
        self.assertEqual(date(2025, 8, 18), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 8, 22), experiment.latest_collectible_batch_end())

    def test_non_default_batch_interval_for_todays_date_is_the_end_date_of_first_batch(
        self,
    ):
        experiment = mock_nimbus_experiment("2025-08-22")
        # Mock experiment starts on 2025-08-18 and has batch duration 5 days
        experiment.batchDuration = 432000

        self.assertEqual(False, experiment.collect_today())
        self.assertEqual(date(2025, 8, 18), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 8, 22), experiment.latest_collectible_batch_end())

    def test_non_default_batch_interval_for_todays_date_is_start_date_of_subsequent_batch(
        self,
    ):
        experiment = mock_nimbus_experiment("2025-08-28")
        # Mock experiment starts on 2025-08-18 and has batch duration 5 days
        experiment.batchDuration = 432000

        self.assertEqual(True, experiment.collect_today())
        self.assertEqual(date(2025, 8, 23), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 8, 27), experiment.latest_collectible_batch_end())

    def test_non_default_batch_interval_for_todays_date_in_middle_of_subsequent_batch(
        self,
    ):
        experiment = mock_nimbus_experiment("2025-08-31")
        # Mock experiment starts on 2025-08-18 and has batch duration 5 days
        experiment.batchDuration = 432000

        self.assertEqual(False, experiment.collect_today())
        self.assertEqual(date(2025, 8, 23), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 8, 27), experiment.latest_collectible_batch_end())

    def test_non_default_batch_interval_for_todays_date_is_end_date_of_subsequent_batch(
        self,
    ):
        experiment = mock_nimbus_experiment("2025-09-01")
        # Mock experiment starts on 2025-08-18 and has batch duration 5 days
        experiment.batchDuration = 432000

        self.assertEqual(False, experiment.collect_today())
        self.assertEqual(date(2025, 8, 23), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 8, 27), experiment.latest_collectible_batch_end())
