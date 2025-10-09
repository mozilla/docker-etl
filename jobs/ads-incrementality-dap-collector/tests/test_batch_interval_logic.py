from datetime import date
from unittest import TestCase

from tests.test_mocks import mock_nimbus_experiment  # noqa: E402


class TestHelpers(TestCase):

    # Tests for default batch duration of 7 days
    def test_batch_interval_for_future_experiment_start_date(self):#, todays_date):
        mock_date = date(2025, 8, 1)
        # Mock experiment starts on 2025-08-18 and has batch duration 7 days
        experiment = mock_nimbus_experiment()

        self.assertEqual(False, experiment.collect_today(mock_date))
        self.assertEqual(date(2025, 8, 18), experiment.latest_collectible_batch_start(mock_date))
        self.assertEqual(date(2025, 8, 24), experiment.latest_collectible_batch_end(mock_date))

    def test_batch_interval_for_todays_date_is_the_experiment_start_date(self):
        mock_date = date(2025, 8, 18)
        # Mock experiment starts on 2025-08-18 and has batch duration 7 days
        experiment = mock_nimbus_experiment()

        self.assertEqual(False, experiment.collect_today(mock_date))
        self.assertEqual(date(2025, 8, 18), experiment.latest_collectible_batch_start(mock_date))
        self.assertEqual(date(2025, 8, 24), experiment.latest_collectible_batch_end(mock_date))

    def test_batch_interval_for_todays_date_in_middle_of_first_batch(self):
        mock_date = date(2025, 8, 22)
        # Mock experiment starts on 2025-08-18 and has batch duration 7 days
        experiment = mock_nimbus_experiment()

        self.assertEqual(False, experiment.collect_today(mock_date))
        self.assertEqual(date(2025, 8, 18), experiment.latest_collectible_batch_start(mock_date))
        self.assertEqual(date(2025, 8, 24), experiment.latest_collectible_batch_end(mock_date))

    def test_batch_interval_for_todays_date_is_the_end_date_of_first_batch(self):
        mock_date = date(2025, 8, 24)
        # Mock experiment starts on 2025-08-18 and has batch duration 7 days
        experiment = mock_nimbus_experiment()

        self.assertEqual(False, experiment.collect_today(mock_date))
        self.assertEqual(date(2025, 8, 18), experiment.latest_collectible_batch_start(mock_date))
        self.assertEqual(date(2025, 8, 24), experiment.latest_collectible_batch_end(mock_date))

    def test_batch_interval_for_todays_date_is_start_date_of_subsequent_batch(self):
        mock_date = date(2025, 9, 8)
        # Mock experiment starts on 2025-08-18 and has batch duration 7 days
        experiment = mock_nimbus_experiment()

        self.assertEqual(True, experiment.collect_today(mock_date))
        self.assertEqual(date(2025, 9, 1), experiment.latest_collectible_batch_start(mock_date))
        self.assertEqual(date(2025, 9, 7), experiment.latest_collectible_batch_end(mock_date))

    def test_batch_interval_for_todays_date_in_middle_of_subsequent_batch(self):
        mock_date = date(2025, 10, 7)
        # Mock experiment starts on 2025-08-18 and has batch duration 7 days
        experiment = mock_nimbus_experiment()

        self.assertEqual(True, experiment.collect_today(mock_date))
        self.assertEqual(date(2025, 9, 29), experiment.latest_collectible_batch_start(mock_date))
        self.assertEqual(date(2025, 10, 5), experiment.latest_collectible_batch_end(mock_date))

    def test_batch_interval_for_todays_date_is_end_date_of_subsequent_batch(self):
        mock_date = date(2025, 9, 14)
        # Mock experiment starts on 2025-08-18 and has batch duration 7 days
        experiment = mock_nimbus_experiment()

        self.assertEqual(True, experiment.collect_today(mock_date))
        self.assertEqual(date(2025, 9, 1), experiment.latest_collectible_batch_start(mock_date))
        self.assertEqual(date(2025, 9, 7), experiment.latest_collectible_batch_end(mock_date))

    # Tests for non-default batch durations
    def test_non_default_batch_interval_for_todays_date_is_in_the_middle_of_first_batch(self):
        mock_date = date(2025, 8, 20)
        experiment = mock_nimbus_experiment()
        # Mock experiment starts on 2025-08-18 and has batch duration 5 days
        experiment.batchDuration = 432000

        self.assertEqual(False, experiment.collect_today(mock_date))
        self.assertEqual(date(2025, 8, 18), experiment.latest_collectible_batch_start(mock_date))
        self.assertEqual(date(2025, 8, 22), experiment.latest_collectible_batch_end(mock_date))

    def test_non_default_batch_interval_for_todays_date_is_the_end_date_of_first_batch(self):
        mock_date = date(2025, 8, 22)
        experiment = mock_nimbus_experiment()
        # Mock experiment starts on 2025-08-18 and has batch duration 5 days
        experiment.batchDuration = 432000

        self.assertEqual(False, experiment.collect_today(mock_date))
        self.assertEqual(date(2025, 8, 18), experiment.latest_collectible_batch_start(mock_date))
        self.assertEqual(date(2025, 8, 22), experiment.latest_collectible_batch_end(mock_date))

    def test_non_default_batch_interval_for_todays_date_is_start_date_of_subsequent_batch(self):
        mock_date = date(2025, 8, 28)
        experiment = mock_nimbus_experiment()
        # Mock experiment starts on 2025-08-18 and has batch duration 5 days
        experiment.batchDuration = 432000

        self.assertEqual(True, experiment.collect_today(mock_date))
        self.assertEqual(date(2025, 8, 23), experiment.latest_collectible_batch_start(mock_date))
        self.assertEqual(date(2025, 8, 27), experiment.latest_collectible_batch_end(mock_date))

    def test_non_default_batch_interval_for_todays_date_in_middle_of_subsequent_batch(self):
        mock_date = date(2025, 8, 31)
        experiment = mock_nimbus_experiment()
        # Mock experiment starts on 2025-08-18 and has batch duration 5 days
        experiment.batchDuration = 432000

        self.assertEqual(True, experiment.collect_today(mock_date))
        self.assertEqual(date(2025, 8, 23), experiment.latest_collectible_batch_start(mock_date))
        self.assertEqual(date(2025, 8, 27), experiment.latest_collectible_batch_end(mock_date))

    def test_non_default_batch_interval_for_todays_date_is_end_date_of_subsequent_batch(self):
        mock_date = date(2025, 9, 1)
        experiment = mock_nimbus_experiment()
        # Mock experiment starts on 2025-08-18 and has batch duration 5 days
        experiment.batchDuration = 432000

        self.assertEqual(True, experiment.collect_today(mock_date))
        self.assertEqual(date(2025, 8, 23), experiment.latest_collectible_batch_start(mock_date))
        self.assertEqual(date(2025, 8, 27), experiment.latest_collectible_batch_end(mock_date))
