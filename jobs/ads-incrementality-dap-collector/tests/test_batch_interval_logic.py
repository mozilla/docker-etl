from datetime import date
from unittest import TestCase
from unittest.mock import patch

from tests.test_mocks import (  # noqa: E402
    mock_nimbus_experiment
)


class TestHelpers(TestCase):

    # Tests for default batch duration of 7 days
    @patch("tests.test_mocks.NimbusExperiment.todays_date")
    def test_batch_interval_for_future_experiment_start_date(self, todays_date):
        mock_date = date(2025, 8, 1)
        todays_date.return_value = mock_date
        # Mock experiment starts on 2025-08-18 and has batch duration 7 days
        experiment = mock_nimbus_experiment()

        self.assertEqual(False, experiment.collect_today())
        self.assertEqual(date(2025, 8, 18), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 8, 24), experiment.latest_collectible_batch_end())

    @patch("tests.test_mocks.NimbusExperiment.todays_date")
    def test_batch_interval_for_todays_date_is_the_experiment_start_date(self, todays_date):
        mock_date = date(2025, 8, 18)
        todays_date.return_value = mock_date
        # Mock experiment starts on 2025-08-18 and has batch duration 7 days
        experiment = mock_nimbus_experiment()

        self.assertEqual(False, experiment.collect_today())
        self.assertEqual(date(2025, 8, 18), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 8, 24), experiment.latest_collectible_batch_end())

    @patch("tests.test_mocks.NimbusExperiment.todays_date")
    def test_batch_interval_for_todays_date_in_middle_of_first_batch(self, todays_date):
        mock_date = date(2025, 8, 22)
        todays_date.return_value = mock_date
        # Mock experiment starts on 2025-08-18 and has batch duration 7 days
        experiment = mock_nimbus_experiment()

        self.assertEqual(False, experiment.collect_today())
        self.assertEqual(date(2025, 8, 18), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 8, 24), experiment.latest_collectible_batch_end())

    @patch("tests.test_mocks.NimbusExperiment.todays_date")
    def test_batch_interval_for_todays_date_is_the_end_date_of_first_batch(self, todays_date):
        mock_date = date(2025, 8, 24)
        todays_date.return_value = mock_date
        # Mock experiment starts on 2025-08-18 and has batch duration 7 days
        experiment = mock_nimbus_experiment()

        self.assertEqual(False, experiment.collect_today())
        self.assertEqual(date(2025, 8, 18), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 8, 24), experiment.latest_collectible_batch_end())

    @patch("tests.test_mocks.NimbusExperiment.todays_date")
    def test_batch_interval_for_todays_date_is_start_date_of_subsequent_batch(self, todays_date):
        mock_date = date(2025, 9, 8)
        todays_date.return_value = mock_date
        # Mock experiment starts on 2025-08-18 and has batch duration 7 days
        experiment = mock_nimbus_experiment()

        self.assertEqual(True, experiment.collect_today())
        self.assertEqual(date(2025, 9, 1), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 9, 7), experiment.latest_collectible_batch_end())

    @patch("tests.test_mocks.NimbusExperiment.todays_date")
    def test_batch_interval_for_todays_date_in_middle_of_subsequent_batch(self, todays_date):
        mock_date = date(2025, 10, 7)
        todays_date.return_value = mock_date
        # Mock experiment starts on 2025-08-18 and has batch duration 7 days
        experiment = mock_nimbus_experiment()

        self.assertEqual(True, experiment.collect_today())
        self.assertEqual(date(2025, 9, 29), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 10, 5), experiment.latest_collectible_batch_end())

    @patch("tests.test_mocks.NimbusExperiment.todays_date")
    def test_batch_interval_for_todays_date_is_end_date_of_subsequent_batch(self, todays_date):
        mock_date = date(2025, 9, 14)
        todays_date.return_value = mock_date
        # Mock experiment starts on 2025-08-18 and has batch duration 7 days
        experiment = mock_nimbus_experiment()

        self.assertEqual(True, experiment.collect_today())
        self.assertEqual(date(2025, 9, 1), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 9, 7), experiment.latest_collectible_batch_end())

    # Tests for non-default batch durations
    @patch("tests.test_mocks.NimbusExperiment.todays_date")
    def test_non_default_batch_interval_for_todays_date_is_in_the_middle_of_first_batch(self, todays_date):
        mock_date = date(2025, 8, 20)
        todays_date.return_value = mock_date
        experiment = mock_nimbus_experiment()
        # Mock experiment starts on 2025-08-18 and has batch duration 5 days
        experiment.batchDuration = 432000

        self.assertEqual(False, experiment.collect_today())
        self.assertEqual(date(2025, 8, 18), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 8, 22), experiment.latest_collectible_batch_end())

    @patch("tests.test_mocks.NimbusExperiment.todays_date")
    def test_non_default_batch_interval_for_todays_date_is_the_end_date_of_first_batch(self, todays_date):
        mock_date = date(2025, 8, 22)
        todays_date.return_value = mock_date
        experiment = mock_nimbus_experiment()
        # Mock experiment starts on 2025-08-18 and has batch duration 5 days
        experiment.batchDuration = 432000

        self.assertEqual(False, experiment.collect_today())
        self.assertEqual(date(2025, 8, 18), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 8, 22), experiment.latest_collectible_batch_end())

    @patch("tests.test_mocks.NimbusExperiment.todays_date")
    def test_non_default_batch_interval_for_todays_date_is_start_date_of_subsequent_batch(self, todays_date):
        mock_date = date(2025, 8, 28)
        todays_date.return_value = mock_date
        experiment = mock_nimbus_experiment()
        # Mock experiment starts on 2025-08-18 and has batch duration 5 days
        experiment.batchDuration = 432000

        self.assertEqual(True, experiment.collect_today())
        self.assertEqual(date(2025, 8, 23), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 8, 27), experiment.latest_collectible_batch_end())

    @patch("tests.test_mocks.NimbusExperiment.todays_date")
    def test_non_default_batch_interval_for_todays_date_in_middle_of_subsequent_batch(self, todays_date):
        mock_date = date(2025, 8, 31)
        todays_date.return_value = mock_date
        experiment = mock_nimbus_experiment()
        # Mock experiment starts on 2025-08-18 and has batch duration 5 days
        experiment.batchDuration = 432000

        self.assertEqual(True, experiment.collect_today())
        self.assertEqual(date(2025, 8, 23), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 8, 27), experiment.latest_collectible_batch_end())

    @patch("tests.test_mocks.NimbusExperiment.todays_date")
    def test_non_default_batch_interval_for_todays_date_is_end_date_of_subsequent_batch(self, todays_date):
        mock_date = date(2025, 9, 1)
        todays_date.return_value = mock_date
        experiment = mock_nimbus_experiment()
        # Mock experiment starts on 2025-08-18 and has batch duration 5 days
        experiment.batchDuration = 432000

        self.assertEqual(True, experiment.collect_today())
        self.assertEqual(date(2025, 8, 23), experiment.latest_collectible_batch_start())
        self.assertEqual(date(2025, 8, 27), experiment.latest_collectible_batch_end())
