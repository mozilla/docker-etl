import os
from datetime import datetime
from unittest import TestCase
from unittest.mock import call, patch

import pytest
from click.testing import CliRunner

from macroeconomic_indices.main import (
    get_macro_data,
    main,
)


TEST_API_DATA = {
    "symbol": "MOZ",
    "historical": [
        {
            "date": "2023-01-01",
            "open": 100.5,
            "high": 300.5,
            "low": 50.5,
            "close": 200.5,
            "adjClose": 190.5,
            "volume": 1000,
            "somethingElse": 500,
        },
        {
            "date": "2023-01-02",
            "open": 200.5,
            "high": 400.5,
            "low": 150.5,
            "close": 300.5,
            "adjClose": 290.5,
            "volume": 2000,
            "somethingElse": 500,
        },
    ],
}

TEST_MACRO_DATA = [
    {
        "symbol": "MOZ",
        "market_date": "2023-01-01",
        "open": 100.5,
        "close": 200.5,
        "adj_close": 190.5,
        "high": 300.5,
        "low": 50.5,
        "volume": 1000,
    },
    {
        "symbol": "MOZ",
        "market_date": "2023-01-02",
        "open": 200.5,
        "close": 300.5,
        "adj_close": 290.5,
        "high": 400.5,
        "low": 150.5,
        "volume": 2000,
    },
]


class TestMacroeconomicIndices(TestCase):
    @patch("macroeconomic_indices.main.get_macro_data")
    @patch("macroeconomic_indices.main.load_data_to_bq")
    def test_main(self, mock_load_data, mock_get_data):
        runner = CliRunner()
        mock_macro_data = [{"a_key": "a_value"}]
        mock_get_data.return_value = mock_macro_data

        with patch.dict(os.environ, {"FMP_API_KEY": "zzz"}):
            runner.invoke(
                main,
                ["--project-id", "test-project", "--submission-date", "2023-01-01"],
            )

        mock_get_data.assert_called_with(
            "zzz", datetime(2023, 1, 1), datetime(2023, 1, 1)
        )
        mock_load_data.assert_called_with(
            project_id="test-project",
            macro_data=mock_macro_data,
            partition=datetime(2023, 1, 1),
        )

    @patch("macroeconomic_indices.main.get_macro_data")
    @patch("macroeconomic_indices.main.load_data_to_bq")
    def test_backfill(self, mock_load_data, mock_get_data):
        runner = CliRunner()
        mock_macro_data = [{"a_key": "a_value"}]
        mock_get_data.return_value = mock_macro_data

        with patch.dict(os.environ, {"FMP_API_KEY": "zzz"}):
            runner.invoke(
                main,
                [
                    "--project-id",
                    "test-project",
                    "--backfill",
                    "--start-date",
                    "2023-01-01",
                    "--end-date",
                    "2023-01-03",
                ],
            )

        mock_get_data.assert_called_with(
            "zzz", datetime(2023, 1, 1), datetime(2023, 1, 3)
        )
        mock_load_data.assert_called_with(
            project_id="test-project",
            macro_data=mock_macro_data,
        )

    @patch("macroeconomic_indices.main.get_macro_data")
    @patch("macroeconomic_indices.main.load_data_to_bq")
    def test_invalid_parameters(self, mock_load_data, mock_get_data):
        runner = CliRunner()

        # API_KEY must be set
        with patch.dict(os.environ, {}):
            with pytest.raises(AssertionError) as e:
                runner.invoke(
                    main,
                    ["--project-id", "test-project", "--submission-date", "2023-01-01"],
                )
                assert str(e) == "Environment variable FMP_API_KEY must be set"

        with patch.dict(os.environ, {"FMP_API_KEY": "zzz"}):
            # project-id is a required parameter
            result = runner.invoke(main, ["--submission-date", "2023-01-01"])
            assert result.exit_code == 2
            assert result.exception is not None
            assert "Missing option '--project-id'." in result.stdout

            # if backfill, start-date and end-date are required
            with pytest.raises(AssertionError) as e:
                runner.invoke(main, ["--project-id", "test-project", "--backfill"])
                assert str(e) == "You must provide a start and end date to backfill"

            # if not backfill, submission-date is required
            with pytest.raises(AssertionError) as e:
                runner.invoke(
                    main,
                    [
                        "--project-id",
                        "test-project",
                    ],
                )
                assert (
                    str(e)
                    == "You must provide a submission date or --backfill + start and end date"  # noqa: E501
                )

    @patch("macroeconomic_indices.main.FOREX_TICKERS", ["ZZZUSD=X"])
    @patch("macroeconomic_indices.main.get_index_ticker")
    @patch("macroeconomic_indices.main.get_forex_ticker")
    def test_get_macro_data(self, mock_get_forex_ticker, mock_get_index_ticker):
        mock_get_index_ticker.return_value = {}
        mock_get_forex_ticker.return_value = TEST_API_DATA

        macro_data = get_macro_data(
            api_key="zzz",
            start_date=datetime(2023, 1, 1),
            end_date=datetime(2023, 1, 2),
        )
        mock_get_index_ticker.assert_has_calls(
            [
                call(
                    api_key="zzz",
                    ticker="^DJI",
                    start_date=datetime(2023, 1, 1),
                    end_date=datetime(2023, 1, 2),
                ),
                call(
                    api_key="zzz",
                    ticker="^GSPC",
                    start_date=datetime(2023, 1, 1),
                    end_date=datetime(2023, 1, 2),
                ),
                call(
                    api_key="zzz",
                    ticker="^IXIC",
                    start_date=datetime(2023, 1, 1),
                    end_date=datetime(2023, 1, 2),
                ),
            ]
        )
        mock_get_forex_ticker.assert_has_calls(
            [
                call(
                    api_key="zzz",
                    ticker="ZZZUSD=X",
                    start_date=datetime(2023, 1, 1),
                    end_date=datetime(2023, 1, 2),
                ),
            ]
        )
        assert macro_data == TEST_MACRO_DATA
