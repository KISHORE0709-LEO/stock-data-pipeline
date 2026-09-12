"""
Unit and integration tests for stock_pipeline logic.
Validates data parsing, missing-data handling, error detection, and idempotent upserts.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# Add scripts directory to path for import
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts")))

from stock_pipeline import (
    APIResponseException,
    DatabaseException,
    PipelineException,
    RateLimitException,
    create_table_if_needed,
    fetch_stock_data,
    parse_and_validate_records,
    upsert_stock_records,
)


class TestStockPipeline(unittest.TestCase):
    """Test suite for the stock data pipeline components."""

    def setUp(self):
        self.sample_valid_api_response = {
            "Meta Data": {
                "1. Information": "Daily Prices (open, high, low, close) and Volumes",
                "2. Symbol": "AAPL",
                "3. Last Refreshed": "2025-01-10",
                "4. Output Size": "Compact",
                "5. Time Zone": "US/Eastern",
            },
            "Time Series (Daily)": {
                "2025-01-10": {
                    "1. open": "240.5000",
                    "2. high": "242.8000",
                    "3. low": "239.1000",
                    "4. close": "241.2500",
                    "5. volume": "48512300",
                },
                "2025-01-09": {
                    "1. open": "238.0000",
                    "2. high": "241.0000",
                    "3. low": "237.5000",
                    "4. close": "240.1000",
                    "5. volume": "51200300",
                },
            },
        }

    # --------------------------------------------------------------------------
    # 1. Parsing & Validation Tests
    # --------------------------------------------------------------------------
    def test_parse_and_validate_valid_data(self):
        """Verify that valid daily time series data is accurately parsed."""
        records, stats = parse_and_validate_records("AAPL", self.sample_valid_api_response)

        self.assertEqual(stats["total_received"], 2)
        self.assertEqual(stats["valid_records"], 2)
        self.assertEqual(stats["skipped_records"], 0)
        self.assertEqual(len(records), 2)

        record_01_10 = next(r for r in records if r["timestamp"] == "2025-01-10")
        self.assertEqual(record_01_10["symbol"], "AAPL")
        self.assertEqual(record_01_10["open"], 240.5000)
        self.assertEqual(record_01_10["high"], 242.8000)
        self.assertEqual(record_01_10["low"], 239.1000)
        self.assertEqual(record_01_10["close"], 241.2500)
        self.assertEqual(record_01_10["volume"], 48512300)

    def test_parse_missing_and_corrupt_data_gracefully(self):
        """Verify that records with missing fields, negative values, or corrupt strings are skipped."""
        corrupt_response = {
            "Time Series (Daily)": {
                "2025-01-10": {  # Valid
                    "1. open": "100.0",
                    "2. high": "110.0",
                    "3. low": "95.0",
                    "4. close": "105.0",
                    "5. volume": "1000",
                },
                "2025-01-09": {  # Missing close price
                    "1. open": "100.0",
                    "2. high": "110.0",
                    "3. low": "95.0",
                    "5. volume": "1000",
                },
                "2025-01-08": {  # Corrupt string for high
                    "1. open": "100.0",
                    "2. high": "N/A",
                    "3. low": "95.0",
                    "4. close": "105.0",
                    "5. volume": "1000",
                },
                "2025-01-07": {  # Negative volume
                    "1. open": "100.0",
                    "2. high": "110.0",
                    "3. low": "95.0",
                    "4. close": "105.0",
                    "5. volume": "-500",
                },
                "2025-01-06": {  # Inverted low > high
                    "1. open": "100.0",
                    "2. high": "90.0",
                    "3. low": "120.0",
                    "4. close": "105.0",
                    "5. volume": "1000",
                },
                "bad-date-format": {  # Bad date
                    "1. open": "100.0",
                    "2. high": "110.0",
                    "3. low": "95.0",
                    "4. close": "105.0",
                    "5. volume": "1000",
                },
            }
        }
        records, stats = parse_and_validate_records("TEST", corrupt_response)

        self.assertEqual(stats["total_received"], 6)
        self.assertEqual(stats["valid_records"], 1)
        self.assertEqual(stats["skipped_records"], 5)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["timestamp"], "2025-01-10")

    def test_parse_empty_payload(self):
        """Verify that empty payload returns 0 records without crashing."""
        records, stats = parse_and_validate_records("EMPTY", {})
        self.assertEqual(records, [])
        self.assertEqual(stats["total_received"], 0)
        self.assertEqual(stats["valid_records"], 0)
        self.assertEqual(stats["skipped_records"], 0)

    # --------------------------------------------------------------------------
    # 2. API Fetching & Error Detection Tests
    # --------------------------------------------------------------------------
    @patch("stock_pipeline.requests.get")
    def test_fetch_missing_api_key(self, mock_get):
        """Verify that missing API key raises PipelineException."""
        with patch.dict(os.environ, {"ALPHA_VANTAGE_API_KEY": ""}):
            with self.assertRaises(PipelineException):
                fetch_stock_data("AAPL", api_key=None)

    @patch("stock_pipeline.requests.get")
    def test_fetch_rate_limit_detection(self, mock_get):
        """Verify that rate limit payload raises RateLimitException."""
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = {
            "Note": "Thank you for using Alpha Vantage! Our standard API call frequency is 5 calls per minute."
        }
        mock_get.return_value = mock_resp

        with self.assertRaises(RateLimitException):
            fetch_stock_data("AAPL", api_key="DUMMY_KEY")

    @patch("stock_pipeline.requests.get")
    def test_fetch_invalid_symbol_error_detection(self, mock_get):
        """Verify that API error message payload raises APIResponseException."""
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = {
            "Error Message": "Invalid API call. Please check your parameters or symbol."
        }
        mock_get.return_value = mock_resp

        with self.assertRaises(APIResponseException):
            fetch_stock_data("INVALID_SYM", api_key="DUMMY_KEY")

    # --------------------------------------------------------------------------
    # 3. Database Upsert & Idempotency Tests
    # --------------------------------------------------------------------------
    @patch("stock_pipeline.execute_batch")
    def test_upsert_executes_batch_with_on_conflict(self, mock_execute_batch):
        """Verify that upsert_stock_records executes parameterized batch and commits."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        records = [
            {
                "symbol": "AAPL",
                "timestamp": "2025-01-10",
                "open": 240.5,
                "high": 242.8,
                "low": 239.1,
                "close": 241.25,
                "volume": 48512300,
            }
        ]

        count = upsert_stock_records(mock_conn, records)
        self.assertEqual(count, 1)
        mock_execute_batch.assert_called_once()
        # Verify ON CONFLICT is present in the executed SQL
        executed_sql = mock_execute_batch.call_args[0][1]
        self.assertIn("ON CONFLICT (symbol, timestamp)", executed_sql)
        self.assertIn("DO UPDATE SET", executed_sql)
        mock_conn.commit.assert_called_once()

    def test_upsert_empty_records(self):
        """Verify that empty records list skips DB execute cleanly."""
        mock_conn = MagicMock()
        count = upsert_stock_records(mock_conn, [])
        self.assertEqual(count, 0)
        mock_conn.commit.assert_not_called()

    @patch("stock_pipeline.execute_batch")
    def test_upsert_rolls_back_on_failure(self, mock_execute_batch):
        """Verify that database error triggers transaction rollback."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_execute_batch.side_effect = Exception("Disk or syntax failure")

        records = [
            {
                "symbol": "AAPL",
                "timestamp": "2025-01-10",
                "open": 240.5,
                "high": 242.8,
                "low": 239.1,
                "close": 241.25,
                "volume": 48512300,
            }
        ]

        with self.assertRaises(DatabaseException):
            upsert_stock_records(mock_conn, records)

        mock_conn.rollback.assert_called_once()
        mock_conn.commit.assert_not_called()


if __name__ == "__main__":
    unittest.main()
