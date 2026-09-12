"""
Verification of the 6 core failure & resilience scenarios defined in the assignment:
1. Invalid API key
2. API timeout
3. API rate limit
4. Missing / malformed stock data
5. Duplicate record (idempotent upsert via ON CONFLICT)
6. PostgreSQL unavailable / rollback on failure
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch
import requests

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts")))

from stock_pipeline import (
    APIResponseException,
    DatabaseException,
    PipelineException,
    RateLimitException,
    fetch_stock_data,
    get_db_connection,
    parse_and_validate_records,
    upsert_stock_records,
)


class TestScenarios(unittest.TestCase):
    """Explicit tests for all 6 required pipeline scenarios."""

    # --------------------------------------------------------------------------
    # Scenario 1: Invalid API Key
    # --------------------------------------------------------------------------
    @patch("stock_pipeline.requests.get")
    def test_scenario_1_invalid_api_key(self, mock_get):
        """Alpha Vantage returns an error payload when the API key or parameters are invalid."""
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = {
            "Error Message": "Invalid API call. Please check your parameters or API key."
        }
        mock_get.return_value = mock_resp

        with self.assertRaises(APIResponseException) as ctx:
            fetch_stock_data("AAPL", api_key="INVALID_KEY_123")
        self.assertIn("Invalid API call", str(ctx.exception))

    # --------------------------------------------------------------------------
    # Scenario 2: API Timeout
    # --------------------------------------------------------------------------
    @patch("stock_pipeline.requests.get")
    def test_scenario_2_api_timeout(self, mock_get):
        """API request times out due to network latency; raises requests.Timeout for Airflow retry."""
        mock_get.side_effect = requests.exceptions.Timeout("Connection to Alpha Vantage timed out.")

        with self.assertRaises(requests.exceptions.Timeout):
            fetch_stock_data("AAPL", api_key="VALID_KEY")

    # --------------------------------------------------------------------------
    # Scenario 3: API Rate Limit
    # --------------------------------------------------------------------------
    @patch("stock_pipeline.requests.get")
    def test_scenario_3_api_rate_limit(self, mock_get):
        """Free tier 5 calls/min or 25 calls/day limit reached; detects note and raises RateLimitException."""
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.json.return_value = {
            "Information": "Thank you for using Alpha Vantage! Our standard API call frequency is 25 requests per day."
        }
        mock_get.return_value = mock_resp

        with self.assertRaises(RateLimitException) as ctx:
            fetch_stock_data("AAPL", api_key="VALID_KEY")
        self.assertIn("API quota exceeded", str(ctx.exception))

    # --------------------------------------------------------------------------
    # Scenario 4: Missing Stock Data
    # --------------------------------------------------------------------------
    def test_scenario_4_missing_stock_data(self):
        """Payload has one corrupted record (missing '4. close') and one valid record; skips invalid safely."""
        raw_payload = {
            "Time Series (Daily)": {
                "2025-01-10": {
                    "1. open": "150.0",
                    "2. high": "155.0",
                    "3. low": "149.0",
                    # "4. close" is MISSING
                    "5. volume": "100000",
                },
                "2025-01-09": {
                    "1. open": "148.0",
                    "2. high": "151.0",
                    "3. low": "147.0",
                    "4. close": "150.0",
                    "5. volume": "120000",
                },
            }
        }
        records, stats = parse_and_validate_records("MSFT", raw_payload)
        self.assertEqual(stats["total_received"], 2)
        self.assertEqual(stats["valid_records"], 1)
        self.assertEqual(stats["skipped_records"], 1)
        self.assertEqual(records[0]["timestamp"], "2025-01-09")

    # --------------------------------------------------------------------------
    # Scenario 5: Duplicate Record (Idempotent Upsert)
    # --------------------------------------------------------------------------
    @patch("stock_pipeline.execute_batch")
    def test_scenario_5_duplicate_record(self, mock_execute_batch):
        """Verify that running upsert with existing records uses ON CONFLICT DO UPDATE and updates without error."""
        mock_conn = MagicMock()
        records = [
            {
                "symbol": "GOOGL",
                "timestamp": "2025-01-10",
                "open": 180.0,
                "high": 185.0,
                "low": 179.0,
                "close": 184.0,
                "volume": 25000000,
            }
        ]

        # First run (Insert)
        count_1 = upsert_stock_records(mock_conn, records)
        self.assertEqual(count_1, 1)

        # Second run with same record (Update / Idempotent)
        count_2 = upsert_stock_records(mock_conn, records)
        self.assertEqual(count_2, 1)

        # Both calls must utilize the ON CONFLICT query to guarantee idempotency
        for call_args in mock_execute_batch.call_args_list:
            sql_query = call_args[0][1]
            self.assertIn("ON CONFLICT (symbol, timestamp)", sql_query)
            self.assertIn("DO UPDATE SET", sql_query)

    # --------------------------------------------------------------------------
    # Scenario 6: PostgreSQL Unavailable
    # --------------------------------------------------------------------------
    def test_scenario_6_postgres_unavailable(self):
        """Database connection failure raises DatabaseException with context, initiating Airflow retry."""
        mock_psycopg2 = MagicMock()
        mock_psycopg2.connect.side_effect = Exception("could not connect to server: Connection refused")

        with patch("stock_pipeline.psycopg2", mock_psycopg2):
            with patch.dict(os.environ, {"POSTGRES_HOST": "invalid_host"}):
                with self.assertRaises(DatabaseException) as ctx:
                    get_db_connection()
                self.assertIn("Database connection failed", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
