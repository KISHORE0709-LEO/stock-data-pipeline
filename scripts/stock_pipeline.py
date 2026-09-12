"""
Stock Market Data Pipeline Module.

This module provides the core data engineering logic to:
1. Fetch daily stock market data from the Alpha Vantage API.
2. Parse and validate JSON time-series records.
3. Connect to PostgreSQL and safely upsert records into the database.
4. Provide clean exception handling and logging for Airflow orchestration.
"""

import datetime
import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

try:
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extras import execute_batch
except ImportError:
    psycopg2 = None
    sql = None
    execute_batch = None
import requests

# ------------------------------------------------------------------------------
# Logging Configuration
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("stock_pipeline")


# ------------------------------------------------------------------------------
# Custom Pipeline Exceptions
# ------------------------------------------------------------------------------
class PipelineException(Exception):
    """Base exception for all pipeline errors."""
    pass


class RateLimitException(PipelineException):
    """Raised when the Alpha Vantage rate limit is encountered."""
    pass


class APIResponseException(PipelineException):
    """Raised when the API returns an error response or invalid structure."""
    pass


class DatabaseException(PipelineException):
    """Raised when a database connection or query operation fails."""
    pass


# ------------------------------------------------------------------------------
# Database Connectivity & Schema Helpers
# ------------------------------------------------------------------------------
def get_db_connection() -> Any:
    """
    Establish a connection to the PostgreSQL database using environment variables.

    Returns:
        psycopg2 connection object.

    Raises:
        DatabaseException: If connection cannot be established or driver missing.
    """
    if psycopg2 is None:
        raise DatabaseException(
            "psycopg2 is not installed. Please install psycopg2-binary or run inside Docker container."
        )

    host = os.getenv("POSTGRES_HOST", "postgres")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    db = os.getenv("POSTGRES_DB", "stockdb")
    user = os.getenv("POSTGRES_USER", "stockuser")
    password = os.getenv("POSTGRES_PASSWORD", "stockpassword")

    logger.info("Connecting to PostgreSQL at %s:%d/%s as user '%s'...", host, port, db, user)
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=db,
            user=user,
            password=password,
            connect_timeout=10,
        )
        conn.autocommit = False
        logger.info("Database connection established successfully.")
        return conn
    except Exception as exc:
        logger.error("Failed to connect to PostgreSQL at %s:%d/%s: %s", host, port, db, exc)
        raise DatabaseException(f"Database connection failed: {exc}") from exc


def create_table_if_needed(conn: Any) -> None:
    """
    Ensure the stock_prices table and corresponding indexes exist in PostgreSQL.

    Args:
        conn: Open psycopg2 database connection.

    Raises:
        DatabaseException: If table creation fails.
    """
    ddl_statement = """
    CREATE TABLE IF NOT EXISTS stock_prices (
        id SERIAL PRIMARY KEY,
        symbol VARCHAR(10) NOT NULL,
        timestamp DATE NOT NULL,
        open NUMERIC(12, 4) NOT NULL,
        high NUMERIC(12, 4) NOT NULL,
        low NUMERIC(12, 4) NOT NULL,
        close NUMERIC(12, 4) NOT NULL,
        volume BIGINT NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT uq_symbol_timestamp UNIQUE (symbol, timestamp)
    );

    CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol_timestamp
    ON stock_prices (symbol, timestamp DESC);
    """
    try:
        with conn.cursor() as cur:
            cur.execute(ddl_statement)
        conn.commit()
        logger.info("Ensured stock_prices table and unique constraint exist.")
    except Exception as exc:
        conn.rollback()
        logger.error("Failed to initialize database table: %s", exc)
        raise DatabaseException(f"Failed to create table: {exc}") from exc


# ------------------------------------------------------------------------------
# Alpha Vantage API Fetching
# ------------------------------------------------------------------------------
def fetch_stock_data(symbol: str, api_key: Optional[str] = None) -> Dict[str, Any]:
    """
    Fetch daily stock prices for a symbol from the Alpha Vantage API.

    Args:
        symbol: Stock ticker symbol (e.g., 'AAPL').
        api_key: Alpha Vantage API key. If None, reads from ALPHA_VANTAGE_API_KEY.

    Returns:
        Raw parsed JSON dictionary from Alpha Vantage.

    Raises:
        PipelineException: If API key is missing.
        RateLimitException: If API call rate limits are exceeded.
        APIResponseException: If the API returns an error payload.
        requests.RequestException: If network, HTTP, or timeout errors occur.
    """
    cleaned_symbol = symbol.strip().upper()
    resolved_api_key = api_key or os.getenv("ALPHA_VANTAGE_API_KEY", "").strip()

    if not resolved_api_key or resolved_api_key == "YOUR_API_KEY":
        msg = (
            f"Missing or placeholder ALPHA_VANTAGE_API_KEY for symbol '{cleaned_symbol}'. "
            "Please configure a valid API key in your environment/.env."
        )
        logger.error(msg)
        raise PipelineException(msg)

    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": cleaned_symbol,
        "apikey": resolved_api_key,
        "datatype": "json",
    }

    logger.info("Fetching daily stock data for ticker '%s' from Alpha Vantage...", cleaned_symbol)

    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
    except requests.Timeout as exc:
        logger.error("Request timed out while fetching stock data for %s: %s", cleaned_symbol, exc)
        raise
    except requests.RequestException as exc:
        logger.error("HTTP/Network error while fetching stock data for %s: %s", cleaned_symbol, exc)
        raise

    try:
        payload = response.json()
    except json.JSONDecodeError as exc:
        logger.error("Failed to decode JSON response for symbol %s: %s", cleaned_symbol, exc)
        raise APIResponseException(f"Invalid JSON returned for {cleaned_symbol}") from exc

    # Alpha Vantage returns error information inside 200 OK JSON payloads
    if "Error Message" in payload:
        err_msg = payload.get("Error Message", "Unknown Alpha Vantage API error.")
        logger.error("Alpha Vantage API error for '%s': %s", cleaned_symbol, err_msg)
        raise APIResponseException(f"API Error for {cleaned_symbol}: {err_msg}")

    # Alpha Vantage returns Note or Information when hitting rate limits
    if "Note" in payload:
        note_msg = payload.get("Note", "")
        logger.warning("Alpha Vantage call rate limit reached for '%s': %s", cleaned_symbol, note_msg)
        raise RateLimitException(f"Rate limit exceeded: {note_msg}")

    if "Information" in payload:
        info_msg = payload.get("Information", "")
        logger.warning("Alpha Vantage quota / frequency notice for '%s': %s", cleaned_symbol, info_msg)
        raise RateLimitException(f"API quota exceeded: {info_msg}")

    if "Time Series (Daily)" not in payload:
        logger.error(
            "Expected 'Time Series (Daily)' key missing in API response for symbol '%s'. Payload keys: %s",
            cleaned_symbol,
            list(payload.keys()),
        )
        raise APIResponseException(f"Missing time-series data in response for {cleaned_symbol}")

    logger.info("Successfully received raw stock data for ticker '%s'.", cleaned_symbol)
    return payload


# ------------------------------------------------------------------------------
# Data Parsing & Validation
# ------------------------------------------------------------------------------
def parse_and_validate_records(
    symbol: str, raw_json: Dict[str, Any]
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    Parse the Alpha Vantage JSON response, validate schema types and constraints,
    and filter out invalid or malformed data records.

    Args:
        symbol: Stock ticker symbol.
        raw_json: Raw API response dictionary.

    Returns:
        A tuple of (valid_records_list, statistics_dict).
        Each record in valid_records_list is a dictionary:
        {
            'symbol': str,
            'timestamp': str (YYYY-MM-DD),
            'open': float,
            'high': float,
            'low': float,
            'close': float,
            'volume': int
        }
    """
    cleaned_symbol = symbol.strip().upper()
    time_series = raw_json.get("Time Series (Daily)")

    if not isinstance(time_series, dict) or not time_series:
        logger.warning("No time series records found to parse for symbol '%s'.", cleaned_symbol)
        return [], {"total_received": 0, "valid_records": 0, "skipped_records": 0}

    valid_records: List[Dict[str, Any]] = []
    skipped_count = 0
    total_count = len(time_series)

    logger.info("Parsing %d daily records for ticker '%s'...", total_count, cleaned_symbol)

    for date_str, daily_data in time_series.items():
        if not isinstance(daily_data, dict):
            logger.warning("[%s] Skipping non-dict record for date %s", cleaned_symbol, date_str)
            skipped_count += 1
            continue

        # 1. Validate date format
        try:
            parsed_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            logger.warning("[%s] Skipping record with invalid date format '%s'", cleaned_symbol, date_str)
            skipped_count += 1
            continue

        # 2. Extract and validate required OHLCV fields
        raw_open = daily_data.get("1. open")
        raw_high = daily_data.get("2. high")
        raw_low = daily_data.get("3. low")
        raw_close = daily_data.get("4. close")
        raw_volume = daily_data.get("5. volume")

        if None in (raw_open, raw_high, raw_low, raw_close, raw_volume):
            logger.warning("[%s] Skipping date %s due to missing OHLCV field(s).", cleaned_symbol, date_str)
            skipped_count += 1
            continue

        try:
            val_open = float(raw_open)
            val_high = float(raw_high)
            val_low = float(raw_low)
            val_close = float(raw_close)
            val_volume = int(raw_volume)
        except (ValueError, TypeError) as conv_err:
            logger.warning(
                "[%s] Skipping date %s due to numeric conversion error: %s",
                cleaned_symbol,
                date_str,
                conv_err,
            )
            skipped_count += 1
            continue

        # 3. Validate numerical sanity (prices and volume must be non-negative)
        if val_open < 0 or val_high < 0 or val_low < 0 or val_close < 0 or val_volume < 0:
            logger.warning(
                "[%s] Skipping date %s with negative price/volume values.",
                cleaned_symbol,
                date_str,
            )
            skipped_count += 1
            continue

        if val_low > val_high:
            logger.warning(
                "[%s] Skipping date %s where low price (%f) exceeds high price (%f).",
                cleaned_symbol,
                date_str,
                val_low,
                val_high,
            )
            skipped_count += 1
            continue

        valid_records.append(
            {
                "symbol": cleaned_symbol,
                "timestamp": parsed_date.strftime("%Y-%m-%d"),
                "open": round(val_open, 4),
                "high": round(val_high, 4),
                "low": round(val_low, 4),
                "close": round(val_close, 4),
                "volume": val_volume,
            }
        )

    stats = {
        "total_received": total_count,
        "valid_records": len(valid_records),
        "skipped_records": skipped_count,
    }

    logger.info(
        "Finished parsing '%s': %d valid records, %d skipped out of %d received.",
        cleaned_symbol,
        stats["valid_records"],
        stats["skipped_records"],
        stats["total_received"],
    )
    return valid_records, stats


# ------------------------------------------------------------------------------
# PostgreSQL Idempotent Upsert
# ------------------------------------------------------------------------------
def upsert_stock_records(
    conn: Any, records: List[Dict[str, Any]]
) -> int:
    """
    Perform an atomic, parameterized PostgreSQL batch upsert into stock_prices.
    Uses ON CONFLICT (symbol, timestamp) DO UPDATE to ensure idempotency.

    Args:
        conn: Active psycopg2 database connection with autocommit=False.
        records: List of validated stock record dictionaries.

    Returns:
        Number of records processed in the upsert.

    Raises:
        DatabaseException: If transaction fails, rolls back and raises.
    """
    if not records:
        logger.info("No records provided to upsert. Skipping database operation.")
        return 0

    if execute_batch is None:
        raise DatabaseException("psycopg2 is not installed. Cannot perform execute_batch.")

    upsert_sql = """
    INSERT INTO stock_prices (symbol, timestamp, open, high, low, close, volume, updated_at)
    VALUES (%(symbol)s, %(timestamp)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s, CURRENT_TIMESTAMP)
    ON CONFLICT (symbol, timestamp)
    DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume,
        updated_at = CURRENT_TIMESTAMP;
    """

    logger.info("Starting atomic upsert of %d records into stock_prices...", len(records))

    try:
        with conn.cursor() as cur:
            execute_batch(cur, upsert_sql, records, page_size=100)
        conn.commit()
        logger.info("Successfully committed upsert of %d records to PostgreSQL.", len(records))
        return len(records)
    except Exception as exc:
        conn.rollback()
        logger.error("Database upsert failed. Transaction rolled back: %s", exc)
        raise DatabaseException(f"Failed to upsert records into PostgreSQL: {exc}") from exc


# ------------------------------------------------------------------------------
# End-to-End Pipeline Execution (CLI / Standalone Entrypoint)
# ------------------------------------------------------------------------------
def get_configured_symbols() -> List[str]:
    """Retrieve and sanitize the list of target stock tickers from environment."""
    raw_symbols = os.getenv("STOCK_SYMBOLS", "AAPL,MSFT,GOOGL")
    symbols = [s.strip().upper() for s in raw_symbols.split(",") if s.strip()]
    return symbols or ["AAPL", "MSFT", "GOOGL"]


def run_pipeline(symbols: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Execute the end-to-end stock pipeline for all target stock symbols.

    Args:
        symbols: Optional custom list of symbols. Defaults to STOCK_SYMBOLS env var.

    Returns:
        Dictionary containing pipeline run summary and record counts.
    """
    target_symbols = symbols or get_configured_symbols()
    logger.info("=================================================================")
    logger.info("Starting Stock Market Data Pipeline for tickers: %s", target_symbols)
    logger.info("=================================================================")

    conn = get_db_connection()
    try:
        create_table_if_needed(conn)

        total_upserted = 0
        symbol_summaries: Dict[str, Any] = {}

        for sym in target_symbols:
            logger.info("--- Processing ticker: %s ---", sym)
            try:
                raw_data = fetch_stock_data(sym)
                valid_records, stats = parse_and_validate_records(sym, raw_data)
                upserted = upsert_stock_records(conn, valid_records)
                total_upserted += upserted
                symbol_summaries[sym] = {
                    "status": "SUCCESS",
                    "received": stats["total_received"],
                    "valid": stats["valid_records"],
                    "skipped": stats["skipped_records"],
                    "upserted": upserted,
                }
            except (RateLimitException, APIResponseException, requests.RequestException) as err:
                logger.error("Failed processing symbol %s: %s", sym, err)
                symbol_summaries[sym] = {"status": "FAILED", "error": str(err)}
                # Re-raise to let orchestrator or caller handle failure
                raise

        logger.info("=================================================================")
        logger.info("Pipeline completed successfully! Total records upserted: %d", total_upserted)
        logger.info("=================================================================")

        return {
            "status": "SUCCESS",
            "total_upserted": total_upserted,
            "symbols": symbol_summaries,
        }
    finally:
        conn.close()
        logger.info("PostgreSQL database connection closed.")


if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception as e:
        logger.exception("Stock Data Pipeline terminated with fatal error: %s", e)
        sys.exit(1)
