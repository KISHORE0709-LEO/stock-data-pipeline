"""
Airflow DAG: Stock Market Data Pipeline.

Orchestrates daily fetching, schema validation, and idempotent PostgreSQL upserts
for stock market time-series data retrieved from Alpha Vantage.
"""

from datetime import datetime, timedelta
import logging
import os
import sys
import time
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator

# Ensure the 'scripts' directory is discoverable by Airflow workers/scheduler
SCRIPTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts"))
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

from stock_pipeline import (
    create_table_if_needed,
    fetch_stock_data,
    get_configured_symbols,
    get_db_connection,
    parse_and_validate_records,
    upsert_stock_records,
)

logger = logging.getLogger("airflow.task")

# ------------------------------------------------------------------------------
# Default Task Arguments & Retry Policies
# ------------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    # Automatic resilience: retry transient API, network, or DB hiccups up to 3 times
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
}


# ------------------------------------------------------------------------------
# Task Callables
# ------------------------------------------------------------------------------
def task_init_database(**context: Any) -> None:
    """Ensure PostgreSQL connection is healthy and the target table schema exists."""
    logger.info("Initializing database schema...")
    conn = get_db_connection()
    try:
        create_table_if_needed(conn)
        logger.info("Database schema check and initialization successful.")
    finally:
        conn.close()


def task_fetch_stock_data(**context: Any) -> Dict[str, Any]:
    """
    Fetch raw stock market data for all configured symbols from Alpha Vantage API.
    Pushes raw responses to XCom for downstream validation.
    """
    symbols = get_configured_symbols()
    logger.info("Starting API fetch task for configured symbols: %s", symbols)

    raw_payloads: Dict[str, Any] = {}
    for i, sym in enumerate(symbols):
        if i > 0:
            logger.info("Pausing 15 seconds to respect Alpha Vantage API rate limit (1 req/sec)...")
            time.sleep(15)
        logger.info("Fetching data for symbol: %s", sym)
        payload = fetch_stock_data(sym)
        raw_payloads[sym] = payload

    logger.info("API fetch completed for %d symbols.", len(raw_payloads))
    return raw_payloads


def task_parse_and_validate(**context: Any) -> Dict[str, List[Dict[str, Any]]]:
    """
    Pull raw JSON from upstream fetch task, validate required fields and types,
    and filter out corrupt records.
    """
    ti = context["ti"]
    raw_payloads: Dict[str, Any] = ti.xcom_pull(task_ids="fetch_stock_data")

    if not raw_payloads:
        raise ValueError("No raw stock data received from upstream fetch task.")

    cleaned_data: Dict[str, List[Dict[str, Any]]] = {}
    total_valid = 0
    total_skipped = 0

    for symbol, raw_json in raw_payloads.items():
        valid_records, stats = parse_and_validate_records(symbol, raw_json)
        cleaned_data[symbol] = valid_records
        total_valid += stats["valid_records"]
        total_skipped += stats["skipped_records"]
        logger.info(
            "Symbol %s: %d valid records, %d skipped.",
            symbol,
            stats["valid_records"],
            stats["skipped_records"],
        )

    logger.info(
        "Validation complete. Total valid records across all tickers: %d (Skipped: %d)",
        total_valid,
        total_skipped,
    )
    return cleaned_data


def task_upsert_to_postgres(**context: Any) -> int:
    """
    Pull validated records and perform atomic batch upsert into PostgreSQL.
    Guarantees idempotency via ON CONFLICT (symbol, timestamp).
    """
    ti = context["ti"]
    cleaned_data: Dict[str, List[Dict[str, Any]]] = ti.xcom_pull(task_ids="parse_and_validate")

    if not cleaned_data:
        logger.warning("No validated records to upsert.")
        return 0

    conn = get_db_connection()
    total_upserted = 0
    try:
        for symbol, records in cleaned_data.items():
            logger.info("Upserting %d records for symbol %s...", len(records), symbol)
            count = upsert_stock_records(conn, records)
            total_upserted += count
            logger.info("Successfully upserted %d records for %s.", count, symbol)

        logger.info("Pipeline upsert complete! Total rows upserted/updated: %d", total_upserted)
        return total_upserted
    finally:
        conn.close()


# ------------------------------------------------------------------------------
# DAG Definition
# ------------------------------------------------------------------------------
with DAG(
    dag_id="stock_market_data_pipeline",
    default_args=default_args,
    description="Automated daily stock data ingestion from Alpha Vantage to PostgreSQL",
    # --------------------------------------------------------------------------
    # SCHEDULE CONFIGURATION:
    # Set to "@daily" for daily runs at midnight (standard for free API limits).
    # To switch to hourly runs, simply change the schedule to "@hourly".
    # --------------------------------------------------------------------------
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["stocks", "finance", "etl", "postgres"],
) as dag:

    init_db = PythonOperator(
        task_id="initialize_database",
        python_callable=task_init_database,
        doc_md="Verifies database connectivity and creates stock_prices table and indexes if needed.",
    )

    fetch_data = PythonOperator(
        task_id="fetch_stock_data",
        python_callable=task_fetch_stock_data,
        doc_md="Queries the Alpha Vantage TIME_SERIES_DAILY endpoint using requests.",
    )

    parse_validate = PythonOperator(
        task_id="parse_and_validate",
        python_callable=task_parse_and_validate,
        doc_md="Parses JSON, validates date and OHLCV formats, and discards malformed entries.",
    )

    upsert_postgres = PythonOperator(
        task_id="upsert_to_postgres",
        python_callable=task_upsert_to_postgres,
        doc_md="Executes transactional batch upsert (INSERT ... ON CONFLICT DO UPDATE).",
    )

    # Clean orchestration dependency pipeline
    init_db >> fetch_data >> parse_validate >> upsert_postgres
