-- ==============================================================================
-- Initialization DDL for Stock Data Pipeline Database
-- ==============================================================================
-- This script runs automatically on initial PostgreSQL container startup
-- via /docker-entrypoint-initdb.d/init.sql
-- ==============================================================================

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

-- Index for high-performance time-series querying by ticker and date
CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol_timestamp
ON stock_prices (symbol, timestamp DESC);
