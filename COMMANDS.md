# Complete Pipeline Command Reference Cheatsheet

This cheatsheet contains every single command you need to build, run, monitor, test, and debug the **Stock Market Data Pipeline**.

---

## Table of Contents

0. [Makefile Quick Reference (Recommended)](#0-makefile-quick-reference-recommended)
1. [Prerequisites & Docker Setup](#1-prerequisites--docker-setup)
2. [Starting the Entire Pipeline](#2-starting-the-entire-pipeline)
3. [Checking Container Health & Logs](#3-checking-container-health--logs)
4. [Interacting with Apache Airflow](#4-interacting-with-apache-airflow)
5. [Querying PostgreSQL Database](#5-querying-postgresql-database)
6. [Running Test Suites](#6-running-test-suites)
7. [Stopping & Resetting the Pipeline](#7-stopping--resetting-the-pipeline)
8. [Quick Troubleshooting Commands](#8-quick-troubleshooting-commands)

---

## 0. Makefile Quick Reference (Recommended)

For the cleanest, most professional developer experience, a standard `Makefile` is included:

| Command | Description |
| :--- | :--- |
| `make` or `make help` | Show formatted list of all available commands |
| **`make start`** | **Build & start services, wait for ready, display clickable Airflow link** |
| `make stop` | Gracefully stop running containers (preserves database) |
| `make restart` | Restart all pipeline containers |
| `make status` | Check status, health, and port mappings of all containers |
| `make logs` | Stream live consolidated logs from all services |
| `make test` | Run all 15 unit and resilience scenario tests |
| `make trigger` | Trigger pipeline DAG in Airflow immediately |
| `make db-count` | Query row counts and date ranges in PostgreSQL |
| `make db-shell` | Open interactive `psql` shell in database container |
| `make clean` | Stop and tear down containers and network (preserves data) |
| `make reset` | Hard reset: wipe containers, networks, AND database volumes |

---

## 1. Prerequisites & Docker Setup

### Check if Docker CLI is installed and running
```powershell
# In PowerShell or Command Prompt
docker --version
docker compose version
docker info
```

> **Note**: If `docker` is not recognized, open **Docker Desktop** from your Windows Start Menu and wait until the whale icon in your system tray shows **"Engine running"** (solid green).
> If you don't have Docker Desktop installed, download it from [docker.com](https://www.docker.com/products/docker-desktop/) or run:
> ```powershell
> winget install Docker.DockerDesktop
> ```

### Verify `.env` Configuration
Ensure your `.env` exists and contains your API key:
```powershell
# PowerShell: inspect .env
Get-Content .env
```

---

## 2. Starting the Entire Pipeline

### Step 2.1: Build and Launch Containers in the Background
Run this from the project root (`d:\Kishore\New_project\stock-data-pipeline`):
```bash
docker compose up --build -d
```
* **`--build`**: Builds the custom Airflow image defined in `Dockerfile` with all Python requirements (`psycopg2-binary`, `requests`, `python-dotenv`).
* **`-d`**: Runs all 4 containers in detached mode (in the background).

### Step 2.2: Verify All Containers Are Running
```bash
docker compose ps
```
You should see 3 running services and 1 exited service (expected):
* `stock_postgres` (Up, Healthy)
* `airflow_init` (Exited 0 - database migrations completed)
* `airflow_webserver` (Up, Healthy)
* `airflow_scheduler` (Up, Healthy)

---

## 3. Checking Container Health & Logs

### View Consolidated Live Logs
```bash
# Stream logs from all services in real time
docker compose logs -f
```

### View Specific Container Logs
```bash
# Airflow Scheduler (where tasks are orchestrated)
docker compose logs -f airflow-scheduler

# Airflow Webserver (UI and API)
docker compose logs -f airflow-webserver

# PostgreSQL Database
docker compose logs -f postgres

# Airflow Init (migrations and admin user setup)
docker compose logs airflow-init
```

---

## 4. Interacting with Apache Airflow

### Access Host Endpoints
* **Airflow Web UI**: [http://localhost:8080](http://localhost:8080)
  * **Username**: `admin`
  * **Password**: `admin`
* **Airflow Health Endpoint**: [http://localhost:8080/health](http://localhost:8080/health)

### CLI Commands (Inside Docker)

#### List All Active DAGs
```bash
docker compose exec airflow-webserver airflow dags list
```

#### Unpause the Pipeline DAG
```bash
docker compose exec airflow-webserver airflow dags unpause stock_market_data_pipeline
```

#### Manually Trigger a DAG Run Immediately
```bash
docker compose exec airflow-webserver airflow dags trigger stock_market_data_pipeline
```

#### Check Recent DAG Execution Runs
```bash
docker compose exec airflow-webserver airflow dags list-runs -d stock_market_data_pipeline --state success,running,failed
```

#### Check Task Instance Statuses for a Run
```bash
docker compose exec airflow-webserver airflow tasks states-for-dag-run stock_market_data_pipeline manual__<EXECUTION_DATE>
```

#### Test a Specific Task in Isolation (Without DB state change)
```bash
docker compose exec airflow-scheduler airflow tasks test stock_market_data_pipeline fetch_stock_data 2025-01-01
```

---

## 5. Querying PostgreSQL Database

### Direct `psql` Interactive Shell Inside Docker
```bash
docker compose exec -it postgres psql -U stockuser -d stockdb
```

### Run One-Off SQL Queries from Host Terminal

#### 1. Check Total Rows Ingested
```bash
docker compose exec postgres psql -U stockuser -d stockdb -c "SELECT count(*) FROM stock_prices;"
```

#### 2. View Summary by Ticker (Symbol, Row Count, Date Range)
```bash
docker compose exec postgres psql -U stockuser -d stockdb -c "
SELECT 
    symbol, 
    COUNT(*) AS total_days, 
    MIN(timestamp) AS earliest_date, 
    MAX(timestamp) AS latest_date 
FROM stock_prices 
GROUP BY symbol 
ORDER BY symbol;
"
```

#### 3. View Latest 10 Ingested Records with OHLCV Prices
```bash
docker compose exec postgres psql -U stockuser -d stockdb -c "
SELECT 
    symbol, 
    timestamp, 
    open, 
    high, 
    low, 
    close, 
    volume, 
    updated_at 
FROM stock_prices 
ORDER BY timestamp DESC, symbol ASC 
LIMIT 10;
"
```

#### 4. Verify Idempotency Constraint (Unique Index)
```bash
docker compose exec postgres psql -U stockuser -d stockdb -c "
SELECT conname, pg_get_constraintdef(c.oid) 
FROM pg_constraint c 
JOIN pg_class t ON c.conrelid = t.oid 
WHERE t.relname = 'stock_prices';
"
```

---

## 6. Running Test Suites

### Run All 15 Unit & Scenario Tests Locally (Fastest)
```powershell
python -m unittest discover -s tests -v
```

### Run Tests Inside the Docker Airflow Container
```bash
docker compose exec airflow-scheduler python -m unittest discover -s /opt/airflow/tests -v
```

### Run Specific Test Scenario Suite
```powershell
# Tests the 6 resilience scenarios (Invalid key, rate limit, timeout, corrupt data, idempotency, DB offline)
python -m unittest tests/test_scenarios.py -v

# Tests individual pipeline functions
python -m unittest tests/test_stock_pipeline.py -v
```

---

## 7. Stopping & Resetting the Pipeline

### Gracefully Stop Containers (Preserves Ingested Data)
```bash
docker compose stop
```

### Restart Stopped Containers
```bash
docker compose start
```

### Tear Down Containers & Networks (Preserves Database Volume)
```bash
docker compose down
```

### Hard Reset: Wipe Containers, Networks, AND Database Volumes
> ⚠️ **CAUTION**: This deletes the PostgreSQL database and logs to start completely fresh!
```bash
docker compose down -v
```

---

## 8. Quick Troubleshooting Commands

| Symptom | Diagnosis Command | Remedy Command |
| :--- | :--- | :--- |
| **Docker not responding** | `docker info` | Launch Docker Desktop from Windows Start Menu. |
| **Airflow UI not opening on port 8080** | `docker compose ps` | Check if `airflow_webserver` is healthy. Run `docker compose restart airflow-webserver`. |
| **PostgreSQL connection refused** | `docker compose logs postgres` | Verify port 5432 is not in use by another local PostgreSQL service (`netstat -ano \| findstr 5432`). |
| **Tasks stuck or failing** | `docker compose logs --tail=100 airflow-scheduler` | Inspect task error stack trace. Check `.env` for valid `ALPHA_VANTAGE_API_KEY`. |
| **Container exited with code 1** | `docker compose logs airflow-init` | Check if database migrations finished properly. Run `docker compose run --rm airflow-init`. |
