# ==============================================================================
# Stock Market Data Pipeline - Makefile
# ==============================================================================
# A professional, recruiter-friendly interface to build, operate, test,
# and inspect the containerized Apache Airflow & PostgreSQL pipeline.
# ==============================================================================

.DEFAULT_GOAL := help

# Configuration variables
PYTHON ?= python
DOCKER_COMPOSE := docker compose

# Service & Database defaults
AIRFLOW_URL := http://localhost:8080
HEALTH_URL  := http://localhost:8080/health
DB_USER     := stockuser
DB_NAME     := stockdb
DAG_ID      := stock_market_data_pipeline

# ------------------------------------------------------------------------------
# Help Target (Default)
# ------------------------------------------------------------------------------
.PHONY: help
help:
	@echo ""
	@echo "========================================================================"
	@echo "          STOCK MARKET DATA PIPELINE - COMMAND REFERENCE"
	@echo "========================================================================"
	@echo "  Usage: make [target]"
	@echo ""
	@echo "  Lifecycle Commands:"
	@echo "    start       Build and launch pipeline services, wait until ready,"
	@echo "                and display clickable Airflow Web UI URL"
	@echo "    stop        Gracefully stop running containers (preserves DB data)"
	@echo "    restart     Restart all pipeline services"
	@echo "    status      Display container statuses, health, and port mappings"
	@echo "    logs        Follow live consolidated logs across all containers"
	@echo "    clean       Tear down containers and network (preserves DB volume)"
	@echo "    reset       Hard reset: wipe containers, networks, AND volumes"
	@echo ""
	@echo "  Pipeline & Database Operations:"
	@echo "    trigger     Trigger the data pipeline DAG in Airflow immediately"
	@echo "    test        Run the 15-test unit & scenario resilience test suite"
	@echo "    db-count    Query row counts and date ranges in PostgreSQL"
	@echo "    db-shell    Open an interactive psql shell inside PostgreSQL"
	@echo "    env         Generate .env from .env.example if missing"
	@echo "========================================================================"
	@echo ""

# ------------------------------------------------------------------------------
# Environment Setup
# ------------------------------------------------------------------------------
.PHONY: env
env:
	@$(PYTHON) -c "import os, shutil; os.path.exists('.env') or (os.path.exists('.env.example') and shutil.copy('.env.example', '.env') and print('[INFO] Initialized .env from .env.example'))"

# ------------------------------------------------------------------------------
# Lifecycle Targets
# ------------------------------------------------------------------------------
.PHONY: start
start: env
	@echo ""
	@echo "[1/3] Building container images and launching services..."
	@$(DOCKER_COMPOSE) up --build -d
	@echo ""
	@echo "[2/3] Waiting for Airflow and database services to initialize..."
	@$(PYTHON) scripts/wait_for_services.py 60
	@echo ""
	@echo "[3/3] Current container status:"
	@$(DOCKER_COMPOSE) ps
	@echo ""
	@echo "========================================================================"
	@echo "          🚀 STOCK MARKET DATA PIPELINE IS READY!"
	@echo "========================================================================"
	@echo ""
	@echo "  🔗 Airflow Web UI:      $(AIRFLOW_URL)"
	@echo "     👉 Hold Ctrl (or Cmd) and click the link above to open!"
	@echo ""
	@echo "  👤 Airflow Credentials: Username: admin  |  Password: admin"
	@echo "  🐘 PostgreSQL Host:     localhost:5432   |  DB: $(DB_NAME)  |  User: $(DB_USER)"
	@echo "  🩺 Airflow Health:      $(HEALTH_URL)"
	@echo ""
	@echo "  Useful Next Steps:"
	@echo "    - Trigger pipeline run:  make trigger"
	@echo "    - Stream container logs: make logs"
	@echo "    - Inspect stored data:   make db-count"
	@echo "    - Stop pipeline:         make stop"
	@echo "========================================================================"
	@echo ""

.PHONY: stop
stop:
	@echo "[INFO] Gracefully stopping pipeline containers..."
	@$(DOCKER_COMPOSE) stop
	@echo "[OK] All pipeline services stopped."

.PHONY: restart
restart:
	@echo "[INFO] Restarting pipeline services..."
	@$(DOCKER_COMPOSE) restart
	@echo "[INFO] Waiting for services to re-synchronize..."
	@$(PYTHON) scripts/wait_for_services.py 30
	@echo "[OK] Pipeline services restarted."
	@echo "🔗 Airflow UI: $(AIRFLOW_URL)"

.PHONY: status
status:
	@echo "========================================================================"
	@echo "                     PIPELINE CONTAINER STATUS"
	@echo "========================================================================"
	@$(DOCKER_COMPOSE) ps
	@echo ""
	@echo "🔗 Airflow Web UI: $(AIRFLOW_URL)"
	@echo "🩺 Airflow Health: $(HEALTH_URL)"

.PHONY: logs
logs:
	@$(DOCKER_COMPOSE) logs -f

.PHONY: clean
clean:
	@echo "[INFO] Stopping and removing containers and networks..."
	@$(DOCKER_COMPOSE) down
	@echo "[OK] Clean complete. Persistent database volumes were preserved."

.PHONY: reset
reset:
	@echo "[WARNING] Performing hard reset: destroying containers, networks, AND volumes..."
	@$(DOCKER_COMPOSE) down -v
	@echo "[OK] Hard reset complete. Database and logs volumes deleted."

# ------------------------------------------------------------------------------
# Operations & Testing Targets
# ------------------------------------------------------------------------------
.PHONY: trigger
trigger:
	@echo "[INFO] Triggering DAG '$(DAG_ID)' in Airflow..."
	@$(DOCKER_COMPOSE) exec airflow-webserver airflow dags trigger $(DAG_ID)
	@echo "[OK] DAG triggered successfully! View execution at: $(AIRFLOW_URL)"

.PHONY: test
test:
	@echo "========================================================================"
	@echo "                  RUNNING PIPELINE TEST SUITE (15 TESTS)"
	@echo "========================================================================"
	@$(PYTHON) -m unittest discover -s tests -v

.PHONY: db-count
db-count:
	@echo "========================================================================"
	@echo "              POSTGRESQL - INGESTED STOCK DATA SUMMARY"
	@echo "========================================================================"
	@$(DOCKER_COMPOSE) exec postgres psql -U $(DB_USER) -d $(DB_NAME) -c "SELECT symbol, COUNT(*) AS total_records, MIN(timestamp) AS earliest_date, MAX(timestamp) AS latest_date FROM stock_prices GROUP BY symbol ORDER BY symbol;"

.PHONY: db-shell
db-shell:
	@echo "[INFO] Connecting to interactive PostgreSQL shell (type '\q' to exit)..."
	@$(DOCKER_COMPOSE) exec -it postgres psql -U $(DB_USER) -d $(DB_NAME)
