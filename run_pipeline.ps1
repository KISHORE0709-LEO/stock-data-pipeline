# ==============================================================================
# Stock Market Data Pipeline - PowerShell Launcher
# ==============================================================================

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "          STOCK MARKET DATA PIPELINE - POWERSHELL LAUNCHER            " -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host ""

# 1. Check if Docker CLI is present
$dockerCmd = Get-Command docker -ErrorAction SilentlyContinue
if (-not $dockerCmd) {
    Write-Host "[ERROR] Docker CLI was not found in PATH." -ForegroundColor Red
    Write-Host ""
    Write-Host "Please do the following:" -ForegroundColor Yellow
    Write-Host "  1. If Docker Desktop is installed, open it from the Start Menu."
    Write-Host "  2. Wait for Docker Engine to start (solid green whale icon in system tray)."
    Write-Host "  3. If Docker Desktop is not installed, download from: https://www.docker.com/products/docker-desktop/"
    Write-Host "     Or run in an admin terminal: winget install Docker.DockerDesktop"
    Write-Host ""
    exit 1
}

# 2. Check if Docker daemon is responsive
try {
    $null = docker info 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Docker engine not running"
    }
}
catch {
    Write-Host "[ERROR] Docker Desktop is installed but the Docker daemon is NOT running." -ForegroundColor Red
    Write-Host "Please start the Docker Desktop app and wait for the engine to initialize." -ForegroundColor Yellow
    exit 1
}

Write-Host "[OK] Docker engine is active." -ForegroundColor Green

# 3. Check .env
if (-not (Test-Path ".env")) {
    if (Test-Path ".env.example") {
        Write-Host "[INFO] .env not found. Initializing from .env.example..." -ForegroundColor Yellow
        Copy-Item ".env.example" ".env"
    } else {
        Write-Host "[ERROR] Missing .env file!" -ForegroundColor Red
        exit 1
    }
}

# 4. Build and start containers
Write-Host "[INFO] Building images and launching containers via Docker Compose..." -ForegroundColor Cyan
docker compose up --build -d

Write-Host "[INFO] Waiting 10 seconds for initial container health checks..." -ForegroundColor Gray
Start-Sleep -Seconds 10

# 5. Display status
Write-Host ""
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "                     CONTAINER STATUS                                 " -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan
docker compose ps

Write-Host ""
Write-Host "======================================================================" -ForegroundColor Green
Write-Host "                     PIPELINE READY                                   " -ForegroundColor Green
Write-Host "======================================================================" -ForegroundColor Green
Write-Host "  * Airflow Web UI:      http://localhost:8080" -ForegroundColor White
Write-Host "    Username / Password: admin / admin" -ForegroundColor White
Write-Host "  * Airflow Health:      http://localhost:8080/health" -ForegroundColor White
Write-Host "  * PostgreSQL Host:     localhost:5432 (stockdb / stockuser)" -ForegroundColor White
Write-Host ""
Write-Host "Useful Commands:" -ForegroundColor Yellow
Write-Host "  - View live logs:      docker compose logs -f"
Write-Host "  - Trigger DAG:         docker compose exec airflow-webserver airflow dags trigger stock_market_data_pipeline"
Write-Host "  - Query database:      docker compose exec postgres psql -U stockuser -d stockdb -c 'SELECT count(*) FROM stock_prices;'"
Write-Host "  - Stop pipeline:       docker compose down"
Write-Host "======================================================================" -ForegroundColor Green
