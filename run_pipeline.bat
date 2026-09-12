@echo off
setlocal enabledelayedexpansion

echo ======================================================================
echo           STOCK MARKET DATA PIPELINE - 1-CLICK LAUNCHER
echo ======================================================================
echo.

:: 1. Check if Docker is available
where docker >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Docker command not found in PATH!
    echo.
    echo Please make sure Docker Desktop is installed and running:
    echo  1. Open 'Docker Desktop' from your Windows Start Menu.
    echo  2. Wait until the whale icon turns green (Engine running).
    echo  3. Run this script again.
    echo.
    echo If Docker Desktop is not installed, download it from:
    echo    https://www.docker.com/products/docker-desktop/
    echo.
    pause
    exit /b 1
)

:: 2. Check if Docker daemon is running
docker info >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Docker daemon is not running!
    echo Please open Docker Desktop and wait until the engine starts.
    echo.
    pause
    exit /b 1
)

echo [OK] Docker daemon is running.
echo.

:: 3. Verify .env file
if not exist ".env" (
    if exist ".env.example" (
        echo [INFO] .env not found. Copying from .env.example...
        copy .env.example .env
        echo [WARNING] Created default .env file. Please edit .env to add your ALPHA_VANTAGE_API_KEY if needed.
    ) else (
        echo [ERROR] .env file is missing!
        pause
        exit /b 1
    )
)

echo [INFO] Building images and starting pipeline containers...
docker compose up --build -d

echo.
echo [INFO] Waiting for containers to initialize...
timeout /t 10 /nobreak >nul

echo.
echo ======================================================================
echo                     CONTAINER STATUS
echo ======================================================================
docker compose ps

echo.
echo ======================================================================
echo                     SERVICES READY
echo ======================================================================
echo  - Airflow Webserver: http://localhost:8080
echo    Credentials:       admin / admin
echo.
echo  - PostgreSQL Host:   localhost:5432
echo    Database / User:   stockdb / stockuser
echo.
echo  - To view live logs: docker compose logs -f
echo  - To stop pipeline:  docker compose down
echo ======================================================================
echo.
pause
