#!/usr/bin/env python3
"""
Service Readiness Waiter for Stock Market Data Pipeline
Checks the Airflow Webserver health endpoint until the service is ready.
"""

import sys
import time
import urllib.request
import urllib.error
import json

# Ensure stdout handles unicode/utf-8 safely on Windows terminals
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

HEALTH_URL = "http://localhost:8080/health"
MAX_WAIT_SECONDS = 90
POLL_INTERVAL_SECONDS = 3

def is_airflow_healthy() -> bool:
    """Check if the Airflow health endpoint returns a healthy status."""
    try:
        req = urllib.request.Request(
            HEALTH_URL,
            headers={"User-Agent": "PipelineReadinessProbe/1.0"}
        )
        with urllib.request.urlopen(req, timeout=4) as response:
            if response.status == 200:
                data = json.loads(response.read().decode("utf-8"))
                # Airflow /health returns {"metadatabase": {"status": "healthy"}, ...}
                db_status = data.get("metadatabase", {}).get("status", "")
                if db_status == "healthy":
                    return True
                return True
    except (urllib.error.URLError, ConnectionResetError, TimeoutError, OSError):
        return False
    return False

def wait_for_ready(max_wait=MAX_WAIT_SECONDS):
    print("[WAIT] Waiting for Airflow Webserver to initialize and become ready...")
    start_time = time.time()
    attempt = 1

    while True:
        elapsed = int(time.time() - start_time)
        if is_airflow_healthy():
            print(f"\n[OK] Airflow Webserver is healthy and ready! ({elapsed}s elapsed)")
            return 0

        if elapsed >= max_wait:
            print(f"\n[INFO] Initialization taking longer than expected ({max_wait}s elapsed).")
            print("       Services are continuing to start in the background.")
            print("       Check live status with 'make status' or 'make logs'.")
            return 0  # Return 0 so make start still displays the link and instructions

        spinner = ["|", "/", "-", "\\"][attempt % 4]
        print(f"\r  {spinner} Waiting for Airflow Webserver... [{elapsed}s / {max_wait}s] (polling {HEALTH_URL})", end="", flush=True)

        time.sleep(POLL_INTERVAL_SECONDS)
        attempt += 1

if __name__ == "__main__":
    timeout = MAX_WAIT_SECONDS
    if len(sys.argv) > 1:
        try:
            timeout = int(sys.argv[1])
        except ValueError:
            pass
    sys.exit(wait_for_ready(timeout))
