"""
weather_watcher.py — Auto-import watcher for the Weather Project
=================================================================
Monitors 4 folders for new CSV / ZIP files and routes them to the
correct importer automatically.

Prerequisites:
    pip install pyodbc sqlalchemy pandas watchdog

Env vars (must be set before running):
    $env:AZURE_SQL_USER     = "your_username"
    $env:AZURE_SQL_PASSWORD = "your_password"

Run (foreground):
    python weather_watcher.py

Run (background, detached):
    Start-Process python -ArgumentList "weather_watcher.py" -WindowStyle Hidden
"""

import os, time, logging
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from sqlalchemy import create_engine
import urllib.parse

# Re-use importers from db_setup
from db_setup import (
    get_engine,
    import_historical_zip,
    import_forecasts,
    import_forecast_accuracy,
    import_city_errors,
    merge_chunks,
)
import pandas as pd

# ── Config ───────────────────────────────────────────────────────────────────

ROOT     = Path(__file__).parent
LOG_FILE = ROOT / "import_log.txt"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("watcher")

# ── Routing logic ─────────────────────────────────────────────────────────────

def route_file(path: Path, engine):
    """Dispatch a newly arrived file to the correct importer."""
    suffix = path.suffix.lower()
    rel    = path.relative_to(ROOT)

    try:
        if suffix == ".zip" and "Historical" in path.name:
            log.info(f"Historical ZIP detected: {rel}")
            import_historical_zip(engine, path)

        elif suffix == ".csv" and path.parent == ROOT / "Forecast_Data":
            log.info(f"Forecast CSV detected: {rel}")
            merge_chunks(
                engine,
                pd.read_csv(path, chunksize=50_000),
                "stg_forecasts", "forecasts",
                key_cols=["city", "time", "retrieved_at"],
                date_cols=["time", "retrieved_at"],
            )

        elif suffix == ".csv" and path.parent == ROOT / "forecast_by_lead_time":
            log.info(f"Lead-time CSV detected: {rel}")
            merge_chunks(
                engine,
                pd.read_csv(path, chunksize=50_000),
                "stg_accuracy", "forecast_accuracy",
                key_cols=["city", "forecast_issue_time", "forecast_valid_time"],
                date_cols=["forecast_issue_time", "forecast_valid_time", "valid_date"],
            )

        elif suffix == ".csv" and "city_weather_error" in path.name:
            log.info(f"Error metrics CSV detected: {rel}")
            import_city_errors(engine)

        else:
            log.debug(f"Ignored (no route): {rel}")
            return

        log.info(f"Import complete: {rel}")

    except Exception as e:
        log.error(f"Import FAILED for {rel}: {e}", exc_info=True)

# ── Event handler ─────────────────────────────────────────────────────────────

class ImportHandler(FileSystemEventHandler):
    def __init__(self, engine):
        self.engine = engine
        self._seen  = set()

    def on_created(self, event):
        if event.is_directory:
            return
        path = Path(event.src_path)
        if path in self._seen:
            return
        self._seen.add(path)
        # Brief delay to let the file finish writing
        time.sleep(2)
        route_file(path, self.engine)

    def on_moved(self, event):
        """Handle files moved/renamed into a watched folder."""
        if not event.is_directory:
            route_file(Path(event.dest_path), self.engine)

# ── Main ──────────────────────────────────────────────────────────────────────

WATCH_DIRS = [
    ROOT,                               # Historical ZIPs dropped here
    ROOT / "Forecast_Data",
    ROOT / "forecast_by_lead_time",
    ROOT / "Email_Data_Csv" / "data",
]

def main():
    log.info(f"Starting weather watcher  |  log → {LOG_FILE}")

    engine   = get_engine()
    handler  = ImportHandler(engine)
    observer = Observer()

    for d in WATCH_DIRS:
        if d.exists():
            observer.schedule(handler, str(d), recursive=False)
            log.info(f"Watching: {d}")
        else:
            log.warning(f"Watch dir not found, skipping: {d}")

    observer.start()
    log.info("Watcher running. Press Ctrl+C to stop.")

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        log.info("Shutting down watcher...")
        observer.stop()

    observer.join()
    log.info("Watcher stopped.")


if __name__ == "__main__":
    main()
