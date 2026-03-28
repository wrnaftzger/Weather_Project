"""
github_sync.py — Pull latest data from GitHub and import changed files into Azure SQL.

Usage:
    python github_sync.py           # pull and import only changed files
    python github_sync.py --force   # pull and re-run all importers regardless

Env vars required:
    AZURE_SQL_USER
    AZURE_SQL_PASSWORD
"""

import argparse
import logging
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent
LOG_FILE = ROOT / "github_sync_log.txt"


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("github_sync")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


def git(*args) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )


def get_head() -> str | None:
    """Return current HEAD SHA, or None if repo has no commits yet."""
    result = git("rev-parse", "HEAD")
    if result.returncode != 0:
        return None
    return result.stdout.strip()


def changed_files(old_head: str, new_head: str) -> list[str]:
    result = git("diff", "--name-only", old_head, new_head)
    if result.returncode != 0:
        return []
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def run_import(engine, files: list[str], log: logging.Logger) -> None:
    from db_setup import (
        import_historical_zip,
        import_forecasts,
        import_forecast_accuracy,
        import_city_errors,
        import_cities,
    )

    ran_forecasts = False
    ran_accuracy = False
    ran_city_errors = False
    ran_cities = False

    for f in files:
        p = Path(f)

        # Historical ZIPs in repo root
        if p.parent == Path(".") and p.name.startswith("Historical_Weather_Data_") and p.suffix == ".zip":
            zip_path = ROOT / p.name
            if zip_path.exists():
                log.info("Importing historical zip: %s", p.name)
                import_historical_zip(engine, zip_path)
            else:
                log.warning("Historical zip not found locally: %s", zip_path)

        # Forecast CSVs
        elif p.parts[0] == "Forecast_Data" and p.suffix == ".csv" and not ran_forecasts:
            log.info("Importing all forecast CSVs (triggered by: %s)", p.name)
            import_forecasts(engine)
            ran_forecasts = True

        # Forecast accuracy CSVs
        elif p.parts[0] == "forecast_by_lead_time" and p.suffix == ".csv" and not ran_accuracy:
            log.info("Importing all forecast_accuracy CSVs (triggered by: %s)", p.name)
            import_forecast_accuracy(engine)
            ran_accuracy = True

        # City error CSVs
        elif (len(p.parts) >= 2 and p.parts[0] == "Email_Data_Csv"
              and p.suffix == ".csv" and not ran_city_errors):
            log.info("Importing city error metrics (triggered by: %s)", p.name)
            import_city_errors(engine)
            ran_city_errors = True

        # City reference files
        elif p.name in ("cities_with_distance_to_sea.csv", "worldcities.csv") and not ran_cities:
            log.info("Importing cities (triggered by: %s)", p.name)
            import_cities(engine)
            ran_cities = True


def run_all(engine, log: logging.Logger) -> None:
    """Run every importer (used for --force or first-run fallback)."""
    import db_setup
    log.info("Running full db_setup.main() ...")
    db_setup.main()


def main() -> int:
    parser = argparse.ArgumentParser(description="Pull GitHub changes and sync to Azure SQL.")
    parser.add_argument("--force", action="store_true",
                        help="Skip 'already up to date' check; re-run all importers.")
    args = parser.parse_args()

    log = setup_logging()
    log.info("=== github_sync started ===")

    # Capture HEAD before pull
    old_head = get_head()
    first_run = old_head is None
    if first_run:
        log.info("No previous commit detected — treating as first run.")

    # Pull
    log.info("Running: git pull origin main")
    pull = subprocess.run(
        ["git", "pull", "origin", "main"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    log.info("git pull stdout: %s", pull.stdout.strip())
    if pull.stderr.strip():
        log.debug("git pull stderr: %s", pull.stderr.strip())

    if pull.returncode != 0:
        log.error("git pull failed (exit %d): %s", pull.returncode, pull.stderr.strip())
        return 1

    new_head = get_head()

    # Already up to date?
    if not args.force and not first_run and "Already up to date." in pull.stdout:
        log.info("Repository already up to date. Nothing to import.")
        return 0

    # Build engine
    from db_setup import get_engine
    try:
        engine = get_engine()
    except KeyError as e:
        log.error("Missing environment variable: %s", e)
        return 1

    # First run → full setup
    if first_run or args.force:
        if first_run:
            log.info("First run: executing full db_setup.main()")
        else:
            log.info("--force flag set: re-running all importers")
        try:
            run_all(engine, log)
        except Exception:
            log.exception("Error during full import")
            return 1
        log.info("=== github_sync finished (full import) ===")
        return 0

    # Diff-based import
    files = changed_files(old_head, new_head)
    if not files:
        log.info("No relevant files changed between %s and %s.", old_head[:8], new_head[:8])
        log.info("=== github_sync finished (no changes) ===")
        return 0

    log.info("Changed files (%d): %s", len(files), ", ".join(files))
    try:
        run_import(engine, files, log)
    except Exception:
        log.exception("Error during import")
        return 1

    log.info("=== github_sync finished successfully ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
