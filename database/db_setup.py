"""
db_setup.py — Weather Project Azure SQL initializer
====================================================
Creates all 6 tables and imports every data file.
Safe to re-run; all imports use MERGE (upsert) deduplication.

Prerequisites:
    pip install pyodbc sqlalchemy pandas

Env vars (set before running):
    $env:AZURE_SQL_USER     = "your_username"
    $env:AZURE_SQL_PASSWORD = "your_password"

Run:
    python db_setup.py
"""

import os, sys, zipfile, json
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse

# ── Progress tracking ────────────────────────────────────────────────────────
# Tracks which files have been fully imported so restarts skip completed work.

PROGRESS_FILE = Path(__file__).parent / "db_setup_progress.json"

def _load_progress() -> set:
    if PROGRESS_FILE.exists():
        try:
            return set(json.loads(PROGRESS_FILE.read_text()))
        except Exception:
            pass
    return set()

def _mark_done(key: str, completed: set):
    completed.add(key)
    PROGRESS_FILE.write_text(json.dumps(sorted(completed)))

# ── Config ───────────────────────────────────────────────────────────────────

ROOT     = Path(__file__).parent
SERVER   = "sluweather.database.windows.net"
DATABASE = "Weather"
DRIVER   = "ODBC Driver 18 for SQL Server"

# ── Connection ───────────────────────────────────────────────────────────────

def get_engine():
    user     = os.environ["AZURE_SQL_USER"]
    password = os.environ["AZURE_SQL_PASSWORD"]
    params = urllib.parse.quote_plus(
        f"Driver={{{DRIVER}}};"
        f"Server=tcp:{SERVER},1433;"
        f"Database={DATABASE};"
        f"UID={user};"
        f"PWD={password};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        fast_executemany=True,
        pool_pre_ping=True,      # test connection before use; discard stale ones
        pool_recycle=1800,       # recycle connections after 30 min to prevent Azure idle drops
    )

# ── Schema DDL ───────────────────────────────────────────────────────────────

TABLES = [
    # 1. cities
    """
IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.cities') AND type='U')
BEGIN
    CREATE TABLE dbo.cities (
        city_id            INT           IDENTITY(1,1) PRIMARY KEY,
        city               NVARCHAR(200) NOT NULL,
        city_ascii         NVARCHAR(200),
        lat                FLOAT,
        lng                FLOAT,
        country            NVARCHAR(100),
        iso2               NVARCHAR(10),
        iso3               NVARCHAR(10),
        admin_name         NVARCHAR(200),
        capital            NVARCHAR(100),
        population         BIGINT,
        worldcities_id     BIGINT,
        distance_to_sea_km FLOAT,
        CONSTRAINT uq_cities UNIQUE (city_ascii, iso2)
    );
    CREATE INDEX idx_cities_name    ON dbo.cities (city_ascii);
    CREATE INDEX idx_cities_country ON dbo.cities (iso2);
END
""",
    # 2. city_name_mappings
    """
IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.city_name_mappings') AND type='U')
BEGIN
    CREATE TABLE dbo.city_name_mappings (
        mapping_id INT           IDENTITY(1,1) PRIMARY KEY,
        raw_name   NVARCHAR(200) NOT NULL CONSTRAINT uq_mapping UNIQUE,
        canonical  NVARCHAR(200) NOT NULL,
        source     NVARCHAR(100)
    );
    CREATE INDEX idx_mapping_raw ON dbo.city_name_mappings (raw_name);
END
""",
    # 3. historical_weather
    """
IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.historical_weather') AND type='U')
BEGIN
    CREATE TABLE dbo.historical_weather (
        record_id            INT           IDENTITY(1,1) PRIMARY KEY,
        city                 NVARCHAR(200) NOT NULL,
        time                 DATETIME2     NOT NULL,
        temperature_2m       FLOAT, relative_humidity_2m FLOAT, dew_point_2m FLOAT,
        apparent_temperature FLOAT, precipitation        FLOAT, rain         FLOAT,
        showers              FLOAT, snowfall             FLOAT, snow_depth   FLOAT,
        weather_code         INT,   pressure_msl         FLOAT, surface_pressure FLOAT,
        cloud_cover          FLOAT, wind_speed_10m       FLOAT, wind_gusts_10m   FLOAT,
        retrieved_at         DATETIME2,
        CONSTRAINT uq_historical UNIQUE (city, time)
    );
    CREATE INDEX idx_hist_city ON dbo.historical_weather (city);
    CREATE INDEX idx_hist_time ON dbo.historical_weather (time);
END
""",
    # 4. forecasts
    """
IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.forecasts') AND type='U')
BEGIN
    CREATE TABLE dbo.forecasts (
        forecast_id              INT           IDENTITY(1,1) PRIMARY KEY,
        city                     NVARCHAR(200) NOT NULL,
        time                     DATETIME2     NOT NULL,
        retrieved_at             DATETIME2     NOT NULL,
        temperature_2m           FLOAT, relative_humidity_2m      FLOAT, dew_point_2m          FLOAT,
        apparent_temperature     FLOAT, precipitation_probability FLOAT, precipitation         FLOAT,
        rain                     FLOAT, showers                   FLOAT, snowfall              FLOAT,
        snow_depth               FLOAT, weather_code              INT,   pressure_msl          FLOAT,
        surface_pressure         FLOAT, cloud_cover               FLOAT, visibility            FLOAT,
        wind_speed_10m           FLOAT, wind_gusts_10m            FLOAT,
        CONSTRAINT uq_forecast UNIQUE (city, time, retrieved_at)
    );
    CREATE INDEX idx_fcast_city ON dbo.forecasts (city);
    CREATE INDEX idx_fcast_time ON dbo.forecasts (time);
END
""",
    # 5. forecast_accuracy
    """
IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.forecast_accuracy') AND type='U')
BEGIN
    CREATE TABLE dbo.forecast_accuracy (
        accuracy_id               INT           IDENTITY(1,1) PRIMARY KEY,
        city                      NVARCHAR(200) NOT NULL,
        forecast_issue_time       DATETIME2     NOT NULL,
        forecast_valid_time       DATETIME2     NOT NULL,
        valid_date                DATE, valid_hour INT, lead_time_hours FLOAT, lead_time_group NVARCHAR(50),
        temperature_2m            FLOAT, relative_humidity_2m      FLOAT, dew_point_2m          FLOAT,
        apparent_temperature      FLOAT, precipitation_probability FLOAT, precipitation         FLOAT,
        rain                      FLOAT, showers                   FLOAT, snowfall              FLOAT,
        snow_depth                FLOAT, weather_code              INT,   pressure_msl          FLOAT,
        surface_pressure          FLOAT, cloud_cover               FLOAT, visibility            FLOAT,
        wind_speed_10m            FLOAT, wind_gusts_10m            FLOAT,
        CONSTRAINT uq_accuracy UNIQUE (city, forecast_issue_time, forecast_valid_time)
    );
    CREATE INDEX idx_acc_city  ON dbo.forecast_accuracy (city);
    CREATE INDEX idx_acc_issue ON dbo.forecast_accuracy (forecast_issue_time);
END
""",
    # 6. city_error_metrics
    """
IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.city_error_metrics') AND type='U')
BEGIN
    CREATE TABLE dbo.city_error_metrics (
        metric_id           INT           IDENTITY(1,1) PRIMARY KEY,
        city                NVARCHAR(200) NOT NULL CONSTRAINT uq_city_error UNIQUE,
        average_error       FLOAT, valid_percentage    FLOAT, removed_percentage FLOAT,
        status              NVARCHAR(50),
        filter_threshold    FLOAT, keep_city_threshold FLOAT
    );
    CREATE INDEX idx_err_city ON dbo.city_error_metrics (city);
END
""",
]

# ── MERGE helper ──────────────────────────────────────────────────────────────

def merge_into(engine, df, stg, target, key_cols, update_on_match=True):
    """
    Write df to a staging table (dbo.stg), then MERGE into dbo.target.
    key_cols: columns forming the unique key (join condition).
    update_on_match: if True, update matched rows; if False, insert-only.
    Retries up to 3 times on transient TCP/network errors (08S01, 08001).
    """
    if df.empty:
        return

    import time as _time
    from sqlalchemy.exc import OperationalError, PendingRollbackError, SQLAlchemyError, IntegrityError

    # Strip whitespace from string key columns
    for c in key_cols:
        if c in df.columns and df[c].dtype == object:
            df[c] = df[c].str.strip()

    # Case-insensitive dedup to match SQL Server's default CI_AS collation.
    # pandas drop_duplicates is case-sensitive; "Ras el Oued" and "Ras El Oued"
    # are the same key in SQL Server but different strings in pandas.
    dedup_keys = df[key_cols].copy()
    for c in key_cols:
        if dedup_keys[c].dtype == object:
            dedup_keys[c] = dedup_keys[c].str.lower()
    df = df[~dedup_keys.duplicated(keep="first")]

    val_cols = [c for c in df.columns if c not in key_cols]

    # NULL-safe JOIN using plain equality (Azure SQL is CI_AS so case-insensitive by default).
    # Python already strips key string columns above, so RTRIM/LTRIM not needed in SQL.
    # Wrapping keys in functions (LOWER, RTRIM) would prevent index use on 45k+ row tables.
    def _join_clause(c):
        return f"(t.[{c}] = s.[{c}] OR (t.[{c}] IS NULL AND s.[{c}] IS NULL))"

    join = " AND ".join(_join_clause(c) for c in key_cols)
    all_c  = key_cols + val_cols
    ins_c  = ", ".join(f"[{c}]" for c in all_c)
    ins_v  = ", ".join(f"s.[{c}]" for c in all_c)

    matched_clause = ""
    if update_on_match and val_cols:
        upd = ", ".join(f"t.[{c}]=s.[{c}]" for c in val_cols)
        matched_clause = f"WHEN MATCHED THEN UPDATE SET {upd}"

    merge_sql = f"""
MERGE dbo.[{target}] AS t
USING dbo.[{stg}] AS s ON ({join})
{matched_clause}
WHEN NOT MATCHED THEN INSERT ({ins_c}) VALUES ({ins_v});
"""

    for attempt in range(20):  # up to 20 attempts, 60s between each
        try:
            # Flush pool on first attempt only if this is a retry (attempt > 0)
            if attempt > 0:
                engine.dispose()
            with engine.begin() as con:
                con.execute(text(f"DROP TABLE IF EXISTS dbo.[{stg}]"))
            df.to_sql(stg, engine, schema="dbo", if_exists="replace", index=False, chunksize=2000)
            with engine.begin() as con:
                con.execute(text(merge_sql))
                con.execute(text(f"DROP TABLE IF EXISTS dbo.[{stg}]"))
            print(f"    OK {len(df):>8,} rows  ->  {target}")
            return
        except IntegrityError as e:
            # Duplicate key: rows already exist in target with a different precision.
            # Log and skip — the data is already present.
            engine.dispose()
            print(f"    ⚠ Skipped chunk (IntegrityError — rows already present): {e.orig.args[1][:120]}")
            return
        except (OperationalError, PendingRollbackError) as e:
            # Transient errors: TCP drop (08S01/10060), timeout (08001), or
            # PendingRollbackError from a connection left in bad state after a TCP drop.
            if attempt < 19:
                print(f"    ⚠ Transient DB error on attempt {attempt+1}/20, retrying in 60s... ({type(e).__name__})")
                engine.dispose()
                _time.sleep(60)
            else:
                raise


def merge_chunks(engine, reader, stg, target, key_cols,
                 date_cols=None, update_on_match=False):
    """Iterate pandas chunk reader, coerce dates, then merge each chunk."""
    total = 0
    for chunk in reader:
        chunk.columns = [c.strip() for c in chunk.columns]
        if date_cols:
            for col in date_cols:
                if col in chunk.columns:
                    # floor to microseconds — DATETIME2 via pyodbc preserves pandas
                    # nanoseconds, causing precision mismatches across runs
                    chunk[col] = pd.to_datetime(chunk[col], errors="coerce").dt.floor("us")
        # Drop rows with NULL in any key column (e.g. corrupt/conflict-marker rows)
        before = len(chunk)
        chunk = chunk.dropna(subset=[c for c in key_cols if c in chunk.columns])
        dropped = before - len(chunk)
        if dropped:
            print(f"    Skipped {dropped} row(s) with NULL key columns")
        # Deduplicate within the chunk — MERGE source must not contain duplicate keys
        dedup_cols = [c for c in key_cols if c in chunk.columns]
        if not dedup_cols:
            print(f"    SKIP chunk — none of key columns {key_cols} found in file")
            return
        chunk = chunk.drop_duplicates(subset=dedup_cols)
        if chunk.empty:
            continue
        merge_into(engine, chunk, stg, target, key_cols, update_on_match)
        total += len(chunk)
    print(f"    Total: {total:,}")

# ── Importers ─────────────────────────────────────────────────────────────────

def import_cities(engine):
    print("\n[cities]")
    with engine.connect() as con:
        existing = con.execute(text("SELECT COUNT(*) FROM dbo.cities")).scalar()
    if existing:
        print(f"  {existing:,} rows already in dbo.cities — running upsert (skipping existing)")

    path = ROOT / "cities_with_distance_to_sea.csv"
    df   = pd.read_csv(path)
    df.columns = [c.strip().strip('"') for c in df.columns]
    df = df.rename(columns={"id": "worldcities_id",
                             "Distance_to_Sea_km": "distance_to_sea_km"})
    for col in ["lat", "lng", "population", "worldcities_id", "distance_to_sea_km"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    keep = ["city", "city_ascii", "lat", "lng", "country", "iso2", "iso3",
            "admin_name", "capital", "population", "worldcities_id", "distance_to_sea_km"]
    df = df[[c for c in keep if c in df.columns]]
    merge_into(engine, df, "stg_cities", "cities",
               key_cols=["city_ascii", "iso2"], update_on_match=True)


def import_historical_zip(engine, zip_path: Path, completed: set):
    key = zip_path.name
    if key in completed:
        print(f"\n[historical_weather]  {key}  SKIP (already done)")
        return
    print(f"\n[historical_weather]  {zip_path.name}")
    with zipfile.ZipFile(zip_path) as z:
        for entry in z.namelist():
            if not entry.endswith(".csv"):
                continue
            print(f"  Reading {entry} ...")
            with z.open(entry) as f:
                merge_chunks(
                    engine,
                    pd.read_csv(f, chunksize=50_000),
                    "stg_historical", "historical_weather",
                    key_cols=["city", "time"],
                    date_cols=["time", "retrieved_at"],
                    update_on_match=False,
                )
    _mark_done(key, completed)


def import_forecasts(engine, completed: set):
    for path in sorted((ROOT / "Forecast_Data").glob("*.csv")):
        if path.name in completed:
            print(f"\n[forecasts]  {path.name}  SKIP (already done)")
            continue
        print(f"\n[forecasts]  {path.name}")
        merge_chunks(
            engine,
            pd.read_csv(path, chunksize=50_000),
            "stg_forecasts", "forecasts",
            key_cols=["city", "time", "retrieved_at"],
            date_cols=["time", "retrieved_at"],
            update_on_match=False,
        )
        _mark_done(path.name, completed)


def import_forecast_accuracy(engine, completed: set):
    # summary_by_lead_time.csv is aggregate stats, not row-level — skip it
    SKIP = {"summary_by_lead_time.csv"}
    for path in sorted((ROOT / "forecast_by_lead_time").glob("*.csv")):
        if path.name in SKIP:
            print(f"\n[forecast_accuracy]  {path.name}  SKIP (summary file)")
            continue
        if path.name in completed:
            print(f"\n[forecast_accuracy]  {path.name}  SKIP (already done)")
            continue
        print(f"\n[forecast_accuracy]  {path.name}")
        merge_chunks(
            engine,
            pd.read_csv(path, chunksize=50_000),
            "stg_accuracy", "forecast_accuracy",
            key_cols=["city", "forecast_issue_time", "forecast_valid_time"],
            date_cols=["forecast_issue_time", "forecast_valid_time", "valid_date"],
            update_on_match=False,
        )
        _mark_done(path.name, completed)


def import_city_errors(engine, completed: set):
    key = "city_weather_error.csv"
    if key in completed:
        print(f"\n[city_error_metrics]  SKIP (already done)")
        return
    print("\n[city_error_metrics]")
    path = ROOT / "Email_Data_Csv" / "data" / key
    df   = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    merge_into(engine, df, "stg_errors", "city_error_metrics",
               key_cols=["city"], update_on_match=True)
    _mark_done(key, completed)

# ── Status report ─────────────────────────────────────────────────────────────

def status_report(engine):
    tables = ["cities", "city_name_mappings", "historical_weather",
              "forecasts", "forecast_accuracy", "city_error_metrics"]
    print("\n── Import Status ──────────────────────────────────────────")
    with engine.connect() as con:
        for t in tables:
            try:
                n = con.execute(text(f"SELECT COUNT(*) FROM dbo.[{t}]")).scalar()
                print(f"  {t:<30}  {n:>10,} rows")
            except Exception as e:
                print(f"  {t:<30}  ERROR: {e}")
    print("───────────────────────────────────────────────────────────\n")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"Weather DB Setup  |  {SERVER}  |  {DATABASE}\n")

    completed = _load_progress()
    if completed:
        print(f"  Resuming — {len(completed)} file(s) already done: {sorted(completed)}\n")

    engine = get_engine()

    # Verify connection (retry up to 5 times for transient startup TCP errors)
    import time as _time
    for _attempt in range(1, 6):
        try:
            with engine.connect() as con:
                ver = con.execute(text("SELECT @@VERSION")).scalar()
            break
        except Exception as _e:
            if _attempt == 5:
                raise
            wait = 10 * _attempt
            print(f"  Connection attempt {_attempt} failed ({type(_e).__name__}), retrying in {wait}s...")
            engine.dispose()
            _time.sleep(wait)
    print(f"Connected  OK  {ver[:70]}...\n")

    # Create schema
    print("[schema] Creating tables ...")
    for ddl in TABLES:
        with engine.begin() as con:
            con.execute(text(ddl.strip()))
    print("  OK All tables ready\n")

    # Reference data
    import_cities(engine)

    # Historical (ZIPs)
    for zname in [
        "Historical_Weather_Data_2026_01.zip",
        "Historical_Weather_Data_2026_02_part1.zip",
        "Historical_Weather_Data_2026_02_part2.zip",
    ]:
        zp = ROOT / zname
        if zp.exists():
            import_historical_zip(engine, zp, completed)
        else:
            print(f"\nSKIP (not found): {zname}")

    # Forecast and accuracy data
    import_forecasts(engine, completed)
    import_forecast_accuracy(engine, completed)
    import_city_errors(engine, completed)

    status_report(engine)
    print("Setup complete.")


if __name__ == "__main__":
    main()
