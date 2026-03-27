"""
_status_check.py — Comprehensive Azure SQL DB status check.
Run via:  python tools/_status_check.py
Requires env vars: AZURE_SQL_USER, AZURE_SQL_PASSWORD
"""
import os, time, urllib.parse
from sqlalchemy import create_engine, text

SERVER   = "sluweather.database.windows.net"
DATABASE = "Weather"
DRIVER   = "ODBC Driver 18 for SQL Server"


def get_engine():
    username = os.environ["AZURE_SQL_USER"]
    password = os.environ["AZURE_SQL_PASSWORD"]
    params = urllib.parse.quote_plus(
        f"Driver={{{DRIVER}}};"
        f"Server=tcp:{SERVER},1433;"
        f"Database={DATABASE};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        fast_executemany=True,
    )


# ── Connection with retry ─────────────────────────────────────────────────────
engine = None
for attempt in range(1, 4):
    try:
        print(f"[Attempt {attempt}/3] Connecting to {SERVER} / {DATABASE} ...")
        eng = get_engine()
        with eng.connect() as con:
            con.execute(text("SELECT 1"))
        engine = eng
        print(f"  ✓ Connected successfully on attempt {attempt}\n")
        break
    except Exception as e:
        print(f"  ✗ Attempt {attempt} failed: {e}")
        if attempt < 3:
            print("  Sleeping 15 s before retry ...")
            time.sleep(15)

if engine is None:
    print("\n❌  Could not connect after 3 attempts. Aborting.")
    raise SystemExit(1)


# ── Helper ────────────────────────────────────────────────────────────────────
def run(con, sql, label="query"):
    try:
        return con.execute(text(sql))
    except Exception as e:
        print(f"  ✗ Query failed [{label}]: {e}")
        return None


TABLES = [
    "cities",
    "city_name_mappings",
    "historical_weather",
    "forecasts",
    "forecast_accuracy",
    "city_error_metrics",
]

with engine.connect() as con:

    # ── 1. Row counts ─────────────────────────────────────────────────────────
    print("=" * 65)
    print("1.  ROW COUNTS")
    print("=" * 65)
    for tbl in TABLES:
        r = run(con, f"SELECT COUNT(*) FROM dbo.{tbl}", tbl)
        if r:
            n = r.scalar()
            print(f"   {tbl:<30} {n:>12,} rows")
        else:
            print(f"   {tbl:<30}       ERROR / NOT FOUND")

    # ── 2. historical_weather time range ──────────────────────────────────────
    print()
    print("=" * 65)
    print("2.  historical_weather  —  MIN / MAX time")
    print("=" * 65)
    r = run(con, "SELECT MIN(time), MAX(time) FROM dbo.historical_weather", "hist_time")
    if r:
        row = r.fetchone()
        print(f"   MIN time : {row[0]}")
        print(f"   MAX time : {row[1]}")

    # ── 3. forecasts time range ───────────────────────────────────────────────
    print()
    print("=" * 65)
    print("3.  forecasts  —  MIN / MAX time")
    print("=" * 65)
    r = run(con, "SELECT MIN(time), MAX(time) FROM dbo.forecasts", "fc_time")
    if r:
        row = r.fetchone()
        print(f"   MIN time : {row[0]}")
        print(f"   MAX time : {row[1]}")

    # ── 4. forecasts retrieved_at range ──────────────────────────────────────
    print()
    print("=" * 65)
    print("4.  forecasts  —  MIN / MAX retrieved_at")
    print("=" * 65)
    r = run(con, "SELECT MIN(retrieved_at), MAX(retrieved_at) FROM dbo.forecasts", "fc_retrieved")
    if r:
        row = r.fetchone()
        print(f"   MIN retrieved_at : {row[0]}")
        print(f"   MAX retrieved_at : {row[1]}")

    # ── 5. Sample forecasts ───────────────────────────────────────────────────
    print()
    print("=" * 65)
    print("5.  forecasts  —  3 sample rows  (most recent retrieved_at)")
    print("=" * 65)
    r = run(
        con,
        "SELECT TOP 3 city, time, retrieved_at, temperature_2m "
        "FROM dbo.forecasts ORDER BY retrieved_at DESC",
        "fc_sample",
    )
    if r:
        rows = r.fetchall()
        print(f"   {'city':<30} {'time':<22} {'retrieved_at':<22} {'temp_2m':>8}")
        print(f"   {'-'*30} {'-'*22} {'-'*22} {'-'*8}")
        for row in rows:
            print(
                f"   {str(row[0]):<30} {str(row[1]):<22} "
                f"{str(row[2]):<22} {str(row[3]):>8}"
            )

    # ── 6. Sample historical_weather ──────────────────────────────────────────
    print()
    print("=" * 65)
    print("6.  historical_weather  —  3 sample rows  (most recent time)")
    print("=" * 65)
    r = run(
        con,
        "SELECT TOP 3 city, time, temperature_2m "
        "FROM dbo.historical_weather ORDER BY time DESC",
        "hist_sample",
    )
    if r:
        rows = r.fetchall()
        print(f"   {'city':<30} {'time':<22} {'temp_2m':>8}")
        print(f"   {'-'*30} {'-'*22} {'-'*8}")
        for row in rows:
            print(f"   {str(row[0]):<30} {str(row[1]):<22} {str(row[2]):>8}")

    # ── 7. DISTINCT city counts ───────────────────────────────────────────────
    print()
    print("=" * 65)
    print("7.  DISTINCT city counts")
    print("=" * 65)
    r = run(con, "SELECT COUNT(DISTINCT city) FROM dbo.historical_weather", "hist_cities")
    if r:
        print(f"   historical_weather   distinct cities : {r.scalar():>6,}")
    r = run(con, "SELECT COUNT(DISTINCT city) FROM dbo.forecasts", "fc_cities")
    if r:
        print(f"   forecasts            distinct cities : {r.scalar():>6,}")

    print()
    print("=" * 65)
    print("✅  Status check complete.")
    print("=" * 65)
