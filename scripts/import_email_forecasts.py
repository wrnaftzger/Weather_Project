"""
import_email_forecasts.py — Import email forecast data to Azure SQL
=====================================================================
Imports the processed email forecast CSV files into a new table.

Prerequisites:
    Set env vars: $env:AZURE_SQL_USER, $env:AZURE_SQL_PASSWORD

Run:
    python import_email_forecasts.py
"""

import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse

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
        pool_pre_ping=True,
        pool_recycle=1800,
    )

# ── Schema DDL ───────────────────────────────────────────────────────────────

EMAIL_FORECASTS_TABLE = """
IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.email_forecasts') AND type='U')
BEGIN
    CREATE TABLE dbo.email_forecasts (
        email_forecast_id INT           IDENTITY(1,1) PRIMARY KEY,
        date_and_time     DATETIME2     NOT NULL,
        city              NVARCHAR(200) NOT NULL,
        previous_lo       INT,
        previous_hi       INT,
        previous_precip   FLOAT,
        today_lo          INT,
        today_hi          INT,
        today_outlook     NVARCHAR(50),
        tomorrow_lo       INT,
        tomorrow_hi       INT,
        tomorrow_outlook  NVARCHAR(50),
        CONSTRAINT uq_email_forecast UNIQUE (city, date_and_time)
    );
    CREATE INDEX idx_email_fcast_city ON dbo.email_forecasts (city);
    CREATE INDEX idx_email_fcast_date ON dbo.email_forecasts (date_and_time);
END
"""

SELECTED_CITIES_TABLE = """
IF NOT EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.email_forecasts_selected') AND type='U')
BEGIN
    CREATE TABLE dbo.email_forecasts_selected (
        selected_id       INT           IDENTITY(1,1) PRIMARY KEY,
        date_and_time     DATETIME2     NOT NULL,
        city              NVARCHAR(200) NOT NULL,
        previous_lo       INT,
        previous_hi       INT,
        previous_precip   FLOAT,
        today_lo          INT,
        today_hi          INT,
        today_outlook     NVARCHAR(50),
        tomorrow_lo       INT,
        tomorrow_hi       INT,
        tomorrow_outlook  NVARCHAR(50),
        email_period      NVARCHAR(50),
        source_file       NVARCHAR(200),
        CONSTRAINT uq_selected_forecast UNIQUE (city, date_and_time, email_period)
    );
    CREATE INDEX idx_selected_city ON dbo.email_forecasts_selected (city);
    CREATE INDEX idx_selected_date ON dbo.email_forecasts_selected (date_and_time);
END
"""

# ── MERGE helper ──────────────────────────────────────────────────────────────

def merge_into(engine, df, stg, target, key_cols):
    """MERGE staging table into target using key_cols as unique constraint."""
    if df.empty:
        return
    
    from sqlalchemy.exc import OperationalError, IntegrityError
    import time
    
    # Strip whitespace from string key columns
    for c in key_cols:
        if c in df.columns and df[c].dtype == object:
            df[c] = df[c].str.strip()
    
    # Deduplicate
    df = df.drop_duplicates(subset=key_cols)
    
    val_cols = [c for c in df.columns if c not in key_cols]
    
    def _join_clause(c):
        return f"(t.[{c}] = s.[{c}] OR (t.[{c}] IS NULL AND s.[{c}] IS NULL))"
    
    join = " AND ".join(_join_clause(c) for c in key_cols)
    all_c  = key_cols + val_cols
    ins_c  = ", ".join(f"[{c}]" for c in all_c)
    ins_v  = ", ".join(f"s.[{c}]" for c in all_c)
    
    matched_clause = ""
    if val_cols:
        upd = ", ".join(f"t.[{c}]=s.[{c}]" for c in val_cols)
        matched_clause = f"WHEN MATCHED THEN UPDATE SET {upd}"
    
    merge_sql = f"""
MERGE dbo.[{target}] AS t
USING dbo.[{stg}] AS s ON ({join})
{matched_clause}
WHEN NOT MATCHED THEN INSERT ({ins_c}) VALUES ({ins_v});
"""
    
    for attempt in range(5):
        try:
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
            engine.dispose()
            print(f"    ⚠ Skipped (duplicate): {e.orig.args[1][:120]}")
            return
        except OperationalError as e:
            if attempt < 4:
                print(f"    ⚠ Retry {attempt+1}/5 after error...")
                engine.dispose()
                time.sleep(10)
            else:
                raise

# ── Importers ─────────────────────────────────────────────────────────────────

def import_email_data(engine):
    print("\n[email_forecasts] Importing email_data.csv ...")
    path = ROOT / "Email_Data_Csv" / "data" / "output_csv" / "email_data.csv"
    
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    df['date_and_time'] = pd.to_datetime(df['date_and_time']).dt.floor("us")
    
    merge_into(engine, df, "stg_email", "email_forecasts", 
               key_cols=["city", "date_and_time"])


def import_selected_cities(engine):
    print("\n[email_forecasts_selected] Importing selected_cities.csv ...")
    path = ROOT / "Email_Data_Csv" / "data" / "output_csv" / "selected_cities.csv"
    
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    df['date_and_time'] = pd.to_datetime(df['date_and_time']).dt.floor("us")
    
    merge_into(engine, df, "stg_selected", "email_forecasts_selected",
               key_cols=["city", "date_and_time", "email_period"])


def status_report(engine):
    print("\n── Import Status ──────────────────────────────────────────")
    with engine.connect() as con:
        for table in ["email_forecasts", "email_forecasts_selected"]:
            try:
                n = con.execute(text(f"SELECT COUNT(*) FROM dbo.[{table}]")).scalar()
                print(f"  {table:<35}  {n:>10,} rows")
            except Exception as e:
                print(f"  {table:<35}  ERROR: {e}")
    print("───────────────────────────────────────────────────────────\n")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"Email Forecasts Import  |  {SERVER}  |  {DATABASE}\n")
    
    engine = get_engine()
    
    # Verify connection
    with engine.connect() as con:
        ver = con.execute(text("SELECT @@VERSION")).scalar()
    print(f"Connected OK: {ver[:70]}...\n")
    
    # Create tables
    print("[schema] Creating email forecast tables ...")
    with engine.begin() as con:
        con.execute(text(EMAIL_FORECASTS_TABLE))
        con.execute(text(SELECTED_CITIES_TABLE))
    print("  OK Tables ready\n")
    
    # Import data
    import_email_data(engine)
    import_selected_cities(engine)
    
    status_report(engine)
    print("Import complete.")


if __name__ == "__main__":
    main()
