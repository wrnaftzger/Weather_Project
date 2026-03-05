import os
import pandas as pd
import pyodbc
from sqlalchemy import create_engine, text
from datetime import datetime

# --- SQL Server connection details ---
server = "sluweather.database.windows.net"
database = "Weather"
username = "CloudSA651686c0"
driver = "ODBC Driver 18 for SQL Server"
password = os.environ.get("AZURE_SQL_PASSWORD")  # From GitHub secret

# --- CSV folder path ---
csv_folder = "Forecast_Data"  # folder containing your forecast CSVs

def get_engine():
    """
    Create a SQLAlchemy engine using pyodbc connection.
    """
    conn_str = (
        f"Driver={{{driver}}};"
        f"Server=tcp:{server},1433;"
        f"Database={database};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
    )
    conn = pyodbc.connect(conn_str)
    return create_engine("mssql+pyodbc://", creator=lambda: conn, fast_executemany=True)


def upload_forecast_csvs():
    """
    Upload all CSVs in Forecast_Data to the 'forecasts' table,
    skipping duplicates based on (city, time, retrieved_at).
    """
    engine = get_engine()

    # Fetch existing keys to avoid duplicates
    existing = pd.read_sql(
        text("SELECT city, time, retrieved_at FROM forecasts"),
        engine
    )

    # Iterate through CSVs
    for file in os.listdir(csv_folder):
        if not file.endswith(".csv"):
            continue

        path = os.path.join(csv_folder, file)
        df = pd.read_csv(path)

        # Convert datetime columns
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
        df["retrieved_at"] = pd.to_datetime(df["retrieved_at"], errors="coerce")

        # Drop rows with missing time
        df = df.dropna(subset=["time"])

        # Remove rows already in the table
        merged = df.merge(
            existing,
            on=["city", "time", "retrieved_at"],
            how="left",
            indicator=True
        )
        new_rows = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])

        if new_rows.empty:
            print(f"{file} → no new rows to insert")
            continue

        # Insert new rows
        new_rows.to_sql(
            "forecasts",
            engine,
            if_exists="append",
            index=False,
            chunksize=1000
        )

        print(f"{file} → inserted {len(new_rows)} rows")


if __name__ == "__main__":
    print(f"Starting forecast upload at {datetime.utcnow()} UTC")
    upload_forecast_csvs()
    print(f"Finished forecast upload at {datetime.utcnow()} UTC")