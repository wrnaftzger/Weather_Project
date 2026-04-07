import os
import pandas as pd
import pyodbc
from sqlalchemy import create_engine, text
from datetime import datetime

server = "sluweather.database.windows.net"
database = "Weather"
username = "CloudSA651686c0"
driver = "ODBC Driver 18 for SQL Server"
password = os.environ.get("AZURE_SQL_PASSWORD")

raw_folder = "data/forecasts"
binned_folder = "data/forecast_by_lead_time"

def get_engine():
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


def upload_raw_forecasts(engine):
    existing = pd.read_sql(
        text("SELECT city, time, retrieved_at FROM forecasts"),
        engine
    )

    for file in os.listdir(raw_folder):
        if not file.endswith(".csv"):
            continue

        path = os.path.join(raw_folder, file)
        df = pd.read_csv(path)

        df["time"] = pd.to_datetime(df["time"], errors="coerce")
        df["retrieved_at"] = pd.to_datetime(df["retrieved_at"], errors="coerce")
        df = df.dropna(subset=["time"])

        merged = df.merge(
            existing,
            on=["city", "time", "retrieved_at"],
            how="left",
            indicator=True
        )
        new_rows = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])

        if new_rows.empty:
            print(f"{file} → no new raw rows")
            continue

        new_rows.to_sql(
            "forecasts",
            engine,
            if_exists="append",
            index=False,
            chunksize=1000
        )

        print(f"{file} → inserted {len(new_rows)} raw rows")


def upload_binned_forecasts(engine):
    existing = pd.read_sql(
        text("SELECT city, forecast_valid_time, lead_time_hours FROM forecast_by_lead_time"),
        engine
    )

    for file in os.listdir(binned_folder):
        if not file.endswith(".csv"):
            continue

        path = os.path.join(binned_folder, file)
        df = pd.read_csv(path)

        df["forecast_valid_time"] = pd.to_datetime(df["forecast_valid_time"], errors="coerce")
        df["forecast_issue_time"] = pd.to_datetime(df["forecast_issue_time"], errors="coerce")
        df["lead_time_hours"] = df["lead_time_hours"].astype(int)

        df = df.dropna(subset=["forecast_valid_time"])

        merged = df.merge(
            existing,
            on=["city", "forecast_valid_time", "lead_time_hours"],
            how="left",
            indicator=True
        )
        new_rows = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])

        if new_rows.empty:
            print(f"{file} → no new binned rows")
            continue

        new_rows.to_sql(
            "forecast_by_lead_time",
            engine,
            if_exists="append",
            index=False,
            chunksize=1000
        )

        print(f"{file} → inserted {len(new_rows)} binned rows")


if __name__ == "__main__":
    print(f"Starting upload at {datetime.utcnow()} UTC")

    engine = get_engine()

    upload_raw_forecasts(engine)
    upload_binned_forecasts(engine)

    print(f"Finished upload at {datetime.utcnow()} UTC")