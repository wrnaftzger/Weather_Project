from __future__ import annotations

import os
import platform
import urllib.parse

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.exc import DBAPIError as SQLAlchemyDBAPIError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.exc import ProgrammingError as SQLAlchemyProgrammingError

load_dotenv()


SERVER = os.getenv("AZURE_SQL_SERVER", "sluweather.database.windows.net")
DATABASE = os.getenv("AZURE_SQL_DATABASE", "Weather")
ODBC_DRIVER = os.getenv("AZURE_SQL_ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
SQL_DIALECT = os.getenv("AZURE_SQL_DIALECT", "auto").strip().lower()


def _is_missing_forecast_accuracy_error(exc: Exception) -> bool:
    message = str(exc)
    msg_lower = message.lower()
    return (
        "forecast_accuracy" in msg_lower
        and (
            "invalid object name" in msg_lower
            or "object name" in msg_lower
            or "programmingerror" in msg_lower
            or "(208" in msg_lower
            or "208," in msg_lower
        )
    )


def _resolve_sqlalchemy_dialect() -> str:
    if SQL_DIALECT in {"pyodbc", "pymssql"}:
        return SQL_DIALECT
    return "pyodbc" if platform.system() == "Windows" else "pymssql"


def get_engine():
    user = os.environ["AZURE_SQL_USER"]
    password = os.environ["AZURE_SQL_PASSWORD"]

    dialect = _resolve_sqlalchemy_dialect()
    if dialect == "pyodbc":
        params = urllib.parse.quote_plus(
            f"Driver={{{ODBC_DRIVER}}};"
            f"Server=tcp:{SERVER},1433;"
            f"Database={DATABASE};"
            f"UID={user};"
            f"PWD={password};"
            "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        )
        return create_engine(
            f"mssql+pyodbc:///?odbc_connect={params}",
            pool_pre_ping=True,
        )

    return create_engine(
        URL.create(
            "mssql+pymssql",
            username=user,
            password=password,
            host=SERVER,
            port=1433,
            database=DATABASE,
            query={"charset": "utf8"},
        ),
        pool_pre_ping=True,
    )


def fetch_historical_error_training_data(lookback_days: int, sample_limit: int) -> pd.DataFrame:
    query_forecast_accuracy = f"""
    SELECT TOP ({int(sample_limit)})
        fa.city,
        fa.forecast_issue_time AS issue_time,
        fa.forecast_valid_time AS valid_time,
        COALESCE(
            fa.lead_time_hours,
            CAST(DATEDIFF(minute, fa.forecast_issue_time, fa.forecast_valid_time) AS FLOAT) / 60.0
        ) AS lead_time_hours,
        fa.temperature_2m AS forecast_temp,
        h.temperature_2m AS actual_temp,
        c.lat,
        c.lng
    FROM forecast_accuracy fa
    JOIN historical_weather h
        ON h.city = fa.city
       AND h.time = fa.forecast_valid_time
    JOIN cities c
        ON c.city_ascii = fa.city
    WHERE fa.temperature_2m IS NOT NULL
      AND h.temperature_2m IS NOT NULL
      AND c.lat IS NOT NULL
      AND c.lng IS NOT NULL
      AND fa.forecast_valid_time >= DATEADD(day, -{int(lookback_days)}, GETUTCDATE())
    ORDER BY fa.forecast_valid_time DESC;
    """
    query_forecasts_fallback = f"""
    SELECT TOP ({int(sample_limit)})
        f.city,
        f.retrieved_at AS issue_time,
        f.time AS valid_time,
        CAST(DATEDIFF(minute, f.retrieved_at, f.time) AS FLOAT) / 60.0 AS lead_time_hours,
        f.temperature_2m AS forecast_temp,
        h.temperature_2m AS actual_temp,
        c.lat,
        c.lng
    FROM forecasts f
    JOIN historical_weather h
        ON h.city = f.city
       AND h.time = f.time
    JOIN cities c
        ON c.city_ascii = f.city
    WHERE f.temperature_2m IS NOT NULL
      AND h.temperature_2m IS NOT NULL
      AND c.lat IS NOT NULL
      AND c.lng IS NOT NULL
      AND f.time >= DATEADD(day, -{int(lookback_days)}, GETUTCDATE())
      AND f.retrieved_at IS NOT NULL
      AND f.retrieved_at <= f.time
    ORDER BY f.time DESC;
    """
    with get_engine().connect() as conn:
        try:
            df = pd.read_sql(query_forecast_accuracy, conn)
        except (
            SQLAlchemyProgrammingError,
            SQLAlchemyDBAPIError,
            SQLAlchemyError,
            pd.errors.DatabaseError,
        ) as exc:
            if not _is_missing_forecast_accuracy_error(exc):
                raise
            print(
                "[WARN] Table 'forecast_accuracy' not found. "
                "Falling back to training from 'forecasts' + 'historical_weather'."
            )
            df = pd.read_sql(query_forecasts_fallback, conn)
    if df.empty:
        raise RuntimeError("No historical forecast/actual pairs found for model training.")
    return df


def fetch_latest_station_forecasts() -> pd.DataFrame:
    query = """
    WITH ranked AS (
        SELECT
            f.city,
            f.retrieved_at AS issue_time,
            f.time AS valid_time,
            CAST(DATEDIFF(minute, f.retrieved_at, f.time) AS FLOAT) / 60.0 AS lead_time_hours,
            f.temperature_2m AS forecast_temp,
            f.apparent_temperature,
            f.relative_humidity_2m,
            f.precipitation,
            f.wind_speed_10m,
            f.wind_gusts_10m,
            c.lat,
            c.lng,
            c.country,
            ROW_NUMBER() OVER (
                PARTITION BY f.city
                ORDER BY ABS(DATEDIFF(minute, f.time, GETUTCDATE())), f.retrieved_at DESC
            ) AS rn
        FROM forecasts f
        JOIN cities c
            ON c.city_ascii = f.city
        WHERE f.temperature_2m IS NOT NULL
          AND c.lat IS NOT NULL
          AND c.lng IS NOT NULL
          AND f.retrieved_at >= DATEADD(day, -14, GETUTCDATE())
    )
    SELECT
        city,
        issue_time,
        valid_time,
        lead_time_hours,
        forecast_temp,
        apparent_temperature,
        relative_humidity_2m,
        precipitation,
        wind_speed_10m,
        wind_gusts_10m,
        lat,
        lng,
        country
    FROM ranked
    WHERE rn = 1
    ORDER BY city;
    """
    with get_engine().connect() as conn:
        df = pd.read_sql(query, conn)
    if df.empty:
        raise RuntimeError("No forecast station data returned from database.")
    return df


def fetch_linear_model_predictions(lead_days: int) -> pd.DataFrame:
    """Fetch predictions from the R linear model for the specified lead_days.

    Reads from the database table linear_model_predictions.
    """
    query = f"""
    SELECT
        city,
        country,
        lat,
        lng,
        predicted_temp,
        pred_temp_full,
        pred_temp_adj,
        valid_date,
        lead_days,
        created_at
    FROM dbo.linear_model_predictions
    WHERE lead_days = {int(lead_days)}
    ORDER BY city;
    """

    try:
        with get_engine().connect() as conn:
            df = pd.read_sql(query, conn)
    except (
        SQLAlchemyProgrammingError,
        SQLAlchemyDBAPIError,
        SQLAlchemyError,
        pd.errors.DatabaseError,
    ) as exc:
        raise FileNotFoundError(
            f"linear_model_predictions table not found or query failed: {exc}"
        )

    if df.empty:
        raise FileNotFoundError(
            f"No predictions found for lead_days={lead_days}. "
            "Run predict_weather.R to generate predictions."
        )

    # Rename columns to match what globe_plotting expects
    df = df.rename(columns={
        "predicted_temp": "corrected_temp",
        "pred_temp_full": "forecast_temp",
        "pred_temp_adj": "predicted_error",
    })

    # Add required columns that globe_plotting expects
    df["valid_time"] = pd.to_datetime(df.get("valid_date", ""), errors="coerce")
    df["issue_time"] = pd.to_datetime(df.get("created_at", ""), errors="coerce")

    return df
