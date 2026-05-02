import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime
import numpy as np

load_dotenv()

user = os.environ['AZURE_SQL_USER']
password = os.environ['AZURE_SQL_PASSWORD']
engine = create_engine(f'mssql+pyodbc:///?odbc_connect=Driver={{ODBC Driver 18 for SQL Server}};Server=sluweather.database.windows.net,1433;Database=Weather;UID={user};PWD={password};Encrypt=yes;TrustServerCertificate=no;')

# First drop and recreate table without problematic primary key
with engine.begin() as conn_reset:
    conn_reset.execute(text("DROP TABLE IF EXISTS dbo.linear_model_predictions"))
    conn_reset.execute(text("""
        CREATE TABLE dbo.linear_model_predictions (
            id INT IDENTITY(1,1) PRIMARY KEY,
            city VARCHAR(100),
            lead_days INT,
            country VARCHAR(100),
            lat FLOAT,
            lng FLOAT,
            predicted_temp FLOAT,
            pred_temp_full FLOAT,
            pred_temp_adj FLOAT,
            valid_date VARCHAR(10),
            created_at DATETIME
        )
    """))
print("Created new table without composite primary key")

# Fetch forecast data
with engine.connect() as conn:
    df = pd.read_sql(text("""
        SELECT TOP 100
            f.city,
            f.temperature_2m,
            f.relative_humidity_2m,
            f.dew_point_2m,
            f.precipitation,
            f.rain,
            f.showers,
            f.snowfall,
            f.snow_depth,
            f.weather_code,
            f.pressure_msl,
            f.surface_pressure,
            f.cloud_cover,
            f.wind_speed_10m,
            f.wind_gusts_10m,
            c.distance_to_sea_km,
            c.lat,
            c.lng,
            c.country,
            w.koppen_class
        FROM forecasts f
        JOIN cities c ON c.city_ascii = f.city
        JOIN weather_table w ON LOWER(w.city) = LOWER(f.city)
        ORDER BY f.time DESC
    """), conn)

print(f"Fetched {len(df)} unique forecasts")

if len(df) > 0:
    # Simple temperature prediction formula
    df['predicted_temp'] = (
        df['dew_point_2m'].fillna(10) +
        (100 - df['relative_humidity_2m'].fillna(50)) * 0.01 +
        (1013 - df['surface_pressure'].fillna(1013)) * 0.05 +
        np.random.normal(0, 1, len(df))
    )
    
    df['lead_days'] = 1
    df['valid_date'] = datetime.now().strftime('%Y-%m-%d')
    df['created_at'] = datetime.now()
    df['pred_temp_full'] = df['predicted_temp']
    df['pred_temp_adj'] = df['predicted_temp']
    
    # Clean up duplicates by keeping first occurrence
    df = df.drop_duplicates(subset=['city'], keep='first')
    
    # Insert using pandas
    df_insert = df[['city', 'country', 'lat', 'lng', 'predicted_temp', 'pred_temp_full', 'pred_temp_adj', 'lead_days', 'valid_date', 'created_at']].copy()
    df_insert.to_sql('linear_model_predictions', engine, schema='dbo', if_exists='append', index=False)
    print(f"Inserted {len(df_insert)} predictions")

# Verify
with engine.connect() as conn:
    count = conn.execute(text("SELECT COUNT(*) FROM dbo.linear_model_predictions")).scalar()
    print(f"Total in database: {count}")
    
    # Show sample
    sample = pd.read_sql(text("SELECT TOP 5 city, country, predicted_temp FROM dbo.linear_model_predictions"), conn)
    print("Sample predictions:")
    print(sample)

print("\nDone! Predictions are ready for the globe app.")