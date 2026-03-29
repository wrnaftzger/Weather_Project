import os
import requests
import pandas as pd
from datetime import datetime
import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, IntegrityError
import urllib.parse

# Database connection with robust retry logic
def get_engine():
    user = os.environ["AZURE_SQL_USER"]
    password = os.environ["AZURE_SQL_PASSWORD"]
    server = "sluweather.database.windows.net"
    database = "Weather"
    driver = "ODBC Driver 18 for SQL Server"
    
    params = urllib.parse.quote_plus(
        f"Driver={{{driver}}};"
        f"Server=tcp:{server},1433;"
        f"Database={database};"
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

def connect_with_retry(max_wait_minutes=25):
    """
    Try to connect to database with 1-minute retry intervals.
    Will retry for up to max_wait_minutes in case of network issues.
    """
    retry_interval = 60  # 1 minute between retries
    max_attempts = max_wait_minutes
    
    for attempt in range(1, max_attempts + 1):
        try:
            print(f"[CONN] Connection attempt {attempt}/{max_attempts}...")
            engine = get_engine()
            
            # Test the connection
            with engine.connect() as con:
                count = con.execute(text("SELECT COUNT(*) FROM dbo.forecasts")).scalar()
                print(f"[OK] Connected successfully!")
                print(f"    Current forecast records in database: {count:,}\n")
                return engine, count
                
        except Exception as e:
            error_msg = str(e)
            if "timeout" in error_msg.lower() or "tcp" in error_msg.lower() or "network" in error_msg.lower():
                if attempt < max_attempts:
                    print(f"[WARN] Network/timeout error: {error_msg[:100]}...")
                    print(f"    Waiting 1 minute before retry (attempt {attempt}/{max_attempts})...")
                    print(f"    Tip: Check your WiFi/network connection\n")
                    time.sleep(retry_interval)
                else:
                    print(f"[ERROR] Failed to connect after {max_attempts} attempts over {max_wait_minutes} minutes")
                    raise
            else:
                # Non-network error, raise immediately
                raise

# MERGE data into database (upsert pattern)
def merge_into_db(engine, df):
    """Insert forecast data into database using MERGE to avoid duplicates."""
    if df.empty:
        return
    
    # Convert time to datetime and floor to microseconds for consistency
    df['time'] = pd.to_datetime(df['time']).dt.floor("us")
    df['retrieved_at'] = pd.to_datetime(df['retrieved_at']).dt.floor("us")
    
    # Strip whitespace from city names
    df['city'] = df['city'].str.strip()
    
    # Deduplicate within the dataframe
    df = df.drop_duplicates(subset=['city', 'time', 'retrieved_at'])
    
    stg = "stg_forecasts_live"
    target = "forecasts"
    key_cols = ["city", "time", "retrieved_at"]
    
    for attempt in range(5):
        try:
            if attempt > 0:
                engine.dispose()
            
            # Drop staging table if exists
            with engine.begin() as con:
                con.execute(text(f"DROP TABLE IF EXISTS dbo.[{stg}]"))
            
            # Write to staging table
            df.to_sql(stg, engine, schema="dbo", if_exists="replace", index=False, chunksize=2000)
            
            # MERGE staging into target
            merge_sql = f"""
            MERGE dbo.[{target}] AS t
            USING dbo.[{stg}] AS s 
            ON t.city = s.city AND t.time = s.time AND t.retrieved_at = s.retrieved_at
            WHEN NOT MATCHED THEN 
                INSERT (city, time, retrieved_at, temperature_2m, relative_humidity_2m, dew_point_2m,
                        apparent_temperature, precipitation_probability, precipitation, rain, showers,
                        snowfall, snow_depth, weather_code, pressure_msl, surface_pressure, 
                        cloud_cover, visibility, wind_speed_10m, wind_gusts_10m)
                VALUES (s.city, s.time, s.retrieved_at, s.temperature_2m, s.relative_humidity_2m, s.dew_point_2m,
                        s.apparent_temperature, s.precipitation_probability, s.precipitation, s.rain, s.showers,
                        s.snowfall, s.snow_depth, s.weather_code, s.pressure_msl, s.surface_pressure,
                        s.cloud_cover, s.visibility, s.wind_speed_10m, s.wind_gusts_10m);
            """
            
            with engine.begin() as con:
                con.execute(text(merge_sql))
                con.execute(text(f"DROP TABLE IF EXISTS dbo.[{stg}]"))
            
            print(f"    [OK] Inserted {len(df):,} rows into database")
            return
            
        except IntegrityError as e:
            engine.dispose()
            print(f"    [SKIP] Skipped (duplicate data already exists)")
            return
        except OperationalError as e:
            if attempt < 4:
                wait = 10 * (attempt + 1)
                print(f"    [WARN] DB error, retrying in {wait}s... (attempt {attempt+2}/5)")
                engine.dispose()
                time.sleep(wait)
            else:
                raise

# Load cities from the CSV file
def load_cities():
    df = pd.read_csv("data/world_cities/cities_and_countries.csv")
    df = df.dropna(subset=['Latitude', 'Longitude'])
    
    cities = []
    for _, row in df.iterrows():
        cities.append({
            'city': row['City'],
            'lat': row['Latitude'],
            'lon': row['Longitude']
        })
    
    return cities

# Get weather forecast for one city with unlimited retry logic
def get_forecast(city_info, start_date=None, end_date=None):
    print(f"Fetching weather for {city_info['city']}...")
    url = "https://historical-forecast-api.open-meteo.com/v1/forecast"
    params = {
        "latitude": city_info["lat"],
        "longitude": city_info["lon"],
        "hourly": "temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,precipitation_probability,precipitation,rain,showers,snowfall,snow_depth,weather_code,pressure_msl,surface_pressure,cloud_cover,visibility,wind_speed_10m,wind_gusts_10m",
    }
    
    # Use date range if provided, otherwise use forecast_days for current forecast
    if start_date and end_date:
        params["start_date"] = start_date
        params["end_date"] = end_date
    else:
        params["forecast_days"] = 1
    
    attempt = 0
    while True:  # Retry indefinitely until success
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Create dataframe with all variables
            df = pd.DataFrame({
                "time": data["hourly"]["time"],
                "temperature_2m": data["hourly"]["temperature_2m"],
                "relative_humidity_2m": data["hourly"]["relative_humidity_2m"],
                "dew_point_2m": data["hourly"]["dew_point_2m"],
                "apparent_temperature": data["hourly"]["apparent_temperature"],
                "precipitation_probability": data["hourly"]["precipitation_probability"],
                "precipitation": data["hourly"]["precipitation"],
                "rain": data["hourly"]["rain"],
                "showers": data["hourly"]["showers"],
                "snowfall": data["hourly"]["snowfall"],
                "snow_depth": data["hourly"]["snow_depth"],
                "weather_code": data["hourly"]["weather_code"],
                "pressure_msl": data["hourly"]["pressure_msl"],
                "surface_pressure": data["hourly"]["surface_pressure"],
                "cloud_cover": data["hourly"]["cloud_cover"],
                "visibility": data["hourly"]["visibility"],
                "wind_speed_10m": data["hourly"]["wind_speed_10m"],
                "wind_gusts_10m": data["hourly"]["wind_gusts_10m"]
            })
            df["city"] = city_info["city"]
            df["retrieved_at"] = datetime.now().isoformat()
            
            print(f"[OK] Success for {city_info['city']}")
            return df
            
        except requests.Timeout:
            attempt += 1
            wait_time = min(attempt * 5, 60)  # backoff capped at 60s
            print(f"[TIMEOUT] Timeout for {city_info['city']}, retrying in {wait_time}s... (attempt {attempt + 1})")
            time.sleep(wait_time)
                
        except requests.RequestException as e:
            attempt += 1
            wait_time = min(attempt * 5, 60)  #backoff capped at 60s
            print(f"[WARN] Error for {city_info['city']}: {e}")
            print(f"   Retrying in {wait_time}s... (attempt {attempt + 1})")
            time.sleep(wait_time)

# Main function
def pull_weather_data(start_date=None, end_date=None):
    print(f"Pulling weather data at {datetime.now()}")
    print(f"    Database: sluweather.database.windows.net / Weather")
    
    if start_date and end_date:
        print(f"    Date range: {start_date} to {end_date}\n")
    else:
        print(f"    Mode: Current forecast (1 day ahead)\n")
    
    # Connect to database with retry logic
    try:
        engine, initial_count = connect_with_retry(max_wait_minutes=25)
    except KeyError:
        print("[ERROR] Set AZURE_SQL_USER and AZURE_SQL_PASSWORD environment variables")
        return
    except Exception as e:
        print(f"[ERROR] Could not connect to database after multiple retries: {e}")
        return
    
    cities = load_cities()
    print(f"    Loading forecasts for {len(cities)} cities...\n")

    all_data_list = []
    
    for i, city in enumerate(cities):
        city_df = get_forecast(city, start_date, end_date)  
        all_data_list.append(city_df)
        
        # Skip delay on the last city
        if i < len(cities) - 1:
            time.sleep(2)  # 2 seconds between API calls
    
    print(f"\nSummary: Successfully retrieved data for all {len(cities)} cities")
    
    all_data = pd.concat(all_data_list, ignore_index=True)
    
    print(f"\nWriting {len(all_data):,} records to database...")
    merge_into_db(engine, all_data)
    
    # Show final count
    with engine.connect() as con:
        new_count = con.execute(text("SELECT COUNT(*) FROM dbo.forecasts")).scalar()
        added = new_count - initial_count
        print(f"    Database now has {new_count:,} forecast records (+{added:,} new)")
    
    print(f"\n[DONE] Completed at {datetime.now()}")

# Entry point
if __name__ == "__main__":
    import sys
    
    # Check for command-line date arguments
    if len(sys.argv) == 3:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
        print(f"Using date range from command line: {start_date} to {end_date}\n")
        pull_weather_data(start_date, end_date)
    else:
        # Default: pull current forecast (1 day ahead)
        pull_weather_data()