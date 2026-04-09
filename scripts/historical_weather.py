import os
import requests
import pandas as pd
from datetime import datetime
import time

# Load cities from the CSV file
def load_cities():
    df = pd.read_csv("../cities_and_countries.csv")
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
def get_forecast(city_info):
    print(f"Fetching weather for {city_info['city']}...")
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": city_info["lat"],
        "longitude": city_info["lon"],
        "start_date": "2026-04-08",
        "end_date": "2026-04-08",
        "hourly": "temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,precipitation,rain,showers,snowfall,snow_depth,weather_code,pressure_msl,surface_pressure,cloud_cover,wind_speed_10m,wind_gusts_10m"
    }
    
    attempt = 0
    while True:  # Retry indefinitely until success
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Create dataframe with all available historical variables
            df = pd.DataFrame({
                "time": data["hourly"]["time"],
                "temperature_2m": data["hourly"]["temperature_2m"],
                "relative_humidity_2m": data["hourly"]["relative_humidity_2m"],
                "dew_point_2m": data["hourly"]["dew_point_2m"],
                "apparent_temperature": data["hourly"]["apparent_temperature"],
                "precipitation": data["hourly"]["precipitation"],
                "rain": data["hourly"]["rain"],
                "showers": data["hourly"]["showers"],
                "snowfall": data["hourly"]["snowfall"],
                "snow_depth": data["hourly"]["snow_depth"],
                "weather_code": data["hourly"]["weather_code"],
                "pressure_msl": data["hourly"]["pressure_msl"],
                "surface_pressure": data["hourly"]["surface_pressure"],
                "cloud_cover": data["hourly"]["cloud_cover"],
                "wind_speed_10m": data["hourly"]["wind_speed_10m"],
                "wind_gusts_10m": data["hourly"]["wind_gusts_10m"]
            })
            df["city"] = city_info["city"]
            df["retrieved_at"] = datetime.now().isoformat()
            
            print(f"[SUCCESS] Data retrieved for {city_info['city']}")
            return df
            
        except requests.Timeout:
            attempt += 1
            wait_time = min(attempt * 5, 60)  # backoff capped at 60s
            print(f"[TIMEOUT] {city_info['city']}, retrying in {wait_time}s... (attempt {attempt + 1})")
            time.sleep(wait_time)
                
        except requests.RequestException as e:
            attempt += 1
            wait_time = min(attempt * 5, 60)  #backoff capped at 60s
            print(f"[ERROR] {city_info['city']}: {e}")
            print(f"        Retrying in {wait_time}s... (attempt {attempt + 1})")
            time.sleep(wait_time)

# Main function
def pull_weather_data():
    print(f"Pulling weather data at {datetime.now()}")
    
    cities = load_cities()

    all_data_list = []
    
    for i, city in enumerate(cities):
        city_df = get_forecast(city)  
        all_data_list.append(city_df)
        
   
        # Skip delay on the last city
        if i < len(cities) - 1:
            time.sleep(2)  # 2 seconds 
    
    print(f"\n[SUMMARY] Successfully retrieved data for all {len(cities)} cities")
    
    all_data = pd.concat(all_data_list, ignore_index=True)
    
    # Create monthly rotating files (YYYY_MM format)
    folder = "Forecast_Data" 
    os.makedirs(folder, exist_ok=True)  # Create folder if it doesn't exist
    
    month_year = datetime.now().strftime("%Y_%m")
    filename = os.path.join(folder, f"Historical_Weather_Data_{month_year}.csv")
    
    # Append to monthly CSV
    file_exists = os.path.exists(filename)
    all_data.to_csv(
        filename,
        index=False,
        mode='a',  
        header=not file_exists  
    )
    
    print(f"[DONE] Saved data to {filename} at {datetime.now()}")

# Entry point
if __name__ == "__main__":
    pull_weather_data()