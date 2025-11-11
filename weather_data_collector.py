import os
import requests
import pandas as pd
from datetime import datetime
import time

# Load cities from the CSV file
def load_cities():
    df = pd.read_csv("cities_and_countries.csv")
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
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": city_info["lat"],
        "longitude": city_info["lon"],
        "hourly": "temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,precipitation_probability,precipitation,rain,showers,snowfall,snow_depth,weather_code,pressure_msl,surface_pressure,cloud_cover,visibility,wind_speed_10m,wind_gusts_10m",
        "forecast_days": 1
    }
    
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
            
            print(f"✅ Success for {city_info['city']}")
            return df
            
        except requests.Timeout:
            attempt += 1
            wait_time = min(attempt * 5, 60)  # Exponential backoff capped at 60s
            print(f"⏱️ Timeout for {city_info['city']}, retrying in {wait_time}s... (attempt {attempt + 1})")
            time.sleep(wait_time)
                
        except requests.RequestException as e:
            attempt += 1
            wait_time = min(attempt * 5, 60)  # Exponential backoff capped at 60s
            print(f"⚠️ Error for {city_info['city']}: {e}")
            print(f"   Retrying in {wait_time}s... (attempt {attempt + 1})")
            time.sleep(wait_time)

# Main function
def pull_weather_data():
    print(f"Pulling weather data at {datetime.now()}")
    
    cities = load_cities()
    all_data_list = []
    
    for i, city in enumerate(cities):
        city_df = get_forecast(city)  # Will always succeed eventually
        all_data_list.append(city_df)
        
        # Add delay between EVERY request to be more respectful to the API
        # Skip delay on the last city
        if i < len(cities) - 1:
            time.sleep(2)  # 2 seconds between each request
    
    print(f"\n📊 Summary: Successfully retrieved data for all {len(cities)} cities")
    
    all_data = pd.concat(all_data_list, ignore_index=True)
    
    # Append to CSV instead of rewriting
    file_exists = os.path.exists("us_city_forecasts.csv")
    all_data.to_csv(
        "us_city_forecasts.csv",
        index=False,
        mode='a',  # append mode
        header=not file_exists  # only write header if file doesn't exist
    )
    
    print(f"✅ Done! Saved data for {len(cities)} cities at {datetime.now()}")

# Entry point
if __name__ == "__main__":
    pull_weather_data()