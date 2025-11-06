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

# Get weather forecast for one city with retry logic
def get_forecast(city_info, max_retries=3):
    print(f"Fetching weather for {city_info['city']}...")
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": city_info["lat"],
        "longitude": city_info["lon"],
        "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        "forecast_days": 1
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=30)  # Increased timeout
            response.raise_for_status()
            
            data = response.json()
            df = pd.DataFrame({
                "time": data["hourly"]["time"],
                "temperature_2m": data["hourly"]["temperature_2m"],
                "relative_humidity_2m": data["hourly"]["relative_humidity_2m"],
                "wind_speed_10m": data["hourly"]["wind_speed_10m"]
            })
            df["city"] = city_info["city"]
            df["retrieved_at"] = datetime.now().isoformat()
            
            return df
            
        except requests.Timeout:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5  # Exponential backoff: 5s, 10s, 15s
                print(f"⏱️ Timeout for {city_info['city']}, retrying in {wait_time}s... (attempt {attempt + 2}/{max_retries})")
                time.sleep(wait_time)
            else:
                print(f"⚠️ Failed to get data for {city_info['city']} after {max_retries} attempts: Timeout")
                return None
                
        except requests.RequestException as e:
            print(f"⚠️ Failed to get data for {city_info['city']}: {e}")
            return None
    
    return None

# Main function
def pull_weather_data():
    print(f"Pulling weather data at {datetime.now()}")
    
    cities = load_cities()
    all_data_list = []
    successful = 0
    failed = 0
    
    for i, city in enumerate(cities):
        city_df = get_forecast(city)
        if city_df is not None:
            all_data_list.append(city_df)
            successful += 1
        else:
            failed += 1
        
        # Add delay between EVERY request to be more respectful to the API
        # Skip delay on the last city
        if i < len(cities) - 1:
            time.sleep(2)  # 2 seconds between each request
    
    print(f"\n📊 Summary: {successful} successful, {failed} failed out of {len(cities)} cities")
    
    if not all_data_list:
        print("❌ No data retrieved. Exiting.")
        return
    
    all_data = pd.concat(all_data_list, ignore_index=True)
    
    # Append to CSV instead of rewriting
    file_exists = os.path.exists("us_city_forecasts.csv")
    all_data.to_csv(
        "us_city_forecasts.csv",
        index=False,
        mode='a',  # append mode
        header=not file_exists  # only write header if file doesn't exist
    )
    
    print(f"✅ Done! Saved data for {successful} cities at {datetime.now()}")

# Entry point
if __name__ == "__main__":
    pull_weather_data()