import os
import requests
import pandas as pd
from datetime import datetime

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

# Get weather forecast for one city
def get_forecast(city_info):
    print(f"Fetching weather for {city_info['city']}...")
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": city_info["lat"],
        "longitude": city_info["lon"],
        "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        "forecast_days": 1
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()  # raise error if request fails
    except requests.RequestException as e:
        print(f"⚠️ Failed to get data for {city_info['city']}: {e}")
        return None
    
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

# Main function
def pull_weather_data():
    print(f"Pulling weather data at {datetime.now()}")
    
    cities = load_cities()
    all_data_list = []

    for city in cities:
        city_df = get_forecast(city)
        if city_df is not None:
            all_data_list.append(city_df)
    
    if not all_data_list:
        print("No data retrieved. Exiting.")
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
    
    print(f"✅ Done! Saved data for {len(cities)} cities at {datetime.now()}")

# Entry point
if __name__ == "__main__":
    pull_weather_data()
