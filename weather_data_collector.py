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
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": city_info["lat"],
        "longitude": city_info["lon"],
        "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        "forecast_days": 1
    }
    
    response = requests.get(url, params=params)
    response.raise_for_status()  # ensure any failed request throws an error
    data = response.json()
    
    df = pd.DataFrame({
        "time": data["hourly"]["time"],
        "temperature_2m": data["hourly"]["temperature_2m"],
        "relative_humidity_2m": data["hourly"]["relative_humidity_2m"],
        "wind_speed_10m": data["hourly"]["wind_speed_10m"]
    })
    df["city"] = city_info["city"]
    
    return df

# Main function
def pull_weather_data():
    print(f"Pulling weather data at {datetime.now()}")
    
    cities = load_cities()
    all_data = pd.concat([get_forecast(c) for c in cities], ignore_index=True)
    all_data["retrieved_at"] = datetime.now().isoformat()
    
    # Save to CSV
    try:
        existing = pd.read_csv("us_city_forecasts.csv")
        all_data = pd.concat([existing, all_data], ignore_index=True)
    except FileNotFoundError:
        pass
    
    all_data.to_csv("us_city_forecasts.csv", index=False)
    print(f"✅ Done! Saved data for {len(cities)} cities at {datetime.now()}")

# Entry point
if __name__ == "__main__":
    pull_weather_data()
