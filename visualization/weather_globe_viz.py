"""
weather_globe_viz.py — 3D Interactive Globe Weather Visualization
==================================================================
Creates an interactive 3D globe showing weather data using Plotly.

Features:
- 3D globe with city markers
- Color-coded by temperature
- Size-coded by precipitation
- Animated timeline (play through historical data)
- Click cities for detailed info

Requirements:
    pip install plotly pandas sqlalchemy pyodbc
"""

import os
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from sqlalchemy import create_engine, text
import urllib.parse
from datetime import datetime

# Database connection
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
        pool_pre_ping=True,
    )

def fetch_latest_weather():
    """Fetch the most recent weather data for all cities."""
    engine = get_engine()
    
    query = """
    WITH LatestWeather AS (
        SELECT 
            h.city,
            h.time,
            h.temperature_2m,
            h.precipitation,
            h.wind_speed_10m,
            h.weather_code,
            c.lat,
            c.lng,
            c.country,
            c.population,
            ROW_NUMBER() OVER (PARTITION BY h.city ORDER BY h.time DESC) as rn
        FROM historical_weather h
        JOIN cities c ON h.city = c.city_ascii
    )
    SELECT 
        city, time, temperature_2m, precipitation, wind_speed_10m,
        weather_code, lat, lng, country, population
    FROM LatestWeather
    WHERE rn = 1
    """
    
    print("📡 Fetching latest weather data from database...")
    df = pd.read_sql(query, engine)
    print(f"✅ Loaded {len(df)} cities")
    return df

def fetch_weather_timeline(limit_days=30):
    """Fetch weather data over time for animation."""
    engine = get_engine()
    
    # Get recent data - use TOP with ORDER BY for better performance
    query = """
    SELECT TOP 5000
        h.city,
        h.time,
        h.temperature_2m,
        h.precipitation,
        h.wind_speed_10m,
        c.lat,
        c.lng
    FROM historical_weather h
    JOIN cities c ON h.city = c.city_ascii
    ORDER BY h.time DESC
    """
    
    print(f"📡 Fetching weather timeline...")
    df = pd.read_sql(query, engine)
    print(f"✅ Loaded {len(df)} records")
    return df

def create_3d_globe(df):
    """Create an interactive 3D globe with weather data."""
    
    # Add hover text
    df['hover_text'] = (
        '<b>' + df['city'] + '</b><br>' +
        'Temp: ' + df['temperature_2m'].round(1).astype(str) + '°C<br>' +
        'Precip: ' + df['precipitation'].round(1).astype(str) + ' mm<br>' +
        'Wind: ' + df['wind_speed_10m'].round(1).astype(str) + ' km/h<br>' +
        'Pop: ' + df['population'].fillna(0).astype(int).apply(lambda x: f'{x:,}')
    )
    
    # Create 3D scatter plot on globe
    fig = go.Figure()
    
    fig.add_trace(go.Scattergeo(
        lon=df['lng'],
        lat=df['lat'],
        text=df['hover_text'],
        mode='markers',
        marker=dict(
            size=df['precipitation'] * 3 + 5,  # Size by precipitation
            color=df['temperature_2m'],  # Color by temperature
            colorscale='RdYlBu_r',  # Red (hot) to Blue (cold)
            cmin=-20,
            cmax=40,
            colorbar=dict(
                title="Temperature (°C)",
                thickness=15,
                len=0.5,
            ),
            line=dict(width=0.5, color='white'),
            opacity=0.8,
        ),
        hovertemplate='%{text}<extra></extra>',
        name='Cities'
    ))
    
    fig.update_geos(
        projection_type="orthographic",  # 3D globe projection
        showcountries=True,
        countrycolor="lightgray",
        showcoastlines=True,
        coastlinecolor="gray",
        showland=True,
        landcolor="rgb(243, 243, 243)",
        showocean=True,
        oceancolor="rgb(204, 229, 255)",
        bgcolor="rgb(10, 10, 30)",
    )
    
    fig.update_layout(
        title={
            'text': f'🌍 Global Weather Conditions - {df["time"].max().strftime("%Y-%m-%d %H:%M")}',
            'x': 0.5,
            'xanchor': 'center',
            'font': {'size': 24, 'color': 'white'}
        },
        paper_bgcolor="rgb(10, 10, 30)",
        font=dict(color='white'),
        height=800,
        margin=dict(l=0, r=0, t=80, b=0),
    )
    
    return fig

def create_animated_globe(df):
    """Create an animated globe showing weather changes over time."""
    
    # Convert time to datetime and create date column
    df['time'] = pd.to_datetime(df['time'])
    df['date_hour'] = df['time'].dt.strftime('%Y-%m-%d %H:00')
    
    # Sort by time
    df = df.sort_values('time')
    
    # Create hover text
    df['hover_text'] = (
        '<b>' + df['city'] + '</b><br>' +
        'Temp: ' + df['temperature_2m'].round(1).astype(str) + '°C<br>' +
        'Precip: ' + df['precipitation'].round(1).astype(str) + ' mm'
    )
    
    # Create animated figure
    fig = px.scatter_geo(
        df,
        lon='lng',
        lat='lat',
        color='temperature_2m',
        size='precipitation',
        hover_name='city',
        hover_data={
            'temperature_2m': ':.1f',
            'precipitation': ':.1f',
            'wind_speed_10m': ':.1f',
            'lng': False,
            'lat': False,
        },
        animation_frame='date_hour',
        color_continuous_scale='RdYlBu_r',
        range_color=[-20, 40],
        projection='orthographic',
        title='🌍 Animated Weather Changes Over Time',
    )
    
    fig.update_geos(
        showcountries=True,
        countrycolor="lightgray",
        showcoastlines=True,
        coastlinecolor="gray",
        showland=True,
        landcolor="rgb(243, 243, 243)",
        showocean=True,
        oceancolor="rgb(204, 229, 255)",
        bgcolor="rgb(10, 10, 30)",
    )
    
    fig.update_layout(
        paper_bgcolor="rgb(10, 10, 30)",
        font=dict(color='white'),
        height=800,
        coloraxis_colorbar=dict(title="Temp (°C)"),
    )
    
    return fig

def main():
    print("🌍 Weather Globe Visualization Generator\n")
    
    # Fetch data
    df_latest = fetch_latest_weather()
    
    # Create static globe with latest data
    print("\n📊 Creating 3D globe...")
    fig1 = create_3d_globe(df_latest)
    fig1.write_html("weather_globe_static.html")
    print("✅ Saved: weather_globe_static.html")
    
    # Create animated globe
    print("\n🎬 Creating animated globe...")
    df_timeline = fetch_weather_timeline(limit_days=7)  # Last 7 days
    fig2 = create_animated_globe(df_timeline)
    fig2.write_html("weather_globe_animated.html")
    print("✅ Saved: weather_globe_animated.html")
    
    print("\n🎉 Done! Open the HTML files in your browser:")
    print("   - weather_globe_static.html (interactive 3D globe)")
    print("   - weather_globe_animated.html (animated timeline)")
    print("\n💡 Tip: You can rotate the globe by clicking and dragging!")

if __name__ == "__main__":
    main()
