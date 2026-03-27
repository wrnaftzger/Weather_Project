"""
weather_dashboard.py — Interactive Plotly Dash Weather Dashboard
=================================================================
Multi-page interactive dashboard with forecast analysis.

Features:
- Page 1: Global map with city selector
- Page 2: Forecast vs Actual comparison
- Page 3: Forecast accuracy analysis by lead time
- Page 4: City ranking and error metrics

Requirements:
    pip install dash plotly pandas sqlalchemy pyodbc
    
Run:
    python weather_dashboard.py
    Then open: http://localhost:8050
"""

import os
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from dash import Dash, dcc, html, Input, Output, callback
from sqlalchemy import create_engine, text
import urllib.parse

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

# Data fetching functions
def fetch_city_list():
    """Get list of all cities with coordinates."""
    engine = get_engine()
    query = """
    SELECT DISTINCT 
        city_ascii as city,
        lat,
        lng,
        country,
        population
    FROM cities
    WHERE lat IS NOT NULL AND lng IS NOT NULL
    ORDER BY city_ascii
    """
    return pd.read_sql(query, engine)

def fetch_city_weather_history(city_name, days=30):
    """Fetch weather history for a specific city."""
    engine = get_engine()
    query = f"""
    SELECT TOP 1000
        time,
        temperature_2m,
        precipitation,
        wind_speed_10m,
        relative_humidity_2m,
        weather_code
    FROM historical_weather
    WHERE city = '{city_name}'
        AND time >= DATEADD(day, -{days}, GETDATE())
    ORDER BY time DESC
    """
    return pd.read_sql(query, engine)

def fetch_forecast_vs_actual(city_name, limit=500):
    """Fetch forecast vs actual comparison."""
    engine = get_engine()
    query = f"""
    SELECT TOP {limit}
        forecast_valid_time as time,
        lead_time_hours,
        temperature_2m as forecast_temp
    FROM forecast_accuracy
    WHERE city = '{city_name}'
    ORDER BY forecast_valid_time DESC
    """
    df_forecast = pd.read_sql(query, engine)
    
    query_actual = f"""
    SELECT TOP {limit}
        time,
        temperature_2m as actual_temp
    FROM historical_weather
    WHERE city = '{city_name}'
    ORDER BY time DESC
    """
    df_actual = pd.read_sql(query_actual, engine)
    
    return df_forecast, df_actual

def fetch_accuracy_by_lead_time():
    """Fetch accuracy metrics grouped by lead time."""
    engine = get_engine()
    query = """
    SELECT 
        lead_time_group,
        COUNT(*) as forecast_count,
        AVG(ABS(temperature_2m - (
            SELECT TOP 1 h.temperature_2m 
            FROM historical_weather h 
            WHERE h.city = fa.city AND h.time = fa.forecast_valid_time
        ))) as avg_error
    FROM forecast_accuracy fa
    WHERE lead_time_group IS NOT NULL
    GROUP BY lead_time_group
    ORDER BY 
        CASE lead_time_group
            WHEN '0h' THEN 1
            WHEN '1-6h' THEN 2
            WHEN '7-12h' THEN 3
            WHEN '13-18h' THEN 4
            WHEN '19-24h' THEN 5
        END
    """
    return pd.read_sql(query, engine)

def fetch_city_error_metrics():
    """Fetch error metrics for all cities."""
    engine = get_engine()
    query = """
    SELECT 
        city,
        average_error,
        valid_percentage,
        status
    FROM city_error_metrics
    WHERE average_error IS NOT NULL
    ORDER BY average_error ASC
    """
    return pd.read_sql(query, engine)

# Initialize Dash app
app = Dash(__name__)
app.title = "Weather Forecast Dashboard"

# Load initial data
print("📡 Loading city data...")
cities_df = fetch_city_list()
print(f"✅ Loaded {len(cities_df)} cities")

# App layout
app.layout = html.Div([
    html.H1("🌍 Weather Forecast Analysis Dashboard", 
            style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': 30}),
    
    dcc.Tabs([
        # Tab 1: Global Overview
        dcc.Tab(label='🗺️ Global Map', children=[
            html.Div([
                html.H3("Select a City:"),
                dcc.Dropdown(
                    id='city-dropdown',
                    options=[{'label': city, 'value': city} for city in cities_df['city']],
                    value=cities_df['city'].iloc[0],
                    style={'width': '400px', 'marginBottom': '20px'}
                ),
                dcc.Graph(id='global-map', style={'height': '600px'}),
                html.Div(id='city-stats', style={'marginTop': '20px', 'fontSize': '18px'})
            ], style={'padding': '20px'})
        ]),
        
        # Tab 2: Forecast vs Actual
        dcc.Tab(label='📊 Forecast vs Actual', children=[
            html.Div([
                html.H3("Temperature Forecast Comparison", style={'marginBottom': '20px'}),
                dcc.Graph(id='forecast-comparison', style={'height': '500px'}),
            ], style={'padding': '20px'})
        ]),
        
        # Tab 3: Accuracy by Lead Time
        dcc.Tab(label='🎯 Accuracy Analysis', children=[
            html.Div([
                html.H3("Forecast Accuracy by Lead Time", style={'marginBottom': '20px'}),
                dcc.Graph(id='accuracy-leadtime', style={'height': '400px'}),
                html.H3("City Error Distribution", style={'marginTop': '40px', 'marginBottom': '20px'}),
                dcc.Graph(id='city-error-histogram', style={'height': '400px'}),
            ], style={'padding': '20px'})
        ]),
        
        # Tab 4: City Rankings
        dcc.Tab(label='🏆 City Rankings', children=[
            html.Div([
                html.H3("Top 20 Cities by Forecast Accuracy", style={'marginBottom': '20px'}),
                dcc.Graph(id='city-ranking', style={'height': '600px'}),
            ], style={'padding': '20px'})
        ]),
    ]),
], style={'fontFamily': 'Arial, sans-serif', 'padding': '20px'})

# Callbacks
@callback(
    [Output('global-map', 'figure'),
     Output('city-stats', 'children')],
    Input('city-dropdown', 'value')
)
def update_global_map(selected_city):
    # Highlight selected city on map
    cities_df['selected'] = cities_df['city'] == selected_city
    
    fig = px.scatter_geo(
        cities_df,
        lon='lng',
        lat='lat',
        hover_name='city',
        hover_data={'country': True, 'population': ':,', 'lng': False, 'lat': False},
        color='selected',
        color_discrete_map={True: 'red', False: 'blue'},
        size='population',
        size_max=15,
        projection='natural earth',
        title='Global City Coverage'
    )
    
    fig.update_layout(
        showlegend=False,
        geo=dict(
            showcountries=True,
            countrycolor='lightgray',
            showcoastlines=True,
            coastlinecolor='gray',
        ),
        height=600
    )
    
    city_info = cities_df[cities_df['city'] == selected_city].iloc[0]
    stats_text = html.Div([
        html.H4(f"📍 {city_info['city']}, {city_info['country']}", style={'color': '#e74c3c'}),
        html.P(f"📊 Population: {city_info['population']:,.0f}" if pd.notna(city_info['population']) else "Population: N/A"),
        html.P(f"🌐 Coordinates: {city_info['lat']:.2f}°, {city_info['lng']:.2f}°"),
    ])
    
    return fig, stats_text

@callback(
    Output('forecast-comparison', 'figure'),
    Input('city-dropdown', 'value')
)
def update_forecast_comparison(selected_city):
    df_forecast, df_actual = fetch_forecast_vs_actual(selected_city, limit=200)
    
    fig = go.Figure()
    
    # Actual temperature
    fig.add_trace(go.Scatter(
        x=df_actual['time'],
        y=df_actual['actual_temp'],
        mode='lines',
        name='Actual Temperature',
        line=dict(color='blue', width=2),
    ))
    
    # Forecast temperature (sample - you can filter by lead_time)
    if not df_forecast.empty:
        df_forecast_recent = df_forecast[df_forecast['lead_time_hours'] <= 24]
        fig.add_trace(go.Scatter(
            x=df_forecast_recent['time'],
            y=df_forecast_recent['forecast_temp'],
            mode='markers',
            name='24h Forecast',
            marker=dict(color='red', size=4, opacity=0.6),
        ))
    
    fig.update_layout(
        title=f"Temperature: Forecast vs Actual - {selected_city}",
        xaxis_title="Date",
        yaxis_title="Temperature (°C)",
        hovermode='x unified',
        height=500
    )
    
    return fig

@callback(
    Output('accuracy-leadtime', 'figure'),
    Input('city-dropdown', 'value')  # Dummy input to trigger load
)
def update_accuracy_leadtime(_):
    df = fetch_accuracy_by_lead_time()
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=df['lead_time_group'],
        y=df['avg_error'],
        marker_color='indianred',
        text=df['avg_error'].round(2),
        textposition='outside',
    ))
    
    fig.update_layout(
        title="Average Temperature Error by Forecast Lead Time",
        xaxis_title="Lead Time",
        yaxis_title="Average Error (°C)",
        height=400
    )
    
    return fig

@callback(
    Output('city-error-histogram', 'figure'),
    Input('city-dropdown', 'value')
)
def update_city_error_histogram(_):
    df = fetch_city_error_metrics()
    
    fig = px.histogram(
        df,
        x='average_error',
        nbins=30,
        title="Distribution of City Forecast Errors",
        labels={'average_error': 'Average Error (°C)'},
        color_discrete_sequence=['steelblue']
    )
    
    fig.update_layout(height=400)
    
    return fig

@callback(
    Output('city-ranking', 'figure'),
    Input('city-dropdown', 'value')
)
def update_city_ranking(_):
    df = fetch_city_error_metrics().head(20)
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        y=df['city'],
        x=df['average_error'],
        orientation='h',
        marker=dict(
            color=df['average_error'],
            colorscale='RdYlGn_r',
            colorbar=dict(title="Error (°C)")
        ),
        text=df['average_error'].round(2),
        textposition='outside',
    ))
    
    fig.update_layout(
        title="Top 20 Cities with Best Forecast Accuracy (Lowest Error)",
        xaxis_title="Average Error (°C)",
        yaxis_title="City",
        height=600,
        yaxis={'categoryorder': 'total ascending'}
    )
    
    return fig

# Run the app
if __name__ == '__main__':
    print("\n🚀 Starting Weather Dashboard...")
    print("📱 Open your browser to: http://localhost:8050")
    print("🛑 Press Ctrl+C to stop\n")
    app.run(debug=True, port=8050)
