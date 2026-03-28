# Weather Project

Global weather data collection and analysis system with Azure SQL database backend.

## 📁 Project Structure

```
Weather_Project/
├── scripts/               # Data collection & processing
│   ├── weather_data_collector.py          # Main weather data collector
│   ├── sequential_historical_collector.py # Batch historical data collection
│   └── import_email_forecasts.py          # Email forecast importer
├── database/              # Database setup & utilities
│   ├── db_setup.py        # Azure SQL schema & import scripts
│   └── sqldb.py           # Database connection utilities
├── visualization/         # Dashboards & visualizations
│   ├── weather_dashboard.py  # Plotly Dash dashboard
│   ├── weather_globe_viz.py  # 3D globe visualizations
│   └── outputs/           # Generated HTML visualizations
├── data/                  # Data files
│   ├── cities/            # City reference data
│   ├── historical_zips/   # Historical weather ZIP archives
│   ├── forecasts/         # Forecast CSV files
│   └── email_forecasts/   # Email-based forecast data
└── tools/                 # Utility scripts
```

## 🚀 Quick Start

### Collect Current Forecast Data
```bash
cd scripts
python weather_data_collector.py
```

### Collect Historical Data (2022-2025)
```bash
cd scripts
python sequential_historical_collector.py
```

### Run Interactive Dashboard
```bash
cd visualization
python weather_dashboard.py
```
Visit: http://localhost:8050

## 🗄️ Database

**Azure SQL Server**: sluweather.database.windows.net  
**Database**: Weather

### Tables
- cities - City reference data (45K+ cities)
- orecasts - Weather forecast data
- historical_weather - Historical weather records
- orecast_accuracy - Forecast accuracy metrics
- city_error_metrics - Error analysis by city

### Environment Variables
```bash
export AZURE_SQL_USER="your_username"
export AZURE_SQL_PASSWORD="your_password"
```

## 📊 Features

- **Real-time data collection** from Open-Meteo API
- **186 major cities** worldwide coverage
- **Historical data** back to 2022
- **Interactive 3D globe** visualizations
- **Forecast accuracy tracking**
- **Robust retry logic** for network resilience

## 🛠️ Requirements

```
pandas
sqlalchemy
pyodbc
plotly
dash
requests
```

Install: `pip install pandas sqlalchemy pyodbc plotly dash requests`

---

*Azure SQL database-backed weather data collection and analysis system*
