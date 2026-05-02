# Command-line arguments
args <- commandArgs(trailingOnly = TRUE)
target_lead_days <- as.integer(args[1])

if (is.na(target_lead_days) || target_lead_days < 1 || target_lead_days > 7) {
  cat("Error: lead_days must be between 1 and 7\n")
  quit(status = 1)
}

cat("Running prediction for lead_days:", target_lead_days, "\n")

# Load libraries
library(DBI)
library(odbc)
library(tidyverse)
library(lubridate)
library(lme4)
library(lmerTest)

# Database connection
con <- dbConnect(odbc::odbc(),
  Driver = "SQL Server",
  Server = "sluweather.database.windows.net",
  Database = "Weather",
  UID = "CloudSA651686c0",
  PWD = "Weather!",
  Port = 1433
)

# Load saved models
model_file <- "C:/Users/Panic/Capstone/Weather_Project/linearmodel/weather_linear_models.RData"
if (!file.exists(model_file)) {
  cat("Error: Model file not found:", model_file, "\n")
  quit(status = 1)
}
load(model_file)

# Get the latest available forecast date
query_latest <- "
  SELECT TOP 1 CAST(time AS DATE) as latest_date
  FROM forecasts
  ORDER BY time DESC
"
latest_date_df <- dbGetQuery(con, query_latest)
target_date_str <- as.character(latest_date_df$latest_date[1])

cat("Using latest forecast date:", target_date_str, "\n")

# Query forecast weather data for target date
query_forecast <- paste0("
  SELECT TOP 100
    f.city,
    f.time AS valid_time,
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
  WHERE CAST(f.time AS DATE) = '", target_date_str, "'
    AND f.retrieved_at >= DATEADD(day, -14, GETUTCDATE())
")

forecast_data <- dbGetQuery(con, query_forecast)

if (nrow(forecast_data) == 0) {
  cat("Error: No forecast data found for date:", target_date_str, "\n")
  quit(status = 1)
}

cat("Fetched", nrow(forecast_data), "rows from forecasts table\n")

# Prepare data
forecast_data <- forecast_data %>%
  mutate(
    city = tolower(city),
    distance_to_sea_km_sc = scale(distance_to_sea_km)
  )

# Make predictions
forecast_data$pred_temp_full <- tryCatch({
  predict(historical_weather_full_lm, newdata = forecast_data, allow.new.levels = TRUE)
}, error = function(e) {
  cat("Warning: Full model prediction failed:", e$message, "\n")
  NA
})

forecast_data$pred_temp_adj <- tryCatch({
  predict(historical_weather_adjusted_lm, newdata = forecast_data, allow.new.levels = TRUE)
}, error = function(e) {
  cat("Warning: Adjusted model prediction failed:", e$message, "\n")
  NA
})

forecast_data$predicted_temp <- forecast_data$pred_temp_adj

# Create table if not exists
table_check <- dbGetQuery(con, "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'linear_model_predictions'")
if (nrow(table_check) == 0) {
  dbExecute(con, "
    CREATE TABLE linear_model_predictions (
      city VARCHAR(100),
      lead_days INT,
      country VARCHAR(100),
      lat FLOAT,
      lng FLOAT,
      predicted_temp FLOAT,
      pred_temp_full FLOAT,
      pred_temp_adj FLOAT,
      valid_date VARCHAR(10),
      created_at DATETIME,
      PRIMARY KEY (city, lead_days)
    )
  ")
  cat("Created table linear_model_predictions\n")
} else {
  # Delete existing rows for this lead_days
  delete_query <- paste0("DELETE FROM dbo.linear_model_predictions WHERE lead_days = ", target_lead_days)
  dbExecute(con, delete_query)
  cat("Deleted existing rows for lead_days =", target_lead_days, "\n")
}

# Insert all data at once using sqlWrite
forecast_data_insert <- forecast_data %>%
  select(city, country, lat, lng, predicted_temp, pred_temp_full, pred_temp_adj) %>%
  mutate(
    lead_days = target_lead_days,
    valid_date = target_date_str,
    created_at = as.character(Sys.time()),
    predicted_temp = as.numeric(predicted_temp),
    pred_temp_full = as.numeric(pred_temp_full),
    pred_temp_adj = as.numeric(pred_temp_adj),
    lat = as.numeric(lat),
    lng = as.numeric(lng)
  )

# Use dbWriteTable for bulk insert
dbWriteTable(con, "dbo.linear_model_predictions", forecast_data_insert, append = TRUE, overwrite = FALSE)

cat("Successfully inserted", nrow(forecast_data_insert), "predictions into database\n")
cat("Prediction complete for lead_days:", target_lead_days, "\n")

dbDisconnect(con)