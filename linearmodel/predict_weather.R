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
  PWD = "Password!",
  Port = 1433
)

# Load saved models
model_file <- file.path(dirname(parent.frame(2)$ofile), "weather_linear_models.RData")
if (!file.exists(model_file)) {
  cat("Error: Model file not found:", model_file, "\n")
  quit(status = 1)
}
load(model_file)

# Get target date
target_date <- Sys.Date() + target_lead_days
target_date_str <- as.character(target_date)

cat("Target date:", target_date_str, "\n")

# Query forecast weather data for target date
query_forecast <- paste0("
  SELECT
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

# Scale distance_to_sea_km using the same scale as training
scale_factor <- attr(scale(forecast_data$distance_to_sea_km), "scaled:center")
scale_sd <- attr(scale(forecast_data$distance_to_sea_km), "scaled:scale")

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

# Use adjusted model as primary prediction
forecast_data$predicted_temp <- forecast_data$pred_temp_adj
forecast_data$valid_date <- target_date_str
forecast_data$lead_days <- target_lead_days
forecast_data$created_at <- as.character(Sys.time())

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
      valid_date DATE,
      created_at DATETIME,
      PRIMARY KEY (city, lead_days)
    )
  ")
  cat("Created table linear_model_predictions\n")
}

# MERGE (upsert) into database
for (i in 1:nrow(forecast_data)) {
  row <- forecast_data[i, ]
  merge_query <- paste0("
    MERGE INTO dbo.linear_model_predictions AS target
    USING (SELECT 
      '", tolower(row$city), "' AS city,
      ", row$lead_days, " AS lead_days,
      '", row$country, "' AS country,
      ", row$lat, " AS lat,
      ", row$lng, " AS lng,
      ", row$predicted_temp, " AS predicted_temp,
      ", ifelse(is.na(row$pred_temp_full), "NULL", row$pred_temp_full), " AS pred_temp_full,
      ", ifelse(is.na(row$pred_temp_adj), "NULL", row$pred_temp_adj), " AS pred_temp_adj,
      '", row$valid_date, "' AS valid_date,
      '", row$created_at, "' AS created_at
    ) AS source
    ON target.city = source.city AND target.lead_days = source.lead_days
    WHEN MATCHED THEN
      UPDATE SET
        predicted_temp = source.predicted_temp,
        pred_temp_full = source.pred_temp_full,
        pred_temp_adj = source.pred_temp_adj,
        valid_date = source.valid_date,
        created_at = source.created_at
    WHEN NOT MATCHED THEN
      INSERT (city, lead_days, country, lat, lng, predicted_temp, pred_temp_full, pred_temp_adj, valid_date, created_at)
      VALUES (source.city, source.lead_days, source.country, source.lat, source.lng, source.predicted_temp, source.pred_temp_full, source.pred_temp_adj, source.valid_date, source.created_at);
  ")
  tryCatch({
    dbExecute(con, merge_query)
  }, error = function(e) {
    cat("Warning: MERGE failed for", row$city, ":", e$message, "\n")
  })
}

cat("Successfully inserted/updated", nrow(forecast_data), "predictions in database\n")
cat("Prediction complete for lead_days:", target_lead_days, "\n")

dbDisconnect(con)