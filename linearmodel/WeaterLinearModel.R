library(DBI)
library(odbc)
library(tidyverse)
library(ggplot2)
library(lubridate)
library(glmnet)
library(dplyr)
library(lme4)
library(lmerTest)
library(performance)
con <- dbConnect(odbc::odbc(),
                 Driver = "SQL Server",
                 Server = "sluweather.database.windows.net", 
                 Database = "Weather",
                 UID = "CloudSA651686c0",
                 PWD = "Password!", #Change to the password!!
                 Port = 1433)  


query_hist_1 = "SELECT * FROM dbo.historical_weather"
query_hist_2 = "SELECT city, distance_to_sea_km FROM dbo.cities"
query_hist_3 = "SELECT city, koppen_class FROM dbo.weather_table"

data_hist_1 <- dbGetQuery(con, query_hist_1)
data_cities = dbGetQuery(con, query_hist_2)
data_weather_table <- dbGetQuery(con, query_hist_3)


data_weather_table <- data_weather_table %>%
  mutate(city = tolower(city))

data_cities <- data_cities %>%
  mutate(city = tolower(city))

data_city_info <- inner_join(
  data_weather_table,
  data_cities,
  by = "city",
  suffix = c("_weather", "_cities")
)
data_city_info <- data_city_info %>%
  transmute(
    city,
    distance_to_sea_km = distance_to_sea_km,
    koppen_class = koppen_class
  )

data_hist_1 <- data_hist_1 %>%
  mutate(city = tolower(city))

missing_cities <- setdiff(unique(data_hist_1$city), unique(data_city_info$city))


data_hist_1 <- data_hist_1 %>%
  left_join(data_city_info, by = "city")




data_hist_1 <- data_hist_1 %>%
  filter(!is.na(distance_to_sea_km), !is.na(koppen_class))
head(data_hist_1)
data_hist_1_time <- data_hist_1 %>%
  mutate(time = ymd_hms(time)) 
data_hist_1_train <- data_hist_1_time %>%
  filter(year(time) %in% 2022:2023,
         hour(time) == 0,
         minute(time) == 0,
         second(time) == 0)
data_hist_1_test <- data_hist_1_time %>%
  filter(year(time) %in% 2025,
         hour(time) == 0,
         minute(time) == 0,
         second(time) == 0)

rm(data_hist_1_time)
head(data_hist_1_train)
head(data_hist_1_test)

data_hist_1_train$distance_to_sea_km_sc <- scale(data_hist_1_train$distance_to_sea_km)
data_hist_1_test$distance_to_sea_km_sc <- scale(data_hist_1_test$distance_to_sea_km)



#---------------------------------------------------------
#models




historical_weather_full_lm <- lmer(
  temperature_2m ~ relative_humidity_2m * distance_to_sea_km_sc + 
    dew_point_2m * distance_to_sea_km_sc +
    precipitation + rain + showers + snowfall + snow_depth +
    weather_code + pressure_msl + surface_pressure + cloud_cover +
    wind_speed_10m + wind_gusts_10m + koppen_class +
    (1 | city),
  data = data_hist_1_train,
  control = lmerControl(optimizer = "bobyqa")
)
summary(historical_weather_full_lm)


historical_weather_adjusted_lm <- lmer(
  temperature_2m ~ dew_point_2m * distance_to_sea_km_sc +
    precipitation + weather_code + surface_pressure +
    cloud_cover + wind_gusts_10m + distance_to_sea_km + koppen_class +
    (1 | city),
  data = data_hist_1_train,
  control = lmerControl(optimizer = "bobyqa")
)

summary(historical_weather_adjusted_lm)


#---------------------------------------------------------
#graphs


cities_available <- unique(data_hist_1_test$city)
random_city <- sample(cities_available, 1)

random_city

data_city <- data_hist_1_test %>%
  filter(city == random_city) %>%
  arrange(time)
koppen_class = unique(data_city$koppen_class)[[1]]
distance = unique(data_city$distance_to_sea_km)[[1]]

data_city$pred_temp_all <- predict(historical_weather_full_lm, newdata = data_city, allow.new.levels = FALSE)
data_city$pred_temp_adj <- predict(historical_weather_adjusted_lm, newdata = data_city, allow.new.levels = FALSE)

ggplot(data_city, aes(x = time)) +
  geom_line(aes(y = temperature_2m, colour = "Observed"), linewidth = 0.7) +
  geom_line(aes(y = pred_temp_all, colour = "Predicted_all"), linewidth = 0.7, linetype = "dashed") +
  geom_line(aes(y = pred_temp_adj, colour = "Predicted_adj"), linewidth = 0.7, linetype = "dashed") +
  scale_colour_manual(values = c("Observed" = "black", "Predicted_all" = "red", "Predicted_adj" = "blue")) +
  
  labs(
    title = paste("Observed vs Predicted Temperature at Midnight (", random_city, ", ", koppen_class, ", ", distance  ,"km, 2023–2024)", sep = ""),
    x = "Date",
    y = "Temperature 2m",
    colour = ""
  ) +
  theme_minimal()


save(historical_weather_full_lm, historical_weather_adjusted_lm, file = "/to/directory/") #change the directory

