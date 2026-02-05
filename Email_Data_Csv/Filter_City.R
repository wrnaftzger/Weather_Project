library(tidyverse)

setwd("Weather_Project/Email_Data_Csv/data/output_csv/")

filter_threshold <- 15  # Threshold for temperature difference
keep_city_threshold <- 0.75  # Threshold for valid data percentage

file_list <- list.files(pattern = "*.csv")

data_list <- lapply(file_list, function(file) {
  read_csv(file)
})

data <- bind_rows(data_list)

data <- data %>%
  mutate(date_and_time = as.POSIXct(date_and_time, format="%m/%d/%Y %H:%M"))

data <- data %>%
  mutate(date_only = as.Date(date_and_time))

process_city_data <- function(city_data, filter_threshold) {
  
  city_data <- city_data %>%
    group_by(date_only) %>%
    filter(date_and_time == min(date_and_time)) %>%
    ungroup()
  
  city_data <- city_data %>%
    arrange(date_and_time)
  
  city_data <- city_data %>%
    mutate(
      next_day_date = date_and_time + days(1),
      day_after_next_date = date_and_time + days(2)
    ) %>%
    left_join(select(city_data, date_and_time, actual_lo = today_lo, actual_hi = today_hi), 
              by = c("next_day_date" = "date_and_time"), relationship = "many-to-many") %>%
    left_join(select(city_data, date_and_time, actual_tomorrow_lo = today_lo, actual_tomorrow_hi = today_hi), 
              by = c("day_after_next_date" = "date_and_time"), relationship = "many-to-many") 
  
  valid_data <- city_data %>%
    filter(
      (abs(today_lo - actual_lo) < filter_threshold) &
        (abs(today_hi - actual_hi) < filter_threshold) &
        (abs(tomorrow_lo - actual_tomorrow_lo) < filter_threshold) &
        (abs(tomorrow_hi - actual_tomorrow_hi) < filter_threshold)
    )
  
  total_rows <- nrow(city_data)
  removed_rows <- total_rows - nrow(valid_data)
  valid_percentage <- nrow(valid_data) / total_rows
  average_error <- mean(c(
    abs(valid_data$today_lo - valid_data$actual_lo),
    abs(valid_data$today_hi - valid_data$actual_hi),
    abs(valid_data$tomorrow_lo - valid_data$actual_tomorrow_lo),
    abs(valid_data$tomorrow_hi - valid_data$actual_tomorrow_hi)
  ), na.rm = TRUE)
  
  list(
    valid_data = valid_data,
    average_error = average_error,
    removed_percentage = removed_rows / total_rows,
    valid_percentage = valid_percentage
  )
}

kept_cities <- list()
removed_cities <- list()
city_results <- list()

for (city_name in unique(data$city)) {
  city_data <- data %>% filter(city == city_name)
  
  result <- process_city_data(city_data, filter_threshold)
  
  city_results[[city_name]] <- list(
    city = city_name,
    average_error = result$average_error,
    valid_percentage = result$valid_percentage,
    removed_percentage = result$removed_percentage,
    status = ifelse(result$valid_percentage >= keep_city_threshold, "Kept", "Removed")
  )
  
  if (result$valid_percentage >= keep_city_threshold) {
    kept_cities <- append(kept_cities, city_name)
  } else {
    removed_cities <- append(removed_cities, city_name)
  }
}

city_summary <- do.call(rbind, lapply(city_results, function(x) as.data.frame(t(unlist(x)))))

city_summary <- city_summary %>%
  mutate(filter_threshold = filter_threshold, keep_city_threshold = keep_city_threshold)

setwd("Weather_Project/Email_Data_Csv/data/")

write_csv(city_summary, "city_weather_error.csv")
