import pandas as pd
import os

def append_new_file_to_cleaned_data(new_file, output_folder):
    print(f"Processing: {new_file}")
    
    # Load new forecast file
    df = pd.read_csv(new_file)

    # Safely convert timestamps
    df['forecast_valid_time'] = pd.to_datetime(df['time'], errors='coerce')
    df['forecast_issue_time'] = pd.to_datetime(df['retrieved_at'], errors='coerce')
    df = df.dropna(subset=['forecast_valid_time', 'forecast_issue_time'])

    # Calculate lead time in hours
    df['lead_time_hours'] = (df['forecast_valid_time'] - df['forecast_issue_time']).dt.total_seconds() / 3600
    df['lead_time_hours'] = df['lead_time_hours'].round().astype(int)

    # Add grouping variables
    df['valid_date'] = df['forecast_valid_time'].dt.date
    df['valid_hour'] = df['forecast_valid_time'].dt.hour

    # Keep only 1–22h lead times
    df_clean = df[(df['lead_time_hours'] >= 1) & (df['lead_time_hours'] <= 22)].copy()

    # Remove duplicates within the new file
    df_clean = df_clean.drop_duplicates(
        subset=['city', 'forecast_valid_time', 'lead_time_hours'],
        keep='first'
    )

    # Columns to keep in output
    columns_to_keep = [
        'city', 'forecast_issue_time', 'forecast_valid_time',
        'valid_date', 'valid_hour', 'lead_time_hours',
        'temperature_2m', 'relative_humidity_2m', 'dew_point_2m',
        'apparent_temperature', 'precipitation_probability', 'precipitation',
        'rain', 'showers', 'snowfall', 'snow_depth', 'weather_code',
        'pressure_msl', 'surface_pressure', 'cloud_cover', 'visibility',
        'wind_speed_10m', 'wind_gusts_10m'
    ]

    df_clean = df_clean[columns_to_keep].copy()

    os.makedirs(output_folder, exist_ok=True)

    # Process only lead times with data
    unique_hours = sorted(df_clean['lead_time_hours'].unique())

    for hour in unique_hours:
        df_group = df_clean[df_clean['lead_time_hours'] == hour].copy()
        if df_group.empty:
            continue

        df_group['lead_time_group'] = f"{hour}h"
        df_group = df_group.sort_values(['city', 'forecast_valid_time', 'lead_time_hours'])

        output_file = os.path.join(output_folder, f"forecast_{hour}h.csv")
        merge_cols = ['city', 'forecast_valid_time', 'lead_time_hours']

        if os.path.exists(output_file):
            existing_df = pd.read_csv(output_file)

            # Ensure dtypes match for safe merging
            existing_df['forecast_valid_time'] = pd.to_datetime(existing_df['forecast_valid_time'], errors='coerce')
            existing_df['forecast_issue_time'] = pd.to_datetime(existing_df['forecast_issue_time'], errors='coerce')
            existing_df['lead_time_hours'] = existing_df['lead_time_hours'].astype(int)

            # Only add rows not already in existing CSV
            new_rows = df_group.merge(
                existing_df[merge_cols],
                on=merge_cols,
                how='left',
                indicator=True
            )
            new_rows = new_rows[new_rows['_merge'] == 'left_only'].drop(columns=['_merge'])

            if not new_rows.empty:
                combined = pd.concat([existing_df, new_rows], ignore_index=True)
                combined = combined.sort_values(['city', 'forecast_valid_time', 'lead_time_hours'])
                combined.to_csv(output_file, index=False)
                print(f"{hour}h: +{len(new_rows)} new rows")
            else:
                print(f"{hour}h: +0 new rows")
        else:
            # Create new file if rows exist
            df_group.to_csv(output_file, index=False)
            print(f"{hour}h: Created with {len(df_group)} rows")


if __name__ == "__main__":
    input_folder = os.path.join(os.getcwd(), "data/forecasts")
    output_folder = os.path.join(os.getcwd(), "data/forecast_by_lead_time")

    # Process all CSVs in input folder
    for file in os.listdir(input_folder):
        if file.endswith(".csv"):
            new_file = os.path.join(input_folder, file)
            append_new_file_to_cleaned_data(new_file, output_folder)

            