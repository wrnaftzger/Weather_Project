#!/usr/bin/env python3
"""
Forecast Accuracy Analysis

Compares forecast temperatures against actual (previous) temperatures:
- Today forecasts (today_hi, today_lo) vs truth (previous_hi, previous_lo)
- Tomorrow forecasts (tomorrow_hi, tomorrow_lo) vs truth (previous_hi, previous_lo)

Generates:
- Accuracy metrics (MAE, RMSE, Bias) by city and overall
- Scatter plots showing predicted vs actual
- Error distribution histograms
- Summary statistics

Output: Email_Data_Csv/data/output_csv/forecast_accuracy/
"""
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# Set plotting style
sns.set_theme(style="whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)


def calculate_metrics(actual, predicted):
    """Calculate MAE, RMSE, and Bias for predictions vs actuals."""
    # Remove NaN values
    mask = ~(pd.isna(actual) | pd.isna(predicted))
    actual_clean = actual[mask]
    predicted_clean = predicted[mask]
    
    if len(actual_clean) == 0:
        return {'mae': np.nan, 'rmse': np.nan, 'bias': np.nan, 'n': 0}
    
    errors = predicted_clean - actual_clean
    mae = np.abs(errors).mean()
    rmse = np.sqrt((errors ** 2).mean())
    bias = errors.mean()
    
    return {
        'mae': mae,
        'rmse': rmse,
        'bias': bias,
        'n': len(actual_clean)
    }


def create_timeseries_plot(df_city, temp_type, city_name, filename, output_dir):
    """
    Create time-series line plot showing actual temps, today forecast, and tomorrow forecast.
    
    Args:
        df_city: DataFrame for a specific city with columns: date_and_time, actual_today_*, 
                 actual_tomorrow_*, today_*, tomorrow_*
        temp_type: 'hi' or 'lo'
        city_name: Name of city for title
        filename: Output filename
        output_dir: Output directory
    """
    # Prepare data
    actual_col = f'actual_today_{temp_type}'
    today_col = f'today_{temp_type}'
    tomorrow_col = f'tomorrow_{temp_type}'
    
    # Create a clean dataset
    plot_df = df_city[['date_and_time', actual_col, today_col, tomorrow_col]].copy()
    plot_df = plot_df.sort_values('date_and_time')
    
    if len(plot_df) == 0:
        print(f"  Skipping {filename} (no valid data)")
        return
    
    fig, ax = plt.subplots(figsize=(16, 6))
    
    temp_label = 'High' if temp_type == 'hi' else 'Low'
    
    # Plot actual temperatures (solid line)
    ax.plot(plot_df['date_and_time'], plot_df[actual_col], 
            label=f'Actual {temp_label}', linewidth=2, color="#00B7FF", alpha=0.9)
    
    # Plot today forecast (dashed line)
    ax.plot(plot_df['date_and_time'], plot_df[today_col], 
            label=f'Today Forecast', linewidth=1.5, linestyle='--', color="#FF0000", alpha=0.8)
    
    # Plot tomorrow forecast (dotted line)
    ax.plot(plot_df['date_and_time'], plot_df[tomorrow_col], 
            label=f'Tomorrow Forecast', linewidth=1.5, linestyle=':', color="#6F00FF", alpha=0.8)
    
    # Calculate metrics for display
    metrics_today = calculate_metrics(plot_df[actual_col], plot_df[today_col])
    metrics_tomorrow = calculate_metrics(
        plot_df[actual_col].shift(-1),  # Shift for proper tomorrow comparison
        plot_df[tomorrow_col]
    )
    
    textstr = (f"Today Forecast: MAE={metrics_today['mae']:.2f}°F, RMSE={metrics_today['rmse']:.2f}°F\n"
               f"Tomorrow Forecast: MAE={metrics_tomorrow['mae']:.2f}°F, RMSE={metrics_tomorrow['rmse']:.2f}°F")
    ax.text(0.02, 0.98, textstr, transform=ax.transAxes, fontsize=10,
            verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.7))
    
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel(f'Temperature (°F)', fontsize=12)
    ax.set_title(f'{city_name} - {temp_label} Temperature Forecast vs Actual (AM)', 
                 fontsize=14, fontweight='bold')
    ax.legend(loc='upper right', fontsize=11)
    ax.grid(True, alpha=0.3)
    
    # Rotate x-axis labels for better readability
    plt.xticks(rotation=45, ha='right')
    
    plt.tight_layout()
    plt.savefig(output_dir / filename, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ Saved {filename}")


def create_error_histogram(actual, predicted, title, filename, output_dir):
    """Create histogram of prediction errors."""
    # Remove NaN values
    mask = ~(pd.isna(actual) | pd.isna(predicted))
    actual_clean = actual[mask]
    predicted_clean = predicted[mask]
    
    if len(actual_clean) == 0:
        print(f"  Skipping {filename} (no valid data)")
        return
    
    errors = predicted_clean - actual_clean
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    ax.hist(errors, bins=50, edgecolor='black', alpha=0.7, color='steelblue')
    ax.axvline(0, color='red', linestyle='--', linewidth=2, label='Zero Error')
    ax.axvline(errors.mean(), color='orange', linestyle='--', linewidth=2, label=f'Mean Error: {errors.mean():.2f}°F')
    
    ax.set_xlabel('Prediction Error (°F)', fontsize=12)
    ax.set_ylabel('Frequency', fontsize=12)
    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig(output_dir / filename, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ Saved {filename}")


def create_metrics_comparison(metrics_df, output_dir):
    """Create bar charts comparing metrics across cities and forecast types."""
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    # MAE comparison
    metrics_pivot = metrics_df.pivot(index='city', columns='forecast_type', values='mae')
    metrics_pivot.plot(kind='bar', ax=axes[0, 0], color=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728'])
    axes[0, 0].set_title('Mean Absolute Error (MAE) by City', fontsize=12, fontweight='bold')
    axes[0, 0].set_ylabel('MAE (°F)', fontsize=11)
    axes[0, 0].set_xlabel('')
    axes[0, 0].legend(title='Forecast Type', fontsize=9)
    axes[0, 0].grid(True, alpha=0.3, axis='y')
    
    # RMSE comparison
    metrics_pivot = metrics_df.pivot(index='city', columns='forecast_type', values='rmse')
    metrics_pivot.plot(kind='bar', ax=axes[0, 1], color=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728'])
    axes[0, 1].set_title('Root Mean Square Error (RMSE) by City', fontsize=12, fontweight='bold')
    axes[0, 1].set_ylabel('RMSE (°F)', fontsize=11)
    axes[0, 1].set_xlabel('')
    axes[0, 1].legend(title='Forecast Type', fontsize=9)
    axes[0, 1].grid(True, alpha=0.3, axis='y')
    
    # Bias comparison
    metrics_pivot = metrics_df.pivot(index='city', columns='forecast_type', values='bias')
    metrics_pivot.plot(kind='bar', ax=axes[1, 0], color=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728'])
    axes[1, 0].set_title('Forecast Bias by City', fontsize=12, fontweight='bold')
    axes[1, 0].set_ylabel('Bias (°F)', fontsize=11)
    axes[1, 0].set_xlabel('City', fontsize=11)
    axes[1, 0].legend(title='Forecast Type', fontsize=9)
    axes[1, 0].axhline(0, color='red', linestyle='--', linewidth=1, alpha=0.5)
    axes[1, 0].grid(True, alpha=0.3, axis='y')
    
    # Sample counts
    metrics_pivot = metrics_df.pivot(index='city', columns='forecast_type', values='n')
    metrics_pivot.plot(kind='bar', ax=axes[1, 1], color=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728'])
    axes[1, 1].set_title('Sample Counts by City', fontsize=12, fontweight='bold')
    axes[1, 1].set_ylabel('Number of Samples', fontsize=11)
    axes[1, 1].set_xlabel('City', fontsize=11)
    axes[1, 1].legend(title='Forecast Type', fontsize=9)
    axes[1, 1].grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig(output_dir / 'metrics_comparison.png', dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  ✓ Saved metrics_comparison.png")


def main():
    repo_root = Path(__file__).resolve().parent.parent
    input_csv = repo_root / 'Email_Data_Csv' / 'data' / 'output_csv' / 'selected_cities.csv'
    output_dir = repo_root / 'Email_Data_Csv' / 'data' / 'output_csv' / 'forecast_accuracy'
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Loading data from {input_csv.name}...")
    df = pd.read_csv(input_csv)
    
    print(f"Loaded {len(df):,} rows")
    
    # Filter to AM rows only (morning forecasts)
    df['date_and_time'] = pd.to_datetime(df['date_and_time'], errors='coerce')
    df['hour'] = df['date_and_time'].dt.hour
    df_am = df[df['hour'] < 12].copy()
    print(f"Filtered to {len(df_am):,} AM rows (morning forecasts)")
    
    # Convert temperature columns to numeric
    temp_cols = ['previous_lo', 'previous_hi', 'today_lo', 'today_hi', 'tomorrow_lo', 'tomorrow_hi']
    for col in temp_cols:
        df_am[col] = pd.to_numeric(df_am[col], errors='coerce')
    
    # Sort by city and date for temporal alignment
    df_am = df_am.sort_values(['city', 'date_and_time']).reset_index(drop=True)
    
    # Create properly aligned comparison datasets
    print("\nPerforming temporal alignment...")
    
    # For each city, shift previous_hi/lo to match forecast dates
    aligned_data = []
    
    for city in df_am['city'].unique():
        city_df = df_am[df_am['city'] == city].copy()
        
        # Create date column (without time) for matching
        city_df['date'] = city_df['date_and_time'].dt.date
        
        # For "today" forecasts: match with next day's previous temps
        city_df['actual_today_hi'] = city_df['previous_hi'].shift(-1)
        city_df['actual_today_lo'] = city_df['previous_lo'].shift(-1)
        
        # For "tomorrow" forecasts: match with 2 days later's previous temps
        city_df['actual_tomorrow_hi'] = city_df['previous_hi'].shift(-2)
        city_df['actual_tomorrow_lo'] = city_df['previous_lo'].shift(-2)
        
        aligned_data.append(city_df)
    
    df = pd.concat(aligned_data, ignore_index=True)
    
    # Remove rows without valid actuals (end of dataset for each city)
    df_today = df.dropna(subset=['actual_today_hi', 'actual_today_lo'])
    df_tomorrow = df.dropna(subset=['actual_tomorrow_hi', 'actual_tomorrow_lo'])
    
    print(f"Today forecasts with matched actuals: {len(df_today):,}")
    print(f"Tomorrow forecasts with matched actuals: {len(df_tomorrow):,}\n")
    
    # Store all metrics for summary
    all_metrics = []
    
    # ========== Error Distributions (Overall) ==========
    print("Generating overall forecast error distribution plots...")
    create_error_histogram(
        df_today['actual_today_hi'], df_today['today_hi'],
        'Today High Temperature Forecast Error Distribution (AM)',
        'error_dist_today_hi_overall.png',
        output_dir
    )
    
    create_error_histogram(
        df_today['actual_today_lo'], df_today['today_lo'],
        'Today Low Temperature Forecast Error Distribution (AM)',
        'error_dist_today_lo_overall.png',
        output_dir
    )
    
    create_error_histogram(
        df_tomorrow['actual_tomorrow_hi'], df_tomorrow['tomorrow_hi'],
        'Tomorrow High Temperature Forecast Error Distribution (AM)',
        'error_dist_tomorrow_hi_overall.png',
        output_dir
    )
    
    create_error_histogram(
        df_tomorrow['actual_tomorrow_lo'], df_tomorrow['tomorrow_lo'],
        'Tomorrow Low Temperature Forecast Error Distribution (AM)',
        'error_dist_tomorrow_lo_overall.png',
        output_dir
    )
    
    # Calculate overall metrics
    print("\nCalculating overall metrics...")
    metrics = calculate_metrics(df_today['actual_today_hi'], df_today['today_hi'])
    all_metrics.append({'city': 'Overall', 'forecast_type': 'Today Hi', **metrics})
    
    metrics = calculate_metrics(df_today['actual_today_lo'], df_today['today_lo'])
    all_metrics.append({'city': 'Overall', 'forecast_type': 'Today Lo', **metrics})
    
    metrics = calculate_metrics(df_tomorrow['actual_tomorrow_hi'], df_tomorrow['tomorrow_hi'])
    all_metrics.append({'city': 'Overall', 'forecast_type': 'Tomorrow Hi', **metrics})
    
    metrics = calculate_metrics(df_tomorrow['actual_tomorrow_lo'], df_tomorrow['tomorrow_lo'])
    all_metrics.append({'city': 'Overall', 'forecast_type': 'Tomorrow Lo', **metrics})
    
    # ========== By-City Time-Series Plots ==========
    print("\nGenerating by-city time-series forecast plots...")
    
    for city in df_today['city'].unique():
        city_df = df[df['city'] == city].copy()
        city_name = city.title()
        
        print(f"\n  {city_name}:")
        
        # Time-series plots showing actual, today forecast, and tomorrow forecast
        create_timeseries_plot(
            city_df, 'hi', city_name,
            f'timeseries_hi_{city.lower()}.png',
            output_dir
        )
        
        create_timeseries_plot(
            city_df, 'lo', city_name,
            f'timeseries_lo_{city.lower()}.png',
            output_dir
        )
        
        # Calculate city-specific metrics
        city_today = df_today[df_today['city'] == city]
        city_tomorrow = df_tomorrow[df_tomorrow['city'] == city]
        
        metrics = calculate_metrics(city_today['actual_today_hi'], city_today['today_hi'])
        all_metrics.append({'city': city_name, 'forecast_type': 'Today Hi', **metrics})
        
        metrics = calculate_metrics(city_today['actual_today_lo'], city_today['today_lo'])
        all_metrics.append({'city': city_name, 'forecast_type': 'Today Lo', **metrics})
        
        metrics = calculate_metrics(city_tomorrow['actual_tomorrow_hi'], city_tomorrow['tomorrow_hi'])
        all_metrics.append({'city': city_name, 'forecast_type': 'Tomorrow Hi', **metrics})
        
        metrics = calculate_metrics(city_tomorrow['actual_tomorrow_lo'], city_tomorrow['tomorrow_lo'])
        all_metrics.append({'city': city_name, 'forecast_type': 'Tomorrow Lo', **metrics})
    
    # ========== Summary Metrics ==========
    print("\n\nGenerating metrics comparison charts...")
    metrics_df = pd.DataFrame(all_metrics)
    
    # Save metrics to CSV
    metrics_csv = output_dir / 'accuracy_metrics.csv'
    metrics_df.to_csv(metrics_csv, index=False)
    print(f"  ✓ Saved accuracy_metrics.csv")
    
    # Create comparison charts
    city_metrics = metrics_df[metrics_df['city'] != 'Overall']
    if len(city_metrics) > 0:
        create_metrics_comparison(city_metrics, output_dir)
    
    # Print summary table
    print("\n" + "="*80)
    print("FORECAST ACCURACY SUMMARY")
    print("="*80)
    print(metrics_df.to_string(index=False))
    print("="*80)
    
    print("\n✓ Analysis complete! All outputs saved to:")
    print(f"  {output_dir}")
    print("\nGenerated files:")
    print("  - 6 time-series plots (2 per city: hi & lo temps)")
    print("  - 4 error distribution histograms (overall)")
    print("  - 1 metrics comparison chart")
    print("  - 1 accuracy metrics CSV")


if __name__ == '__main__':
    main()
