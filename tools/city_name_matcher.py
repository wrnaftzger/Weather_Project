#!/usr/bin/env python3
"""
City Name Matching Tool

Matches city names across different Excel/CSV files and creates a mapping key.
Handles variations in city names (case, spacing, special characters).

Usage:
    python tools/city_name_matcher.py <file1> <file2> [file3...]
    
Example:
    python tools/city_name_matcher.py cities_and_countries.csv Forecast_Data/Historical_Weather_Data_2026_02.csv
"""
import sys
import pandas as pd
from pathlib import Path
import re


def normalize_city_name(city):
    """Normalize city names for matching (lowercase, remove special chars, trim spaces)."""
    if pd.isna(city):
        return ""
    city = str(city).lower().strip()
    # Remove special characters but keep spaces
    city = re.sub(r'[^\w\s-]', '', city)
    # Replace multiple spaces with single space
    city = re.sub(r'\s+', ' ', city)
    return city


def load_file(filepath):
    """Load Excel or CSV file and return DataFrame."""
    path = Path(filepath)
    
    if not path.exists():
        print(f"[ERROR] File not found: {filepath}")
        return None
    
    try:
        if path.suffix.lower() in ['.xlsx', '.xls']:
            df = pd.read_excel(filepath)
        elif path.suffix.lower() == '.csv':
            df = pd.read_csv(filepath, nrows=10000)  # Sample first 10k rows for large files
        else:
            print(f"[ERROR] Unsupported file type: {path.suffix}")
            return None
        
        print(f"Loaded {filepath}: {len(df):,} rows, Columns: {list(df.columns)}")
        return df
    except Exception as e:
        print(f"[ERROR] Failed to load {filepath}: {e}")
        return None


def find_city_columns(df, filename):
    """Identify potential city name columns."""
    city_cols = []
    
    # Look for columns with 'city' in the name
    for col in df.columns:
        if 'city' in col.lower():
            city_cols.append(col)
    
    if not city_cols:
        print(f"\n  Columns in {filename}:")
        for i, col in enumerate(df.columns, 1):
            print(f"    {i}. {col}")
        
        choice = input(f"\n  Enter column number for city names in {filename} (or column name): ").strip()
        
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(df.columns):
                city_cols.append(df.columns[idx])
        except ValueError:
            if choice in df.columns:
                city_cols.append(choice)
            else:
                print(f"[ERROR] Column '{choice}' not found")
                return None
    
    return city_cols[0] if city_cols else None


def match_cities(files):
    """Match city names across multiple files."""
    if len(files) < 2:
        print("[ERROR] Need at least 2 files to compare")
        return
    
    # Load all files
    dataframes = []
    city_columns = []
    
    for filepath in files:
        df = load_file(filepath)
        if df is None:
            return
        
        city_col = find_city_columns(df, Path(filepath).name)
        if city_col is None:
            return
        
        dataframes.append(df)
        city_columns.append(city_col)
    
    # Extract unique cities from each file
    print("\n" + "="*80)
    print("EXTRACTING CITIES")
    print("="*80)
    
    all_cities = {}
    for i, (df, col) in enumerate(zip(dataframes, city_columns)):
        filename = Path(files[i]).name
        cities = df[col].dropna().unique()
        all_cities[filename] = {
            'original': list(cities),
            'normalized': [normalize_city_name(c) for c in cities]
        }
        print(f"{filename}: {len(cities)} unique cities")
    
    # Find matches
    print("\n" + "="*80)
    print("MATCHING CITIES")
    print("="*80)
    
    file1_name = Path(files[0]).name
    file1_cities = all_cities[file1_name]
    
    matches = []
    unmatched_in_file1 = []
    
    for i, city1 in enumerate(file1_cities['original']):
        norm1 = file1_cities['normalized'][i]
        
        match_found = False
        match_row = {'city_in_' + file1_name: city1, 'normalized': norm1}
        
        for j in range(1, len(files)):
            file_name = Path(files[j]).name
            file_cities = all_cities[file_name]
            
            if norm1 in file_cities['normalized']:
                idx = file_cities['normalized'].index(norm1)
                match_row['city_in_' + file_name] = file_cities['original'][idx]
                match_found = True
            else:
                match_row['city_in_' + file_name] = None
        
        matches.append(match_row)
        if not match_found:
            unmatched_in_file1.append(city1)
    
    # Create results dataframe
    results_df = pd.DataFrame(matches)
    
    # Count matches
    matched_count = len(results_df.dropna(subset=[col for col in results_df.columns if col.startswith('city_in_')], how='any'))
    
    print(f"\nTotal cities in {file1_name}: {len(file1_cities['original'])}")
    print(f"Matched cities: {matched_count}")
    print(f"Unmatched cities: {len(unmatched_in_file1)}")
    
    # Save results
    output_file = "city_name_mapping.csv"
    results_df.to_csv(output_file, index=False)
    print(f"\n[DONE] City mapping saved to: {output_file}")
    
    # Show sample
    print("\nSample matches (first 10):")
    print(results_df.head(10).to_string(index=False))
    
    if unmatched_in_file1:
        print(f"\nFirst 10 unmatched cities from {file1_name}:")
        for city in unmatched_in_file1[:10]:
            print(f"  - {city}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python tools/city_name_matcher.py <file1> <file2> [file3...]")
        print("\nExample:")
        print("  python tools/city_name_matcher.py cities_and_countries.csv Forecast_Data/us_city_forecasts.csv")
        sys.exit(1)
    
    files = sys.argv[1:]
    match_cities(files)
