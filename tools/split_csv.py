#!/usr/bin/env python3
"""
Split a large CSV file into 2 equal parts for GitHub upload.
Preserves the header in both files.

Usage:
    python tools/split_csv.py <input_file.csv>
    
Example:
    python tools/split_csv.py Forecast_Data/Historical_Weather_Data_2026_01.csv
"""
import sys
import pandas as pd
from pathlib import Path


def split_csv_file(input_file):
    """Split a CSV file into 2 parts."""
    input_path = Path(input_file)
    
    if not input_path.exists():
        print(f"[ERROR] File not found: {input_file}")
        return
    
    print(f"Loading {input_path.name}...")
    df = pd.read_csv(input_path)
    total_rows = len(df)
    print(f"Total rows: {total_rows:,}")
    
    # Calculate split point (50/50)
    split_point = total_rows // 2
    
    # Split dataframe
    df_part1 = df.iloc[:split_point]
    df_part2 = df.iloc[split_point:]
    
    # Create output filenames
    base_name = input_path.stem  # filename without extension
    part1_name = f"{base_name}_part1.csv"
    part2_name = f"{base_name}_part2.csv"
    
    part1_path = input_path.parent / part1_name
    part2_path = input_path.parent / part2_name
    
    # Save parts
    print(f"\nSaving part 1 ({len(df_part1):,} rows) to {part1_name}...")
    df_part1.to_csv(part1_path, index=False)
    
    print(f"Saving part 2 ({len(df_part2):,} rows) to {part2_name}...")
    df_part2.to_csv(part2_path, index=False)
    
    # Get file sizes
    size1_mb = part1_path.stat().st_size / (1024 * 1024)
    size2_mb = part2_path.stat().st_size / (1024 * 1024)
    original_mb = input_path.stat().st_size / (1024 * 1024)
    
    print(f"\n[DONE] Split complete!")
    print(f"  Original: {input_path.name} ({original_mb:.2f} MB)")
    print(f"  Part 1:   {part1_name} ({size1_mb:.2f} MB)")
    print(f"  Part 2:   {part2_name} ({size2_mb:.2f} MB)")
    
    if size1_mb < 100 and size2_mb < 100:
        print(f"\n[OK] Both parts are under GitHub's 100MB limit")
    else:
        print(f"\n[WARNING] One or both parts still exceed 100MB - may need further splitting")
    
    return part1_path, part2_path


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python tools/split_csv.py <input_file.csv>")
        print("\nExample:")
        print("  python tools/split_csv.py Forecast_Data/Historical_Weather_Data_2026_01.csv")
        sys.exit(1)
    
    input_file = sys.argv[1]
    split_csv_file(input_file)
