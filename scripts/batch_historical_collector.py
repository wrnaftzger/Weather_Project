"""
batch_historical_collector.py — Batch Historical Weather Data Collection
=========================================================================
Collects historical weather data in monthly batches for reliability.
Runs multiple months in parallel for speed.

Usage:
    python batch_historical_collector.py
"""

import os
import subprocess
import multiprocessing
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# Configuration
START_DATE = datetime(2022, 1, 1)
END_DATE = datetime(2025, 11, 10)
PARALLEL_PROCESSES = 3  # Run 3 months at a time

def generate_monthly_ranges(start_date, end_date):
    """Generate list of (start_date, end_date) tuples for each month."""
    ranges = []
    current = start_date
    
    while current <= end_date:
        # Get last day of current month
        next_month = current + relativedelta(months=1)
        month_end = min(next_month - timedelta(days=1), end_date)
        
        ranges.append((
            current.strftime("%Y-%m-%d"),
            month_end.strftime("%Y-%m-%d")
        ))
        
        current = next_month
    
    return ranges

def run_monthly_batch(args):
    """Run weather_data_collector.py for a single month."""
    start_date, end_date = args
    
    print(f"\n{'='*70}")
    print(f"📅 Starting: {start_date} to {end_date}")
    print(f"{'='*70}\n")
    
    # Set environment variables
    env = os.environ.copy()
    env["AZURE_SQL_USER"] = "CloudSA651686c0"
    env["AZURE_SQL_PASSWORD"] = "Weather!"
    
    # Run the collector
    try:
        result = subprocess.run(
            ["python", "weather_data_collector.py", start_date, end_date],
            env=env,
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout per month
        )
        
        if result.returncode == 0:
            print(f"✅ Completed: {start_date} to {end_date}")
            return True
        else:
            print(f"❌ Failed: {start_date} to {end_date}")
            print(f"Error: {result.stderr[:200]}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"⏱️ Timeout: {start_date} to {end_date} (took over 1 hour)")
        return False
    except Exception as e:
        print(f"❌ Error: {start_date} to {end_date}: {e}")
        return False

def main():
    print("🌍 Batch Historical Weather Data Collection")
    print("=" * 70)
    print(f"Date range: {START_DATE.date()} to {END_DATE.date()}")
    print(f"Parallel processes: {PARALLEL_PROCESSES}")
    print("=" * 70)
    
    # Generate monthly batches
    monthly_ranges = generate_monthly_ranges(START_DATE, END_DATE)
    print(f"\n📊 Total batches: {len(monthly_ranges)} months")
    print(f"⏱️  Estimated time: {len(monthly_ranges) // PARALLEL_PROCESSES} hours\n")
    
    # Ask for confirmation
    response = input("Start collection? (yes/no): ")
    if response.lower() not in ['yes', 'y']:
        print("Cancelled.")
        return
    
    print("\n🚀 Starting parallel collection...\n")
    
    # Run batches in parallel
    with multiprocessing.Pool(processes=PARALLEL_PROCESSES) as pool:
        results = pool.map(run_monthly_batch, monthly_ranges)
    
    # Summary
    successful = sum(results)
    failed = len(results) - successful
    
    print("\n" + "=" * 70)
    print("📊 COLLECTION COMPLETE")
    print("=" * 70)
    print(f"✅ Successful: {successful}/{len(monthly_ranges)} months")
    print(f"❌ Failed: {failed}/{len(monthly_ranges)} months")
    
    if failed > 0:
        print("\n⚠️  Some batches failed. Check logs above for details.")
        print("💡 Tip: Re-run this script to retry failed batches.")

if __name__ == "__main__":
    main()
