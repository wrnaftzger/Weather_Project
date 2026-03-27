"""
Sequential Historical Weather Data Collection
Collects data one month at a time (reliable, no Unicode issues)
"""
import subprocess
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# Configuration
START_DATE = datetime(2022, 1, 1)
END_DATE = datetime(2025, 11, 10)

def generate_monthly_batches(start_date, end_date):
    """Generate list of monthly date ranges"""
    batches = []
    current = start_date
    
    while current <= end_date:
        # Calculate end of month
        next_month = current + relativedelta(months=1)
        month_end = min(next_month - timedelta(days=1), end_date)
        
        batches.append({
            'start': current.strftime('%Y-%m-%d'),
            'end': month_end.strftime('%Y-%m-%d'),
            'label': current.strftime('%Y-%m')
        })
        
        current = next_month
    
    return batches

def main():
    # Set environment variables for database connection
    os.environ['AZURE_SQL_USER'] = 'CloudSA651686c0'
    os.environ['AZURE_SQL_PASSWORD'] = 'Weather!'
    
    # Generate monthly batches
    batches = generate_monthly_batches(START_DATE, END_DATE)
    
    print("=" * 70)
    print("🌍 Sequential Historical Weather Data Collection")
    print("=" * 70)
    print(f"Date range: {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}")
    print(f"Total batches: {len(batches)} months")
    print(f"Estimated time: {len(batches) * 11} minutes (~{len(batches) * 11 // 60} hours)")
    print("=" * 70)
    print()
    
    # Ask for confirmation
    response = input("Start collection? (yes/no): ").strip().lower()
    if response != 'yes':
        print("❌ Collection cancelled")
        return
    
    print()
    print("🚀 Starting sequential collection...")
    print("=" * 70)
    
    successful = 0
    failed = 0
    failed_batches = []
    
    for i, batch in enumerate(batches, 1):
        print(f"\n[{i}/{len(batches)}] Processing {batch['label']}: {batch['start']} to {batch['end']}")
        print("-" * 70)
        
        try:
            # Run weather_data_collector.py for this month
            # Pass environment variables explicitly to subprocess
            env = os.environ.copy()
            env['AZURE_SQL_USER'] = 'CloudSA651686c0'
            env['AZURE_SQL_PASSWORD'] = 'Weather!'
            
            result = subprocess.run(
                ['python', 'weather_data_collector.py', batch['start'], batch['end']],
                capture_output=True,
                text=True,
                timeout=3600,  # 1 hour timeout per month
                env=env
            )
            
            if result.returncode == 0:
                successful += 1
                # Extract record count from output
                if "Inserted" in result.stdout:
                    for line in result.stdout.split('\n'):
                        if "Inserted" in line:
                            print(f"✅ {line.strip()}")
                            break
                else:
                    print(f"✅ Completed successfully")
            else:
                failed += 1
                failed_batches.append(batch['label'])
                print(f"❌ Failed with exit code {result.returncode}")
                # Show both stdout and stderr for debugging
                if result.stdout:
                    print(f"Output: {result.stdout[-500:]}")  # Last 500 chars
                if result.stderr:
                    print(f"Error: {result.stderr[-500:]}")
        
        except subprocess.TimeoutExpired:
            failed += 1
            failed_batches.append(batch['label'])
            print(f"❌ Timeout after 1 hour")
        
        except Exception as e:
            failed += 1
            failed_batches.append(batch['label'])
            print(f"❌ Error: {str(e)}")
    
    # Final summary
    print()
    print("=" * 70)
    print("📊 COLLECTION COMPLETE")
    print("=" * 70)
    print(f"✅ Successful: {successful}/{len(batches)} months")
    print(f"❌ Failed: {failed}/{len(batches)} months")
    
    if failed_batches:
        print(f"\n⚠️  Failed batches: {', '.join(failed_batches)}")
        print("💡 Tip: Re-run this script to retry failed batches.")
    else:
        print("\n🎉 All batches completed successfully!")

if __name__ == '__main__':
    main()
