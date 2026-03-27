#!/usr/bin/env python3
"""
Scan all CSV files in Email_Data_Csv/data/output_csv and write a combined
CSV containing only rows for Nashville, Philadelphia, and Seattle.

Creates: Email_Data_Csv/data/output_csv/selected_cities.csv

The script is case-insensitive and looks for whole-word matches in the
`city` column when available; otherwise it searches all fields.
"""
from pathlib import Path
import csv
import re
import sys


TARGET_CITIES = ["nashville", "philadelphia", "seattle"]
CITY_REGEX = re.compile(r"\b(?:" + "|".join(re.escape(c) for c in TARGET_CITIES) + r")\b", flags=re.I)


def find_city_column(fieldnames):
    if not fieldnames:
        return None
    # prefer exact 'city', then any field containing 'city'
    for fn in fieldnames:
        if fn.lower() == 'city':
            return fn
    for fn in fieldnames:
        if 'city' in fn.lower():
            return fn
    return None


def row_matches(row, city_col):
    if city_col and city_col in row and row[city_col] is not None:
        return bool(CITY_REGEX.search(row[city_col]))
    # fallback: search all values
    for v in row.values():
        if v is None:
            continue
        if CITY_REGEX.search(str(v)):
            return True
    return False


def main():
    repo_root = Path(__file__).resolve().parent.parent
    input_dir = repo_root / 'Email_Data_Csv' / 'data' / 'output_csv'
    if not input_dir.exists():
        print(f"Input directory not found: {input_dir}")
        sys.exit(1)

    output_path = input_dir / 'selected_cities.csv'

    csv_files = sorted([p for p in input_dir.iterdir() if p.suffix.lower() == '.csv'])
    # exclude the output file if it already exists in the list
    csv_files = [p for p in csv_files if p.name != output_path.name]

    if not csv_files:
        print(f"No CSV files found in {input_dir}")
        sys.exit(1)

    writer = None
    total_matched = 0
    counts = {c: 0 for c in TARGET_CITIES}

    for csv_path in csv_files:
        try:
            with csv_path.open(newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                if reader.fieldnames is None:
                    continue
                city_col = find_city_column(reader.fieldnames)

                # initialize writer with first file's headers
                if writer is None:
                    out_fieldnames = list(reader.fieldnames) + ['source_file']
                    out_f = output_path.open('w', newline='', encoding='utf-8')
                    writer = csv.DictWriter(out_f, fieldnames=out_fieldnames, extrasaction='ignore')
                    writer.writeheader()

                for row in reader:
                    if row_matches(row, city_col):
                        # compute which target city matched (best-effort)
                        if city_col:
                            joined = str(row.get(city_col, ''))
                        else:
                            joined = ' '.join(str(v) for v in row.values())
                        matched = CITY_REGEX.search(joined)
                        matched_city = matched.group(0).lower() if matched else None
                        if matched_city in counts:
                            counts[matched_city] += 1
                        total_matched += 1
                        row_out = dict(row)
                        row_out['source_file'] = csv_path.name
                        writer.writerow(row_out)
        except Exception as e:
            print(f"Skipping {csv_path.name} due to error: {e}")

    if writer is None:
        print("No output was written (no readable CSVs found).")
    else:
        print(f"Wrote {total_matched} matching rows to {output_path}")
        for k, v in counts.items():
            print(f"  {k.title()}: {v}")


if __name__ == '__main__':
    main()
