
import pandas as pd
import os
import glob

# Date to remove
BAD_DATE = "2026-02-01"

csv_files = glob.glob("breadth_*.csv") + glob.glob("market_breadth_*.csv")

print(f"Removing {BAD_DATE} from {len(csv_files)} files...")

count = 0
for f in csv_files:
    try:
        df = pd.read_csv(f)
        if 'Date' in df.columns:
            if BAD_DATE in df['Date'].values:
                df = df[df['Date'] != BAD_DATE]
                df.to_csv(f, index=False)
                count += 1
    except Exception as e:
        print(f"Error reading {f}: {e}")

print(f"Done. Removed from {count} files.")
