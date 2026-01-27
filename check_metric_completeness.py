
import os
import pandas as pd
import glob

def check_metrics():
    print("Checking CSV files for 'Index_Close'...")
    csv_files = glob.glob("*.csv")
    
    missing_metrics = []
    good_files = 0
    
    for f in csv_files:
        if f == "nifty500_tickers.csv" or f == "market_breadth_history.csv": 
            continue
            
        try:
            df = pd.read_csv(f)
            if 'Index_Close' not in df.columns:
                missing_metrics.append(f)
            else:
                # Check if it has data
                if df['Index_Close'].count() == 0:
                    missing_metrics.append(f"{f} (Column exists but empty)")
                else:
                    good_files += 1
        except Exception as e:
            print(f"Error reading {f}: {e}")
            
    print(f"\nTotal Files Checked: {len(csv_files)}")
    print(f"Good Files: {good_files}")
    print(f"Files Missing Metrics: {len(missing_metrics)}")
    
    if missing_metrics:
        print("\nList of files missing metrics:")
        for f in missing_metrics:
            print(f"- {f}")
            
if __name__ == "__main__":
    check_metrics()
