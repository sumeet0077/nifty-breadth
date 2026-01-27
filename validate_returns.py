
import pandas as pd
from datetime import timedelta
import glob

def check_returns():
    periods = {
        "1 Year": 365
    }
    
    csv_files = glob.glob("breadth_theme_*.csv")
    print(f"Checking returns for {len(csv_files)} files...")
    
    failed = []
    
    for f in csv_files:
        try:
            df = pd.read_csv(f)
            df['Date'] = pd.to_datetime(df['Date'])
            
            if 'Index_Close' not in df.columns:
                print(f"MISSING COL: {f}")
                continue
                
            latest = df.iloc[-1]
            current_date = latest['Date']
            current_price = latest['Index_Close']
            
            target_date = current_date - timedelta(days=365)
            mask = df['Date'] <= target_date
            
            if mask.any():
                past_row = df[mask].iloc[-1]
                past_price = past_row['Index_Close']
                ret = ((current_price - past_price) / past_price) * 100
                # print(f"{f}: {ret:.2f}%")
            else:
                print(f"NO DATA FOR PERIOD: {f} (Start Date: {df['Date'].min()})")
                failed.append(f)
                
        except Exception as e:
            print(f"ERROR: {f} -> {e}")
            failed.append(f)
            
    if not failed:
        print("\nAll themes have valid 1-Year return data.")
    else:
        print(f"\n{len(failed)} themes failed return calc.")

if __name__ == "__main__":
    check_returns()
