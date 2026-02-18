import pandas as pd
import os
import yfinance as yf
from datetime import datetime

# Configuration
BHAVCOPY_DIRS = ["Bhavcopies/2024", "Bhavcopies/2025"]
SOURCE_SYMBOL = "HINDUNILVR" # Backfill using HUL data
TARGET_SYMBOL = "KWIL"
ADJUSTMENT_FACTOR = 0.0191 # 1.91% of HUL price
CUTOFF_DATE = "2026-02-15" # Day before listing (Listing was Feb 16, 2026)
OUTPUT_FILE = "stitched_kwil_history.csv"

def load_backfill_data():
    all_rows = []
    print(f"Scanning Bhavcopies for {SOURCE_SYMBOL} to backfill {TARGET_SYMBOL}...")
    
    for directory in BHAVCOPY_DIRS:
        if not os.path.exists(directory):
            continue
            
        files = sorted([f for f in os.listdir(directory) if f.endswith(".parquet")])
        print(f"Processing {len(files)} files in {directory}...")
        
        for filename in files:
            filepath = os.path.join(directory, filename)
            try:
                df = pd.read_parquet(filepath)
                
                # Filter for HINDUNILVR
                mask = (df['symbol'] == SOURCE_SYMBOL) & (df['series'] == 'EQ')
                row = df[mask]
                
                if not row.empty:
                    date_val = row['trade_date'].iloc[0]
                    close_val = row['close'].iloc[0]
                    
                    all_rows.append({
                        "Date": date_val,
                        "Close": close_val
                    })
            except Exception:
                pass

    if not all_rows:
        return pd.DataFrame()
        
    backfill_df = pd.DataFrame(all_rows)
    backfill_df['Date'] = pd.to_datetime(backfill_df['Date'])
    backfill_df.sort_values('Date', inplace=True)
    
    # Filter up to cutoff
    backfill_df = backfill_df[backfill_df['Date'] <= CUTOFF_DATE]
    
    # Apply 1.91% Factor
    backfill_df['Close'] = backfill_df['Close'] * ADJUSTMENT_FACTOR
    
    print(f"Backfill data derived: {len(backfill_df)} rows (Max Date: {backfill_df['Date'].max()})")
    return backfill_df

def fetch_live_data():
    print("Fetching live KWIL.NS data...")
    try:
        kwil = yf.download("KWIL.NS", period="max", progress=False, auto_adjust=True)
        if kwil.empty:
            print("No live data found for KWIL.NS")
            return pd.DataFrame()
            
        kwil.reset_index(inplace=True)
        
        # Flatten MultiIndex cols if present
        if isinstance(kwil.columns, pd.MultiIndex):
             kwil.columns = [c[0] if isinstance(c, tuple) else c for c in kwil.columns]
             
        live_df = kwil[['Date', 'Close']].copy()
        live_df['Date'] = pd.to_datetime(live_df['Date']).dt.tz_localize(None)
        
        print(f"Live data fetched: {len(live_df)} rows")
        return live_df
    except Exception as e:
        print(f"Error live fetch: {e}")
        return pd.DataFrame()

def main():
    backfill_df = load_backfill_data()
    live_df = fetch_live_data()
    
    if backfill_df.empty and live_df.empty:
        print("No data at all.")
        return

    combined_df = pd.concat([backfill_df, live_df])
    combined_df.sort_values('Date', inplace=True)
    combined_df.drop_duplicates(subset=['Date'], keep='last', inplace=True)
    
    print(f"Stitched {len(combined_df)} rows.")
    combined_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
