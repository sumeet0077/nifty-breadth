import pandas as pd
import yfinance as yf
import requests
import io
import time
import os
from nifty_themes import THEMES

def get_tickers_from_url(url):
    """Generic function to fetch tickers from NSE CSV URL."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        print(f"Fetching from {url}...")
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        df = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
        symbol_col = next((col for col in df.columns if 'Symbol' in col), None)
        if symbol_col:
            raw_tickers = df[symbol_col].dropna().astype(str).tolist()
            cleaned_tickers = []
            
            # Special handling for Nifty 50 CSV known data issues
            is_nifty50 = "nifty50list.csv" in url
            
            for t in raw_tickers:
                # Filter Garbage
                if "DUMMY" in t or "ETERNAL" in t:
                    continue
                
                # Transformations
                if t == "TMPV": 
                    # TATAMOTORS.NS is failing on Yahoo, use BSE as fallback
                    cleaned_tickers.append("TATAMOTORS.BO") 
                    continue
                
                cleaned_tickers.append(t + ".NS")
            
            if is_nifty50:
                # Manually restore missing stocks (likely replaced by DUMMY/ETERNAL in CSV)
                missing = ["INDUSINDBK.NS", "BPCL.NS"]
                for m in missing:
                    if m not in cleaned_tickers:
                        cleaned_tickers.append(m)
                        
            return sorted(list(set(cleaned_tickers)))
        return []
    except Exception as e:
        print(f"Failed to fetch from {url}: {e}")
        return []

def get_index_tickers(index_name):
    """Get tickers for a specific index or theme."""
    
    # Check Custom Themes first
    if index_name in THEMES:
        return THEMES[index_name]

    # Map index names to verified CSV URLs
    # Source: https://nsearchives.nseindia.com/content/indices/
    sector_map = {
        "Nifty 50": "ind_nifty50list.csv",
        "Nifty 500": "ind_nifty500list.csv",
        # Smallcap
        "Nifty Smallcap 500": "ind_niftysmallcap250list.csv", # Fallback
        # Sectors
        "NIFTY AUTO": "ind_niftyautolist.csv",
        "NIFTY BANK": "ind_niftybanklist.csv",
        "NIFTY FINANCIAL SERVICES": "ind_niftyfinancelist.csv",
        "NIFTY FMCG": "ind_niftyfmcglist.csv",
        "NIFTY HEALTHCARE": "ind_niftyhealthcarelist.csv",
        "NIFTY IT": "ind_niftyitlist.csv",
        "NIFTY MEDIA": "ind_niftymedialist.csv",
        "NIFTY METAL": "ind_niftymetallist.csv",
        "NIFTY PHARMA": "ind_niftypharmalist.csv",
        "NIFTY PRIVATE BANK": "ind_nifty_privatebanklist.csv",
        "NIFTY PSU BANK": "ind_niftypsubanklist.csv",
        "NIFTY REALTY": "ind_niftyrealtylist.csv",
        "NIFTY CONSUMER DURABLES": "ind_niftyconsumerdurableslist.csv",
        "NIFTY OIL AND GAS": "ind_niftyoilgaslist.csv"
    }

    if index_name in sector_map:
        base_url = "https://nsearchives.nseindia.com/content/indices/"
        # Nifty Smallcap 500 special check logic handled by map just pointing to 250 for now
        return get_tickers_from_url(base_url + sector_map[index_name])
        
    return []

def fetch_historical_data(tickers, start_date="2014-01-01"):
    """Fetch historical data for tickers in chunks."""
    chunk_size = 50
    all_close_prices = []
    
    print(f"Starting data download for {len(tickers)} stocks from {start_date}...")
    
    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i+chunk_size]
        print(f"Downloading batch {i//chunk_size + 1}/{(len(tickers)-1)//chunk_size + 1} ({len(chunk)} stocks)...")
        
        try:
            # Ticker strings need to be space-separated
            data = yf.download(
                chunk, 
                start=start_date, 
                group_by='ticker', 
                threads=True, 
                progress=False,
                auto_adjust=True
            )
            
            # Extract Close prices
            close_df = pd.DataFrame()
            
            if len(chunk) == 1:
                # specific handling for single ticker
                ticker = chunk[0]
                if not data.empty:
                    close_df[ticker] = data['Close']
            else:
                for ticker in chunk:
                    try:
                        if ticker in data.columns.levels[0]:
                            ticker_data = data[ticker]
                            # verify if 'Close' exists
                            if 'Close' in ticker_data.columns:
                                close_df[ticker] = ticker_data['Close']
                    except Exception as e:
                        pass # Ticker might not be in response
            
            # Clean up: remove columns that are all NaN
            close_df.dropna(axis=1, how='all', inplace=True)
            
            if not close_df.empty:
                all_close_prices.append(close_df)
                
            time.sleep(1) # Polite delay
            
        except Exception as e:
            print(f"Failed to download batch {i}: {e}")
            
    if not all_close_prices:
        return pd.DataFrame()
        
    # Combine all batches
    print("Combining data...")
    full_data = pd.concat(all_close_prices, axis=1)
    
    # Sort index (dates)
    full_data.sort_index(inplace=True)
    
    return full_data

def calculate_breadth(full_data):
    """Calculate market breadth metrics and Equal-Weighted Index."""
    # 1. Calculate 200 SMA
    # min_periods=150 allows for some missing data (holidays, trading suspensions)
    sma_200 = full_data.rolling(window=200, min_periods=150).mean()
    
    # 2. Compare Close vs SMA
    is_above = full_data > sma_200
    
    # 3. Valid Universe per day: Stocks that have a valid Price AND a valid SMA on that day
    valid_universe = sma_200.notna() & full_data.notna()
    
    # 4. Count
    above_count = (is_above & valid_universe).sum(axis=1)
    
    # Total valid stocks on that day
    total_valid = valid_universe.sum(axis=1)
    
    # Calculate Below
    below_count = total_valid - above_count
    
    # Percentage
    percentage = (above_count / total_valid) * 100
    percentage = percentage.fillna(0)
    
    # 5. Calculate Equal-Weighted Index History
    # Calculate daily percent change for each stock
    daily_returns = full_data.pct_change()
    
    # Average daily return of the constituent stocks (Equal Weight)
    # We use mean(axis=1) to get the daily index return
    index_daily_return = daily_returns.mean(axis=1)
    
    # Compute cumulative index value starting at 100
    # (1 + r).cumprod() * 100
    # Fill NaN at start with 0 return (value 1.0)
    index_close = (1 + index_daily_return.fillna(0)).cumprod() * 100
    
    breadth_df = pd.DataFrame({
        'Above': above_count,
        'Below': below_count,
        'Total': total_valid,
        'Percentage': percentage,
        'Index_Close': index_close
    })
    
    return breadth_df

def main():
    # 1. Define all tasks
    broad_indices = [
        ("Nifty 50", "market_breadth_nifty50.csv"),
        ("Nifty 500", "market_breadth_nifty500.csv"),
        ("Nifty Smallcap 500", "market_breadth_smallcap.csv"),
    ]
    
    sector_indices = [
        ("NIFTY AUTO", "breadth_auto.csv"),
        ("NIFTY BANK", "breadth_bank.csv"),
        ("NIFTY FINANCIAL SERVICES", "breadth_finance.csv"),
        ("NIFTY FMCG", "breadth_fmcg.csv"),
        ("NIFTY HEALTHCARE", "breadth_healthcare.csv"),
        ("NIFTY IT", "breadth_it.csv"),
        ("NIFTY MEDIA", "breadth_media.csv"),
        ("NIFTY METAL", "breadth_metal.csv"),
        ("NIFTY PHARMA", "breadth_pharma.csv"),
        ("NIFTY PRIVATE BANK", "breadth_pvtbank.csv"),
        ("NIFTY PSU BANK", "breadth_psubank.csv"),
        ("NIFTY REALTY", "breadth_realty.csv"),
        ("NIFTY CONSUMER DURABLES", "breadth_consumer.csv"),
        ("NIFTY OIL AND GAS", "breadth_oilgas.csv")
    ]
    
    # Add Themes to list (Name, CSV Name)
    # CSV name = breadth_theme_name_sanitized.csv
    theme_indices = []
    sorted_themes = sorted(THEMES.keys())
    for theme_name in sorted_themes:
        # Sanitize filename
        safe_name = theme_name.lower().replace(" ", "_").replace("&", "and").replace("-", "_").replace("(", "").replace(")", "").replace("__", "_")
        filename = f"breadth_theme_{safe_name}.csv"
        theme_indices.append((theme_name, filename))
        
    all_tasks = broad_indices + sector_indices + theme_indices
    
    print(f"Total Indices/Themes to process: {len(all_tasks)}")
    
    # OPTIMIZATION:
    # 1. Collect ALL distinct tickers across ALL tasks
    # 2. Fetch data ONCE
    # 3. Process individually
    
    all_tickers = set()
    task_map = {} # Name -> [Tickers]
    
    print("Gathering ticker lists...")
    for name, filename in all_tasks:
        tickers = get_index_tickers(name)
        if tickers:
            task_map[name] = tickers
            all_tickers.update(tickers)
        else:
            print(f"Warning: No tickers found for {name}")

    print(f"Total Unique Tickers to fetch: {len(all_tickers)}")
    
    # Fetch Master Data
    master_data = fetch_historical_data(list(all_tickers), start_date="2014-01-01")
    
    if master_data.empty:
        print("CRITICAL: No data fetched at all.")
        return

    # Process each task using Master Data
    print("\nProcessing Breadth for all groups...")
    for name, filename in all_tasks:
        try:
            # Check if file exists and skip if needed? No, user wants rebuild.
            
            if name not in task_map:
                continue
                
            tickers = task_map[name]
            # Filter master data for these tickers
            # Intersect with columns present in master_data
            available_tickers = [t for t in tickers if t in master_data.columns]
            
            if not available_tickers:
                print(f"Skipping {name}: No data for constituent tickers.")
                continue
                
            subset_data = master_data[available_tickers].copy()
            
            # Check if sufficient data
            if subset_data.empty:
                continue
                
            breadth_df = calculate_breadth(subset_data)
            breadth_df.to_csv(filename)
            print(f"Saved {filename} ({name})")
            
        except Exception as e:
            print(f"Failed to process {name}: {e}")

if __name__ == "__main__":
    main()
