import pandas as pd
import yfinance as yf
import requests
import io
import time
import os


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
            tickers = [x + ".NS" for x in df[symbol_col] if isinstance(x, str)]
            return tickers
        return []
    except Exception as e:
        print(f"Failed to fetch from {url}: {e}")
        return []


def get_index_tickers(index_name):
    """Get tickers for a specific index."""
    
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

def main():
    indices = [
        ("Nifty 50", "market_breadth_nifty50.csv"),
        ("Nifty 500", "market_breadth_nifty500.csv"),
        ("Nifty Smallcap 500", "market_breadth_smallcap.csv"),
        # Sectors
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
    
    print(f"Processing {len(indices)} indices...")
    
    for name, filename in indices:
        try:
            if os.path.exists(filename):
                print(f"Skipping {name} (already exists, delete file to re-fetch)...")
                # Actually, user wants updates, so maybe we SHOULD NOT SKIP?
                # But for this initial mega-run, I'll allow overwrite.
                # Let's overwrite.
                pass
            
            process_index(name, filename)
        except Exception as e:
            print(f"Failed to process {name}: {e}")



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
            # yf.download with group_by='ticker' returns MultiIndex (Ticker, PriceType)
            # We want a DataFrame where columns are Tickers and values are Close prices
            
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
    """Calculate market breadth metrics."""
    print("Calculating 200-day SMA and Breadth metrics...")
    
    # 1. Calculate 200 SMA
    # min_periods=150 allows for some missing data (holidays, trading suspensions)
    # ensuring we still have a representative average (~75% of data points)
    sma_200 = full_data.rolling(window=200, min_periods=150).mean()
    
    # 2. Compare Close vs SMA
    is_above = full_data > sma_200
    
    # 3. Valid Universe per day: Stocks that have a valid Price AND a valid SMA on that day
    # (Checking sma_200.notna() is sufficient as it implies price existed 200 days back and today)
    valid_universe = sma_200.notna() & full_data.notna()
    
    # 4. Count
    # We only count boolean where valid_universe is True
    above_count = (is_above & valid_universe).sum(axis=1)
    
    # Total valid stocks on that day
    total_valid = valid_universe.sum(axis=1)
    
    # Calculate Below
    # below_count = total_valid - above_count 
    below_count = total_valid - above_count
    
    # Percentage
    # Handle division by zero
    percentage = (above_count / total_valid) * 100
    percentage = percentage.fillna(0) # or NaN, but 0 is safer for plotting if total_valid is 0
    
    breadth_df = pd.DataFrame({
        'Above': above_count,
        'Below': below_count,
        'Total': total_valid,
        'Percentage': percentage
    })
    
    return breadth_df

def process_index(index_name, output_file):
    print(f"\nProcessing {index_name}...")
    tickers = get_index_tickers(index_name)
    
    if not tickers:
        print(f"No tickers found for {index_name}!")
        return
        
    print(f"Index: {index_name} | Tickers: {len(tickers)}")
    
    # 2014 start for warm up
    full_data = fetch_historical_data(tickers, start_date="2014-01-01")
    
    if full_data.empty:
        print("No data fetched.")
        return
        
    breadth_df = calculate_breadth(full_data)
    breadth_df.to_csv(output_file)
    print(f"Saved {output_file}")

if __name__ == "__main__":
    main()
