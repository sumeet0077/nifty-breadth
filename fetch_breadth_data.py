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
    # Priority URLs
    if index_name == "Nifty 500":
        return get_tickers_from_url("https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv")
    elif index_name == "Nifty Smallcap 500":
        # Any of these might work, trying priority order
        urls = [
            "https://nsearchives.nseindia.com/content/indices/ind_niftysmallcap500list.csv", # Try direct match
            "https://nsearchives.nseindia.com/content/indices/ind_niftysmallcap250list.csv"  # Fallback to 250 if 500 fails
        ]
        for url in urls:
            tickers = get_tickers_from_url(url)
            if tickers:
                if "250" in url:
                    print("clean_warning: Nifty Smallcap 500 list not found, used Nifty Smallcap 250 instead.")
                return tickers
        return []
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

def main():
    # 1. Nifty 500
    process_index("Nifty 500", "market_breadth_nifty500.csv")
    
    # 2. Nifty Smallcap
    # Note: User asked for Smallcap 500.
    process_index("Nifty Smallcap 500", "market_breadth_smallcap.csv")

if __name__ == "__main__":
    main()
