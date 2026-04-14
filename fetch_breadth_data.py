import pandas as pd
import yfinance as yf
import requests
import io
import time
import os
import json
import sys
from datetime import datetime, timedelta
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
            
            for t in raw_tickers:
                # Filter Garbage rows (e.g. DUMMY placeholders)
                if "DUMMY" in t:
                    continue
                
                # Ticker transformations for Yahoo Finance compatibility
                overrides = {
                    "LTM": "LTIM.NS"
                }

                if t in overrides:
                    cleaned_tickers.append(overrides[t])
                    continue

                if t == "KWIL":
                    cleaned_tickers.append("KWIL.NS")
                    continue
                
                cleaned_tickers.append(t + ".NS")
                        
            return sorted(list(set(cleaned_tickers)))
        return []
    except Exception as e:
        print(f"Failed to fetch from {url}: {e}")
        return []

def get_index_tickers(index_name):
    """Get tickers for a specific index or theme."""
    
    # Check Custom Themes first
    if index_name in THEMES:
        # Deduplicate to prevent pandas column-name collisions in calculate_breadth
        return list(dict.fromkeys(THEMES[index_name]))

    # Map index names to verified CSV URLs
    # Source: https://nsearchives.nseindia.com/content/indices/
    sector_map = {
        "Nifty 50": "ind_nifty50list.csv",
        "Nifty 500": "ind_nifty500list.csv",
        # Smallcap
        "Nifty Smallcap 250": "ind_niftysmallcap250list.csv", # Fallback
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

    # Nifty 50 now uses the dynamic NSE CSV fetch like all other sectors

    if index_name in sector_map:
        base_url = "https://nsearchives.nseindia.com/content/indices/"
        # Nifty Smallcap 500 special check logic handled by map just pointing to 250 for now
        return get_tickers_from_url(base_url + sector_map[index_name])
        
    return []

def fetch_historical_data(tickers, start_date="2014-01-01"):
    """Fetch historical data for tickers using local adjusted Parquet database."""
    
    parquet_path = "/Users/sumeetdas/Antigravity_NSE_Data/nse_master_adjusted_2014_onwards.parquet"
    
    # ── Symbol Alias Map (Support One-to-Many) ──────────────────────────
    # Format: { "OLD_HISTORICAL_SYMBOL": ["CANONICAL_SYMBOL_1", ...] }
    # This allows demergers (TATAMOTORS -> TMPV, TMCV) and name changes
    # to be bridged seamlessly for continuous 5-year data.
    # ── Symbol Alias Map (Support One-to-Many) ──────────────────────────
    # Format: { "OLD_HISTORICAL_SYMBOL": ["CANONICAL_SYMBOL_1", ...] }
    SYMBOL_ALIASES = {
        "MOTHERSUMI": ["MOTHERSON"],
        "MINDAIND": ["UNOMINDA"],
        "TATAMOTORS": ["TMPV"],
        "LTI": ["LTIM"], 
        "LTM": ["LTIM"], # LTIM -> LTM transition (Feb 27 2026)
        "IIFLWAM": ["360ONE"], # IIFL Wealth -> 360 ONE (Feb 2023)
        "TTKHLTCARE": ["TTKHLT"], # TTK Healthcare Current Symbol Alignment
        "TTKHEALTH": ["TTKHLT"], # Older symbol for TTK Healthcare
        "ANGELBRKG": ["ANGELONE"],
        "CADILAHC": ["ZYDUSLIFE"],
        "MCDOWELL-N": ["UNITDSPR"],
        "IBULHSGFIN": ["SAMMAANCAP"],
        "SRTRANSFIN": ["SHRIRAMFIN"],
        "WABCOINDIA": ["ZFCVINDIA"],
        "WELSPUNIND": ["WELSPUNLIV"],
    }

    # ── Price Scaling for Demergers & Bonus Adjustments ────────────────
    # Maps (OLD_SYMBOL, NEW_SYMBOL) -> Scaling Factor
    # Calibrated to match market-accurate adjusted prices (e.g. Google Finance)
    # This bridges data gaps in the Parquet's automated adjustment logic.
    SYMBOL_SCALING = {
        ("TATAMOTORS", "TMPV"): 0.6885,   # Passenger (Official Ratio)
        ("MOTHERSUMI", "MOTHERSON"): 0.3478, # Full adjustment (Jan+Jun Demergers)
        ("MINDAIND", "UNOMINDA"): 0.50,   # Missing 1:1 Bonus adjustment
    }
    
    # Reverse map for quick lookup during expansion: canonical -> old_symbols
    CANONICAL_TO_OLD: dict[str, list[str]] = {}
    for old, canon_list in SYMBOL_ALIASES.items():
        for canon in canon_list:
            CANONICAL_TO_OLD.setdefault(canon, []).append(old)
    # ──────────────────────────────────────────────────────────────────────
    
    print(f"Starting data scan for {len(tickers)} stocks from {start_date} from Local Parquet Database...")
    
    import pyarrow as pa
    import pyarrow.dataset as ds
    import pandas as pd
    
    # Clean tickers from yfinance suffixes (.NS, .BO) for our local database
    clean_tickers = [t.replace('.NS', '').replace('.BO', '') for t in tickers]
    
    # Expand clean_tickers with any known old symbols so we fetch ALL historical rows
    expanded_tickers = set(clean_tickers)
    for ct in clean_tickers:
        if ct in CANONICAL_TO_OLD:
            expanded_tickers.update(CANONICAL_TO_OLD[ct])
    expanded_tickers = list(expanded_tickers)
    
    # Convert start_date string to datetime/timestamp for pyarrow filtering
    start_dt = pd.to_datetime(start_date)
    
    try:
        dataset = ds.dataset(parquet_path, format="parquet", partitioning="hive")
        
        # We only need trade_date, symbol, and adj_close to build our history tables
        columns_to_read = ['trade_date', 'symbol', 'series', 'adj_close']
        
        # Filter for our specific symbols AND dates >= start_date AND series in ['EQ', 'BE', 'ST', 'SM']
        filter_expr = (ds.field('symbol').isin(expanded_tickers)) & (ds.field('trade_date') >= pa.scalar(start_dt)) & (ds.field('series').isin(['EQ', 'BE', 'ST', 'SM']))
        
        # Read the filtered table
        table = dataset.to_table(columns=columns_to_read, filter=filter_expr)
        df = table.to_pandas()
        
        # Cast float32 to float64 to ensure json.dump can serialize the final performance calculations
        df['adj_close'] = df['adj_close'].astype(float)
        
        if df.empty:
            return pd.DataFrame()
        
        # Remap alias symbols to their canonical names (with row duplication and scaling)
        rows_to_add = []
        for old_sym, canon_list in SYMBOL_ALIASES.items():
            mask = df['symbol'] == old_sym
            if mask.any():
                old_data = df[mask].copy()
                # Remove the old symbol from the main df to avoid duplicates during replacement
                df = df[~mask]
                # Add a copy for each canonical symbol
                for canon in canon_list:
                    new_data = old_data.copy()
                    new_data['symbol'] = canon
                    
                    # Apply scaling factor if it exists for this (old, new) pair
                    scale_factor = SYMBOL_SCALING.get((old_sym, canon))
                    if scale_factor:
                        new_data['adj_close'] *= scale_factor
                        
                    rows_to_add.append(new_data)
        
        if rows_to_add:
            df = pd.concat([df] + rows_to_add, ignore_index=True)
            
        # Reconstruct the expected 'TICKER.NS' strings to perfectly match downstream pipeline
        df['ticker_with_suffix'] = df['symbol'].astype(str) + '.NS'
        
        # Pivot the dataframe so that we get a Date Index with Ticker columns containing Adj Close
        pivot_df = df.pivot_table(index='trade_date', columns='ticker_with_suffix', values='adj_close')
        pivot_df.index.name = 'Date'
        
        # Ensure the index is timezone-naive just to be safe
        if pivot_df.index.tz is not None:
            pivot_df.index = pivot_df.index.tz_localize(None)
            
        # --- SELF-HEALING ANOMALY DETECTION (STOCK SPLITS/DEMERGERS) ---
        # If the Parquet DB hasn't adjusted a very recent stock split, it will look like a massive crash.
        # We detect >45% overnight drops (to catch 1:2 splits but safely ignore market crashes) 
        # in the last 15 days, and use yfinance to surgically overwrite the column temporarily.
        recent_df = pivot_df.tail(15)
        pct_returns = recent_df.pct_change()
        split_anomalies = []
        
        for col in pct_returns.columns:
            if (pct_returns[col] < -0.45).any():
                split_anomalies.append(col)
                
        if split_anomalies:
            print(f"Self-Healing Triggered: Detected massive corporate action anomalies for {split_anomalies}")
            for ticker in split_anomalies:
                try:
                    print(f"Fetching adjusted history for {ticker} from Yahoo Finance fallback...")
                    # Fetch 5 years to correctly recalibrate 5Y Return metrics
                    yf_data = yf.download(ticker, start=pivot_df.index[0], end=pivot_df.index[-1] + pd.Timedelta(days=1), progress=False, auto_adjust=True)
                    if not yf_data.empty:
                        if ticker in yf_data.columns.get_level_values(0):
                            clean_series = yf_data[ticker]['Close']
                        elif 'Close' in yf_data.columns.get_level_values(0):
                            clean_series = yf_data['Close'].iloc[:, 0]
                        elif 'Close' in yf_data.columns:
                            clean_series = yf_data['Close']
                        else:
                            continue
                            
                        if clean_series.index.tz is not None:
                            clean_series.index = clean_series.index.tz_localize(None)
                        
                        # Reindex clean_series perfectly to the Parquet dates to capture special weekend trading dates.
                        # Forward-fill any YFinance missing dates using the previous trading day's true adjusted price.
                        clean_series = clean_series.reindex(pivot_df.index).ffill().bfill()
                        
                        # Overwrite the broken Parquet column with the newly adjusted history securely
                        pivot_df[ticker] = clean_series
                except Exception as e:
                    print(f"Failed to apply fallback for {ticker}: {e}")
                    
        full_data = pivot_df
        
    except Exception as e:
        print(f"Failed to scan Parquet database: {e}")
        return pd.DataFrame()

    # === CUSTOM STITCHING LOGIC FOR TMPV.NS ===
    if "TMPV.NS" in full_data.columns and os.path.exists("stitched_tmpv_history.csv"):
        print("Injecting stitched history for TMPV.NS...")
        try:
            stitched_df = pd.read_csv("stitched_tmpv_history.csv")
            stitched_df['Date'] = pd.to_datetime(stitched_df['Date'])
            stitched_df.set_index('Date', inplace=True)
            stitched_series = stitched_df['Close']
            
            # Reindex full_data to include older dates from stitched series if needed
            full_data = full_data.reindex(full_data.index.union(stitched_series.index))
            
            # Combine stitched data with live data (live data takes precedence)
            full_data['TMPV.NS'] = full_data['TMPV.NS'].combine_first(stitched_series)
            
            # Sort again after union
            full_data.sort_index(inplace=True)
            print(f"Injected {len(stitched_series)} rows for TMPV.NS")
            
        except Exception as e:
            print(f"Error injecting stitched TMPV data: {e}")

    # === CUSTOM STITCHING LOGIC FOR KWIL.NS ===
    if "KWIL.NS" in full_data.columns and os.path.exists("stitched_kwil_history.csv"):
        print("Injecting stitched history for KWIL.NS...")
        try:
            stitched_df = pd.read_csv("stitched_kwil_history.csv")
            stitched_df['Date'] = pd.to_datetime(stitched_df['Date'])
            stitched_df.set_index('Date', inplace=True)
            stitched_series = stitched_df['Close']
            
            full_data = full_data.reindex(full_data.index.union(stitched_series.index))
            full_data['KWIL.NS'] = full_data['KWIL.NS'].combine_first(stitched_series)
            
            full_data.sort_index(inplace=True)
            print(f"Injected {len(stitched_series)} rows for KWIL.NS")
            
        except Exception as e:
            print(f"Error injecting stitched KWIL data: {e}")
    # ==========================================

    # === CUSTOM STITCHING LOGIC FOR LTIM.NS (formerly LTI.NS) ===
    if "LTIM.NS" in full_data.columns and os.path.exists("stitched_ltm_history.csv"):
        print("Injecting stitched history for LTIM.NS (from LTI.NS)...")
        try:
            # Check if CSV has the standard Date,Close header or the non-standard Price,Close header
            with open("stitched_ltm_history.csv", "r") as f:
                first_line = f.readline().strip()
                
            if first_line.startswith("Price"):
                # Handle non-standard format: Skip first 3 rows (Price, Ticker, Date)
                stitched_df = pd.read_csv("stitched_ltm_history.csv", skiprows=3, names=["Date", "Close"])
            else:
                stitched_df = pd.read_csv("stitched_ltm_history.csv")
                
            stitched_df['Date'] = pd.to_datetime(stitched_df['Date'])
            stitched_df.set_index('Date', inplace=True)
            stitched_series = stitched_df['Close']
            
            full_data = full_data.reindex(full_data.index.union(stitched_series.index))
            full_data['LTIM.NS'] = full_data['LTIM.NS'].combine_first(stitched_series)
            
            full_data.sort_index(inplace=True)
            print(f"Injected {len(stitched_series)} rows for LTIM.NS")
            
        except Exception as e:
            print(f"Error injecting stitched LTIM data: {e}")
    # ==========================================
    
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
    
    # Total valid stocks on that day (have SMA)
    total_valid = valid_universe.sum(axis=1)
    
    # Calculate Below
    below_count = total_valid - above_count
    
    # New Stocks (have price but no SMA)
    new_stock_universe = full_data.notna() & sma_200.isna()
    new_stock_count = new_stock_universe.sum(axis=1)
    
    # True Total that the dashboard will display (Above + Below + New Stock)
    total_trading = total_valid + new_stock_count
    
    # Percentage (only uses valid_universe since new stocks can't be above/below)
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
        'Total': total_trading,
        'Percentage': percentage,
        'Index_Close': index_close
    })
    
    # --- HOLIDAY FILTERING ---
    # Filter out holidays where only a handful of stocks have data.
    # A simple Total > 0 check fails when 1 stock has data (e.g., Jan 15 2026 → 100%).
    # Use 10% of peak constituent count as a dynamic threshold, but be lenient for small themes.
    peak_total = breadth_df['Total'].max()
    if peak_total <= 15:
        min_threshold = 1 # Small themes: as long as 1 stock is trading, it's a valid theme day
    else:
        min_threshold = max(5, peak_total * 0.1) # Large indices: 10% threshold
    breadth_df = breadth_df[breadth_df['Total'] >= min_threshold]
    
    # --- SMA WARM-UP TRIMMING ---
    # The 200-SMA needs ~150 trading days to produce valid values.
    # Until then, Above + Below = 0 with 0% breadth. Trim these rows so
    # charts start from the first date with meaningful SMA coverage.
    valid_sma_count = breadth_df['Above'] + breadth_df['Below']
    breadth_df = breadth_df[valid_sma_count > 0]
    
    
    # 6. Extract Latest Detailed Status (Above/Below)
    last_idx = -1
    if not full_data.empty:
        latest_prices = full_data.iloc[last_idx]
        latest_smas = sma_200.iloc[last_idx]
        
        above_list = []
        below_list = []
        new_stock_list = []
        
        for ticker in full_data.columns:
            price = latest_prices.get(ticker)
            sma = latest_smas.get(ticker)
            
            if pd.notna(price) and pd.notna(sma):
                if price > sma:
                    above_list.append(ticker)
                else:
                    below_list.append(ticker)
            elif pd.notna(price):
                # Stock has a valid price but no SMA yet (e.g., recent IPO/listing)
                new_stock_list.append(ticker)
    else:
        above_list, below_list, new_stock_list = [], [], []

    return breadth_df, above_list, below_list, new_stock_list

def calculate_constituent_performance(master_data, all_tickers, nifty_data):
    """
    Computes performance metrics (1d, 1w, 1m, 3m, 6m, 1y, 3y, 5y) and 5/10/20/50-Day RS 
    for all tickers in master_data.
    Returns: Dict[ticker, Dict[metric, float]]
    """
    periods = {
        "1D": 1,
        "1W": 7,
        "1M": 30,
        "3M": 90,
        "6M": 180,
        "1Y": 365,
        "3Y": 365 * 3,
        "5Y": 365 * 5
    }
    
    perf_dict = {}
    
    # Calculate Benchmark RS Baseline
    nifty_latest = 0
    nifty_5d = 0
    nifty_10d = 0
    nifty_20d = 0
    nifty_50d = 0
    if not nifty_data.empty:
        try:
            # Handle yfinance multi-index for ^NSEI
            if '^NSEI' in nifty_data.columns.get_level_values(0):
                nifty_clean = nifty_data['^NSEI']['Close'].dropna()
            else:
                nifty_clean = nifty_data['Close']['^NSEI'].dropna()
                
            if len(nifty_clean) >= 1:
                nifty_latest = nifty_clean.iloc[-1]
            if len(nifty_clean) >= 6:
                nifty_5d = nifty_clean.iloc[-6]
            if len(nifty_clean) >= 11:
                nifty_10d = nifty_clean.iloc[-11]
            if len(nifty_clean) >= 21:
                nifty_20d = nifty_clean.iloc[-21]
            if len(nifty_clean) >= 51:
                nifty_50d = nifty_clean.iloc[-51]
        except Exception as e:
            print(f"Warning: Failed to extract Nifty baseline for RS calculations: {e}")
                 
    for ticker in all_tickers:
        if ticker not in master_data.columns:
            continue
            
        ticker_series = master_data[ticker].dropna()
        if ticker_series.empty:
            continue
            
        latest_val = ticker_series.iloc[-1]
        current_date = ticker_series.index[-1]
        
        metrics = {}
        
        # Absolute Returns (Calendar Days)
        for p_name, days in periods.items():
            target_date = current_date - timedelta(days=days)
            mask = ticker_series.index <= target_date
            if mask.any():
                past_val = ticker_series[mask].iloc[-1]
                if past_val > 0:
                    metrics[p_name] = ((latest_val - past_val) / past_val) * 100
                else:
                    metrics[p_name] = None
            else:
                metrics[p_name] = None
                
        # RS (5D) against Nifty 50 (Trading Days)
        rs_5 = None
        if nifty_latest > 0 and nifty_5d > 0 and len(ticker_series) >= 6:
            t_past_val = ticker_series.iloc[-6]
            if t_past_val > 0:
                current_ratio = latest_val / nifty_latest
                past_ratio = t_past_val / nifty_5d
                rs_5 = ((current_ratio - past_ratio) / past_ratio) * 100
        metrics["RS (5D)"] = rs_5

        # RS (10D) against Nifty 50 (Trading Days)
        rs_10 = None
        if nifty_latest > 0 and nifty_10d > 0 and len(ticker_series) >= 11:
            t_past_val = ticker_series.iloc[-11]
            if t_past_val > 0:
                current_ratio = latest_val / nifty_latest
                past_ratio = t_past_val / nifty_10d
                rs_10 = ((current_ratio - past_ratio) / past_ratio) * 100
        metrics["RS (10D)"] = rs_10

        # RS (20D) against Nifty 50 (Trading Days)
        rs_20 = None
        if nifty_latest > 0 and nifty_20d > 0 and len(ticker_series) >= 21:
            t_past_val = ticker_series.iloc[-21]
            if t_past_val > 0:
                current_ratio = latest_val / nifty_latest
                past_ratio = t_past_val / nifty_20d
                rs_20 = ((current_ratio - past_ratio) / past_ratio) * 100
        metrics["RS (20D)"] = rs_20

        # RS (50D) against Nifty 50 (Trading Days)
        rs_50 = None
        if nifty_latest > 0 and nifty_50d > 0 and len(ticker_series) >= 51:
            t_past_val = ticker_series.iloc[-51]
            if t_past_val > 0:
                current_ratio = latest_val / nifty_latest
                past_ratio = t_past_val / nifty_50d
                rs_50 = ((current_ratio - past_ratio) / past_ratio) * 100
        metrics["RS (50D)"] = rs_50
        
        perf_dict[ticker] = metrics
        
    return perf_dict

def main():
    # 1. Define all tasks
    broad_indices = [
        ("Nifty 50", "market_breadth_nifty50.csv"),
        ("Nifty 500", "market_breadth_nifty500.csv"),
        ("Nifty Smallcap 250", "market_breadth_smallcap.csv"),
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
    
    master_data = fetch_historical_data(list(all_tickers), start_date="2014-01-01")
    
    if master_data.empty:
        print("CRITICAL: No data fetched at all. Exiting with Error.")
        sys.exit(1)

    print("Fetching benchmark data for RS calculations (^NSEI)...")
    nifty_data = yf.download("^NSEI", start="2023-01-01", group_by="ticker", threads=False)

    print("Pre-calculating constituent performance metrics (1D to 5Y)...")
    constituent_perf = calculate_constituent_performance(master_data, all_tickers, nifty_data)

    # Store detailed status for UI
    market_details = {}
    
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
                
            breadth_df, above_list, below_list, new_stock_list = calculate_breadth(subset_data)
            breadth_df.to_csv(filename)
            print(f"Saved {filename} ({name})")
            
            market_details[name] = {
                "above": above_list,
                "below": below_list,
                "new_stock": new_stock_list
            }
            
        except Exception as e:
            print(f"Failed to process {name}: {e}")

    # Save detailed status JSON
    try:
        with open("market_status_latest.json", "w") as f:
            json.dump(market_details, f, indent=4)
        print("Saved market_status_latest.json")
    except Exception as e:
        print(f"Failed to save JSON: {e}")

    # Save constituent performance JSON
    try:
        with open("constituent_performance_latest.json", "w") as f:
            json.dump(constituent_perf, f, indent=4)
        print("Saved constituent_performance_latest.json")
    except Exception as e:
        print(f"Failed to save performance JSON: {e}")

if __name__ == "__main__":
    main()
