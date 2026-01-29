
import pandas as pd
import yfinance as yf
import numpy as np
import warnings
from fetch_breadth_data import get_index_tickers, fetch_historical_data

warnings.simplefilter(action='ignore', category=FutureWarning)

def backtest_nifty500_strategy():
    print("--- Starting Backtest: Nifty 500 Mean Reversion Strategy ---")
    print("Strategy: Buy Falling Knives (<200 SMA) when Market Breadth < 20%. Sell when Breadth > 80%.")
    print("Note: Uses CURRENT Nifty 500 constituents (Survivorship Bias applies).")

    # 1. Get Tickers
    tickers = get_index_tickers("Nifty 500")
    print(f"Tickers found: {len(tickers)}")
    
    # 2. Fetch Data
    # Fetch ample history to ensure 200 SMA is valid by 2015/2016
    start_date = "2010-01-01" 
    print("Fetching historical data (this may take 2-3 minutes)...")
    full_data = fetch_historical_data(tickers, start_date=start_date)
    
    if full_data.empty:
        print("Error: No data fetched.")
        return

    # 3. Calculate 200 SMA & Breadth
    print("Calculating Technicals...")
    sma_200 = full_data.rolling(window=200, min_periods=150).mean()
    
    # Boolean mask: Is Stock > SMA?
    is_above = full_data > sma_200
    
    # Breadth Calculation
    valid_universe = sma_200.notna() & full_data.notna()
    counts_above = (is_above & valid_universe).sum(axis=1)
    counts_total = valid_universe.sum(axis=1)
    
    breadth_pct = (counts_above / counts_total) * 100
    breadth_pct = breadth_pct.fillna(0)
    
    # 4. Identify Market Cycles
    # Regimes:
    # NEUTRAL: Breadth between 20 and 80
    # OVERSOLD_ZONE: Breadth < 20
    # OVERBOUGHT_ZONE: Breadth > 80
    
    # Logic:
    # We enter the trade ON THE DAY it crosses BELOW 20% (Entry Day).
    # We identify the basket of stocks that are < 200 SMA on that day.
    # We hold EXACTLY that basket.
    # We exit ON THE DAY it crosses ABOVE 80% (Exit Day).
    
    trades = []
    
    in_trade = False
    entry_date = None
    entry_basket = [] # List of tickers
    entry_prices = {} # Ticker -> Price
    
    dates = breadth_pct.index
    
    # Start loop after 200 SMA valid period (approx 1 year in)
    start_idx = 250 
    
    prices_df = full_data
    
    for i in range(start_idx, len(dates)):
        date = dates[i]
        pct = breadth_pct.iloc[i]
        prev_pct = breadth_pct.iloc[i-1]
        
        # BUY SIGNAL: Cross BELOW 20%
        if not in_trade and pct < 20 and prev_pct >= 20:
            in_trade = True
            entry_date = date
            
            # Select constituents
            # Universe: Valid stocks that are BELOW 200 SMA
            # Check status on this day
            day_sma = sma_200.iloc[i]
            day_price = prices_df.iloc[i]
            
            # Basket = Tickers where Price < SMA (and data exists)
            # Using the boolean dataframe `is_above` directly
            # ~is_above means Below (or NaN). Need to filter NaNs.
            
            basket = []
            prices = {}
            for t in tickers:
                if t not in prices_df.columns: continue
                # Must be valid
                if pd.isna(day_price[t]) or pd.isna(day_sma[t]): continue
                
                if day_price[t] < day_sma[t]:
                    basket.append(t)
                    prices[t] = day_price[t]
            
            entry_basket = basket
            entry_prices = prices
            
            print(f"Cycle Start: {date.date()} | Breadth: {pct:.2f}% | Basket Size: {len(basket)} stocks")

        # SELL SIGNAL: Cross ABOVE 80%
        elif in_trade and pct > 80 and prev_pct <= 80:
            in_trade = False
            exit_date = date
            
            # Calculate Returns
            # Portfolio Value = Sum of (Exit Price / Entry Price) * 1 unit of currency?
            # Or Equal Weight: Total Return = Average of Individual Returns
            
            day_price = prices_df.iloc[i]
            
            stock_returns = []
            valid_exits = 0
            
            for t in entry_basket:
                buy_px = entry_prices.get(t)
                sell_px = day_price.get(t)
                
                if pd.isna(sell_px):
                    # Stock might have delisted or paused.
                    # Assumption: Sell at last available price or 0? 
                    # Using last available price in dataset up to this date is safest approximation
                    # Or just skip? Skipping implies 0 loss which is biased.
                    # Let's count as -100%? No, too harsh for data gaps.
                    # Let's look up last valid price.
                    last_valid_idx = prices_df[t].loc[:date].last_valid_index()
                    if last_valid_idx:
                        sell_px = prices_df[t].loc[last_valid_idx]
                    else:
                        sell_px = 0 # Should not happen if we bought it
                
                if buy_px > 0:
                    ret = (sell_px - buy_px) / buy_px
                    stock_returns.append(ret)
                    valid_exits += 1
            
            avg_return = np.mean(stock_returns) if stock_returns else 0.0
            
            duration_days = (exit_date - entry_date).days
            
            trades.append({
                "Entry Date": entry_date.date(),
                "Exit Date": exit_date.date(),
                "Duration (Days)": duration_days,
                "Breadth Entry": f"{breadth_pct.loc[entry_date]:.1f}%",
                "Breadth Exit": f"{pct:.1f}%",
                "Stocks Bought": len(entry_basket),
                "Avg Return": f"{avg_return*100:.2f}%",
                "Total Profit (on 10k/stock)": f"₹{(10000 * len(entry_basket) * (1 + avg_return)) - (10000 * len(entry_basket)):,.0f}",
                "ROI Multiple": f"{1+avg_return:.2f}x"
            })
            
            print(f"Cycle End:   {date.date()} | Duration: {duration_days}d | Return: {avg_return*100:.2f}%")

    # Output Results
    print("\n--- Backtest Results ---")
    if not trades:
        print("No completed cycles found in the data period.")
    else:
        results_df = pd.DataFrame(trades)
        print(results_df.to_markdown(index=False))
        
        # Export to Desktop
        export_path = "/Users/sumeetdas/Desktop/backtest_results_Nifty500.csv"
        results_df.to_csv(export_path, index=False)
        print(f"\n[SUCCESS] Results exported to: {export_path}")

    # Summary
    if trades:
        avg_cycle_ret = np.mean([float(t['Avg Return'].strip('%')) for t in trades])
        print(f"\nAverage Return per Cycle: {avg_cycle_ret:.2f}%")

if __name__ == "__main__":
    backtest_nifty500_strategy()
