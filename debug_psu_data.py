
import yfinance as yf
import pandas as pd

tickers = ["SBIN.NS", "BANKBARODA.NS", "PNB.NS", "CANBK.NS", "INDIANB.NS", "UNIONBANK.NS", "IOB.NS"]
print(f"Fetching data for {len(tickers)} PSU Banks...")

data = yf.download(tickers, start="2026-01-29", end="2026-02-04", group_by='ticker', progress=False)

print("\n--- Close Prices ---")
close_df = pd.DataFrame()
for t in tickers:
    if t in data.columns.levels[0]:
        close_df[t] = data[t]['Close']
        
print(close_df)

print("\n--- Daily Returns (%) ---")
print(close_df.pct_change() * 100)
