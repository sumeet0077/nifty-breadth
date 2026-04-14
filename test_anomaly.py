import pandas as pd
import yfinance as yf

# Mock pivot_df
idx = pd.date_range('2026-02-01', '2026-03-05', freq='B')
df = pd.DataFrame(index=idx)
df['ANGELONE.NS'] = 2500
df.loc['2026-02-26':, 'ANGELONE.NS'] = 250
df['NORMAL.NS'] = 100

print("Before:")
print(df.tail(10))

# Anomaly detection & Fallback
import warnings
warnings.filterwarnings('ignore')

recent_df = df.tail(15) 
pct_returns = recent_df.pct_change()
split_anomalies = []

for col in pct_returns.columns:
    # Check if any single daily drop is worse than -60% (classic stock split / demerger signature)
    if (pct_returns[col] < -0.60).any():
        split_anomalies.append(col)

if split_anomalies:
    print(f"Self-Healing Triggered: Detected massive corporate action anomalies for {split_anomalies}")
    for ticker in split_anomalies:
        try:
            print(f"Fetching adjusted history for {ticker} from Yahoo Finance fallback...")
            # Fetch 5 years to be safe for all timeframe calculations (1D to 5Y)
            yf_data = yf.download(ticker, start=df.index[0], end=df.index[-1] + pd.Timedelta(days=1), progress=False, auto_adjust=True)
            if not yf_data.empty:
                if 'Close' in yf_data.columns.get_level_values(0):
                    clean_series = yf_data['Close'].iloc[:, 0]
                elif 'Close' in yf_data.columns:
                    clean_series = yf_data['Close']
                else:
                    continue
                    
                # Standardize index
                clean_series.index = clean_series.index.tz_localize(None)
                
                # Update our pivot_df
                # df.update will overwrite only the non-NaN values where the indices match
                df[ticker].update(clean_series)
        except Exception as e:
            print(f"Failed to substitute {ticker}: {e}")

print("\nAfter:")
print(df.tail(10))
