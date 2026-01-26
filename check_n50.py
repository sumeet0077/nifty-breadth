
import pandas as pd
import yfinance as yf
import requests
import io

def check_n50():
    url = "https://nsearchives.nseindia.com/content/indices/ind_nifty50list.csv"
    s = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    tickers = [x + ".NS" for x in df['Symbol']]
    print(f"Nifty 50 Tickers ({len(tickers)}):")
    
    # Download 1y data to check validity
    data = yf.download(tickers, period="1y", group_by='ticker', progress=False, threads=True)
    
    missing = []
    insufficient = []
    
    for t in tickers:
        try:
            # Check availability
            if t not in data.columns.levels[0]:
                missing.append(t)
                continue
                
            # Check count
            valid_days = data[t]['Close'].notna().sum()
            if valid_days < 150:
                insufficient.append(f"{t} ({valid_days} days)")
        except:
            missing.append(t)
            
    print("\nMissing/Failed Download:")
    print(missing)
    print("\nInsufficient Data (<150 days):")
    print(insufficient)

if __name__ == "__main__":
    check_n50()
