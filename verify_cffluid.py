import yfinance as yf
import pyarrow.parquet as pq
import pandas as pd

symbols = ['CFFLUID']
parquet_path = "/Users/sumeetdas/Antigravity_NSE_Data/nse_master_adjusted_2014_onwards.parquet"
df = pq.read_table(parquet_path, columns=['trade_date', 'symbol', 'adj_close']).to_pandas()

end_date = df['trade_date'].max()
start_date = end_date - pd.DateOffset(years=5)

for symbol in symbols:
    group = df[df['symbol'] == symbol]
    past_mask = group['trade_date'] >= start_date
    if not past_mask.any(): continue
        
    start_row = group[past_mask].iloc[0]
    end_row = group.iloc[-1]
    
    pq_start = start_row['adj_close']
    pq_end = end_row['adj_close']
    pq_return = ((pq_end - pq_start) / pq_start) * 100
    
    yf_sym = f"{symbol}.NS"
    yf_data = yf.download(yf_sym, start=start_row['trade_date'], end=end_row['trade_date'] + pd.Timedelta(days=1), progress=False, auto_adjust=True)
    
    if yf_sym in yf_data.columns.get_level_values(0):
        yf_prices = yf_data[yf_sym]['Close']
    elif 'Close' in yf_data.columns.get_level_values(0):
        yf_prices = yf_data['Close'].iloc[:, 0]
    elif 'Close' in yf_data.columns:
        yf_prices = yf_data['Close']
    else:
        yf_prices = pd.Series()
        
    if not yf_prices.empty:
        yf_start = yf_prices.iloc[0]
        yf_end = yf_prices.iloc[-1]
        yf_return = ((yf_end - yf_start) / yf_start) * 100
        yf_return_str = f"{yf_return:.2f}%"
    else:
        yf_return_str = "Error"
        
    print(f"{symbol:<12} | {pq_return:>14.2f}% | {yf_return_str:>14} | ")

