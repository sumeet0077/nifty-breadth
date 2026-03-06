import pyarrow.parquet as pq
import pandas as pd

parquet_path = "/Users/sumeetdas/Antigravity_NSE_Data/nse_master_adjusted_2014_onwards.parquet"
df = pq.read_table(parquet_path, columns=['trade_date', 'symbol', 'close', 'adj_close']).to_pandas()

# Filter out dates before 5 years ago
end_date = df['trade_date'].max()
start_date = end_date - pd.DateOffset(years=5)

results = []

for symbol, group in df.groupby('symbol'):
    # Get closest price to 5 years ago
    past_mask = group['trade_date'] >= start_date
    if not past_mask.any(): continue
        
    start_row = group[past_mask].iloc[0]
    end_row = group.iloc[-1]
    
    # Calculate 5Y return using UNADJUSTED close (what Yahoo Charts show for demergers)
    raw_start = start_row['close']
    raw_end = end_row['close']
    if raw_start == 0: continue
    raw_return = ((raw_end - raw_start) / raw_start) * 100
    
    # Calculate 5Y return using ADJUSTED close (What the new Parquet system outputs)
    adj_start = start_row['adj_close']
    adj_end = end_row['adj_close']
    if adj_start == 0: continue
    adj_return = ((adj_end - adj_start) / adj_start) * 100
    
    diff = abs(adj_return - raw_return)
    
    # Only flag massive differences (divergence > 10% in final return metrics)
    if diff > 10.0:
        results.append({
            'symbol': symbol,
            'raw_return': raw_return,
            'adj_return': adj_return,
            'divergence': diff
        })

# Sort by largest divergence
results.sort(key=lambda x: x['divergence'], reverse=True)

print("Top Stocks with massive Historical Data Improvements (Parquet vs Raw Chart):")
print(f"{'Symbol':<15} | {'Old Raw Return (Chart)':<25} | {'New Parquet Return':<20} | {'Divergence'}")
print("-" * 80)

# Limit to top 20
for r in results[:20]:
    print(f"{r['symbol']:<15} | {r['raw_return']:>15.2f}% (Incorrect)   | {r['adj_return']:>14.2f}% | {r['divergence']:.2f}%")

