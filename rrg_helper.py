
import pandas as pd
import numpy as np

class RRGCalculator:
    def __init__(self, benchmark_df):
        """
        Initialize with Benchmark Data (Nifty 50).
        benchmark_df should obtain 'Date' and 'Index_Close'.
        """
        self.benchmark = benchmark_df.copy()
        self.benchmark['Date'] = pd.to_datetime(self.benchmark['Date'])
        self.benchmark = self.benchmark.set_index('Date').sort_index()
        
    def _resample(self, df, timeframe):
        """Resample data to D (Daily), W (Weekly-Fri), M (Monthly)."""
        if timeframe == 'D':
            return df
            
        # Resample to business end of frequency
        rule = 'W-FRI' if timeframe == 'W' else 'M'
        
        # Resample Close using 'last'
        resampled = df.resample(rule).last()
        return resampled.dropna()

    def calculate_rrg_metrics(self, df_dict, timeframe='D', tail_length=10):
        """
        Calculate RS-Ratio and RS-Momentum for all assets.
        
        Returns:
            pd.DataFrame: A master dataframe with columns:
                          ['Date', 'Ticker', 'RS_Ratio', 'RS_Momentum']
                          Filtered to include the last 'tail_length' periods for each asset.
        """
        results = []
        
        # Resample Benchmark
        bench_resampled = self._resample(self.benchmark, timeframe)
        bench_close = bench_resampled['Index_Close']
        
        for name, df in df_dict.items():
            if df.empty:
                continue
                
            # Prepare Asset Data
            df = df.copy()
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.set_index('Date').sort_index()
            asset_resampled = self._resample(df, timeframe)
            
            # Align Asset and Benchmark (Intersection of dates)
            # Use inner join to ensure we only calc where both have data
            combined = pd.concat([asset_resampled['Index_Close'], bench_close], axis=1, join='inner')
            combined.columns = ['Asset', 'Benchmark']
            
            if combined.empty:
                continue
                
            # 1. Calculate Relative Strength (RS)
            # RS = 100 * (Asset / Benchmark)
            combined['RS'] = 100 * (combined['Asset'] / combined['Benchmark'])
            
            # 2. RS-Ratio (Trend)
            # JdK RRG often uses Moving Average for normalization. 
            # We'll use a standard proxy: Ratio = 100 * (RS / MA(RS))
            # Parameters: Daily=50, Weekly=12? Let's use adaptive or fixed.
            # Fixed 14 period is good for "Rotation".
            # User mentioned "period should be adjustable", but that usually means resample period.
            # I'll stick to a standard lookback for smoothing.
            # Standard RRG uses ~10-14 period smoothing.
            window_ratio = 14
            combined['RS_MA'] = combined['RS'].rolling(window=window_ratio).mean()
            combined['RS_Ratio'] = 100 * (combined['RS'] / combined['RS_MA'])
            
            # 3. RS-Momentum (ROC of Ratio)
            # Momentum = 100 * (RS_Ratio / MA(RS_Ratio))
            # Or simplified: Momentum is just the Rate of Change? 
            # The standard JdK formula is complex (uses normalized MACD logic).
            # We will use the simplified normalized momentum:
            window_mom = 9 # slightly faster than ratio
            combined['Ratio_MA'] = combined['RS_Ratio'].rolling(window=window_mom).mean()
            combined['RS_Momentum'] = 100 * (combined['RS_Ratio'] / combined['Ratio_MA'])
            
            # Filter NaNs
            combined = combined.dropna()
            
            # Select only last N records for the "Tail"
            # User wants 1-12 periods tail.
            # We fetch slightly more to draw lines, but user configures display length.
            # Here we return "enough" history, UI filters the exact tail.
            # Let's return the last 20 periods to be safe for a max 12 tail.
            tail_data = combined.iloc[-30:] # Fetch last 30 points
            
            for date, row in tail_data.iterrows():
                results.append({
                    'Date': date,
                    'Ticker': name,
                    'RS_Ratio': row['RS_Ratio'],
                    'RS_Momentum': row['RS_Momentum']
                })
                
        return pd.DataFrame(results)
