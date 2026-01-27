
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime, timedelta
from nifty_themes import THEMES
import os
from rrg_helper import RRGCalculator

# ---------------------------------------------------------
# PAGE CONFIGURATION
# ---------------------------------------------------------
st.set_page_config(
    page_title="Nifty 500 Market Breadth",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ---------------------------------------------------------
# CUSTOM STYLING (Dark Theme Optimization)
# ---------------------------------------------------------
st.markdown("""
<style>
    /* Global Background */
    .stApp {
        background-color: #0e1117;
    }
    
    /* Metrics Styling */
    div[data-testid="stMetric"] {
        background-color: #1f2937;
        padding: 15px;
        border-radius: 8px;
        border: 1px solid #374151;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    }
    div[data-testid="stMetricLabel"] {
        font-size: 0.9rem !important;
        color: #9ca3af !important;
    }
    div[data-testid="stMetricValue"] {
        font-size: 1.8rem !important;
        color: #f3f4f6 !important;
    }
    
    /* Headers */
    h1, h2, h3 {
        color: #f3f4f6;
        font-family: 'Inter', sans-serif;
    }
    
    /* Chart Container */
    .js-plotly-plot {
        border-radius: 8px;
        overflow: hidden;
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------
# DATA LOADING
# ---------------------------------------------------------
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_data_v2(file_path):
    try:
        # Load the CSV
        df = pd.read_csv(file_path)
        df['Date'] = pd.to_datetime(df['Date'])
        # Filter: Only show 2015 onwards
        df = df[df['Date'] >= "2015-01-01"]
        return df.sort_values('Date')
    except FileNotFoundError:
        return None

@st.cache_data(ttl=3600)
def get_performance_summary_v3(config_map):
    """Load all CSVs and calculate performance metrics for a heatmap + RS."""
    summary_data = []
    
    # Load Nifty 50 Baseline first
    baseline_file = config_map.get("Nifty 50", {}).get('file')
    baseline_df = None
    
    if baseline_file and os.path.exists(baseline_file):
        baseline_df = load_data_v2(baseline_file)
    
    # Pre-calculate Baseline values for RS (20 Days)
    nifty_latest_price = 0
    nifty_20d_price = 0
    
    if baseline_df is not None and not baseline_df.empty and 'Index_Close' in baseline_df.columns:
        latest = baseline_df.iloc[-1]
        nifty_latest_price = latest['Index_Close']
        target_date = latest['Date'] - timedelta(days=20)
        mask = baseline_df['Date'] <= target_date
        if mask.any():
            nifty_20d_price = baseline_df[mask].iloc[-1]['Index_Close']
            
    periods = {
        "1 Day": 1,
        "1 Week": 7,
        "1 Month": 30,
        "3 Months": 90,
        "6 Months": 180,
        "1 Year": 365,
        "3 Years": 365*3,
        "5 Years": 365*5
    }
    
    for name, config in config_map.items():
        file_path = config['file']
        if not os.path.exists(file_path):
            continue
            
        try:
            df = load_data_v2(file_path)
            if df is None or df.empty or 'Index_Close' not in df.columns:
                continue
                
            latest = df.iloc[-1]
            current_price = latest['Index_Close']
            current_date = latest['Date']
            
            row = {"Theme/Index": name}
            
            # Standard Periods
            for p_name, days in periods.items():
                target_date = current_date - timedelta(days=days)
                mask = df['Date'] <= target_date
                if mask.any():
                    past_row = df[mask].iloc[-1]
                    past_price = past_row['Index_Close']
                    if past_price > 0:
                        ret = ((current_price - past_price) / past_price) * 100
                        row[p_name] = ret
                    else:
                        row[p_name] = None
                else:
                    row[p_name] = None
            
            # RS Calculation
            rs_val = None
            if nifty_latest_price > 0 and nifty_20d_price > 0:
                t_target = current_date - timedelta(days=20)
                t_mask = df['Date'] <= t_target
                if t_mask.any():
                    t_past_price = df[t_mask].iloc[-1]['Index_Close']
                    if t_past_price > 0:
                        current_ratio = current_price / nifty_latest_price
                        past_ratio = t_past_price / nifty_20d_price
                        rs_val = ((current_ratio - past_ratio) / past_ratio) * 100
            
            row["RS (20D)"] = rs_val
            
            summary_data.append(row)
        except Exception:
            continue
            
    return pd.DataFrame(summary_data)

st.sidebar.title("Configuration")

# Config Map
index_config = {
    "Nifty 50": {"file": "market_breadth_nifty50.csv", "title": "Nifty 50", "description": "Top 50 Blue-chip Companies"},
    "Nifty 500": {"file": "market_breadth_nifty500.csv", "title": "Nifty 500", "description": "Top 500 Companies"},
    "Nifty Smallcap 500": {"file": "market_breadth_smallcap.csv", "title": "Nifty Smallcap 250", "description": "Smallcap Segment"},
    "NIFTY AUTO": {"file": "breadth_auto.csv", "title": "Nifty Auto", "description": "Automobile Sector"},
    "NIFTY BANK": {"file": "breadth_bank.csv", "title": "Nifty Bank", "description": "Banking Sector"},
    "NIFTY FINANCIAL SERVICES": {"file": "breadth_finance.csv", "title": "Nifty Financial Services", "description": "Financial Services (Banks, NBFCs, Insurance)"},
    "NIFTY FMCG": {"file": "breadth_fmcg.csv", "title": "Nifty FMCG", "description": "Fast Moving Consumer Goods"},
    "NIFTY HEALTHCARE": {"file": "breadth_healthcare.csv", "title": "Nifty Healthcare", "description": "Healthcare & Hospitals"},
    "NIFTY IT": {"file": "breadth_it.csv", "title": "Nifty IT", "description": "Information Technology"},
    "NIFTY MEDIA": {"file": "breadth_media.csv", "title": "Nifty Media", "description": "Media & Entertainment"},
    "NIFTY METAL": {"file": "breadth_metal.csv", "title": "Nifty Metal", "description": "Metals & Mining"},
    "NIFTY PHARMA": {"file": "breadth_pharma.csv", "title": "Nifty Pharma", "description": "Pharmaceuticals"},
    "NIFTY PRIVATE BANK": {"file": "breadth_pvtbank.csv", "title": "Nifty Private Bank", "description": "Private Sector Banks"},
    "NIFTY PSU BANK": {"file": "breadth_psubank.csv", "title": "Nifty PSU Bank", "description": "Public Sector Banks"},
    "NIFTY REALTY": {"file": "breadth_realty.csv", "title": "Nifty Realty", "description": "Real Estate"},
    "NIFTY CONSUMER DURABLES": {"file": "breadth_consumer.csv", "title": "Nifty Consumer Durables", "description": "Consumer Durables"},
    "NIFTY OIL AND GAS": {"file": "breadth_oilgas.csv", "title": "Nifty Oil & Gas", "description": "Oil, Gas & Petroleum"}
}

for theme_name in THEMES:
    safe_name = theme_name.lower().replace(" ", "_").replace("&", "and").replace("-", "_").replace("(", "").replace(")", "").replace("__", "_")
    filename = f"breadth_theme_{safe_name}.csv"
    index_config[theme_name] = {
        "file": filename,
        "title": theme_name,
        "description": f"Custom Theme: {theme_name}"
    }

# Category Selection
category = st.sidebar.radio(
    "Market Segment",
    ["Broad Market", "Sectoral Indices", "Industries", "Performance Overview", "Sector Rotation (RRG)"],
    index=0
)

# ---------------------------------------------------------
# VIEW LOGIC
# ---------------------------------------------------------

if category == "Sector Rotation (RRG)":
    st.title("Relative Rotation Graph (RRG)")
    st.markdown("*Cycle analysis of themes vs Nifty 50*")
    
    col1, col2 = st.columns(2)
    with col1:
        timeframe = st.selectbox("Timeframe", ["Daily", "Weekly", "Monthly"], index=1)
    with col2:
        tail = st.slider("Tail Length (Periods)", 1, 12, 5)
        
    # Multiselect for filtering with Bulk Actions
    # EXCLUDE Broad Market indices as per user request
    theme_keys = [k for k in index_config.keys() if "Nifty" not in k and "NIFTY" not in k]
    rrg_keys = sorted([k for k in THEMES.keys() if k in index_config])
    
    # Initialize Session State
    if 'rrg_multiselect' not in st.session_state or not st.session_state['rrg_multiselect']:
        st.session_state['rrg_multiselect'] = rrg_keys 
        
    b1, b2, b3 = st.columns([1, 1, 5])
    if b1.button("Select All", type="secondary"):
        st.session_state['rrg_multiselect'] = rrg_keys
        st.rerun()
    if b2.button("Deselect All", type="secondary"):
        st.session_state['rrg_multiselect'] = []
        st.rerun()
    
    selected_rrg_themes = st.multiselect("Select Themes to Display", rrg_keys, key='rrg_multiselect')
    
    # Quadrant Filter (Checkboxes)
    st.write(" **Filter by Phase:**")
    q1, q2, q3, q4 = st.columns(4)
    show_leading = q1.checkbox("Leading", value=True)
    show_weakening = q2.checkbox("Weakening", value=True)
    show_lagging = q3.checkbox("Lagging", value=True)
    show_improving = q4.checkbox("Improving", value=True)
    
    selected_quadrants = []
    if show_leading: selected_quadrants.append("Leading")
    if show_weakening: selected_quadrants.append("Weakening")
    if show_lagging: selected_quadrants.append("Lagging")
    if show_improving: selected_quadrants.append("Improving")
        
    tf_map = {"Daily": "D", "Weekly": "W", "Monthly": "M"}
    
    if not selected_rrg_themes:
        st.warning("Please select at least one theme to display.")
    else:
        with st.spinner("Calculating RRG Metrics..."):
            baseline_file = index_config["Nifty 50"]["file"]
            baseline_df = load_data_v2(baseline_file)
            
            if baseline_df is None:
                st.error("Nifty 50 data missing for baseline.")
            else:
                calculator = RRGCalculator(baseline_df)
                data_dict = {}
                
                # Load selected themes
                for name in selected_rrg_themes:
                    config = index_config.get(name)
                    if config:
                        fpath = config['file']
                        if os.path.exists(fpath):
                            d = load_data_v2(fpath)
                            if d is not None and not d.empty:
                                data_dict[name] = d
                
                if not data_dict:
                    st.error("No data loaded for selected themes. Please check data files.")
                else:
                    rrg_df = calculator.calculate_rrg_metrics(
                        data_dict, 
                        timeframe=tf_map[timeframe], 
                        tail_length=tail
                    )
                    
                    if rrg_df.empty:
                        st.error("RRG Calculation returned no data. Check invalid dates or insufficient history.")
                        st.stop()

                    # Filter by Quadrant
                    # We need to check the LAST point for each ticker to determine current quadrant
                    last_points = rrg_df.sort_values('Date').groupby('Ticker').last()
                    
                    def get_quadrant(row):
                        r = row['RS_Ratio']
                        m = row['RS_Momentum']
                        if r > 100 and m > 100: return "Leading"
                        if r > 100 and m < 100: return "Weakening"
                        if r < 100 and m < 100: return "Lagging"
                        if r < 100 and m > 100: return "Improving"
                        return "Unknown"
                        
                    last_points['Quadrant'] = last_points.apply(get_quadrant, axis=1)
                    ticker_quadrant_map = last_points['Quadrant'].to_dict()
                    
                    # Filter RRG DF
                    valid_tickers = [t for t, q in ticker_quadrant_map.items() if q in selected_quadrants]
                    rrg_view = rrg_df[rrg_df['Ticker'].isin(valid_tickers)].groupby('Ticker').tail(tail)
                    
                    if rrg_view.empty:
                        st.warning("No themes match the selected quadrants.")
                    else:
                        min_x = rrg_view['RS_Ratio'].min()
                        max_x = rrg_view['RS_Ratio'].max()
                        min_y = rrg_view['RS_Momentum'].min()
                        max_y = rrg_view['RS_Momentum'].max()
                        
                        pad = 1.5
                        x_range = [min_x - pad, max_x + pad]
                        y_range = [min_y - pad, max_y + pad]
                        
                        fig = go.Figure()
                        
                        # Background Quadrants - Use very large coordinates to cover dynamic range
                        rect_max = 1000 
                        rect_min = -1000
                        
                        fig.add_shape(type="rect", x0=100, y0=100, x1=rect_max, y1=rect_max, fillcolor="rgba(34, 197, 94, 0.05)", line_width=0, layer="below")
                        fig.add_shape(type="rect", x0=100, y0=rect_min, x1=rect_max, y1=100, fillcolor="rgba(234, 179, 8, 0.05)", line_width=0, layer="below")
                        fig.add_shape(type="rect", x0=rect_min, y0=rect_min, x1=100, y1=100, fillcolor="rgba(239, 68, 68, 0.05)", line_width=0, layer="below")
                        fig.add_shape(type="rect", x0=rect_min, y0=100, x1=100, y1=rect_max, fillcolor="rgba(59, 130, 246, 0.05)", line_width=0, layer="below")
                        
                        unique_tickers = rrg_view['Ticker'].unique()
                        colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf', '#ffffff', '#00ff00', '#ffff00', '#0000ff', '#ff00ff']
                        
                        for i, ticker in enumerate(unique_tickers):
                            t_data = rrg_view[rrg_view['Ticker'] == ticker]
                            color = colors[i % len(colors)]
                            
                            # Main Trail
                            fig.add_trace(go.Scatter(
                                x=t_data['RS_Ratio'],
                                y=t_data['RS_Momentum'],
                                mode='lines+markers',
                                name=ticker,
                                marker=dict(size=4, symbol="circle", color=color, opacity=0.7),
                                line=dict(width=2, color=color),
                                hovertemplate=f"<b>{ticker}</b><br>Ratio: %{{x:.2f}}<br>Mom: %{{y:.2f}}<extra></extra>"
                            ))
                            
                            # Arrowhead
                            if len(t_data) >= 2:
                                head = t_data.iloc[-1]
                                prev = t_data.iloc[-2]
                                dx = head['RS_Ratio'] - prev['RS_Ratio']
                                dy = head['RS_Momentum'] - prev['RS_Momentum']
                                angle = np.degrees(np.arctan2(dy, dx)) - 90
                                
                                fig.add_trace(go.Scatter(
                                    x=[head['RS_Ratio']],
                                    y=[head['RS_Momentum']],
                                    mode='markers+text',
                                    name=ticker,
                                    text=[ticker],
                                    textposition="top center",
                                    textfont=dict(color=color, size=12, weight="bold"),
                                    marker=dict(symbol="triangle-up", size=12, color=color, angle=angle, line=dict(width=1, color='white')),
                                    hoverinfo='skip',
                                    showlegend=False
                                ))
                            else:
                                head = t_data.iloc[-1]
                                fig.add_trace(go.Scatter(
                                    x=[head['RS_Ratio']],
                                    y=[head['RS_Momentum']],
                                    mode='markers+text',
                                    text=[ticker],
                                    marker=dict(symbol="circle", size=10, color=color),
                                    showlegend=False
                                ))

                        # Watermarks (Refined: Smaller, greater transparency)
                        wm_color = "rgba(128,128,128,0.15)" # Subtle gray or keep colored but very faint? User said "more transparent"
                        # Actually user said "names of quadrants... bring it to extreme corners and make it 70% transparent"
                        # I used colored text before. I will keep colored but lower opacity (0.15) and smaller size (25).
                        
                        # Top-Right (Leading)
                        fig.add_annotation(xref="paper", yref="paper", x=0.98, y=0.98, text="LEADING", showarrow=False, font=dict(color="rgba(34, 197, 94, 0.15)", size=30, weight="bold"), xanchor="right", yanchor="top")
                        # Bottom-Right (Weakening)
                        fig.add_annotation(xref="paper", yref="paper", x=0.98, y=0.02, text="WEAKENING", showarrow=False, font=dict(color="rgba(234, 179, 8, 0.15)", size=30, weight="bold"), xanchor="right", yanchor="bottom")
                        # Bottom-Left (Lagging)
                        fig.add_annotation(xref="paper", yref="paper", x=0.02, y=0.02, text="LAGGING", showarrow=False, font=dict(color="rgba(239, 68, 68, 0.15)", size=30, weight="bold"), xanchor="left", yanchor="bottom")
                        # Top-Left (Improving)
                        fig.add_annotation(xref="paper", yref="paper", x=0.02, y=0.98, text="IMPROVING", showarrow=False, font=dict(color="rgba(59, 130, 246, 0.15)", size=30, weight="bold"), xanchor="left", yanchor="top")

                        fig.update_layout(
                            title=f"Sector Rotation (vs Nifty 50) - {timeframe}",
                            xaxis_title="RS-Ratio (Trend)",
                            yaxis_title="RS-Momentum (ROC)",
                            xaxis=dict(range=x_range, zeroline=True, zerolinecolor="gray", zerolinewidth=1), 
                            yaxis=dict(range=y_range, zeroline=True, zerolinecolor="gray", zerolinewidth=1),
                            template="plotly_dark",
                            height=850,
                            showlegend=False
                        )
                        st.plotly_chart(fig, use_container_width=True)

elif category == "Performance Overview":
    st.title("Market Performance Heatmap")
    st.markdown("*Comparative returns of all sectors and themes based on Equal-Weighted Index*")
    
    with st.spinner("Calculating performance across all themes..."):
        perf_summary = get_performance_summary_v3(index_config)
    
    if not perf_summary.empty:
        if "1 Year" in perf_summary.columns:
            perf_summary = perf_summary.sort_values("1 Year", ascending=False)
            
        def color_return(val):
            if pd.isna(val): return ""
            color = '#22c55e' if val >= 0 else '#ef4444' 
            return f'color: {color}; font-weight: bold;'
            
        st.dataframe(
            perf_summary.style.map(color_return, subset=perf_summary.columns[1:]).format("{:.2f}%", subset=perf_summary.columns[1:]),
            height=800,
            use_container_width=True,
            hide_index=True
        )

else:
    # Single Index View logic
    selected_index = None
    if category == "Broad Market":
        selected_index = st.sidebar.radio("Select Index", ["Nifty 50", "Nifty 500", "Nifty Smallcap 500"])
    elif category == "Sectoral Indices":
        sector_options = [
            "NIFTY AUTO", "NIFTY BANK", "NIFTY FINANCIAL SERVICES", "NIFTY FMCG",
            "NIFTY HEALTHCARE", "NIFTY IT", "NIFTY MEDIA", "NIFTY METAL",
            "NIFTY PHARMA", "NIFTY PRIVATE BANK", "NIFTY PSU BANK", 
            "NIFTY REALTY", "NIFTY CONSUMER DURABLES", "NIFTY OIL AND GAS"
        ]
        selected_index = st.sidebar.radio("Select Sector", sector_options)
    elif category == "Industries":
        industry_options = sorted(THEMES.keys())
        selected_index = st.sidebar.radio("Select Industry", industry_options)

    current_config = index_config.get(selected_index, index_config["Nifty 50"])

    st.title(f"{current_config['title']} Market Breadth")
    st.markdown(f"*{current_config['description']}*")

    df = load_data_v2(current_config['file'])

    if df is not None:
        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else latest
        
        col1, col2, col3, col4 = st.columns(4)
        with col1: st.metric("Total Stocks", int(latest['Total']))
        with col2: st.metric("Stocks > 200 SMA", int(latest['Above']), delta=int(latest['Above'] - prev['Above']))
        with col3: st.metric("Stocks < 200 SMA", int(latest['Below']), delta=int(latest['Below'] - prev['Below']), delta_color="inverse")
        with col4: st.metric("Breadth (%)", f"{latest['Percentage']:.2f}%", delta=f"{latest['Percentage'] - prev['Percentage']:.2f}%")

        if 'Index_Close' in df.columns:
            st.subheader("Performance Trend (Equal Weighted)")
            periods = {"1 Day": 1, "1 Week": 7, "1 Month": 30, "3 Months": 90, "6 Months": 180, "1 Year": 365, "3 Years": 365*3, "5 Years": 365*5}
            metrics = {}
            current_price = latest['Index_Close']
            for name, days in periods.items():
                target_date = latest['Date'] - timedelta(days=days)
                mask = df['Date'] <= target_date
                if mask.any():
                    past_row = df[mask].iloc[-1]
                    if past_row['Index_Close'] > 0:
                        ret = ((current_price - past_row['Index_Close']) / past_row['Index_Close']) * 100
                        metrics[name] = ret
                    else: metrics[name] = None
                else: metrics[name] = None
            perf_df = pd.DataFrame([metrics])
            def color_ret(val):
                if pd.isna(val): return ""
                return f'color: {"#22c55e" if val >= 0 else "#ef4444"}; font-weight: bold'
            st.dataframe(perf_df.style.map(color_ret).format("{:.2f}%"), use_container_width=True, hide_index=True)

        tab1, tab2 = st.tabs(["Breadth Chart", "Constituents"])
        with tab1:
            fig_pct = go.Figure()
            fig_pct.add_trace(go.Scatter(x=df['Date'], y=df['Percentage'], mode='lines', name='% Above 200 SMA', line=dict(color='#3b82f6', width=2), hovertemplate='%{y:.2f}%<extra></extra>'))
            fig_pct.add_hrect(y0=80, y1=100, fillcolor="green", opacity=0.1, layer="below", line_width=0)
            fig_pct.add_hrect(y0=0, y1=20, fillcolor="red", opacity=0.1, layer="below", line_width=0)
            fig_pct.add_hline(y=50, line_dash="dash", line_color="gray", annotation_text="Neutral (50%)")
            fig_pct.update_layout(title="Percentage of Stocks Above 200-Day SMA", yaxis_title="Percentage (%)", xaxis_title="Date", template="plotly_dark", height=500, yaxis=dict(range=[0, 100]), hovermode="x unified")
            st.plotly_chart(fig_pct, use_container_width=True)

            fig_count = go.Figure()
            fig_count.add_trace(go.Scatter(x=df['Date'], y=df['Above'], mode='lines', name='Above', stackgroup='one', line=dict(width=0), fillcolor='rgba(34, 197, 94, 0.6)'))
            fig_count.add_trace(go.Scatter(x=df['Date'], y=df['Below'], mode='lines', name='Below', stackgroup='one', line=dict(width=0), fillcolor='rgba(239, 68, 68, 0.6)'))
            fig_count.update_layout(title="Market Participation", yaxis_title="Stocks", xaxis_title="Date", template="plotly_dark", height=400, hovermode="x unified")
            st.plotly_chart(fig_count, use_container_width=True)

        with tab2:
            st.subheader(f"Constituents of {current_config['title']}")
            if current_config['title'] in THEMES:
                tickers = THEMES[current_config['title']]
                st.write(f"**Total Stocks:** {len(tickers)}")
                st.dataframe(pd.DataFrame(tickers, columns=["Ticker Symbol"]), use_container_width=True, hide_index=True)
            else: st.info("Constituent list available only for Custom Industries.")
    else: st.error(f"Data file not found: {current_config['file']}")
