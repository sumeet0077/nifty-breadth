
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
from nifty_themes import THEMES
import os

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
def get_performance_summary_v2(config_map):
    """Load all CSVs and calculate performance metrics for a heatmap."""
    summary_data = []
    
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
            # Optimize: Read only necessary columns if possible, but load_data does filtering
            df = load_data_v2(file_path)
            if df is None or df.empty or 'Index_Close' not in df.columns:
                continue
                
            latest = df.iloc[-1]
            current_price = latest['Index_Close']
            current_date = latest['Date']
            
            row = {"Theme/Index": name}
            
            for p_name, days in periods.items():
                target_date = current_date - timedelta(days=days)
                # Find closest date strictly before or equal
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
            
            summary_data.append(row)
        except Exception:
            continue
            
    return pd.DataFrame(summary_data)

st.sidebar.title("Configuration")

# Build full config map first to use in all modes
index_config = {
    # Broad
    "Nifty 50": {"file": "market_breadth_nifty50.csv", "title": "Nifty 50", "description": "Top 50 Blue-chip Companies"},
    "Nifty 500": {"file": "market_breadth_nifty500.csv", "title": "Nifty 500", "description": "Top 500 Companies"},
    "Nifty Smallcap 500": {"file": "market_breadth_smallcap.csv", "title": "Nifty Smallcap 250", "description": "Smallcap Segment"},
    
    # Sectors
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

# Dynamically add Themes to config
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
    ["Broad Market", "Sectoral Indices", "Industries", "Performance Overview"],
    index=0
)

# Logic for View
if category == "Performance Overview":
    st.title("Market Performance Heatmap")
    st.markdown("*Comparative returns of all sectors and themes based on Equal-Weighted Index*")
    
    with st.spinner("Calculating performance across all themes..."):
        perf_summary = get_performance_summary_v2(index_config)
    
    if not perf_summary.empty:
        # Sort by 1 Year return by default if available
        if "1 Year" in perf_summary.columns:
            perf_summary = perf_summary.sort_values("1 Year", ascending=False)
            
        # Formatting
        def color_return(val):
            if pd.isna(val):
                return ""
            color = '#22c55e' if val >= 0 else '#ef4444' # Green-500, Red-500
            return f'color: {color}; font-weight: bold;'
            
        st.dataframe(
            perf_summary.style.map(color_return, subset=perf_summary.columns[1:]).format("{:.2f}%", subset=perf_summary.columns[1:]),
            height=800,
            use_container_width=True,
            hide_index=True
        )
    else:
        st.error("No performance data available. Please run data fetcher.")

else:
    # Index Selection based on Category
    selected_index = None

    if category == "Broad Market":
        selected_index = st.sidebar.radio(
            "Select Index",
            ["Nifty 50", "Nifty 500", "Nifty Smallcap 500"]
        )
    elif category == "Sectoral Indices":
        sector_options = [
            "NIFTY AUTO", "NIFTY BANK", "NIFTY FINANCIAL SERVICES", "NIFTY FMCG",
            "NIFTY HEALTHCARE", "NIFTY IT", "NIFTY MEDIA", "NIFTY METAL",
            "NIFTY PHARMA", "NIFTY PRIVATE BANK", "NIFTY PSU BANK", 
            "NIFTY REALTY", "NIFTY CONSUMER DURABLES", "NIFTY OIL AND GAS"
        ]
        selected_index = st.sidebar.radio("Select Sector", sector_options)
    elif category == "Industries":
        # Sort themes alphabetically
        industry_options = sorted(THEMES.keys())
        selected_index = st.sidebar.radio("Select Industry", industry_options)

    current_config = index_config.get(selected_index, index_config["Nifty 50"])

    # ---------------------------------------------------------
    # MAIN LAYOUT (Single Index View)
    # ---------------------------------------------------------
    st.title(f"{current_config['title']} Market Breadth")
    st.markdown(f"*{current_config['description']}*")

    df = load_data_v2(current_config['file'])

    if df is not None:
        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else latest
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Stocks", int(latest['Total']))
        
        with col2:
            st.metric(
                "Stocks > 200 SMA", 
                int(latest['Above']),
                delta=int(latest['Above'] - prev['Above'])
            )
            
        with col3:
            st.metric(
                "Stocks < 200 SMA", 
                int(latest['Below']),
                delta=int(latest['Below'] - prev['Below']),
                delta_color="inverse"
            )
            
        with col4:
            st.metric(
                "Breadth (%)", 
                f"{latest['Percentage']:.2f}%",
                delta=f"{latest['Percentage'] - prev['Percentage']:.2f}%"
            )

        # Performance Table for Single Index
        if 'Index_Close' in df.columns:
            st.subheader("Performance Trend (Equal Weighted)")
            
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

        # Tabs
        tab1, tab2 = st.tabs(["Breadth Chart", "Constituents"])
        
        with tab1:
            # Chart 1: Percentage Above 200 SMA
            fig_pct = go.Figure()

            fig_pct.add_trace(go.Scatter(
                x=df['Date'], 
                y=df['Percentage'],
                mode='lines',
                name='% Above 200 SMA',
                line=dict(color='#3b82f6', width=2),
                hovertemplate='%{y:.2f}%<extra></extra>'
            ))
            
            # Highlighted Bands (Restore requested bands)
            # 80-100% (Green - Overbought/Strong)
            fig_pct.add_hrect(
                y0=80, y1=100,
                fillcolor="green", opacity=0.1,
                layer="below", line_width=0,
            )
            
            # 0-20% (Red - Oversold/Weak)
            fig_pct.add_hrect(
                y0=0, y1=20,
                fillcolor="red", opacity=0.1,
                layer="below", line_width=0,
            )

            # Add 50% reference line
            fig_pct.add_hline(y=50, line_dash="dash", line_color="gray", annotation_text="Neutral (50%)")

            fig_pct.update_layout(
                title="Percentage of Stocks Above 200-Day SMA",
                yaxis_title="Percentage (%)",
                xaxis_title="Date",
                template="plotly_dark",
                height=500,
                yaxis=dict(range=[0, 100]),
                hovermode="x unified"
            )

            st.plotly_chart(fig_pct, use_container_width=True)

            # Chart 2: Stacked Area
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
            else:
                st.info("Constituent list available only for Custom Industries.")

    else:
        st.error(f"Data file not found: {current_config['file']}")
