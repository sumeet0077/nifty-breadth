
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime, timedelta, time
from nifty_themes import THEMES
import os
import json
import subprocess
from rrg_helper import RRGCalculator
from fetch_breadth_data import get_index_tickers
import urllib.parse

# ---------------------------------------------------------
# AUTO-UPDATE LOGIC
# ---------------------------------------------------------
def check_and_update_data():
    # Only run if not already updated in this session
    if 'data_updated' in st.session_state:
        return

    req_file = "market_breadth_nifty50.csv"
    if not os.path.exists(req_file):
        return

    try:
        # Robust Check: Look at the actual DATA, not the file timestamp
        # Read only the last few lines to avoid loading full file
        df = pd.read_csv(req_file) # Loading full file is fine (small < 1MB)
        if df.empty or 'Date' not in df.columns:
            return
            
        last_date_str = df.iloc[-1]['Date']
        last_date = pd.to_datetime(last_date_str).date()
        today = datetime.now().date()
        
        # Logic:
        # If data is already from Today, we are good.
        # If data is old, check if it's late enough to expect new data (6:15 PM)
        
        is_today_data = (last_date == today)
        now_time = datetime.now().time()
        
        # Target: 6:15 PM (18:15)
        update_threshold = time(18, 15)
        can_expect_new_data = now_time >= update_threshold
        
        # Optimization: If server is UTC (Streamlit Cloud), 6 PM IST is 12:30 PM UTC.
        # So 'today' might be different.
        # If last_date is '2026-02-01' and Server 'today' is '2026-02-01', we are good.
        # If Server 'today' matches, we don't update.
        
        if not is_today_data and can_expect_new_data:
            st.warning(f"Data is from {last_date}, checking for updates... (Time: {now_time.strftime('%H:%M')})")
            
            # Additional Check: Don't hammer if recent modification time implies we TRIED updating
            # mod_time = datetime.fromtimestamp(os.path.getmtime(req_file))
            # if (datetime.now() - mod_time).seconds < 300: return # Cooldown
            
            try:
                with st.spinner("Fetching latest market data..."):
                    subprocess.run(["python3", "fetch_breadth_data.py"], check=True)
                st.session_state['data_updated'] = True
                st.cache_data.clear()
                st.success("Data updated! Reloading...")
                st.rerun()
            except Exception as e:
                # If script fails (e.g. Memory Limit on Cloud), Log it but allow App to load old data
                st.error(f"Auto-update skipped (Resource Limit). Using available data. Details: {e}")
                st.session_state['data_updated'] = True # Prevent infinite retry loop
                
    except Exception as e:
        print(f"Update check error: {e}")

# Run check immediately
# Check moved after page_config

# ---------------------------------------------------------
# PAGE CONFIGURATION
# ---------------------------------------------------------
st.set_page_config(
    page_title="Nifty 500 Market Breadth",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Force Clear Cache on Deployment (Fix Missing Budget Data)
if 'cache_cleared_v_budget_fix' not in st.session_state:
    st.cache_data.clear()
    st.session_state['cache_cleared_v_budget_fix'] = True

st.sidebar.caption("App Version: Feb 20 - Performance Metrics & CAGR")

# Run check immediately (AFTER page config to avoid Streamlit error)
check_and_update_data()

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
# ---------------------------------------------------------
# DATA LOADING
# ---------------------------------------------------------
# REMOVING CACHE FOR DEBUGGING
@st.cache_data(ttl=3600)
def load_data_v2(file_path):
    try:
        # Load the CSV
        df = pd.read_csv(file_path)
        df['Date'] = pd.to_datetime(df['Date'])
        # Filter: Only show 2015 onwards
        df = df[df['Date'] >= "2015-01-01"]
        sorted_df = df.sort_values('Date')
        
        return sorted_df
    except FileNotFoundError:
        return None

@st.cache_data
def get_cached_constituents(index_name):
    """Cached wrapper for fetching index tickers (avoids frequent network calls for Nifty lists)."""
    return get_index_tickers(index_name)

@st.cache_data
def load_market_status():
    """Load the latest pre-computed market status details."""
    try:
        with open("market_status_latest.json", "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

@st.cache_data
def load_constituent_performance():
    """Load the latest pre-computed constituent performance metrics."""
    try:
        with open("constituent_performance_latest.json", "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def render_styled_dataframe(styler, height="600px"):
    """Renders a pandas Styler as raw HTML to completely bypass Streamlit's Glide Data Grid layout jumping on mobile."""
    import textwrap
    styler = styler.hide(axis="index")
    html_table = styler.to_html()
    
    css = textwrap.dedent(f"""
        <style>
        .custom-table-wrapper {{
            max-height: {height};
            overflow: auto;
            border-radius: 4px;
            border: 1px solid #333;
            margin-bottom: 1rem;
        }}
        .custom-table-wrapper table {{
            width: 100%;
            border-collapse: collapse;
            font-family: "Source Sans Pro", sans-serif;
            color: #fafafa;
            font-size: 14px;
            text-align: right;
        }}
        .custom-table-wrapper th {{
            background-color: #0e1117;
            position: sticky;
            top: 0;
            padding: 8px 12px;
            border-bottom: 1px solid #333;
            text-align: right;
            z-index: 1;
        }}
        .custom-table-wrapper th:first-child, .custom-table-wrapper td:first-child {{
            text-align: left;
        }}
        .custom-table-wrapper td {{
            padding: 8px 12px;
            border-bottom: 1px solid #222;
        }}
        .custom-table-wrapper tr:hover {{
            background-color: #262730;
        }}
        </style>
    """)
    
    final_html = css + f'<div class="custom-table-wrapper">{html_table}</div>'
    
    # st.html was introduced in st >= 1.34 to natively render raw HTML
    if hasattr(st, "html"):
        st.html(final_html)
    else:
        st.markdown(final_html, unsafe_allow_html=True)


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

# Add manual refresh button to clear cache
if st.sidebar.button("🔄 Refresh Data", help="Click if data seems stale"):
    st.cache_data.clear()
    st.success("Cache cleared! Reloading...")
    st.rerun()

# Config Map
index_config = {
    "Nifty 50": {"file": "market_breadth_nifty50.csv", "title": "Nifty 50", "description": "Top 50 Blue-chip Companies"},
    "Nifty 500": {"file": "market_breadth_nifty500.csv", "title": "Nifty 500", "description": "Top 500 Companies"},
    "Nifty Smallcap 250": {"file": "market_breadth_smallcap.csv", "title": "Nifty Smallcap 250", "description": "Smallcap Segment"},
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
    "NIFTY OIL AND GAS": {"file": "breadth_oilgas.csv", "title": "Nifty Oil & Gas", "description": "Oil, Gas & Petroleum"},
    "Solar Manufacturing": {"file": "breadth_theme_solar_manufacturing.csv", "title": "Solar Manufacturing", "description": "Solar Cells, Modules & EPC"}
}

for theme_name in THEMES:
    safe_name = theme_name.lower().replace(" ", "_").replace("&", "and").replace("-", "_").replace("(", "").replace(")", "").replace("__", "_")
    filename = f"breadth_theme_{safe_name}.csv"
    index_config[theme_name] = {
        "file": filename,
        "title": theme_name,
        "description": f"Custom Theme: {theme_name}"
    }

# Initialize Session State Navigation variables
if "nav_category" not in st.session_state:
    st.session_state.nav_category = "Broad Market"
if "nav_broad" not in st.session_state:
    st.session_state.nav_broad = "Nifty 50"
if "nav_sector" not in st.session_state:
    st.session_state.nav_sector = "NIFTY AUTO"
if "nav_industry" not in st.session_state:
    st.session_state.nav_industry = sorted(THEMES.keys())[0] if THEMES else None

# Define sector options globally so we can use them anywhere
SECTOR_OPTIONS = [
    "NIFTY AUTO", "NIFTY BANK", "NIFTY FINANCIAL SERVICES", "NIFTY FMCG",
    "NIFTY HEALTHCARE", "NIFTY IT", "NIFTY MEDIA", "NIFTY METAL",
    "NIFTY PHARMA", "NIFTY PRIVATE BANK", "NIFTY PSU BANK", 
    "NIFTY REALTY", "NIFTY CONSUMER DURABLES", "NIFTY OIL AND GAS"
]

# Handle LinkColumn Navigation via Query Params
params = st.query_params
if "nav" in params:
    nav_target = urllib.parse.unquote(params["nav"])
    if nav_target in ["Nifty 50", "Nifty 500", "Nifty Smallcap 250"]:
         st.session_state.nav_category = "Broad Market"
         st.session_state.nav_broad = nav_target
    elif nav_target in SECTOR_OPTIONS:
         st.session_state.nav_category = "Sectoral Indices"
         st.session_state.nav_sector = nav_target
    elif nav_target in THEMES.keys():
         st.session_state.nav_category = "Industries"
         st.session_state.nav_industry = nav_target
    
    # Clear query param so a manual category switch later isn't forced back
    st.query_params.clear()

# Category Selection
category = st.sidebar.radio(
    "Market Segment",
    ["Broad Market", "Sectoral Indices", "Industries", "Performance Overview", "Sector Rotation (RRG)"],
    key="nav_category"
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
        
    theme_keys = [k for k in index_config.keys() if "Nifty" not in k and "NIFTY" not in k]
    rrg_keys = sorted([k for k in THEMES.keys() if k in index_config])
    tf_map = {"Daily": "D", "Weekly": "W", "Monthly": "M"}

    # -------------------------------------------------------------------------
    # GLOBAL RRG CALCULATION (Moved Up for Dynamic Filtering)
    # -------------------------------------------------------------------------
    global_rrg_df = pd.DataFrame()
    last_points_global = pd.DataFrame()
    
    with st.spinner("Analyzing Market Breadth..."):
        baseline_file = index_config["Nifty 50"]["file"]
        baseline_df = load_data_v2(baseline_file)
        
        if baseline_df is None:
            st.error("Nifty 50 data missing for baseline.")
        else:
            calculator = RRGCalculator(baseline_df)
            full_data_dict = {}
            
            # Load ALL themes
            for name in rrg_keys:
                config = index_config.get(name)
                if config:
                    fpath = config['file']
                    if os.path.exists(fpath):
                        d = load_data_v2(fpath)
                        if d is not None and not d.empty:
                            full_data_dict[name] = d
            
            if full_data_dict:
                global_rrg_df = calculator.calculate_rrg_metrics(
                    full_data_dict, 
                    timeframe=tf_map[timeframe], 
                    tail_length=tail
                )

    # Calculate Quadrants immediately if data exists
    if not global_rrg_df.empty:
        last_points_global = global_rrg_df.sort_values('Date').groupby('Ticker').last()
        
        def get_quadrant(row):
            r = row['RS_Ratio']
            m = row['RS_Momentum']
            if r > 100 and m > 100: return "Leading"
            if r > 100 and m < 100: return "Weakening"
            if r < 100 and m < 100: return "Lagging"
            if r < 100 and m > 100: return "Improving"
            return "Unknown"
            
        last_points_global['Quadrant'] = last_points_global.apply(get_quadrant, axis=1)

    # -------------------------------------------------------------------------
    # PHASE FILTERING (Multiselect)
    # -------------------------------------------------------------------------
    st.write(" **Filter by Phase:**")
    
    # Initialize Phase Selection
    all_phases = ['Leading', 'Weakening', 'Lagging', 'Improving']
    if 'selected_phases' not in st.session_state:
        st.session_state['selected_phases'] = all_phases
        
    p_col1, p_col2, p_col3 = st.columns([1, 1, 4])
    if p_col1.button("Select All", key="phase_all"):
        st.session_state['selected_phases'] = all_phases
        st.rerun()
    if p_col2.button("Deselect All", key="phase_none"):
        st.session_state['selected_phases'] = []
        st.rerun()
        
    selected_phases = st.multiselect(
        "Select Phases", 
        all_phases, 
        default=all_phases, 
        key='selected_phases',
        label_visibility="collapsed"
    )
    
    # -------------------------------------------------------------------------
    # DYNAMIC THEME SELECTION
    # -------------------------------------------------------------------------
    # Update theme selection based on phases IF phases changed? 
    # Or just always filter the options available? 
    # User asked: "select themes to display should automatically change"
    
    # Logic:
    # 1. subset tickers that match selected phases
    # 2. update session_state['rrg_multiselect'] to match these tickers
    
    if not last_points_global.empty:
        # Get valid tickers for current phases
        valid_tickers_for_phases = last_points_global[last_points_global['Quadrant'].isin(selected_phases)].index.tolist()
        
        # Check if we need to update selection (only if phase selection changed logic is implied, 
        # but to keep it simple and robust per request: "display only the themes in that phase")
        # We will FORCE the selection to match the phases.
        
        # However, we need to allow manual deselection too? 
        # User said: "It should display all themes when all the phases are selected"
        # and "select themes... automatically change to display only themes in that phase"
        
        # Let's derive the selection from phases directly for now, as that seems to be the intent.
        # But we still want the multiselect to show them so user can subtract specific ones.
        
        # To avoid infinite rerun loops, we compare with current state
        # We use a set for comparison to ignore order
        current_selection_set = set(st.session_state.get('rrg_multiselect', []))
        target_selection_set = set(valid_tickers_for_phases)
        
        # IMPORTANT: We only auto-update if the SET of valid tickers is different from current selection.
        # But wait, this prevents manual deselection of a single stock.
        # Better approach:
        # Use a "last_phase_hash" to detect if PHASE selection changed.
        # If PHASES changed -> Reset themes to match new phases.
        # If PHASES didn't change -> Leave themes alone (allows manual select/deselect).
        
        current_phase_hash = hash(tuple(sorted(selected_phases)))
        last_phase_hash = st.session_state.get('last_phase_hash', None)
        
        if current_phase_hash != last_phase_hash:
            st.session_state['rrg_multiselect'] = valid_tickers_for_phases
            st.session_state['last_phase_hash'] = current_phase_hash
            st.rerun()
            
    # -------------------------------------------------------------------------
    # THEME WIDGET
    # -------------------------------------------------------------------------
    t_col1, t_col2, t_col3 = st.columns([1, 1, 4])
    if t_col1.button("Select All Themes", type="secondary"):
        st.session_state['rrg_multiselect'] = rrg_keys
        st.rerun()
    if t_col2.button("Deselect All Themes", type="secondary"):
        st.session_state['rrg_multiselect'] = []
        st.rerun()
        
    selected_rrg_themes = st.multiselect(
        "Select Themes to Display", 
        rrg_keys, 
        key='rrg_multiselect'
    )
    
    # -------------------------------------------------------------------------
    # RENDER CHART
    # -------------------------------------------------------------------------
    if not selected_rrg_themes:
        st.warning("Please select at least one theme to display.")
    elif global_rrg_df.empty:
        st.error("Nifty 50 data missing or calculation failed.")
    else:
        rrg_view = global_rrg_df[global_rrg_df['Ticker'].isin(selected_rrg_themes)].groupby('Ticker').tail(tail)
        
        if rrg_view.empty:
             st.warning("No data for selected themes.")
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
                    customdata=t_data['Date'],
                    mode='lines+markers',
                    name=ticker,
                    marker=dict(size=4, symbol="circle", color=color, opacity=0.7),
                    line=dict(width=2, color=color),
                    hovertemplate=f"<b>{ticker}</b><br>Date: %{{customdata|%d %b %Y}}<br>Ratio: %{{x:.2f}}<br>Mom: %{{y:.2f}}<extra></extra>"
                ))
                
                # Arrowhead (Annotation) & Label
                if len(t_data) >= 2:
                    head = t_data.iloc[-1]
                    prev = t_data.iloc[-2]
                    
                    # Add Annotation Arrow
                    # We use data coordinates for both head (x, y) and tail (ax, ay)
                    fig.add_annotation(
                        x=head['RS_Ratio'],
                        y=head['RS_Momentum'],
                        ax=prev['RS_Ratio'],
                        ay=prev['RS_Momentum'],
                        xref="x", yref="y",
                        axref="x", ayref="y",
                        showarrow=True,
                        arrowhead=2, # Sharp arrow
                        arrowsize=1.2,
                        arrowwidth=2,
                        arrowcolor=color,
                        opacity=0.9
                    )
                    
                    # Label (Annotation Text with Offset)
                    fig.add_annotation(
                        x=head['RS_Ratio'],
                        y=head['RS_Momentum'],
                        text=ticker,
                        showarrow=False,
                        xshift=0,
                        yshift=15,
                        xanchor="center",
                        yanchor="bottom",
                        font=dict(color=color, size=12, weight="bold"),
                        bgcolor="rgba(0,0,0,0.5)", # Semi-transparent background for readability
                        borderpad=2
                    )
                else:
                    head = t_data.iloc[-1]
                    fig.add_trace(go.Scatter(
                        x=[head['RS_Ratio']],
                        y=[head['RS_Momentum']],
                        mode='markers+text',
                        text=[ticker],
                        textposition="top right",
                        marker=dict(symbol="circle", size=8, color=color),
                        textfont=dict(color=color, size=12, weight="bold"),
                        showlegend=False
                    ))

            # Watermarks (Refined: Smaller, greater transparency)
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
                yaxis=dict(range=y_range, zeroline=True, zerolinecolor="gray", zerolinewidth=1, scaleanchor="x", scaleratio=1),
                template="plotly_dark",
                height=850,
                showlegend=False
            )
            st.plotly_chart(fig, width="stretch")
elif category == "Performance Overview":
    st.title("Market Performance Heatmap")
    st.markdown("*Comparative returns of all sectors and themes based on Equal-Weighted Index*")
    
    with st.spinner("Calculating performance across all themes..."):
        perf_summary = get_performance_summary_v3(index_config)
    
    if not perf_summary.empty:
        toggle_cagr = st.toggle("Annualize Returns (CAGR)", value=False, help="Converts 1Y, 3Y, and 5Y returns to Compound Annual Growth Rate")
        
        if toggle_cagr:
            for col, yrs in [("1 Year", 1), ("3 Years", 3), ("5 Years", 5)]:
                if col in perf_summary.columns:
                    perf_summary[col] = perf_summary[col].apply(
                        lambda x: (((1 + x/100)**(1/yrs)) - 1)*100 if pd.notna(x) else pd.NA
                    )
                    
        if "1 Year" in perf_summary.columns:
            perf_summary = perf_summary.sort_values("1 Year", ascending=False)
            
        def color_return(val):
            if pd.isna(val) or not isinstance(val, (int, float)): 
                return ""
            color = '#22c55e' if val >= 0 else '#ef4444' 
            return f'color: {color}; font-weight: bold;'
            
        def safe_format(val):
            if pd.isna(val) or not isinstance(val, (int, float)):
                return str(val) if pd.notna(val) else ""
            return f"{float(val):.2f}%"
            
        numeric_cols = [c for c in perf_summary.columns if c != "Theme/Index"]

        styler = perf_summary.style.map(color_return, subset=numeric_cols).format(safe_format, subset=numeric_cols)
        render_styled_dataframe(styler, height="80vh")

else:
    # Single Index View logic
    selected_index = None
    if category == "Broad Market":
        selected_index = st.sidebar.radio("Select Index", ["Nifty 50", "Nifty 500", "Nifty Smallcap 250"], key="nav_broad")
    elif category == "Sectoral Indices":
        selected_index = st.sidebar.radio("Select Sector", SECTOR_OPTIONS, key="nav_sector")
    elif category == "Industries":
        industry_options = sorted(THEMES.keys())
        selected_index = st.sidebar.radio("Select Industry", industry_options, key="nav_industry")

    current_config = index_config.get(selected_index, index_config["Nifty 50"])

    st.title(f"{current_config['title']} Market Breadth")
    st.markdown(f"*{current_config['description']}*")

    df = load_data_v2(current_config['file'])

    # --- DEBUG SECTION (TEMPORARY) ---
    with st.expander("🛠 System Debug Info (Check this if data seems old)", expanded=True):
        st.write(f"**Current Working Directory:** `{os.getcwd()}`")
        abs_path = os.path.abspath(current_config['file'])
        st.write(f"**Loading File:** `{abs_path}`")
        
        if os.path.exists(abs_path):
            mtime = datetime.fromtimestamp(os.path.getmtime(abs_path))
            st.write(f"**File Modified Time:** {mtime}")
        else:
            st.error("File NOT found on disk!")

        if df is not None and not df.empty:
            st.write("**Last 3 Data Points Loaded:**")
            st.dataframe(df.tail(3))
        else:
            st.write("DataFrame is Empty or None")
    # ---------------------------------

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
            toggle_cagr_idx = st.toggle("Annualize Returns (CAGR)", value=False, key="cagr_idx", help="Converts 1Y, 3Y, and 5Y returns to Compound Annual Growth Rate")
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
                        if toggle_cagr_idx and name in ["1 Year", "3 Years", "5 Years"]:
                            yrs = 1 if name == "1 Year" else (3 if name == "3 Years" else 5)
                            ret = (((1 + ret/100)**(1/yrs)) - 1) * 100
                        metrics[name] = ret
                    else: metrics[name] = None
                else: metrics[name] = None
            perf_df = pd.DataFrame([metrics])
            def color_ret(val):
                if pd.isna(val): return ""
                return f'color: {"#22c55e" if val >= 0 else "#ef4444"}; font-weight: bold'
            render_styled_dataframe(perf_df.style.map(color_ret).format("{:.2f}%"), height="150px")

        tab1, tab2 = st.tabs(["Breadth Chart", "Constituents"])
        with tab1:
            fig_pct = go.Figure()
            fig_pct.add_trace(go.Scatter(x=df['Date'], y=df['Percentage'], mode='lines', name='% Above 200 SMA', line=dict(color='#3b82f6', width=2), hovertemplate='<b>%{x|%d %b %Y}</b><br>%{y:.2f}%<extra></extra>'))
            fig_pct.add_hrect(y0=80, y1=100, fillcolor="green", opacity=0.1, layer="below", line_width=0)
            fig_pct.add_hrect(y0=0, y1=20, fillcolor="red", opacity=0.1, layer="below", line_width=0)
            fig_pct.add_hline(y=50, line_dash="dash", line_color="gray", annotation_text="Neutral (50%)")
            title_text = f"Percentage of Stocks Above 200-Day SMA (Latest: {latest['Date'].strftime('%d %b %Y')})"
            fig_pct.update_layout(title=title_text, yaxis_title="Percentage (%)", xaxis_title="Date", template="plotly_dark", height=500, yaxis=dict(range=[0, 100]), hovermode="x unified", xaxis=dict(hoverformat='%d %b %Y'))
            st.plotly_chart(fig_pct, width="stretch")

            fig_count = go.Figure()
            fig_count.add_trace(go.Scatter(x=df['Date'], y=df['Above'], mode='lines', name='Above', stackgroup='one', line=dict(width=0), fillcolor='rgba(34, 197, 94, 0.6)'))
            fig_count.add_trace(go.Scatter(x=df['Date'], y=df['Below'], mode='lines', name='Below', stackgroup='one', line=dict(width=0), fillcolor='rgba(239, 68, 68, 0.6)'))
            fig_count.update_layout(title="Market Participation", yaxis_title="Stocks", xaxis_title="Date", template="plotly_dark", height=400, hovermode="x unified", xaxis=dict(hoverformat='%d %b %Y'))
            st.plotly_chart(fig_count, width="stretch")

            # Restore original raw lists below the charts
            market_status = load_market_status()
            details = market_status.get(selected_index)
            
            def make_tv_url(ticker):
                clean = ticker.replace(".NS", "").replace(".BO", "")
                tv_symbol = clean.replace("-", "_").replace("&", "_")
                exchange = "BSE" if ".BO" in ticker else "NSE"
                return f"https://www.tradingview.com/chart/?symbol={exchange}:{tv_symbol}"

            tv_link_config = st.column_config.LinkColumn(
                "Ticker", 
                display_text=r"symbol=[A-Z]+:(.*)",
                help="Click to open TradingView Chart"
            )

            if details:
                st.markdown("---")
                c1, c2 = st.columns(2)
                
                with c1:
                    st.success(f"📈 Above 200 SMA ({len(details['above'])})")
                    if details['above']:
                        df_up = pd.DataFrame(details['above'], columns=["Ticker"])
                        df_up["Ticker"] = df_up["Ticker"].apply(make_tv_url)
                        st.dataframe(df_up, column_config={"Ticker": tv_link_config}, use_container_width=False, hide_index=True)
                    else:
                        st.caption("None")
                        
                with c2:
                    st.error(f"📉 Below 200 SMA ({len(details['below'])})")
                    if details['below']:
                        df_down = pd.DataFrame(details['below'], columns=["Ticker"])
                        df_down["Ticker"] = df_down["Ticker"].apply(make_tv_url)
                        st.dataframe(df_down, column_config={"Ticker": tv_link_config}, use_container_width=False, hide_index=True)
                    else:
                        st.caption("None")
                
                new_stocks = details.get('new_stock', [])
                if new_stocks:
                    st.warning(f"🆕 New Stock — Insufficient History for 200 SMA ({len(new_stocks)})")
                    df_new = pd.DataFrame(new_stocks, columns=["Ticker"])
                    df_new["Ticker"] = df_new["Ticker"].apply(make_tv_url)
                    st.dataframe(df_new, column_config={"Ticker": tv_link_config}, use_container_width=False, hide_index=True)

        with tab2:
            st.subheader(f"Constituents of {current_config['title']}")
            
            # Use the functions defined in tab1 above
            
            market_status = load_market_status()
            details = market_status.get(selected_index)
            constituent_perf = load_constituent_performance()

            if details:
                # Merge the market status categories
                status_map = {}
                for t in details.get('above', []): status_map[t] = "Above 200 SMA"
                for t in details.get('below', []): status_map[t] = "Below 200 SMA"
                for t in details.get('new_stock', []): status_map[t] = "New Stock (<200d)"

                # Fallback to offline constituents if NSE blocks the Streamlit Cloud IP
                all_tickers = get_cached_constituents(selected_index)
                if not all_tickers:
                    all_tickers = details.get('above', []) + details.get('below', []) + details.get('new_stock', [])
                
                if all_tickers and category == "Industries":
                    tv_urls = [make_tv_url(t) for t in all_tickers]
                    urls_js = json.dumps(tv_urls)
                    html_code = f"""
                    <div style="text-align: right; margin-bottom: 0px;">
                        <button onclick="openAll()" style="background-color: #2563eb; color: white; border: none; padding: 0.5rem 1rem; border-radius: 4px; font-weight: bold; cursor: pointer; font-family: 'Inter', sans-serif; transition: background-color 0.2s;">
                            ↗️ Open All in TradingView
                        </button>
                    </div>
                    <script>
                    function openAll() {{
                        var urls = {urls_js};
                        var blocked = false;
                        urls.forEach(function(url) {{
                            var newWin = window.open(url, '_blank');
                            if (!newWin || newWin.closed || typeof newWin.closed == 'undefined') {{
                                blocked = true;
                            }}
                        }});
                        if (blocked) {{
                            alert("⚠️ Pop-up Blocker Detected!\\n\\nYour browser is preventing multiple tabs from opening at once.\\n\\nPlease click the Pop-up Blocker icon in your browser's address bar (typically on the right), select 'Always allow pop-ups and redirects from this site', and then try again.");
                        }}
                    }}
                    </script>
                    """
                    components.html(html_code, height=45)
                
                # Setup Toggle
                toggle_cagr = st.toggle("Annualize Returns (CAGR)", value=False, help="Converts 1Y, 3Y, and 5Y returns to Compound Annual Growth Rate")
                
                rows = []
                for ticker in all_tickers:
                    perf = constituent_perf.get(ticker, {})
                    
                    row = {
                        "Ticker": make_tv_url(ticker),
                        "1D": perf.get("1D") if perf.get("1D") is not None else pd.NA,
                        "1W": perf.get("1W") if perf.get("1W") is not None else pd.NA,
                        "1M": perf.get("1M") if perf.get("1M") is not None else pd.NA,
                        "3M": perf.get("3M") if perf.get("3M") is not None else pd.NA,
                        "6M": perf.get("6M") if perf.get("6M") is not None else pd.NA,
                    }
                    
                    # Apply CAGR logic
                    y1 = perf.get("1Y")
                    y3 = perf.get("3Y")
                    y5 = perf.get("5Y")
                    
                    if toggle_cagr:
                        row["1Y"] = y1 if y1 is not None else pd.NA # 1Y CAGR is same as absolute
                        row["3Y"] = (((1 + y3/100)**(1/3)) - 1)*100 if y3 is not None else pd.NA
                        row["5Y"] = (((1 + y5/100)**(1/5)) - 1)*100 if y5 is not None else pd.NA
                    else:
                        row["1Y"] = y1 if y1 is not None else pd.NA
                        row["3Y"] = y3 if y3 is not None else pd.NA
                        row["5Y"] = y5 if y5 is not None else pd.NA
                        
                    # Add RS at the end
                    row["RS (20D)"] = perf.get("RS (20D)") if perf.get("RS (20D)") is not None else pd.NA
                    
                    rows.append(row)
                    
                columns_list = ["Ticker", "1D", "1W", "1M", "3M", "6M", "1Y", "3Y", "5Y", "RS (20D)"]
                df_perf = pd.DataFrame(rows, columns=columns_list)
                
                if not df_perf.empty:
                    # Apply aesthetic number styling natively via HTML to bypass Data Grid bugs
                    styler = df_perf.style.format({
                        "1D": "{:+.2f}%", 
                        "1W": "{:+.2f}%", 
                        "1M": "{:+.2f}%", 
                        "3M": "{:+.2f}%", 
                        "6M": "{:+.2f}%",
                        "1Y": "{:+.2f}%",
                        "3Y": "{:+.2f}%",
                        "5Y": "{:+.2f}%",
                        "RS (20D)": "{:+.2f}%"
                    }).map(
                        lambda x: f"color: {'#22c55e' if x > 0 else '#ef4444' if x < 0 else 'gray'}; font-family: monospace" if pd.notnull(x) else "",
                        subset=["1D", "1W", "1M", "3M", "6M", "1Y", "3Y", "5Y", "RS (20D)"]
                    )
                    render_styled_dataframe(styler, height="600px")
                else:
                    st.warning("No constituent performance data available.")
            else:
                # Fallback to simple list if offline caching hasn't run yet
                tickers = get_cached_constituents(selected_index)
                
                if tickers:
                    st.write(f"**Total Stocks:** {len(tickers)}")
                    if category == "Industries":
                        tv_urls = [make_tv_url(t) for t in tickers]
                        urls_js = json.dumps(tv_urls)
                        html_code = f"""
                        <div style="text-align: right; margin-bottom: 0px;">
                            <button onclick="openAll()" style="background-color: #2563eb; color: white; border: none; padding: 0.5rem 1rem; border-radius: 4px; font-weight: bold; cursor: pointer; font-family: 'Inter', sans-serif; transition: background-color 0.2s;">
                                ↗️ Open All in TradingView
                            </button>
                        </div>
                        <script>
                        function openAll() {{
                            var urls = {urls_js};
                            var blocked = false;
                            urls.forEach(function(url) {{
                                var newWin = window.open(url, '_blank');
                                if (!newWin || newWin.closed || typeof newWin.closed == 'undefined') {{
                                    blocked = true;
                                }}
                            }});
                            if (blocked) {{
                                alert("⚠️ Pop-up Blocker Detected!\\n\\nYour browser is preventing multiple tabs from opening at once.\\n\\nPlease click the Pop-up Blocker icon in your browser's address bar (typically on the right), select 'Always allow pop-ups and redirects from this site', and then try again.");
                            }}
                        }}
                        </script>
                        """
                        components.html(html_code, height=45)
                    
                    df_fallback = pd.DataFrame(tickers, columns=["Ticker Symbol"])
                    df_fallback["Ticker Symbol"] = df_fallback["Ticker Symbol"].apply(make_tv_url)
                    
                    st.dataframe(
                        df_fallback, 
                        column_config={"Ticker Symbol": tv_link_config},
                        use_container_width=False, 
                        hide_index=True
                    )
                else:
                    st.info(f"Constituent list not available for {selected_index}.")
    else: st.error(f"Data file not found: {current_config['file']}")
