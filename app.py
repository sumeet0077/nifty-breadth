
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta

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
# ---------------------------------------------------------
# DATA LOADING
# ---------------------------------------------------------
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_data(file_path):
    try:
        # Load the CSV
        df = pd.read_csv(file_path)
        df['Date'] = pd.to_datetime(df['Date'])
        # Filter: Only show 2015 onwards
        df = df[df['Date'] >= "2015-01-01"]
        return df.sort_values('Date')
    except FileNotFoundError:
        return None

# ---------------------------------------------------------
# SIDEBAR / NAVIGATION
# ---------------------------------------------------------
st.sidebar.title("Configuration")
selected_index = st.sidebar.radio(
    "Select Index",
    ["Nifty 50", "Nifty 500", "Nifty Smallcap"],
    index=1
)

# Map selection to file
index_config = {
    "Nifty 50": {
        "file": "market_breadth_nifty50.csv",
        "title": "Nifty 50",
        "description": "Tracking the top 50 blue-chip companies."
    },
    "Nifty 500": {
        "file": "market_breadth_nifty500.csv",
        "title": "Nifty 500",
        "description": "Tracking the top 500 companies listed on NSE."
    },
    "Nifty Smallcap": {
        "file": "market_breadth_smallcap.csv",
        "title": "Nifty Smallcap",
        "description": "Tracking the Smallcap segment performance."
    }
}

current_config = index_config[selected_index]

# ---------------------------------------------------------
# MAIN LAYOUT
# ---------------------------------------------------------
df = load_data(current_config["file"])

col_title, col_refresh = st.columns([6, 1])
with col_title:
    st.title(f"⚡ {current_config['title']} Market Breadth")
    st.markdown(f"{current_config['description']} **(% Stocks > 200 SMA)**")

if df is None:
    st.warning(f"⚠️ Data file `{current_config['file']}` not found.")
    st.info("Please run the data fetching script to generate it.")
    if st.button("Run Fetch Script Now"):
        with st.spinner("Fetching data for all indices..."):
            import subprocess
            subprocess.run(["python3", "fetch_breadth_data.py"])
        st.rerun()
    st.stop()

# Get latest data points
latest = df.iloc[-1]
prev = df.iloc[-2] if len(df) > 1 else latest
latest_date = latest['Date'].strftime('%d %b %Y')

# ---------------------------------------------------------
# TOP METRICS
# ---------------------------------------------------------
m1, m2, m3, m4 = st.columns(4)

with m1:
    st.metric(
        "Current Breadth (%)",
        f"{latest['Percentage']:.1f}%",
        f"{latest['Percentage'] - prev['Percentage']:.1f}%"
    )

with m2:
    st.metric(
        "Stocks > 200 SMA",
        f"{int(latest['Above'])}",
        f"{int(latest['Above'] - prev['Above'])}"
    )

with m3:
    st.metric(
        "Stocks < 200 SMA",
        f"{int(latest['Below'])}",
        f"{int(latest['Below'] - prev['Below'])}",
        delta_color="inverse"
    )

with m4:
    st.metric(
        "Data As Of",
        latest_date,
        delta=None
    )

st.markdown("---")

# ---------------------------------------------------------
# MAIN CHART: BREADTH PERCENTAGE
# ---------------------------------------------------------
st.subheader("📈 Historical Market Breadth (2015 - Present)")

chart_data = df.copy()

fig_line = go.Figure()

# Green Fill (Above 50%)
fig_line.add_trace(go.Scatter(
    x=chart_data['Date'], 
    y=chart_data['Percentage'],
    mode='lines',
    name='Breadth %',
    line=dict(color='#22d3ee', width=2),
    fill='tozeroy',
    fillcolor='rgba(34, 211, 238, 0.1)',
    hovertemplate='%{y:.1f}%<extra></extra>'
))

# Reference Zones
fig_line.add_hrect(y0=0, y1=20, fillcolor="rgba(239, 68, 68, 0.15)", line_width=0, annotation_text="Oversold", annotation_position="left")
fig_line.add_hrect(y0=80, y1=100, fillcolor="rgba(34, 197, 94, 0.15)", line_width=0, annotation_text="Overbought", annotation_position="left")
fig_line.add_hline(y=50, line_dash="dot", line_color="rgba(255,255,255,0.3)")

fig_line.update_layout(
    template="plotly_dark",
    height=500,
    hovermode="x unified",
    hoverlabel=dict(
        bgcolor="#1f2937",
        font_size=14,
        font_family="Inter"
    ),
    margin=dict(l=20, r=20, t=20, b=20),
    yaxis=dict(
        title="Percentage of Stocks Above 200 SMA",
        range=[0, 100],
        gridcolor="rgba(255,255,255,0.1)"
    ),
    xaxis=dict(
        gridcolor="rgba(255,255,255,0.1)",
        rangeslider=dict(visible=False),
        hoverformat='%d %b %Y'  # Force Day Month Year format
    )
)

st.plotly_chart(fig_line, use_container_width=True)

# ---------------------------------------------------------
# SECONDARY CHART: STACKED AREA (PARTICIPATION)
# ---------------------------------------------------------
st.subheader("📊 Participation Count")

fig_area = go.Figure()

fig_area.add_trace(go.Scatter(
    x=chart_data['Date'], y=chart_data['Above'],
    mode='lines',
    name='Stocks Above',
    stackgroup='one',
    line=dict(width=0),
    fillcolor='rgba(34, 197, 94, 0.6)' # Green
))

fig_area.add_trace(go.Scatter(
    x=chart_data['Date'], y=chart_data['Below'],
    mode='lines',
    name='Stocks Below',
    stackgroup='one',
    line=dict(width=0),
    fillcolor='rgba(239, 68, 68, 0.6)' # Red
))

fig_area.update_layout(
    template="plotly_dark",
    height=400,
    hovermode="x unified",
    margin=dict(l=20, r=20, t=20, b=20),
    yaxis=dict(title="Number of Stocks"),
    legend=dict(orientation="h", y=1.02, x=0.5, xanchor="center"),
    xaxis=dict(hoverformat='%d %b %Y')
)

st.plotly_chart(fig_area, use_container_width=True)

# ---------------------------------------------------------
# RAW DATA EXPANDER
# ---------------------------------------------------------
with st.expander("Explore Raw Data"):
    st.dataframe(df.style.format({
        'Percentage': '{:.2f}%',
        'Above': '{:.0f}',
        'Below': '{:.0f}',
        'Total': '{:.0f}'
    }), use_container_width=True)
