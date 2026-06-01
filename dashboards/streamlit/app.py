"""
Real-Time Stock Market Lakehouse Dashboard
Built with Streamlit — connects to the FastAPI backend.
"""

from __future__ import annotations

import os
import time
import logging
from datetime import datetime, timezone

import httpx
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────
# API_BASE_URL: use http://api:8000 inside Docker, http://localhost:8000 for local dev
API_BASE = os.getenv("API_BASE_URL", "http://api:8000")
REFRESH_INTERVAL = int(os.getenv("DASHBOARD_REFRESH_SECONDS", "10"))

st.set_page_config(
    page_title="Stock Market Lakehouse",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────
# API Client
# ─────────────────────────────────────────────────────────
@st.cache_data(ttl=REFRESH_INTERVAL)
def fetch(endpoint: str, params: dict = None) -> dict:
    try:
        with httpx.Client(timeout=5.0) as client:
            resp = client.get(f"{API_BASE}{endpoint}", params=params or {})
            resp.raise_for_status()
            return resp.json()
    except Exception as exc:
        logger.warning("API call failed %s: %s", endpoint, exc)
        return {}


# ─────────────────────────────────────────────────────────
# Custom CSS
# ─────────────────────────────────────────────────────────
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #1a1a2e, #16213e);
        border-radius: 12px;
        padding: 16px 20px;
        border-left: 4px solid #00d4aa;
        margin-bottom: 10px;
    }
    .positive { color: #00d4aa; font-weight: bold; }
    .negative { color: #ff4b6e; font-weight: bold; }
    .stMetric > label { color: #8892b0 !important; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/color/96/stock-market.png", width=80)
    st.title("Stock Lakehouse")
    st.caption(f"Last refreshed: {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}")
    st.divider()

    symbols_raw = os.getenv("STOCK_SYMBOLS", "AAPL,MSFT,GOOGL,AMZN,TSLA,META,NVDA,JPM,V,JNJ")
    all_symbols = symbols_raw.split(",")
    selected_symbol = st.selectbox("Focus Symbol", all_symbols, index=0)
    history_hours = st.slider("Chart History (hours)", 1, 48, 6)
    auto_refresh = st.checkbox("Auto-refresh every 10s", value=True)

    st.divider()
    st.subheader("System Health")
    health = fetch("/api/health/")
    if health:
        status = health.get("status", "unknown")
        color = "🟢" if status == "healthy" else "🟡"
        st.write(f"{color} Pipeline: **{status.upper()}**")
        for k, v in health.get("checks", {}).items():
            icon = "✅" if v else "❌"
            st.write(f"{icon} {k}")


# ─────────────────────────────────────────────────────────
# Main Dashboard
# ─────────────────────────────────────────────────────────
st.title("📈 Real-Time Stock Market Lakehouse")

# ── Market Summary Banner ─────────────────────────────
summary = fetch("/api/analytics/summary")
if summary:
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Symbols", summary.get("total_symbols", "—"))
    with col2:
        st.metric("Gainers", summary.get("gainers", "—"), delta="▲")
    with col3:
        st.metric("Losers", summary.get("losers", "—"), delta="▼")
    with col4:
        avg_chg = summary.get("avg_change_pct", 0)
        st.metric(
            "Avg Change",
            f"{avg_chg:+.2f}%",
            delta=f"{avg_chg:+.2f}%",
            delta_color="normal",
        )

st.divider()

# ── Live Quotes Table ─────────────────────────────────
col_left, col_right = st.columns([3, 1])

with col_left:
    st.subheader("📊 Live Market Quotes")
    quotes_data = fetch("/api/stocks/")
    if quotes_data and quotes_data.get("data"):
        df = pd.DataFrame(quotes_data["data"])
        df["change_pct"] = df["change_pct"].round(2)
        df["price"] = df["price"].round(2)

        # Color code change
        def color_change(val):
            color = "#00d4aa" if val > 0 else "#ff4b6e"
            return f"color: {color}; font-weight: bold"

        styled = (
            df[["symbol", "price", "open", "high", "low", "change", "change_pct", "volume"]]
            .style
            .applymap(color_change, subset=["change", "change_pct"])
            .format({
                "price": "${:.2f}",
                "open": "${:.2f}",
                "high": "${:.2f}",
                "low": "${:.2f}",
                "change": "{:+.2f}",
                "change_pct": "{:+.2f}%",
                "volume": "{:,.0f}",
            })
        )
        st.dataframe(styled, use_container_width=True, height=350)
    else:
        st.info("Waiting for data from pipeline...")

with col_right:
    st.subheader("🏆 Top Movers")
    gainers = fetch("/api/stocks/gainers/top", {"limit": 5})
    if gainers and gainers.get("data"):
        st.write("**Top Gainers**")
        for q in gainers["data"]:
            st.markdown(
                f"`{q['symbol']}` "
                f"<span class='positive'>+{q['change_pct']:.2f}%</span> "
                f"${q['price']:.2f}",
                unsafe_allow_html=True,
            )
    losers = fetch("/api/stocks/losers/top", {"limit": 5})
    if losers and losers.get("data"):
        st.write("**Top Losers**")
        for q in losers["data"]:
            st.markdown(
                f"`{q['symbol']}` "
                f"<span class='negative'>{q['change_pct']:.2f}%</span> "
                f"${q['price']:.2f}",
                unsafe_allow_html=True,
            )

st.divider()

# ── Candlestick Chart ─────────────────────────────────
st.subheader(f"🕯️ {selected_symbol} — Candlestick Chart ({history_hours}h)")
history = fetch(f"/api/stocks/{selected_symbol}/history", {"hours": history_hours})
if history and history.get("data"):
    df_candle = pd.DataFrame(history["data"])
    fig = go.Figure(data=[go.Candlestick(
        x=df_candle["window_start"],
        open=df_candle["open"],
        high=df_candle["high"],
        low=df_candle["low"],
        close=df_candle["close"],
        name=selected_symbol,
        increasing_line_color="#00d4aa",
        decreasing_line_color="#ff4b6e",
    )])
    fig.update_layout(
        template="plotly_dark",
        title=f"{selected_symbol} Price Action",
        xaxis_title="Time",
        yaxis_title="Price (USD)",
        height=450,
        xaxis_rangeslider_visible=False,
        margin=dict(l=20, r=20, t=40, b=20),
    )
    st.plotly_chart(fig, use_container_width=True)

# ── Moving Average Chart ──────────────────────────────
ma_data = fetch(f"/api/analytics/moving-averages/{selected_symbol}", {"window_minutes": 5})
if ma_data and ma_data.get("data"):
    df_ma = pd.DataFrame(ma_data["data"])
    fig_ma = go.Figure()
    fig_ma.add_trace(go.Scatter(
        x=df_ma["timestamp"], y=df_ma["price"],
        name="Price", line=dict(color="#8892b0", width=1),
    ))
    fig_ma.add_trace(go.Scatter(
        x=df_ma["timestamp"], y=df_ma["ma_5m"],
        name="MA(5m)", line=dict(color="#00d4aa", width=2),
    ))
    fig_ma.update_layout(
        template="plotly_dark",
        title=f"{selected_symbol} — Moving Average (5-min)",
        xaxis_title="Time",
        yaxis_title="Price (USD)",
        height=350,
        margin=dict(l=20, r=20, t=40, b=20),
    )
    st.plotly_chart(fig_ma, use_container_width=True)

st.divider()

# ── Volatility Chart ──────────────────────────────────
col_v1, col_v2 = st.columns(2)

with col_v1:
    st.subheader("⚡ Volatility Heatmap")
    vol_data = fetch("/api/analytics/volatility")
    if vol_data and vol_data.get("data"):
        df_vol = pd.DataFrame(vol_data["data"])
        fig_vol = px.bar(
            df_vol,
            x="symbol",
            y="price_volatility",
            color="price_volatility",
            color_continuous_scale=["#00d4aa", "#ffd700", "#ff4b6e"],
            title="Price Volatility by Symbol",
            template="plotly_dark",
        )
        fig_vol.update_layout(height=350, margin=dict(l=20, r=20, t=40, b=20))
        st.plotly_chart(fig_vol, use_container_width=True)

with col_v2:
    st.subheader("📉 Trend Analysis")
    trends = fetch("/api/trends/", {"hours": history_hours})
    if trends and trends.get("data"):
        df_trends = pd.DataFrame(trends["data"])
        color_map = {
            "uptrend": "#00d4aa",
            "downtrend": "#ff4b6e",
            "sideways": "#ffd700",
        }
        fig_trend = px.bar(
            df_trends,
            x="symbol",
            y="change_over_period_pct",
            color="direction",
            color_discrete_map=color_map,
            title=f"Trend Direction ({history_hours}h)",
            template="plotly_dark",
        )
        fig_trend.update_layout(height=350, margin=dict(l=20, r=20, t=40, b=20))
        st.plotly_chart(fig_trend, use_container_width=True)

st.divider()

# ── Anomaly Alerts ────────────────────────────────────
st.subheader("🚨 Anomaly Alerts")
anomalies = fetch("/api/anomalies/", {"hours": 1})
if anomalies and anomalies.get("data"):
    df_anom = pd.DataFrame(anomalies["data"])
    st.dataframe(df_anom, use_container_width=True)
else:
    st.success("✅ No anomalies detected in the last hour.")

# ── Auto-refresh ──────────────────────────────────────
if auto_refresh:
    time.sleep(REFRESH_INTERVAL)
    st.rerun()
