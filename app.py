"""Streamlit Portfolio Tracker — main app."""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

from degiro_client import connect, get_portfolio, get_dividends
from portfolio import (
    compute_portfolio_value_over_time,
    compute_benchmark_comparison,
    compute_asset_breakdown,
)

st.set_page_config(page_title="Portfolio Tracker", layout="wide")
st.title("Portfolio Tracker")


@st.cache_resource(ttl=900)
def get_trading_api():
    """Cached DEGIRO connection (refreshes every 15 min)."""
    return connect()


@st.cache_data(ttl=900)
def load_portfolio():
    api = get_trading_api()
    return get_portfolio(api)


@st.cache_data(ttl=3600)
def load_dividends(_from_date, _to_date):
    api = get_trading_api()
    return get_dividends(api, _from_date, _to_date)


# --- Sidebar ---
st.sidebar.header("Settings")

default_start = datetime.now() - timedelta(days=365)
start_date = st.sidebar.date_input("Start date", value=default_start)
end_date = st.sidebar.date_input("End date", value=datetime.now())

start_dt = datetime.combine(start_date, datetime.min.time())
end_dt = datetime.combine(end_date, datetime.max.time())

if st.sidebar.button("Refresh data"):
    st.cache_data.clear()
    st.cache_resource.clear()
    st.rerun()

# --- Load data ---
try:
    with st.spinner("Connecting to DEGIRO..."):
        positions = load_portfolio()
except Exception as e:
    st.error(f"Failed to connect to DEGIRO: {e}")
    st.info("Make sure your .env file contains valid DEGIRO_USERNAME and DEGIRO_PASSWORD.")
    st.stop()

if positions.empty:
    st.warning("No positions found in your DEGIRO portfolio.")
    st.stop()

# --- Portfolio Value Over Time ---
st.header("Portfolio Value Over Time")

with st.spinner("Fetching market data..."):
    portfolio_values = compute_portfolio_value_over_time(positions, start_dt, end_dt)

if not portfolio_values.empty:
    fig = px.line(
        portfolio_values.reset_index(),
        x="date",
        y="total",
        title="Total Portfolio Value",
        labels={"total": "Value", "date": "Date"},
    )
    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("Could not compute portfolio value. Check that your positions have valid ticker symbols.")

# --- Benchmark Comparison ---
st.header("Benchmark Comparison (S&P 500)")

if not portfolio_values.empty:
    benchmark = compute_benchmark_comparison(portfolio_values, start_dt, end_dt)

    if not benchmark.empty:
        fig2 = go.Figure()
        fig2.add_trace(
            go.Scatter(
                x=benchmark.index,
                y=benchmark["Portfolio"],
                name="Portfolio",
                mode="lines",
            )
        )
        fig2.add_trace(
            go.Scatter(
                x=benchmark.index,
                y=benchmark["S&P 500"],
                name="S&P 500",
                mode="lines",
                line=dict(dash="dash"),
            )
        )
        fig2.update_layout(
            title="Portfolio vs S&P 500 (Normalized to 100)",
            xaxis_title="Date",
            yaxis_title="Normalized Value",
            hovermode="x unified",
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.warning("Could not generate benchmark comparison.")
else:
    st.info("Portfolio values needed for benchmark comparison.")

# --- Dividends ---
st.header("Dividends")

with st.spinner("Loading dividend data..."):
    dividends = load_dividends(start_dt, end_dt)

if not dividends.empty:
    # Aggregate dividends by month
    div_monthly = dividends.copy()
    div_monthly["month"] = div_monthly["date"].dt.to_period("M").astype(str)
    div_agg = div_monthly.groupby("month")["change"].sum().reset_index()
    div_agg.columns = ["Month", "Amount"]

    fig3 = px.bar(
        div_agg,
        x="Month",
        y="Amount",
        title="Dividends Received Over Time",
        labels={"Amount": "Dividend Amount", "Month": "Month"},
    )
    st.plotly_chart(fig3, use_container_width=True)

    st.subheader("Dividend Details")
    st.dataframe(
        dividends[["date", "description", "change", "currency"]].sort_values(
            "date", ascending=False
        ),
        use_container_width=True,
    )
else:
    st.info("No dividend data found for the selected period.")

# --- Asset Breakdown ---
st.header("Per-Asset Breakdown")

breakdown = compute_asset_breakdown(positions)

if not breakdown.empty:
    col1, col2 = st.columns(2)

    with col1:
        fig4 = px.pie(
            breakdown,
            names="product_name",
            values="value",
            title="Portfolio Composition",
        )
        st.plotly_chart(fig4, use_container_width=True)

    with col2:
        st.dataframe(
            breakdown[["product_name", "value", "pct"]]
            .rename(columns={"product_name": "Asset", "value": "Value", "pct": "%"})
            .reset_index(drop=True),
            use_container_width=True,
        )
else:
    st.warning("Could not compute asset breakdown.")
