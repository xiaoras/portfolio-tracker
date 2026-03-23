"""Streamlit Portfolio Tracker — main app."""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

from degiro_client import (
    connect,
    get_portfolio,
    get_dividends,
    get_transactions_enriched,
    get_cash_deposits,
)
from portfolio import (
    compute_portfolio_value_over_time,
    compute_benchmark_comparison,
    compute_asset_breakdown,
    compute_actual_portfolio_value,
    compute_actual_benchmark_comparison,
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


@st.cache_data(ttl=3600)
def load_transactions(_from_date, _to_date):
    api = get_trading_api()
    return get_transactions_enriched(api, _from_date, _to_date)


@st.cache_data(ttl=3600)
def load_deposits(_from_date, _to_date):
    api = get_trading_api()
    return get_cash_deposits(api, _from_date, _to_date)


# --- Sidebar ---
st.sidebar.header("Settings")

default_start = datetime.now() - timedelta(days=365)
start_date = st.sidebar.date_input("Start date", value=default_start)
end_date = st.sidebar.date_input("End date", value=datetime.now())

start_dt = datetime.combine(start_date, datetime.min.time())
end_dt = datetime.combine(end_date, datetime.max.time())

# For transaction history, always go back to the very beginning
HISTORY_START = datetime(2020, 1, 1)

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


# ===========================================================================
# TABS
# ===========================================================================
tab1, tab2, tab3, tab4 = st.tabs([
    "📈 Current Portfolio",
    "📊 Actual Performance",
    "💰 Dividends",
    "🧩 Per-Asset Breakdown",
])


# ---------------------------------------------------------------------------
# Tab 1 — Current Portfolio (static weights)
# ---------------------------------------------------------------------------
with tab1:
    st.header("Current Portfolio — Value Over Time")
    st.caption(
        "Shows how your **current holdings** (fixed quantities) would have "
        "performed over the selected date range. Does not account for past "
        "buys/sells."
    )

    with st.spinner("Fetching market data..."):
        portfolio_values = compute_portfolio_value_over_time(positions, start_dt, end_dt)

    if not portfolio_values.empty:
        fig = px.line(
            portfolio_values.reset_index(),
            x="date",
            y="total",
            title="Total Portfolio Value (Static Weights)",
            labels={"total": "Value (€)", "date": "Date"},
        )
        fig.update_layout(hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)

        # Benchmark comparison
        st.subheader("Benchmark Comparison (S&P 500)")
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
        st.warning(
            "Could not compute portfolio value. "
            "Check that your positions have valid ticker symbols."
        )


# ---------------------------------------------------------------------------
# Tab 2 — Actual Performance (dynamic weights + TWR)
# ---------------------------------------------------------------------------
with tab2:
    st.header("Actual Performance — Time-Weighted Return")
    st.caption(
        "Reconstructs your **actual portfolio** from transaction history "
        "(all buys & sells). Uses **Time-Weighted Return (TWR)** to measure "
        "pure investment performance, removing the effect of cash deposits. "
        "This makes the comparison with S&P 500 fair."
    )

    with st.spinner("Loading transaction history..."):
        transactions = load_transactions(HISTORY_START, end_dt)
        deposits = load_deposits(HISTORY_START, end_dt)

    if not transactions.empty:
        with st.spinner("Computing actual portfolio value..."):
            actual_values = compute_actual_portfolio_value(
                transactions, start_dt, end_dt
            )

        if not actual_values.empty:
            # Show actual portfolio value over time
            fig_actual = px.line(
                actual_values.reset_index(),
                x="date",
                y="total",
                title="Actual Portfolio Value (Dynamic Weights)",
                labels={"total": "Value (€)", "date": "Date"},
            )
            fig_actual.update_layout(hovermode="x unified")
            st.plotly_chart(fig_actual, use_container_width=True)

            # Show deposits on the same chart for context
            if not deposits.empty:
                dep_in_range = deposits.copy()
                dep_in_range["date"] = dep_in_range["date"].dt.tz_localize(None)
                dep_in_range = dep_in_range[
                    (dep_in_range["date"] >= pd.Timestamp(start_dt))
                    & (dep_in_range["date"] <= pd.Timestamp(end_dt))
                ]
                if not dep_in_range.empty:
                    cumulative_deposits = dep_in_range.sort_values("date")
                    # Show cumulative deposits since HISTORY_START
                    all_deps = deposits.copy()
                    all_deps["date"] = all_deps["date"].dt.tz_localize(None)
                    all_deps = all_deps.sort_values("date")
                    all_deps["cumulative"] = all_deps["change"].cumsum()
                    # Filter to display range
                    all_deps_range = all_deps[
                        (all_deps["date"] >= pd.Timestamp(start_dt))
                        & (all_deps["date"] <= pd.Timestamp(end_dt))
                    ]

                    if not all_deps_range.empty:
                        fig_dep = go.Figure()
                        fig_dep.add_trace(
                            go.Scatter(
                                x=actual_values.reset_index()["date"],
                                y=actual_values["total"],
                                name="Portfolio Value",
                                mode="lines",
                            )
                        )
                        fig_dep.add_trace(
                            go.Scatter(
                                x=all_deps_range["date"],
                                y=all_deps_range["cumulative"],
                                name="Cumulative Deposits",
                                mode="lines",
                                line=dict(dash="dot"),
                            )
                        )
                        fig_dep.update_layout(
                            title="Portfolio Value vs. Total Invested",
                            xaxis_title="Date",
                            yaxis_title="€",
                            hovermode="x unified",
                        )
                        st.plotly_chart(fig_dep, use_container_width=True)

            # TWR vs S&P 500
            st.subheader("TWR: Portfolio vs S&P 500")
            st.caption(
                "Both lines start at 100. The gap shows pure investment "
                "performance, independent of when you deposited money."
            )
            twr_comparison = compute_actual_benchmark_comparison(
                actual_values, deposits, start_dt, end_dt
            )

            if not twr_comparison.empty:
                fig_twr = go.Figure()
                fig_twr.add_trace(
                    go.Scatter(
                        x=twr_comparison.index,
                        y=twr_comparison["Portfolio (TWR)"],
                        name="Portfolio (TWR)",
                        mode="lines",
                    )
                )
                fig_twr.add_trace(
                    go.Scatter(
                        x=twr_comparison.index,
                        y=twr_comparison["S&P 500"],
                        name="S&P 500",
                        mode="lines",
                        line=dict(dash="dash"),
                    )
                )
                fig_twr.update_layout(
                    title="Time-Weighted Return vs S&P 500 (Normalized to 100)",
                    xaxis_title="Date",
                    yaxis_title="Normalized Value",
                    hovermode="x unified",
                )
                st.plotly_chart(fig_twr, use_container_width=True)

                # Summary metrics
                col1, col2, col3 = st.columns(3)
                portfolio_return = twr_comparison["Portfolio (TWR)"].iloc[-1] - 100
                sp_return = twr_comparison["S&P 500"].iloc[-1] - 100
                alpha = portfolio_return - sp_return
                with col1:
                    st.metric("Portfolio TWR", f"{portfolio_return:+.1f}%")
                with col2:
                    st.metric("S&P 500 Return", f"{sp_return:+.1f}%")
                with col3:
                    st.metric(
                        "Alpha (vs S&P)",
                        f"{alpha:+.1f}pp",
                        delta=f"{alpha:+.1f}pp",
                        delta_color="normal",
                    )
            else:
                st.warning("Could not compute TWR comparison.")
        else:
            st.warning(
                "Could not compute actual portfolio value from transactions."
            )
    else:
        st.info("No transaction history found.")


# ---------------------------------------------------------------------------
# Tab 3 — Dividends
# ---------------------------------------------------------------------------
with tab3:
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
            labels={"Amount": "Dividend Amount (€)", "Month": "Month"},
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


# ---------------------------------------------------------------------------
# Tab 4 — Per-Asset Breakdown
# ---------------------------------------------------------------------------
with tab4:
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
                .rename(
                    columns={
                        "product_name": "Asset",
                        "value": "Value (€)",
                        "pct": "%",
                    }
                )
                .reset_index(drop=True),
                use_container_width=True,
            )
    else:
        st.warning("Could not compute asset breakdown.")
