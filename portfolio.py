"""Portfolio analytics: value over time, benchmark comparison, breakdowns."""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from market_data import get_price_history, get_sp500


# ---------------------------------------------------------------------------
# Tab 1 — "Current Portfolio" (static weights, existing logic)
# ---------------------------------------------------------------------------

def compute_portfolio_value_over_time(
    positions: pd.DataFrame,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    """Compute daily portfolio value based on current holdings and historical prices.

    This uses a simplified approach: takes current positions and prices them
    historically. For a more accurate P&L, transaction history would be needed.
    """
    if positions.empty or "symbol" not in positions.columns:
        return pd.DataFrame()

    valid = positions.dropna(subset=["symbol"])
    valid = valid[valid["symbol"].str.len() > 0]

    if valid.empty:
        return pd.DataFrame()

    daily_values = {}

    for _, row in valid.iterrows():
        symbol = row["symbol"]
        size = row.get("size", 0)
        if size == 0:
            continue

        exchange_id = row.get("exchange_id", "")
        isin = row.get("isin", "")

        try:
            prices = get_price_history(
                symbol, start, end,
                exchange_id=str(exchange_id),
                isin=str(isin),
            )
            if prices.empty:
                continue
            daily_values[symbol] = prices["Close"] * size
        except Exception:
            continue

    if not daily_values:
        return pd.DataFrame()

    df = pd.DataFrame(daily_values)
    df["total"] = df.sum(axis=1)
    df.index.name = "date"
    return df


def compute_benchmark_comparison(
    portfolio_values: pd.DataFrame,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    """Normalize portfolio and S&P 500 to percentage returns for comparison."""
    if portfolio_values.empty:
        return pd.DataFrame()

    sp500 = get_sp500(start, end)
    if sp500.empty:
        return pd.DataFrame()

    # Normalize both to 100 at start
    portfolio_series = portfolio_values["total"].dropna()
    sp500_series = sp500["Close"]

    # Align dates
    common_index = portfolio_series.index.intersection(sp500_series.index)
    if len(common_index) == 0:
        return pd.DataFrame()

    portfolio_aligned = portfolio_series.loc[common_index]
    sp500_aligned = sp500_series.loc[common_index]

    result = pd.DataFrame(
        {
            "Portfolio": (portfolio_aligned / portfolio_aligned.iloc[0]) * 100,
            "S&P 500": (sp500_aligned / sp500_aligned.iloc[0]) * 100,
        },
        index=common_index,
    )
    result.index.name = "date"
    return result


def compute_asset_breakdown(positions: pd.DataFrame) -> pd.DataFrame:
    """Compute current value breakdown by asset."""
    if positions.empty:
        return pd.DataFrame()

    required_cols = {"product_name", "value"}
    if not required_cols.issubset(positions.columns):
        # Try to compute value from size and price
        if "size" in positions.columns and "price" in positions.columns:
            positions = positions.copy()
            positions["value"] = positions["size"] * positions["price"]
        else:
            return pd.DataFrame()

    breakdown = positions[["product_name", "value"]].copy()
    breakdown = breakdown.dropna(subset=["value"])
    breakdown = breakdown.sort_values("value", ascending=False)
    breakdown["pct"] = (breakdown["value"] / breakdown["value"].sum()) * 100
    return breakdown


# ---------------------------------------------------------------------------
# Tab 2 — "Actual Performance" (dynamic weights from transaction history)
# ---------------------------------------------------------------------------

def _build_holdings_over_time(
    transactions: pd.DataFrame,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    """Build a daily holdings table from transaction history.

    Returns a DataFrame with date index and one column per productId,
    containing the number of shares held on that date.

    Important: computes cumulative holdings from the EARLIEST transaction,
    then slices to the requested [start, end] range.  This ensures that
    positions accumulated before `start` are correctly reflected.
    """
    if transactions.empty:
        return pd.DataFrame()

    txns = transactions.copy()
    txns["date"] = txns["date"].dt.normalize()

    # quantity is already signed: positive for buys, negative for sells
    txns["signed_qty"] = txns["quantity"]

    # Group by date and productId, sum quantities
    daily_trades = (
        txns.groupby(["date", "productId"])["signed_qty"]
        .sum()
        .unstack(fill_value=0)
    )

    # Ensure tz-naive index
    daily_trades.index = daily_trades.index.tz_localize(None)

    # Build full date range from EARLIEST transaction to `end`
    earliest = daily_trades.index.min()
    full_range = pd.date_range(start=earliest, end=end, freq="B")
    daily_trades = daily_trades.reindex(full_range, fill_value=0)

    # Cumulative sum gives holdings on each day
    holdings = daily_trades.cumsum()

    # Clamp negative holdings to 0 (can happen from share class conversions,
    # corporate actions not reflected in transaction history, etc.)
    holdings = holdings.clip(lower=0)

    # Slice to requested display range
    holdings = holdings.loc[
        (holdings.index >= pd.Timestamp(start))
        & (holdings.index <= pd.Timestamp(end))
    ]

    return holdings


def _get_product_info_map(transactions: pd.DataFrame) -> dict:
    """Extract product info (symbol, isin, exchange_id) from enriched transactions."""
    info_map = {}
    for _, row in transactions.drop_duplicates(subset="productId").iterrows():
        info_map[row["productId"]] = {
            "symbol": row.get("symbol", ""),
            "isin": row.get("isin", ""),
            "exchange_id": str(row.get("exchange_id", "")),
        }
    return info_map


def compute_actual_portfolio_value(
    transactions: pd.DataFrame,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    """Compute daily portfolio value from transaction history (dynamic weights).

    Returns DataFrame with columns: date (index), total, and per-product values.
    """
    holdings = _build_holdings_over_time(transactions, start, end)
    if holdings.empty:
        return pd.DataFrame()

    product_info = _get_product_info_map(transactions)

    daily_values = {}
    for product_id in holdings.columns:
        info = product_info.get(product_id, {})
        symbol = info.get("symbol", "")
        if not symbol:
            continue

        try:
            prices = get_price_history(
                symbol, start, end,
                exchange_id=info.get("exchange_id", ""),
                isin=info.get("isin", ""),
            )
            if prices.empty:
                continue

            # Align holdings and prices
            qty_series = holdings[product_id]
            price_series = prices["Close"]

            # Reindex to common dates
            common = qty_series.index.intersection(price_series.index)
            if len(common) == 0:
                continue

            daily_values[symbol] = qty_series.loc[common] * price_series.loc[common]
        except Exception:
            continue

    if not daily_values:
        return pd.DataFrame()

    df = pd.DataFrame(daily_values)
    df["total"] = df.sum(axis=1)
    df.index.name = "date"
    return df


def compute_twr(
    portfolio_values: pd.DataFrame,
    deposits: pd.DataFrame,
) -> pd.Series:
    """Compute Time-Weighted Return (TWR) series, normalized to 100.

    TWR eliminates the effect of cash flows (deposits/withdrawals) to measure
    pure investment performance. Uses the Modified Dietz method between cash
    flow events.

    Parameters:
        portfolio_values: DataFrame with 'total' column and date index
        deposits: DataFrame with 'date' and 'change' columns for cash deposits
    """
    if portfolio_values.empty:
        return pd.Series(dtype=float)

    total = portfolio_values["total"].dropna()
    if len(total) < 2:
        return pd.Series(dtype=float)

    # Build a series of cash flows indexed by date
    cash_flows = pd.Series(0.0, index=total.index)
    if not deposits.empty:
        dep = deposits.copy()
        dep["date"] = dep["date"].dt.normalize().dt.tz_localize(None)
        # Aggregate deposits per date
        dep_daily = dep.groupby("date")["change"].sum()
        # Align to portfolio dates (map to nearest business day)
        for dt, amount in dep_daily.items():
            # Find nearest date in portfolio index
            idx = total.index.get_indexer([pd.Timestamp(dt)], method="nearest")[0]
            if 0 <= idx < len(cash_flows):
                cash_flows.iloc[idx] += amount

    # Compute sub-period returns between cash flow events
    # TWR = product of (1 + r_i) for each sub-period
    twr_series = pd.Series(100.0, index=total.index)

    for i in range(1, len(total)):
        v_end = total.iloc[i]
        v_start = total.iloc[i - 1]
        cf = cash_flows.iloc[i]  # cash flow at end of period

        # Sub-period return: (V_end - CF) / V_start - 1
        if v_start > 0:
            r = (v_end - cf) / v_start - 1
        else:
            r = 0.0

        # Clamp extreme returns (data glitches)
        r = max(min(r, 0.5), -0.5)  # cap at ±50% daily

        twr_series.iloc[i] = twr_series.iloc[i - 1] * (1 + r)

    return twr_series


def compute_actual_benchmark_comparison(
    portfolio_values: pd.DataFrame,
    deposits: pd.DataFrame,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    """Compute TWR for actual portfolio vs S&P 500, both normalized to 100."""
    twr = compute_twr(portfolio_values, deposits)
    if twr.empty:
        return pd.DataFrame()

    sp500 = get_sp500(start, end)
    if sp500.empty:
        return pd.DataFrame()

    sp500_series = sp500["Close"]
    common_index = twr.index.intersection(sp500_series.index)
    if len(common_index) == 0:
        return pd.DataFrame()

    sp500_aligned = sp500_series.loc[common_index]
    sp500_normalized = (sp500_aligned / sp500_aligned.iloc[0]) * 100

    result = pd.DataFrame(
        {
            "Portfolio (TWR)": twr.loc[common_index],
            "S&P 500": sp500_normalized,
        },
        index=common_index,
    )
    result.index.name = "date"
    return result
