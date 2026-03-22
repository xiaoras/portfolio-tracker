"""Portfolio analytics: value over time, benchmark comparison, breakdowns."""

from datetime import datetime, timedelta

import pandas as pd

from market_data import get_price_history, get_sp500


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

        try:
            prices = get_price_history(symbol, start, end)
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
