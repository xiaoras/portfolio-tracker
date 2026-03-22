"""Market data via yfinance."""

from datetime import datetime

import pandas as pd
import yfinance as yf


def get_price_history(
    symbol: str,
    start: datetime,
    end: datetime,
) -> pd.DataFrame:
    """Fetch historical prices for a given ticker symbol."""
    ticker = yf.Ticker(symbol)
    df = ticker.history(start=start, end=end)
    if not df.empty:
        df.index = df.index.tz_localize(None)
    return df


def get_sp500(start: datetime, end: datetime) -> pd.DataFrame:
    """Fetch S&P 500 index data for benchmarking."""
    return get_price_history("^GSPC", start, end)


def get_prices_for_symbols(
    symbols: list[str],
    start: datetime,
    end: datetime,
) -> dict[str, pd.DataFrame]:
    """Fetch historical prices for multiple symbols."""
    result = {}
    for symbol in symbols:
        try:
            df = get_price_history(symbol, start, end)
            if not df.empty:
                result[symbol] = df
        except Exception:
            continue
    return result
