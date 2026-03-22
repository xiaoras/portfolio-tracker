"""Market data via yfinance with DEGIRO exchange mapping."""

from datetime import datetime

import pandas as pd
import yfinance as yf

# Map DEGIRO exchangeId to yfinance ticker suffix (primary, then fallbacks)
# See https://help.yahoo.com/kb/exchanges-data-providers-yahoo-finance-sln2310.html
DEGIRO_EXCHANGE_TO_YF_SUFFIXES = {
    "194": [".AS", ".DE"],       # Euronext Amsterdam (many ETFs only on .DE)
    "196": [".PA", ".DE"],       # Euronext Paris (some only on .DE)
    "200": [".BR", ".DE"],       # Euronext Brussels
    "206": [".L"],               # London Stock Exchange
    "208": [".MI"],              # Borsa Italiana (Milan)
    "210": [".MC"],              # Bolsa de Madrid
    "212": [".LS"],              # Euronext Lisbon
    "302": [".VI"],              # Wiener Börse (Vienna)
    "390": [".SW"],              # SIX Swiss Exchange
    "454": [".HK"],              # Hong Kong Stock Exchange
    "570": [".L", ".DE"],        # London (international segment)
    "608": [".DE"],              # XETRA (Frankfurt)
    "616": [".F", ".DE"],        # Frankfurt Börse
    "663": [""],                 # NASDAQ / NYSE (US)
    "676": [""],                 # NYSE
    "710": [".PA", ".DE"],       # Euronext Paris (DEGIRO exch 710)
    "712": [".DE"],              # EUWAX (Stuttgart)
    "892": [".TO"],              # Toronto Stock Exchange
    "1001": [".TO"],             # Toronto Stock Exchange
    "1006": [".PA"],             # Euronext derivatives
}


def _prepare_symbol(symbol: str, suffix: str) -> str:
    """Apply exchange-specific symbol transformations."""
    # Toronto tickers: dots become dashes in yfinance (e.g. SRU.UN -> SRU-UN)
    if suffix == ".TO" and "." in symbol:
        symbol = symbol.replace(".", "-")

    # Hong Kong tickers: yfinance expects 4-digit zero-padded
    if suffix == ".HK":
        try:
            symbol = str(int(symbol)).zfill(4)
        except ValueError:
            pass

    return symbol + suffix


def resolve_yf_ticker_candidates(symbol: str, exchange_id: str = "") -> list[str]:
    """Return a list of yfinance ticker candidates to try, in priority order."""
    suffixes = DEGIRO_EXCHANGE_TO_YF_SUFFIXES.get(str(exchange_id), [""])
    candidates = [_prepare_symbol(symbol, s) for s in suffixes]

    # Always add plain symbol as last fallback
    if symbol not in candidates:
        candidates.append(symbol)

    return candidates


def get_price_history(
    symbol: str,
    start: datetime,
    end: datetime,
    exchange_id: str = "",
    isin: str = "",
) -> pd.DataFrame:
    """Fetch historical prices, trying exchange-mapped tickers first, ISIN as last resort."""
    candidates = resolve_yf_ticker_candidates(symbol, exchange_id)

    # Add ISIN as final fallback (may return wrong exchange/currency,
    # but better than no data at all)
    if isin and isin not in candidates:
        candidates.append(isin)

    for yf_ticker in candidates:
        try:
            df = yf.Ticker(yf_ticker).history(start=start, end=end)
            if not df.empty:
                df.index = df.index.tz_localize(None)
                return df
        except Exception:
            continue

    return pd.DataFrame()


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
