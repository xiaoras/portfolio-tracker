"""DEGIRO broker integration via degiro-connector v3."""

import os
from datetime import datetime, date

import pandas as pd
from dotenv import load_dotenv
from degiro_connector.trading.api import API as TradingAPI
from degiro_connector.trading.models.credentials import Credentials
from degiro_connector.trading.models.account import (
    UpdateRequest,
    UpdateOption,
    OverviewRequest,
)
from degiro_connector.trading.models.transaction import HistoryRequest

load_dotenv()


def get_credentials() -> Credentials:
    totp_secret = os.getenv("DEGIRO_TOTP_SECRET")
    return Credentials(
        username=os.getenv("DEGIRO_USERNAME"),
        password=os.getenv("DEGIRO_PASSWORD"),
        totp_secret_key=totp_secret if totp_secret else None,
    )


def connect() -> TradingAPI:
    """Create and connect a DEGIRO trading session."""
    credentials = get_credentials()
    trading_api = TradingAPI(credentials=credentials)
    trading_api.connect()

    # Fetch and set int_account (required for most API calls)
    client_details = trading_api.get_client_details()
    if client_details and "data" in client_details:
        int_account = client_details["data"].get("intAccount")
        trading_api.credentials.int_account = int_account

    return trading_api


def get_portfolio(trading_api: TradingAPI) -> pd.DataFrame:
    """Fetch current portfolio positions."""
    update = trading_api.get_update(
        request_list=[
            UpdateRequest(
                option=UpdateOption.PORTFOLIO,
                last_updated=0,
            ),
        ],
        raw=True,
    )

    if not update or "portfolio" not in update:
        return pd.DataFrame()

    positions = update["portfolio"]["value"]
    records = []
    for pos in positions:
        row = {}
        for item in pos.get("value", []):
            row[item["name"]] = item.get("value")
        # Keep only actual product positions (numeric id, non-zero size)
        if row.get("size", 0) != 0 and row.get("positionType") == "PRODUCT":
            try:
                int(row.get("id", ""))
                records.append(row)
            except (ValueError, TypeError):
                continue

    df = pd.DataFrame(records)
    if df.empty:
        return df

    # Enrich with product names
    product_ids = df["id"].astype(int).tolist()
    products_info = trading_api.get_products_info(
        product_list=product_ids, raw=True
    )

    if products_info and "data" in products_info:
        id_to_name = {}
        id_to_symbol = {}
        id_to_currency = {}
        id_to_isin = {}
        id_to_exchange = {}
        for pid, info in products_info["data"].items():
            id_to_name[int(pid)] = info.get("name", "Unknown")
            id_to_symbol[int(pid)] = info.get("symbol", "")
            id_to_currency[int(pid)] = info.get("currency", "")
            id_to_isin[int(pid)] = info.get("isin", "")
            id_to_exchange[int(pid)] = info.get("exchangeId", "")

        df["product_name"] = df["id"].astype(int).map(id_to_name)
        df["symbol"] = df["id"].astype(int).map(id_to_symbol)
        df["currency"] = df["id"].astype(int).map(id_to_currency)
        df["isin"] = df["id"].astype(int).map(id_to_isin)
        df["exchange_id"] = df["id"].astype(int).map(id_to_exchange)

    return df


def get_transactions(
    trading_api: TradingAPI,
    from_date: datetime,
    to_date: datetime,
) -> pd.DataFrame:
    """Fetch transaction history."""
    request = HistoryRequest(
        from_date=date(from_date.year, from_date.month, from_date.day),
        to_date=date(to_date.year, to_date.month, to_date.day),
    )
    result = trading_api.get_transactions_history(
        transaction_request=request, raw=True
    )

    if not result or "data" not in result:
        return pd.DataFrame()

    df = pd.DataFrame(result["data"])
    if not df.empty and "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], utc=True)
    return df


def get_account_overview(
    trading_api: TradingAPI,
    from_date: datetime,
    to_date: datetime,
) -> pd.DataFrame:
    """Fetch account overview including dividends."""
    overview_request = OverviewRequest(
        from_date=date(from_date.year, from_date.month, from_date.day),
        to_date=date(to_date.year, to_date.month, to_date.day),
    )
    result = trading_api.get_account_overview(
        overview_request=overview_request, raw=True
    )

    # The response nests data under "data" key
    if not result:
        return pd.DataFrame()

    data = result.get("data", result)  # handle both nested and flat
    if "cashMovements" not in data:
        return pd.DataFrame()

    df = pd.DataFrame(data["cashMovements"])
    if not df.empty and "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], utc=True)
    return df


def get_transactions_enriched(
    trading_api: TradingAPI,
    from_date: datetime,
    to_date: datetime,
) -> pd.DataFrame:
    """Fetch transaction history enriched with product info (symbol, isin, exchange)."""
    txns = get_transactions(trading_api, from_date, to_date)
    if txns.empty:
        return txns

    product_ids = txns["productId"].unique().tolist()
    products_info = trading_api.get_products_info(
        product_list=product_ids, raw=True
    )

    if products_info and "data" in products_info:
        pid_map = {}
        for pid, info in products_info["data"].items():
            pid_map[int(pid)] = {
                "symbol": info.get("symbol", ""),
                "product_name": info.get("name", ""),
                "isin": info.get("isin", ""),
                "exchange_id": info.get("exchangeId", ""),
                "product_currency": info.get("currency", ""),
            }

        for col in ["symbol", "product_name", "isin", "exchange_id", "product_currency"]:
            txns[col] = txns["productId"].map(
                lambda pid, c=col: pid_map.get(int(pid), {}).get(c, "")
            )

    return txns


def get_cash_deposits(
    trading_api: TradingAPI,
    from_date: datetime,
    to_date: datetime,
) -> pd.DataFrame:
    """Extract external cash deposits from account overview."""
    df = get_account_overview(trading_api, from_date, to_date)
    if df.empty:
        return df

    # "flatex Deposit" entries are actual bank transfers into the account
    deposit_mask = df["description"].str.contains(
        "flatex Deposit", case=False, na=False
    )
    deposits = df[deposit_mask].copy()
    if not deposits.empty:
        deposits = deposits[deposits["currency"] == "EUR"]
    return deposits


def get_dividends(
    trading_api: TradingAPI,
    from_date: datetime,
    to_date: datetime,
) -> pd.DataFrame:
    """Extract dividend payments from account overview."""
    df = get_account_overview(trading_api, from_date, to_date)
    if df.empty:
        return df

    # Filter for dividend-related entries
    dividend_mask = df["description"].str.contains(
        "dividend|Dividend|DIVIDEND", case=False, na=False
    )
    dividends = df[dividend_mask].copy()
    return dividends
