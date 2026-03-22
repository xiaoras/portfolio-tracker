"""DEGIRO broker integration via degiro-connector."""

import os
from datetime import datetime, timedelta

import pandas as pd
from dotenv import load_dotenv
from degiro_connector.trading.api import API as TradingAPI
from degiro_connector.trading.models.credentials import build_credentials
from degiro_connector.trading.models.account import (
    UpdateRequest as AccountUpdateRequest,
    UpdateOption as AccountUpdateOption,
)
from degiro_connector.trading.models.transaction import (
    TransactionsRequest,
)

load_dotenv()


def get_credentials():
    creds_dict = {
        "int_account": None,
        "username": os.getenv("DEGIRO_USERNAME"),
        "password": os.getenv("DEGIRO_PASSWORD"),
    }
    totp_secret = os.getenv("DEGIRO_TOTP_SECRET")
    if totp_secret:
        creds_dict["totp_secret_key"] = totp_secret
    return build_credentials(override=creds_dict)


def connect() -> TradingAPI:
    """Create and connect a DEGIRO trading session."""
    credentials = get_credentials()
    trading_api = TradingAPI(credentials=credentials)
    trading_api.connect()
    return trading_api


def get_portfolio(trading_api: TradingAPI) -> pd.DataFrame:
    """Fetch current portfolio positions."""
    portfolio = trading_api.get_update(
        request_list=[
            AccountUpdateRequest(
                option=AccountUpdateOption.PORTFOLIO,
                last_updated=0,
            ),
        ],
        raw=True,
    )

    if not portfolio or "portfolio" not in portfolio:
        return pd.DataFrame()

    positions = portfolio["portfolio"]["value"]
    records = []
    for pos in positions:
        row = {}
        for item in pos.get("value", []):
            row[item["name"]] = item.get("value")
        if row.get("size", 0) != 0:
            records.append(row)

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
        for pid, info in products_info["data"].items():
            id_to_name[int(pid)] = info.get("name", "Unknown")
            id_to_symbol[int(pid)] = info.get("symbol", "")
            id_to_currency[int(pid)] = info.get("currency", "")

        df["product_name"] = df["id"].astype(int).map(id_to_name)
        df["symbol"] = df["id"].astype(int).map(id_to_symbol)
        df["currency"] = df["id"].astype(int).map(id_to_currency)

    return df


def get_transactions(
    trading_api: TradingAPI,
    from_date: datetime,
    to_date: datetime,
) -> pd.DataFrame:
    """Fetch transaction history."""
    request = TransactionsRequest(
        from_date=from_date,
        to_date=to_date,
    )
    transactions = trading_api.get_transactions(request=request, raw=True)

    if not transactions:
        return pd.DataFrame()

    df = pd.DataFrame(transactions)
    if not df.empty and "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df


def get_account_overview(
    trading_api: TradingAPI,
    from_date: datetime,
    to_date: datetime,
) -> pd.DataFrame:
    """Fetch account overview including dividends."""
    account_overview = trading_api.get_account_overview(
        from_date=from_date,
        to_date=to_date,
        raw=True,
    )

    if not account_overview or "cashMovements" not in account_overview:
        return pd.DataFrame()

    df = pd.DataFrame(account_overview["cashMovements"])
    if not df.empty and "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
    return df


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
