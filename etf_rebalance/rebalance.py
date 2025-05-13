#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import List
import pandas as pd
from rich.console import Console
from rich.table import Table
import argparse
import yfinance as yf  # type: ignore
import os
from tinydb import TinyDB, Query
from datetime import datetime, timedelta, UTC
import time


TO_INVEST = 160000 - 51000  # Amount to invest in EUR
CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache_db")

os.makedirs(CACHE_DIR, exist_ok=True)
db = TinyDB(os.path.join(CACHE_DIR, "db.json"))
ETFTable = db.table("etf_prices")
FXTable = db.table("fx_rates")


@dataclass
class ETF:
    ticker: str
    name: str
    isin: str
    current_price: float
    yahoo_ticker: str
    currency: str


@dataclass
class PortfolioETF:
    etf: ETF
    current_holdings: int
    target_weight_percent: float


@dataclass
class Portfolio:
    items: List[PortfolioETF]


def save_etf_price(ticker, price):
    ETFTable.upsert(
        {"ticker": ticker, "value": price, "timestamp": datetime.now(UTC).isoformat()},
        Query().ticker == ticker,
    )


def get_etf_price(ticker, fallback):
    row = ETFTable.get(Query().ticker == ticker)
    if row and not is_expired(datetime.fromisoformat(row["timestamp"])):
        return row["value"]

    # Try to fetch from Yahoo Finance if data is expired or missing
    etf = next((e for e in etf_list if e.ticker == ticker), None)
    if etf and etf.yahoo_ticker:
        try:
            return fetch_yahoo_price(etf)
        except Exception:
            if row:  # Return stale data if fetch fails
                print(f"WARNING: Could not fetch price for {ticker}, using stale data.")
                return row["value"]

    return fallback


def save_fx_rate(pair, rate):
    FXTable.upsert(
        {"pair": pair, "value": rate, "timestamp": datetime.now(UTC).isoformat()},
        Query().pair == pair,
    )


def get_fx_rate(base: str, target: str) -> float:
    if base == target:
        return 1.0
    pair = f"{base}{target}"
    row = FXTable.get(Query().pair == pair)
    assert isinstance(row, dict)
    if row and not is_expired(datetime.fromisoformat(row["timestamp"])):
        return row["value"]
    try:
        fx_ticker = yf.Ticker(f"{pair}=X")
        rate = fx_ticker.history(period="1d")["Close"].iloc[-1]
        save_fx_rate(pair, rate)
        return rate
    except Exception:
        print(f"WARNING: Could not fetch FX {base}->{target}, using stale or 1.0.")
        return row["value"] if row else 1.0


def is_expired(ts, max_age_minutes=60):
    return not ts or (datetime.now(UTC) - ts > timedelta(minutes=max_age_minutes))


def fetch_yahoo_price(etf: ETF) -> float:
    """Fetch latest price from Yahoo Finance for given ETF."""
    yf_ticker = yf.Ticker(etf.yahoo_ticker)
    latest_price = yf_ticker.history(period="1d")["Close"].iloc[-1]
    save_etf_price(etf.ticker, latest_price)
    return latest_price


def rebalance_portfolio(
    portfolio: Portfolio, new_investment_amount: float
) -> pd.DataFrame:
    current_total_value = sum(
        item.etf.current_price * item.current_holdings for item in portfolio.items
    )
    future_total_value = current_total_value + new_investment_amount

    results = []

    for item in portfolio.items:
        etf = item.etf
        current_val = etf.current_price * item.current_holdings
        target_val = item.target_weight_percent / 100 * future_total_value
        additional_needed = target_val - current_val
        shares_to_buy = max(0, int(additional_needed // etf.current_price))

        results.append(
            {
                "ETF": etf.name,
                "Ticker": etf.ticker,
                "ISIN": etf.isin,
                "Target Weight (%)": item.target_weight_percent,
                f"Current Price ({target_currency})": etf.current_price,
                "Current Holdings": item.current_holdings,
                "Current Value": current_val,
                "Target Value": target_val,
                "Additional Investment Needed": additional_needed,
                "Shares to Buy": shares_to_buy,
            }
        )

    return pd.DataFrame(results)


parser = argparse.ArgumentParser()
parser.add_argument(
    "--fetch", action="store_true", help="Fetch latest ETF prices from Yahoo Finance"
)
parser.add_argument(
    "--target-currency",
    type=str,
    default="EUR",
    help="Target currency for output values (default: EUR)",
)
args = parser.parse_args()

target_currency = args.target_currency.upper()


etf_list = [
    ETF(
        "LCUJ",
        "Amundi MSCI Japan UCITS ETF Acc",
        "LU1781541252",
        get_etf_price("LCUJ", 15.86),
        "LCUJ.AS",
        "EUR",
    ),
    ETF(
        "IMAE",
        "iShares Core MSCI Europe UCITS ETF Acc",
        "IE00B4K48X80",
        get_etf_price("IMAE", 78.58),
        "IMAE.AS",
        "EUR",
    ),
    ETF(
        "CPXJ",
        "iShares Core MSCI Pacific ex Japan UCITS ETF Acc",
        "IE00B52MJY50",
        get_etf_price("CPXJ", 160.12),
        "CPXJ.AS",
        "EUR",
    ),
    ETF(
        "SXR2",
        "iShares MSCI Canada UCITS ETF Acc",
        "IE00B52SF786",
        get_etf_price("SXR2", 184.92),
        "SXR2.DE",
        "EUR",
    ),
]

etf_map = {etf.ticker: etf for etf in etf_list}

updated_prices = {}

if args.fetch:
    print("Fetching latest prices from Yahoo Finance...")
    for etf in etf_list:
        try:
            latest_price = fetch_yahoo_price(etf)
            print(f"{etf.ticker}: {latest_price:.2f} EUR")
            updated_prices[etf.ticker] = latest_price
        except Exception:
            continue
        time.sleep(1.1)
    if updated_prices:
        print("Saving updated prices to cache.")

for etf in etf_list:
    row = FXTable.get(Query().pair == f"{etf.currency}{target_currency}")
    assert row is None or isinstance(row, dict)
    ts = datetime.fromisoformat(row["timestamp"]) if row else None
    if is_expired(ts):
        fx_rate = get_fx_rate(etf.currency, target_currency)
    else:
        fx_rate = row["value"] if row else 1.0
    etf.current_price *= fx_rate


portfolio_items = [
    PortfolioETF(etf_map["IMAE"], 600, 58),
    PortfolioETF(etf_map["LCUJ"], 0, 22),
    PortfolioETF(etf_map["CPXJ"], 0, 12),
    PortfolioETF(etf_map["SXR2"], 0, 8),
]


def print_result(res: pd.DataFrame):
    console = Console()
    table = Table(show_header=True, header_style="bold magenta")

    for column in res.columns:
        table.add_column(column.replace(" ", "\n"))

    for _, row in res.iterrows():
        table.add_row(
            *[f"{val:.0f}" if isinstance(val, float) else str(val) for val in row]
        )

    console.print(table)


portfolio = Portfolio(items=portfolio_items)
rebalance_result = rebalance_portfolio(portfolio, new_investment_amount=TO_INVEST)

print_result(rebalance_result)
