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


def get_etf_definition(ticker: str) -> ETF | None:
    """Get ETF definition by ticker. Single source of truth for ETF data."""
    fallback_prices = {
        "LCUJ": 15.86,
        "IMAE": 78.58,
        "CPXJ": 160.12,
        "SXR2": 184.92,
    }

    definitions = {
        "LCUJ": ETF(
            "LCUJ",
            "Amundi MSCI Japan UCITS ETF Acc",
            "LU1781541252",
            fallback_prices["LCUJ"],  # Use fallback price as initial
            "LCUJ.AS",
            "EUR",
        ),
        "IMAE": ETF(
            "IMAE",
            "iShares Core MSCI Europe UCITS ETF Acc",
            "IE00B4K48X80",
            fallback_prices["IMAE"],
            "IMAE.AS",
            "EUR",
        ),
        "CPXJ": ETF(
            "CPXJ",
            "iShares Core MSCI Pacific ex Japan UCITS ETF Acc",
            "IE00B52MJY50",
            fallback_prices["CPXJ"],
            "CPXJ.AS",
            "EUR",
        ),
        "SXR2": ETF(
            "SXR2",
            "iShares MSCI Canada UCITS ETF Acc",
            "IE00B52SF786",
            fallback_prices["SXR2"],
            "SXR2.DE",
            "EUR",
        ),
    }
    return definitions.get(ticker)


def get_etf_price(ticker: str, fallback: float) -> float:
    """Get ETF price from cache or Yahoo Finance."""
    row = ETFTable.get(Query().ticker == ticker)
    if (
        row
        and isinstance(row, dict)
        and not is_expired(datetime.fromisoformat(row["timestamp"]))
    ):
        return row["value"]

    # Try to fetch from Yahoo Finance if data is expired or missing
    etf = get_etf_definition(ticker)
    if etf and etf.yahoo_ticker:
        try:
            yf_ticker = yf.Ticker(etf.yahoo_ticker)
            latest_price = yf_ticker.history(period="1d")["Close"].iloc[-1]
            save_etf_price(ticker, latest_price)
            return latest_price
        except Exception as e:
            if row and isinstance(row, dict):  # Return stale data if fetch fails
                print(
                    f"WARNING: Could not fetch price for {ticker}, using stale data: {e}"
                )
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
    if not ts:
        return True
    if not ts.tzinfo:
        ts = ts.replace(tzinfo=UTC)
    return datetime.now(UTC) - ts > timedelta(minutes=max_age_minutes)


def fetch_yahoo_price(etf: ETF) -> float:
    """Fetch latest price from Yahoo Finance for given ETF."""
    yf_ticker = yf.Ticker(etf.yahoo_ticker)
    latest_price = yf_ticker.history(period="1d")["Close"].iloc[-1]
    save_etf_price(etf.ticker, latest_price)
    return latest_price


def rebalance_portfolio(
    portfolio: Portfolio, new_investment_amount: float, target_currency: str
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


def get_etf_list(target_currency: str) -> List[ETF]:
    """Get list of ETFs with current prices."""
    etfs: List[ETF] = []
    for ticker in ["LCUJ", "IMAE", "CPXJ", "SXR2"]:
        etf = get_etf_definition(ticker)
        if etf is not None:
            etfs.append(etf)

    # Set current prices
    for etf in etfs:
        etf.current_price = get_etf_price(
            etf.ticker, fallback=etf.current_price
        )  # Use initial price as fallback

    # Apply FX rates
    for etf in etfs:
        row = FXTable.get(Query().pair == f"{etf.currency}{target_currency}")
        if row and isinstance(row, dict):
            ts = datetime.fromisoformat(row["timestamp"]) if row else None
            if is_expired(ts):
                fx_rate = get_fx_rate(etf.currency, target_currency)
            else:
                fx_rate = row["value"]
        else:
            fx_rate = 1.0
        etf.current_price *= fx_rate

    return etfs


def get_portfolio_items(etf_map) -> List[PortfolioETF]:
    return [
        PortfolioETF(etf_map["IMAE"], 1180, 58),
        PortfolioETF(etf_map["LCUJ"], 0, 22),
        PortfolioETF(etf_map["CPXJ"], 0, 12),
        PortfolioETF(etf_map["SXR2"], 0, 8),
    ]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--fetch",
        action="store_true",
        help="Fetch latest ETF prices from Yahoo Finance",
    )
    parser.add_argument(
        "--target-currency",
        type=str,
        default="EUR",
        help="Target currency for output values (default: EUR)",
    )
    return parser.parse_args()


def main():
    TO_INVEST = 200000 - 51000 - 50000  # Amount to invest in EUR
    args = parse_args()
    target_currency = args.target_currency.upper()

    etf_list = get_etf_list(target_currency)
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

    portfolio_items = get_portfolio_items(etf_map)
    portfolio = Portfolio(items=portfolio_items)
    rebalance_result = rebalance_portfolio(
        portfolio, new_investment_amount=TO_INVEST, target_currency=target_currency
    )
    print(f"Total value: {rebalance_result['Target Value'].sum():.0f} EUR")
    print(f"To invest: {TO_INVEST:.0f} EUR")
    print_result(rebalance_result)


if __name__ == "__main__":
    main()
