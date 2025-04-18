#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import List
import pandas as pd
from rich.console import Console
from rich.table import Table


@dataclass
class ETF:
    ticker: str
    name: str
    isin: str
    current_price: float


@dataclass
class PortfolioETF:
    etf: ETF
    current_holdings: int
    target_weight_percent: float


@dataclass
class Portfolio:
    items: List[PortfolioETF]


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
                "ISIN": etf.isin,
                "Target Weight (%)": item.target_weight_percent,
                "Current Price (EUR)": etf.current_price,
                "Current Holdings": item.current_holdings,
                "Current Value": current_val,
                "Target Value": target_val,
                "Additional Investment Needed": additional_needed,
                "Shares to Buy": shares_to_buy,
            }
        )

    return pd.DataFrame(results)


etf_list = [
    ETF("LCUJ", "Amundi MSCI Japan UCITS ETF Acc", "LU1781541252", 15.86),
    ETF("IMAE", "iShares Core MSCI Europe UCITS ETF Acc", "IE00B4K48X80", 78.58),
    ETF(
        "CPXJ",
        "iShares Core MSCI Pacific ex Japan UCITS ETF Acc",
        "IE00B52MJY50",
        160.12,
    ),
    ETF("SXR2", "iShares MSCI Canada UCITS ETF Acc", "IE00B52SF786", 184.92),
]

etf_map = {etf.ticker: etf for etf in etf_list}

portfolio_items = [
    PortfolioETF(etf_map["IMAE"], 12, 58),
    PortfolioETF(etf_map["LCUJ"], 0, 22),
    PortfolioETF(etf_map["CPXJ"], 0, 12),
    PortfolioETF(etf_map["SXR2"], 0, 8),
]

portfolio = Portfolio(items=portfolio_items)
rebalance_result = rebalance_portfolio(portfolio, new_investment_amount=50000)

console = Console()
table = Table(show_header=True, header_style="bold magenta")

for column in rebalance_result.columns:
    table.add_column(column.replace(" ", "\n"))

for _, row in rebalance_result.iterrows():
    table.add_row(
        *[f"{val:.0f}" if isinstance(val, float) else str(val) for val in row]
    )

console.print(table)
