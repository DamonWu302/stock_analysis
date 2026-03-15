from __future__ import annotations

from datetime import datetime, timedelta
import math
import random
from typing import Iterable

import pandas as pd


def generate_market_snapshot(size: int = 20) -> pd.DataFrame:
    random.seed(7)
    sectors = [
        ("AI", 2.8, 0.71),
        ("半导体", 1.9, 0.67),
        ("新能源", 1.2, 0.61),
        ("机器人", 3.1, 0.78),
        ("医药", 0.4, 0.48),
    ]
    rows = []
    for idx in range(size):
        code = f"{300000 + idx:06d}"
        sector, sector_change, sector_up_ratio = sectors[idx % len(sectors)]
        amount = random.randint(80_000_000, 700_000_000)
        inflow = amount * random.uniform(-0.03, 0.12)
        rows.append(
            {
                "symbol": code,
                "name": f"示例股票{idx + 1}",
                "latest_price": round(random.uniform(8, 88), 2),
                "pct_change": round(random.uniform(-3, 6), 2),
                "volume": random.randint(5_000_000, 60_000_000),
                "amount": amount,
                "sector": sector,
                "sector_change": sector_change,
                "sector_up_ratio": sector_up_ratio,
                "main_net_inflow": round(inflow, 2),
                "main_net_inflow_ratio": round(inflow / amount, 4),
            }
        )
    return pd.DataFrame(rows)


def generate_history(symbol: str, days: int = 180, end_date: datetime | None = None) -> pd.DataFrame:
    random.seed(int(symbol))
    end_date = end_date or datetime.now()
    start_price = random.uniform(8, 35)
    rows = []
    price = start_price

    for offset in range(days):
        day = end_date - timedelta(days=days - offset)
        if day.weekday() >= 5:
            continue

        trend = 0.0016 * offset
        seasonality = math.sin(offset / 6) * 0.015
        shock = random.uniform(-0.025, 0.03)
        pct_move = trend / max(days, 1) + seasonality + shock

        open_price = price
        close_price = max(2.5, price * (1 + pct_move))
        high_price = max(open_price, close_price) * (1 + random.uniform(0.003, 0.025))
        low_price = min(open_price, close_price) * (1 - random.uniform(0.003, 0.02))
        volume = random.randint(3_000_000, 12_000_000) * (1 + abs(pct_move) * 5)
        amount = volume * close_price

        rows.append(
            {
                "trade_date": day.date().isoformat(),
                "open": round(open_price, 2),
                "close": round(close_price, 2),
                "high": round(high_price, 2),
                "low": round(low_price, 2),
                "volume": round(volume, 2),
                "amount": round(amount, 2),
            }
        )
        price = close_price

    return pd.DataFrame(rows)


def generate_benchmark(days: int = 180) -> pd.DataFrame:
    history = generate_history("999999", days=days)
    history["symbol"] = "000001"
    history["name"] = "上证指数"
    return history
