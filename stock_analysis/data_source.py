from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
import os

import pandas as pd
import requests
from requests.adapters import HTTPAdapter

from .config import settings
from .sample_data import generate_benchmark, generate_history, generate_market_snapshot


class MarketDataProvider(ABC):
    name = "base"

    @abstractmethod
    def fetch_market_snapshot(self, limit: int) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def fetch_stock_history(self, symbol: str, days: int) -> pd.DataFrame:
        raise NotImplementedError

    def fetch_stock_history_since(self, symbol: str, start_date: str, days: int) -> pd.DataFrame:
        return self.fetch_stock_history(symbol=symbol, days=days)

    @abstractmethod
    def fetch_benchmark_history(self, days: int) -> pd.DataFrame:
        raise NotImplementedError

    def latest_trade_date(self) -> str:
        return datetime.now().date().isoformat()


class MockDataProvider(MarketDataProvider):
    name = "mock"

    def fetch_market_snapshot(self, limit: int) -> pd.DataFrame:
        return generate_market_snapshot(size=limit or 30)

    def fetch_stock_history(self, symbol: str, days: int) -> pd.DataFrame:
        return generate_history(symbol=symbol, days=days)

    def fetch_stock_history_since(self, symbol: str, start_date: str, days: int) -> pd.DataFrame:
        history = generate_history(symbol=symbol, days=days)
        return history[history["trade_date"] >= start_date].reset_index(drop=True)

    def fetch_benchmark_history(self, days: int) -> pd.DataFrame:
        return generate_benchmark(days=days)

    def latest_trade_date(self) -> str:
        history = generate_benchmark(days=5)
        return str(history["trade_date"].iloc[-1])


class AkshareDataProvider(MarketDataProvider):
    name = "akshare"

    def __init__(self) -> None:
        try:
            import akshare as ak
            from akshare.utils import func as ak_func
        except ImportError as exc:
            raise RuntimeError("AKShare 未安装，请先执行 pip install -r requirements.txt。") from exc

        self.ak = ak
        self.ak_func = ak_func
        self._configure_network()

    def _configure_network(self) -> None:
        proxy = settings.akshare_proxy
        if settings.disable_system_proxy and not proxy:
            for key in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"]:
                os.environ.pop(key, None)

        def request_without_broken_proxy(
            url: str,
            params: dict | None = None,
            timeout: int = 15,
            max_retries: int = 3,
            base_delay: float = 1.0,
            random_delay_range: tuple[float, float] = (0.5, 1.5),
        ) -> requests.Response:
            last_exception = None
            for attempt in range(max_retries):
                try:
                    with requests.Session() as session:
                        session.trust_env = not settings.disable_system_proxy and not proxy
                        session.proxies = {"http": proxy, "https": proxy} if proxy else {}
                        adapter = HTTPAdapter(pool_connections=1, pool_maxsize=1)
                        session.mount("http://", adapter)
                        session.mount("https://", adapter)
                        response = session.get(
                            url,
                            params=params,
                            timeout=timeout,
                            headers={"User-Agent": "Mozilla/5.0"},
                        )
                        response.raise_for_status()
                        return response
                except (requests.RequestException, ValueError) as exc:
                    last_exception = exc
                    if attempt < max_retries - 1:
                        delay = base_delay * (2**attempt) + self.ak_func.random.uniform(*random_delay_range)
                        self.ak_func.time.sleep(delay)
            raise last_exception

        self.ak_func.request_with_retry = request_without_broken_proxy

    def fetch_market_snapshot(self, limit: int) -> pd.DataFrame:
        raise RuntimeError("当前网络环境下 AKShare 实时快照不稳定，请优先使用 baostock 数据源。")

    def fetch_stock_history(self, symbol: str, days: int) -> pd.DataFrame:
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - pd.Timedelta(days=days * 2)).strftime("%Y%m%d")
        return self._fetch_stock_history_range(symbol=symbol, start_date=start_date, end_date=end_date, days=days)

    def fetch_stock_history_since(self, symbol: str, start_date: str, days: int) -> pd.DataFrame:
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = pd.to_datetime(start_date).strftime("%Y%m%d")
        return self._fetch_stock_history_range(symbol=symbol, start_date=start_date, end_date=end_date, days=days)

    def _fetch_stock_history_range(self, symbol: str, start_date: str, end_date: str, days: int) -> pd.DataFrame:
        df = self.ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq",
        )
        rename_map = {
            "日期": "trade_date",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
            "成交额": "amount",
        }
        df = df.rename(columns=rename_map)
        df = df[list(rename_map.values())].tail(days).reset_index(drop=True)
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date.astype(str)
        return df

    def fetch_benchmark_history(self, days: int) -> pd.DataFrame:
        return generate_benchmark(days=days)

    def latest_trade_date(self) -> str:
        history = generate_benchmark(days=5)
        return str(history["trade_date"].iloc[-1])


class BaostockDataProvider(MarketDataProvider):
    name = "baostock"

    def __init__(self) -> None:
        try:
            import baostock as bs
        except ImportError as exc:
            raise RuntimeError("baostock 未安装，请先执行 pip install baostock。") from exc
        self.bs = bs
        self._login()
        self._industry_map = self._load_industry_map()
        self._latest_trade_day: str | None = None

    def __del__(self) -> None:
        try:
            self.bs.logout()
        except Exception:
            pass

    def _login(self) -> None:
        result = self.bs.login()
        if result.error_code != "0":
            raise RuntimeError(f"baostock 登录失败：{result.error_msg}")

    def fetch_market_snapshot(self, limit: int) -> pd.DataFrame:
        trading_day, rows = self._query_all_stock_with_fallback()
        self._latest_trade_day = trading_day
        frame = pd.DataFrame(rows, columns=["symbol", "trade_status", "name"])
        if frame.empty:
            raise RuntimeError(f"baostock 在 {trading_day} 没有返回股票列表。")

        frame = frame[frame["symbol"].map(self._is_target_a_share)].copy()
        frame["symbol"] = frame["symbol"].str.split(".").str[-1]
        frame["name"] = frame["name"].fillna("")
        frame = frame[~frame["name"].str.contains("ST", case=False, na=False)].copy()
        frame["sector"] = frame["symbol"].map(self._industry_map).fillna("未分类")
        frame["latest_price"] = None
        frame["pct_change"] = None
        frame["volume"] = None
        frame["amount"] = None
        frame["sector_change"] = 0.0
        frame["sector_up_ratio"] = 0.0
        frame["main_net_inflow"] = 0.0
        frame["main_net_inflow_ratio"] = 0.0

        if limit and limit > 0:
            frame = frame.head(limit)
        return frame.reset_index(drop=True)

    def fetch_stock_history(self, symbol: str, days: int) -> pd.DataFrame:
        start_date = (datetime.now() - timedelta(days=days * 2)).strftime("%Y-%m-%d")
        end_date = datetime.now().strftime("%Y-%m-%d")
        return self._fetch_stock_history_range(symbol=symbol, start_date=start_date, end_date=end_date, days=days)

    def fetch_stock_history_since(self, symbol: str, start_date: str, days: int) -> pd.DataFrame:
        end_date = datetime.now().strftime("%Y-%m-%d")
        return self._fetch_stock_history_range(symbol=symbol, start_date=start_date, end_date=end_date, days=days)

    def _fetch_stock_history_range(self, symbol: str, start_date: str, end_date: str, days: int) -> pd.DataFrame:
        rs = self.bs.query_history_k_data_plus(
            self._to_bs_symbol(symbol),
            "date,code,open,high,low,close,volume,amount,pctChg",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="2",
        )
        if rs.error_code != "0":
            raise RuntimeError(f"baostock 历史数据获取失败：{rs.error_msg}")

        rows: list[list[str]] = []
        while rs.next():
            rows.append(rs.get_row_data())

        frame = pd.DataFrame(
            rows,
            columns=["trade_date", "code", "open", "high", "low", "close", "volume", "amount", "pctChg"],
        )
        if frame.empty:
            return pd.DataFrame(columns=["trade_date", "open", "close", "high", "low", "volume", "amount"])

        frame = frame[["trade_date", "open", "close", "high", "low", "volume", "amount"]].copy()
        return frame.tail(days).reset_index(drop=True)

    def fetch_benchmark_history(self, days: int) -> pd.DataFrame:
        start_date = (datetime.now() - timedelta(days=days * 2)).strftime("%Y-%m-%d")
        end_date = datetime.now().strftime("%Y-%m-%d")
        rs = self.bs.query_history_k_data_plus(
            "sh.000001",
            "date,code,open,high,low,close,volume,amount,pctChg",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
        )
        if rs.error_code != "0":
            raise RuntimeError(f"baostock 指数历史获取失败：{rs.error_msg}")

        rows: list[list[str]] = []
        while rs.next():
            rows.append(rs.get_row_data())
        frame = pd.DataFrame(
            rows,
            columns=["trade_date", "code", "open", "high", "low", "close", "volume", "amount", "pctChg"],
        )
        return frame[["trade_date", "open", "close", "high", "low", "volume", "amount"]].tail(days).reset_index(drop=True)

    def latest_trade_date(self) -> str:
        if self._latest_trade_day:
            return self._latest_trade_day
        trading_day, _ = self._query_all_stock_with_fallback()
        self._latest_trade_day = trading_day
        return trading_day

    def _load_industry_map(self) -> dict[str, str]:
        rs = self.bs.query_stock_industry()
        if rs.error_code != "0":
            return {}
        rows: list[list[str]] = []
        while rs.next():
            rows.append(rs.get_row_data())
        if not rows:
            return {}
        frame = pd.DataFrame(rows, columns=["date", "symbol", "name", "industry", "industry_level"])
        frame["symbol"] = frame["symbol"].str.split(".").str[-1]
        frame["industry"] = frame["industry"].replace("", "未分类")
        return dict(zip(frame["symbol"], frame["industry"]))

    @staticmethod
    def _to_bs_symbol(symbol: str) -> str:
        code = str(symbol).split(".")[-1]
        if code.startswith(("60", "601", "603", "605")):
            return f"sh.{code}"
        if code.startswith(("00", "001", "002", "003")):
            return f"sz.{code}"
        return f"sh.{code}"

    @staticmethod
    def _is_target_a_share(symbol: str) -> bool:
        return symbol.startswith(
            (
                "sh.600",
                "sh.601",
                "sh.603",
                "sh.605",
                "sz.000",
                "sz.001",
                "sz.002",
                "sz.003",
            )
        )

    @staticmethod
    def _candidate_trade_days() -> list[str]:
        start = datetime.now()
        return [(start - timedelta(days=offset)).strftime("%Y-%m-%d") for offset in range(10)]

    def _query_all_stock_with_fallback(self) -> tuple[str, list[list[str]]]:
        for trading_day in self._candidate_trade_days():
            rs = self.bs.query_all_stock(day=trading_day)
            if rs.error_code != "0":
                continue
            rows: list[list[str]] = []
            while rs.next():
                rows.append(rs.get_row_data())
            if rows:
                return trading_day, rows
        raise RuntimeError("baostock 最近 10 天都没有返回股票列表，请稍后再试。")


def build_provider(name: str) -> MarketDataProvider:
    providers: dict[str, type[MarketDataProvider]] = {
        "mock": MockDataProvider,
        "akshare": AkshareDataProvider,
        "baostock": BaostockDataProvider,
    }
    provider_cls = providers.get(name, MockDataProvider)
    return provider_cls()
