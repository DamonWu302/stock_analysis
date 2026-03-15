from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
import json
import threading

import pandas as pd

from .analyzer import AnalysisResult, build_score_breakdown, score_stock
from .config import settings
from .data_source import build_provider
from .db import Database


@dataclass(slots=True)
class CacheStats:
    cache_hits: int = 0
    incremental_updates: int = 0
    full_refreshes: int = 0
    benchmark_cache_mode: str = "unknown"


class StockAnalysisService:
    def __init__(self) -> None:
        self.db = Database(settings.database_path)
        self.db.initialize()
        self._last_cache_mode = "unknown"

    def start_background_run(self, provider_name: str | None = None, limit: int | None = None) -> int:
        provider_name = provider_name or settings.default_provider
        with self.db.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO analysis_task
                (provider, status, started_at, progress_current, progress_total, message)
                VALUES (?, ?, ?, 0, 0, ?)
                """,
                (provider_name, "running", datetime.now().isoformat(timespec="seconds"), "任务已创建，等待扫描"),
            )
            task_id = int(cursor.lastrowid)

        thread = threading.Thread(target=self._run_task, args=(task_id, provider_name, limit), daemon=True)
        thread.start()
        return task_id

    def _run_task(self, task_id: int, provider_name: str, limit: int | None) -> None:
        try:
            result = self.run(provider_name=provider_name, limit=limit, task_id=task_id)
            self._update_task(
                task_id,
                status="completed",
                finished_at=datetime.now().isoformat(timespec="seconds"),
                message="扫描完成",
                run_id=result["run_id"],
                progress_current=result["sample_size"],
                progress_total=result["sample_size"],
                cache_hits=result["cache_hits"],
                incremental_updates=result["incremental_updates"],
                full_refreshes=result["full_refreshes"],
                benchmark_cache_mode=result["benchmark_cache_mode"],
            )
        except Exception as exc:
            self._update_task(
                task_id,
                status="failed",
                finished_at=datetime.now().isoformat(timespec="seconds"),
                message=str(exc),
            )

    def run(self, provider_name: str | None = None, limit: int | None = None, task_id: int | None = None) -> dict:
        provider_name = provider_name or settings.default_provider
        provider = build_provider(provider_name)
        snapshot = provider.fetch_market_snapshot(limit=limit or settings.analysis_limit)
        cache_stats = CacheStats()
        benchmark = self._get_benchmark_with_cache(provider, settings.history_days, cache_stats)

        if task_id:
            self._update_task(task_id, progress_total=len(snapshot), message="股票池加载完成，开始扫描")

        enriched_rows: list[dict] = []
        results: list[AnalysisResult] = []

        for index, (_, stock) in enumerate(snapshot.iterrows(), start=1):
            history = self._get_history_with_cache(provider, str(stock["symbol"]), settings.history_days, cache_stats)
            if history.empty:
                continue
            self._save_history(str(stock["symbol"]), history)
            stock = self._hydrate_snapshot_from_history(stock.copy(), history)
            enriched_rows.append(stock.to_dict())
            result = score_stock(stock, history, benchmark)
            if result:
                results.append(result)
            if task_id:
                self._save_task_item(
                    task_id=task_id,
                    symbol=str(stock["symbol"]),
                    name=str(stock.get("name", "")),
                    cache_mode=self._last_cache_mode,
                    latest_price=float(stock.get("latest_price") or 0),
                    pct_change=float(stock.get("pct_change") or 0),
                    score=float(result.score if result else 0),
                )

            if task_id and (index == 1 or index % 20 == 0 or index == len(snapshot)):
                self._update_task(
                    task_id,
                    progress_current=index,
                    last_symbol=str(stock["symbol"]),
                    message=f"正在扫描 {stock['symbol']}，已完成 {index}/{len(snapshot)}",
                    cache_hits=cache_stats.cache_hits,
                    incremental_updates=cache_stats.incremental_updates,
                    full_refreshes=cache_stats.full_refreshes,
                    benchmark_cache_mode=cache_stats.benchmark_cache_mode,
                )

        enriched_snapshot = pd.DataFrame(enriched_rows) if enriched_rows else snapshot
        enriched_snapshot = self._enrich_sector_metrics(enriched_snapshot)
        results = self._apply_sector_metrics_to_results(results, enriched_snapshot)
        results.sort(key=lambda item: (item.score, item.pct_change), reverse=True)
        top_results = results[: settings.top_n]

        trade_date = datetime.now().date().isoformat()
        self._save_snapshot(trade_date, enriched_snapshot)
        run_id = self._save_run(provider.name, benchmark, len(enriched_snapshot), top_results, cache_stats)

        return {
            "run_id": run_id,
            "provider": provider.name,
            "trade_date": trade_date,
            "benchmark_name": "上证指数",
            "benchmark_change": self._benchmark_change(benchmark),
            "benchmark_cache_mode": cache_stats.benchmark_cache_mode,
            "sample_size": len(enriched_snapshot),
            "display_size": len(top_results),
            "cache_hits": cache_stats.cache_hits,
            "incremental_updates": cache_stats.incremental_updates,
            "full_refreshes": cache_stats.full_refreshes,
            "results": [asdict(item) for item in top_results],
            "average_score": round(sum(item.score for item in top_results) / len(top_results), 2) if top_results else 0,
        }

    def latest_results(self) -> dict | None:
        with self.db.connect() as conn:
            run = conn.execute(
                """
                SELECT id, provider, created_at, benchmark_name, benchmark_change, sample_size,
                       cache_hits, incremental_updates, full_refreshes, benchmark_cache_mode
                FROM analysis_run
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
            if not run:
                return None
            rows = conn.execute(
                """
                SELECT symbol, name, score, latest_price, pct_change, sector, summary, signals,
                       score_breakdown, score_source, review_updated_at
                FROM analysis_result
                WHERE run_id = ?
                ORDER BY score DESC, pct_change DESC
                """,
                (run["id"],),
            ).fetchall()

        results = []
        for row in rows:
            payload = dict(row)
            payload["signals"] = json.loads(payload["signals"])
            payload["score_breakdown"] = json.loads(payload["score_breakdown"]) if payload.get("score_breakdown") else []
            results.append(payload)

        return {
            "run_id": run["id"],
            "provider": run["provider"],
            "trade_date": str(run["created_at"])[:10],
            "benchmark_name": run["benchmark_name"],
            "benchmark_change": run["benchmark_change"],
            "benchmark_cache_mode": run["benchmark_cache_mode"],
            "sample_size": run["sample_size"],
            "display_size": len(results),
            "cache_hits": run["cache_hits"],
            "incremental_updates": run["incremental_updates"],
            "full_refreshes": run["full_refreshes"],
            "results": results,
            "average_score": round(sum(item["score"] for item in results) / len(results), 2) if results else 0,
        }

    def latest_task(self) -> dict | None:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT id, provider, status, started_at, finished_at, progress_current, progress_total,
                       cache_hits, incremental_updates, full_refreshes, benchmark_cache_mode,
                       message, last_symbol, run_id
                FROM analysis_task
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
        return dict(row) if row else None

    def recent_tasks(self, limit: int = 5) -> list[dict]:
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT id, provider, status, started_at, finished_at, progress_current, progress_total,
                       cache_hits, incremental_updates, full_refreshes, benchmark_cache_mode,
                       message, last_symbol, run_id
                FROM analysis_task
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def task_detail(self, task_id: int) -> dict | None:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT id, provider, status, started_at, finished_at, progress_current, progress_total,
                       cache_hits, incremental_updates, full_refreshes, benchmark_cache_mode,
                       message, last_symbol, run_id
                FROM analysis_task
                WHERE id = ?
                """,
                (task_id,),
            ).fetchone()
            if not row:
                return None
            items = conn.execute(
                """
                SELECT symbol, name, cache_mode, latest_price, pct_change, score, updated_at
                FROM analysis_task_item
                WHERE task_id = ?
                ORDER BY updated_at DESC, symbol ASC
                """,
                (task_id,),
            ).fetchall()
        payload = dict(row)
        payload["items"] = [dict(item) for item in items]
        total = payload.get("progress_total") or 0
        current = payload.get("progress_current") or 0
        payload["progress_percent"] = round((current / total) * 100, 1) if total else 0
        return payload

    def stock_detail(self, symbol: str) -> dict | None:
        with self.db.connect() as conn:
            latest_row = conn.execute(
                """
                SELECT run_id, symbol, name, score, latest_price, pct_change, sector, summary, signals,
                       score_breakdown, score_source, review_updated_at
                FROM analysis_result
                WHERE symbol = ?
                ORDER BY run_id DESC
                LIMIT 1
                """,
                (symbol,),
            ).fetchone()
            history_rows = conn.execute(
                """
                SELECT trade_date, open, close, high, low, volume, amount
                FROM price_history
                WHERE symbol = ?
                ORDER BY trade_date ASC
                """,
                (symbol,),
            ).fetchall()
            benchmark_rows = conn.execute(
                """
                SELECT trade_date, open, close, high, low, volume, amount
                FROM benchmark_history
                WHERE symbol = ?
                ORDER BY trade_date ASC
                """,
                ("000001",),
            ).fetchall()
            snapshot_row = conn.execute(
                """
                SELECT symbol, name, latest_price, pct_change, volume, amount, sector,
                       sector_change, sector_up_ratio, main_net_inflow, main_net_inflow_ratio
                FROM market_snapshot
                WHERE trade_date = (SELECT MAX(trade_date) FROM market_snapshot)
                  AND symbol = ?
                LIMIT 1
                """,
                (symbol,),
            ).fetchone()
            sector_value = None
            if latest_row:
                sector_value = latest_row["sector"]
            elif snapshot_row:
                sector_value = snapshot_row["sector"]

            sector_rows = []
            if sector_value:
                sector_rows = conn.execute(
                    """
                    SELECT symbol, name, latest_price, pct_change
                    FROM market_snapshot
                    WHERE trade_date = (SELECT MAX(trade_date) FROM market_snapshot)
                      AND sector = ?
                    ORDER BY pct_change DESC, symbol ASC
                    LIMIT 12
                    """,
                    (sector_value,),
                ).fetchall()

        if not history_rows or not snapshot_row or not benchmark_rows:
            return None

        if latest_row:
            detail = dict(latest_row)
            detail["signals"] = json.loads(detail["signals"])
            saved_breakdown = json.loads(detail["score_breakdown"]) if detail.get("score_breakdown") else []
        else:
            snapshot = pd.Series(dict(snapshot_row))
            computed = score_stock(snapshot, pd.DataFrame([dict(row) for row in history_rows]), pd.DataFrame([dict(row) for row in benchmark_rows]))
            if not computed:
                return None
            detail = {
                "run_id": None,
                "symbol": computed.symbol,
                "name": computed.name,
                "score": computed.score,
                "latest_price": computed.latest_price,
                "pct_change": computed.pct_change,
                "sector": computed.sector,
                "summary": computed.summary,
                "signals": computed.signals,
                "score_breakdown": computed.score_breakdown,
                "score_source": "system",
                "review_updated_at": None,
            }
            saved_breakdown = computed.score_breakdown

        detail["history"] = [dict(row) for row in history_rows]
        detail["benchmark_history"] = [dict(row) for row in benchmark_rows]
        detail["sector_members"] = [dict(row) for row in sector_rows]
        detail["history_count"] = len(detail["history"])
        detail["benchmark_count"] = len(detail["benchmark_history"])
        if saved_breakdown:
            detail["score_breakdown"] = saved_breakdown
        elif snapshot_row:
            snapshot = pd.Series(dict(snapshot_row))
            detail["score_breakdown"] = build_score_breakdown(
                snapshot,
                pd.DataFrame(detail["history"]),
                pd.DataFrame(detail["benchmark_history"]),
            )
        else:
            detail["score_breakdown"] = []
        return detail

    def lookup_stock_score(self, symbol: str) -> dict | None:
        detail = self.stock_detail(symbol)
        if not detail:
            return None
        return {
            "symbol": detail["symbol"],
            "name": detail["name"],
            "score": detail["score"],
            "latest_price": detail["latest_price"],
            "pct_change": detail["pct_change"],
            "sector": detail["sector"],
            "summary": detail["summary"],
            "signals": detail["signals"],
            "score_breakdown": detail["score_breakdown"],
            "score_source": detail.get("score_source", "system"),
        }

    def apply_review_score(self, symbol: str, proposal: dict) -> dict | None:
        with self.db.connect() as conn:
            current = conn.execute(
                """
                SELECT run_id
                FROM analysis_result
                WHERE symbol = ?
                ORDER BY run_id DESC
                LIMIT 1
                """,
                (symbol,),
            ).fetchone()
            if not current:
                return None

            conn.execute(
                """
                UPDATE analysis_result
                SET score = ?,
                    summary = ?,
                    signals = ?,
                    score_breakdown = ?,
                    score_source = ?,
                    review_updated_at = ?
                WHERE run_id = ? AND symbol = ?
                """,
                (
                    float(proposal["score"]),
                    str(proposal["summary"]),
                    json.dumps(proposal["signals"], ensure_ascii=False),
                    json.dumps(proposal["score_breakdown"], ensure_ascii=False),
                    "ai",
                    datetime.now().isoformat(timespec="seconds"),
                    int(current["run_id"]),
                    symbol,
                ),
            )

        return self.stock_detail(symbol)

    def _update_task(self, task_id: int, **fields) -> None:
        if not fields:
            return
        assignments = ", ".join(f"{key} = ?" for key in fields.keys())
        values = list(fields.values()) + [task_id]
        with self.db.connect() as conn:
            conn.execute(f"UPDATE analysis_task SET {assignments} WHERE id = ?", values)

    def _save_task_item(
        self,
        task_id: int,
        symbol: str,
        name: str,
        cache_mode: str,
        latest_price: float,
        pct_change: float,
        score: float,
    ) -> None:
        with self.db.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO analysis_task_item
                (task_id, symbol, name, cache_mode, latest_price, pct_change, score, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    symbol,
                    name,
                    cache_mode,
                    latest_price,
                    pct_change,
                    score,
                    datetime.now().isoformat(timespec="seconds"),
                ),
            )

    def _save_history(self, symbol: str, history: pd.DataFrame) -> None:
        rows = history[["trade_date", "open", "close", "high", "low", "volume", "amount"]].copy()
        with self.db.connect() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO price_history
                (symbol, trade_date, open, close, high, low, volume, amount)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (symbol, row["trade_date"], row["open"], row["close"], row["high"], row["low"], row["volume"], row["amount"])
                    for _, row in rows.iterrows()
                ],
            )

    def _save_benchmark_history(self, symbol: str, history: pd.DataFrame) -> None:
        rows = history[["trade_date", "open", "close", "high", "low", "volume", "amount"]].copy()
        with self.db.connect() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO benchmark_history
                (symbol, trade_date, open, close, high, low, volume, amount)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (symbol, row["trade_date"], row["open"], row["close"], row["high"], row["low"], row["volume"], row["amount"])
                    for _, row in rows.iterrows()
                ],
            )

    def _load_cached_history(self, symbol: str, days: int) -> pd.DataFrame:
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT trade_date, open, close, high, low, volume, amount
                FROM price_history
                WHERE symbol = ?
                ORDER BY trade_date ASC
                """,
                (symbol,),
            ).fetchall()
        if not rows:
            return pd.DataFrame(columns=["trade_date", "open", "close", "high", "low", "volume", "amount"])
        return pd.DataFrame([dict(row) for row in rows]).tail(days).reset_index(drop=True)

    def _load_cached_benchmark(self, symbol: str, days: int) -> pd.DataFrame:
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT trade_date, open, close, high, low, volume, amount
                FROM benchmark_history
                WHERE symbol = ?
                ORDER BY trade_date ASC
                """,
                (symbol,),
            ).fetchall()
        if not rows:
            return pd.DataFrame(columns=["trade_date", "open", "close", "high", "low", "volume", "amount"])
        return pd.DataFrame([dict(row) for row in rows]).tail(days).reset_index(drop=True)

    def _get_history_with_cache(self, provider, symbol: str, days: int, cache_stats: CacheStats) -> pd.DataFrame:
        cached = self._load_cached_history(symbol, days)
        mode, history = self._resolve_history_cache(provider, symbol, days, cached, benchmark=False)
        self._last_cache_mode = mode
        if mode == "hit":
            cache_stats.cache_hits += 1
        elif mode == "incremental":
            cache_stats.incremental_updates += 1
        else:
            cache_stats.full_refreshes += 1
        return history

    def _get_benchmark_with_cache(self, provider, days: int, cache_stats: CacheStats) -> pd.DataFrame:
        cached = self._load_cached_benchmark("000001", days)
        mode, history = self._resolve_history_cache(provider, "000001", days, cached, benchmark=True)
        cache_stats.benchmark_cache_mode = mode
        self._save_benchmark_history("000001", history)
        return history

    def _resolve_history_cache(self, provider, symbol: str, days: int, cached: pd.DataFrame, benchmark: bool) -> tuple[str, pd.DataFrame]:
        if cached.empty:
            fresh = provider.fetch_benchmark_history(days=days) if benchmark else provider.fetch_stock_history(symbol=symbol, days=days)
            return "full", fresh

        latest_cached_date = pd.to_datetime(cached["trade_date"]).max().date()
        cache_is_fresh = latest_cached_date >= (datetime.now().date() - timedelta(days=3))
        if len(cached) >= days and cache_is_fresh:
            return "hit", cached.tail(days).reset_index(drop=True)

        if len(cached) < days:
            fresh = provider.fetch_benchmark_history(days=days) if benchmark else provider.fetch_stock_history(symbol=symbol, days=days)
            return "full", self._merge_history(cached, fresh, days)

        incremental_start = (latest_cached_date + timedelta(days=1)).isoformat()
        if benchmark:
            fresh = provider.fetch_benchmark_history(days=days)
            fresh = fresh[fresh["trade_date"] >= incremental_start].reset_index(drop=True)
        else:
            fresh = provider.fetch_stock_history_since(symbol=symbol, start_date=incremental_start, days=days)

        if fresh.empty:
            return "hit", cached.tail(days).reset_index(drop=True)
        return "incremental", self._merge_history(cached, fresh, days)

    def _save_snapshot(self, trade_date: str, snapshot: pd.DataFrame) -> None:
        if snapshot.empty:
            return
        columns = [
            "symbol",
            "name",
            "latest_price",
            "pct_change",
            "volume",
            "amount",
            "sector",
            "sector_change",
            "sector_up_ratio",
            "main_net_inflow",
            "main_net_inflow_ratio",
        ]
        payload = snapshot[columns].fillna("").copy()
        with self.db.connect() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO market_snapshot
                (trade_date, symbol, name, latest_price, pct_change, volume, amount, sector,
                 sector_change, sector_up_ratio, main_net_inflow, main_net_inflow_ratio)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        trade_date,
                        row["symbol"],
                        row["name"],
                        row["latest_price"],
                        row["pct_change"],
                        row["volume"],
                        row["amount"],
                        row["sector"],
                        row["sector_change"],
                        row["sector_up_ratio"],
                        row["main_net_inflow"],
                        row["main_net_inflow_ratio"],
                    )
                    for _, row in payload.iterrows()
                ],
            )

    def _save_run(
        self,
        provider: str,
        benchmark: pd.DataFrame,
        sample_size: int,
        results: list[AnalysisResult],
        cache_stats: CacheStats,
    ) -> int:
        benchmark_change = self._benchmark_change(benchmark)
        with self.db.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO analysis_run
                (provider, created_at, benchmark_symbol, benchmark_name, benchmark_change, sample_size,
                 cache_hits, incremental_updates, full_refreshes, benchmark_cache_mode)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    provider,
                    datetime.now().isoformat(timespec="seconds"),
                    "000001",
                    "上证指数",
                    benchmark_change,
                    sample_size,
                    cache_stats.cache_hits,
                    cache_stats.incremental_updates,
                    cache_stats.full_refreshes,
                    cache_stats.benchmark_cache_mode,
                ),
            )
            run_id = int(cursor.lastrowid)
            conn.executemany(
                """
                INSERT OR REPLACE INTO analysis_result
                (run_id, symbol, name, score, latest_price, pct_change, sector, summary, signals,
                 score_breakdown, score_source, review_updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        run_id,
                        item.symbol,
                        item.name,
                        item.score,
                        item.latest_price,
                        item.pct_change,
                        item.sector,
                        item.summary,
                        json.dumps(item.signals, ensure_ascii=False),
                        json.dumps(item.score_breakdown, ensure_ascii=False),
                        "system",
                        None,
                    )
                    for item in results
                ],
            )
        return run_id

    @staticmethod
    def _merge_history(cached: pd.DataFrame, incoming: pd.DataFrame, days: int) -> pd.DataFrame:
        merged = pd.concat([cached, incoming], ignore_index=True) if not cached.empty else incoming.copy()
        merged = merged.drop_duplicates(subset=["trade_date"], keep="last").sort_values("trade_date").reset_index(drop=True)
        return merged.tail(days).reset_index(drop=True)

    @staticmethod
    def _hydrate_snapshot_from_history(snapshot: pd.Series, history: pd.DataFrame) -> pd.Series:
        latest = history.iloc[-1]
        latest_close = pd.to_numeric(pd.Series([latest["close"]]), errors="coerce").iloc[0]
        latest_volume = pd.to_numeric(pd.Series([latest["volume"]]), errors="coerce").iloc[0]
        latest_amount = pd.to_numeric(pd.Series([latest["amount"]]), errors="coerce").iloc[0]
        if len(history) >= 2:
            prev_close = pd.to_numeric(pd.Series([history.iloc[-2]["close"]]), errors="coerce").iloc[0]
            pct_change = round(((latest_close - prev_close) / prev_close) * 100, 2) if prev_close else 0.0
        else:
            pct_change = 0.0
        snapshot["latest_price"] = latest_close
        snapshot["pct_change"] = pct_change
        snapshot["volume"] = latest_volume
        snapshot["amount"] = latest_amount
        return snapshot

    @staticmethod
    def _benchmark_change(benchmark: pd.DataFrame) -> float:
        close_series = pd.to_numeric(benchmark["close"], errors="coerce")
        return round(float(close_series.pct_change().fillna(0).iloc[-1] * 100), 2)

    @staticmethod
    def _enrich_sector_metrics(snapshot: pd.DataFrame) -> pd.DataFrame:
        if snapshot.empty:
            return snapshot
        frame = snapshot.copy()
        frame["pct_change"] = pd.to_numeric(frame["pct_change"], errors="coerce").fillna(0.0)
        sector_stats = (
            frame.groupby("sector", dropna=False)
            .agg(sector_change=("pct_change", "mean"), sector_up_ratio=("pct_change", lambda s: float((s > 0).mean())))
            .reset_index()
        )
        return frame.drop(columns=["sector_change", "sector_up_ratio"], errors="ignore").merge(sector_stats, on="sector", how="left")

    @staticmethod
    def _apply_sector_metrics_to_results(results: list[AnalysisResult], snapshot: pd.DataFrame) -> list[AnalysisResult]:
        if snapshot.empty:
            return results
        sector_map = snapshot.set_index("symbol")[["sector", "sector_change", "sector_up_ratio"]].to_dict("index")
        adjusted: list[AnalysisResult] = []
        for item in results:
            sector_info = sector_map.get(item.symbol)
            if sector_info and sector_info["sector_change"] > 0 and sector_info["sector_up_ratio"] >= 0.5:
                if "板块走强" not in item.signals:
                    item.signals.append("板块走强")
                    item.summary = " / ".join(item.signals)
            adjusted.append(item)
        return adjusted
