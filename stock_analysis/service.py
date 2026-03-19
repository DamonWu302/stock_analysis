from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
import json
import sqlite3
import threading
import time

import pandas as pd

from .analyzer import (
    AnalysisResult,
    _prepare_history,
    build_daily_factor_snapshot,
    build_daily_factor_snapshot_prepared,
    build_daily_score_snapshot,
    build_daily_score_snapshot_prepared,
    build_score_breakdown,
    score_stock,
)
from .backtest import build_backtest_config_schema
from .backtest_runner import BacktestRunner
from .config import settings
from .data_source import build_provider
from .db import Database


@dataclass(slots=True)
class CacheStats:
    cache_hits: int = 0
    incremental_updates: int = 0
    full_refreshes: int = 0
    benchmark_cache_mode: str = "unknown"
    stock_failures: int = 0
    last_failure_symbol: str | None = None
    last_failure_reason: str | None = None


class StockAnalysisService:
    def __init__(self) -> None:
        self.db = Database(settings.database_path)
        self.db.initialize()
        self.backtest_runner = BacktestRunner(self.db)
        self._last_cache_mode = "unknown"
        self._recover_stale_tasks()
        self._recover_stale_backfill_tasks()

    def start_background_run(self, provider_name: str | None = None, limit: int | None = None) -> int:
        provider_name = provider_name or settings.default_provider
        with self.db.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO analysis_task
                (provider, status, phase, started_at, progress_current, progress_total, message)
                VALUES (?, ?, ?, ?, 0, 0, ?)
                """,
                (provider_name, "running", "pending", datetime.now().isoformat(timespec="seconds"), "任务已创建，等待扫描"),
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
                phase="completed",
                finished_at=datetime.now().isoformat(timespec="seconds"),
                message=(
                    "扫描完成"
                    if not result.get("stock_failures")
                    else f"扫描完成，跳过 {result['stock_failures']} 只；最近失败 {result.get('last_failure_symbol') or '-'}: {self._short_error(result.get('last_failure_reason') or 'unknown')}"
                ),
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
                phase="failed",
                finished_at=datetime.now().isoformat(timespec="seconds"),
                message=self._format_task_error(exc),
            )

    def run(self, provider_name: str | None = None, limit: int | None = None, task_id: int | None = None) -> dict:
        provider_name = provider_name or settings.default_provider
        provider = build_provider(provider_name)
        if task_id:
            self._update_task(task_id, message="正在加载股票池", progress_current=0, progress_total=0)
        snapshot = provider.fetch_market_snapshot(limit=limit or settings.analysis_limit)
        trade_date = provider.latest_trade_date()
        if task_id:
            self._update_task(task_id, phase="scanning", message="正在加载股票池")
        cache_stats = CacheStats()
        benchmark = self._get_benchmark_with_cache(provider, settings.history_days, cache_stats, trade_date)
        if task_id:
            self._update_task(task_id, phase="scanning", progress_total=len(snapshot))

        if task_id:
            self._update_task(task_id, progress_total=len(snapshot), message="股票池加载完成，开始扫描")

        enriched_rows: list[dict] = []
        history_cache: dict[str, pd.DataFrame] = {}

        for index, (_, stock) in enumerate(snapshot.iterrows(), start=1):
            symbol = str(stock["symbol"])
            try:
                history = self._get_history_with_cache(provider, symbol, settings.history_days, cache_stats, trade_date)
            except Exception as exc:
                cache_stats.stock_failures += 1
                cache_stats.last_failure_symbol = symbol
                cache_stats.last_failure_reason = str(exc)
                if task_id and (cache_stats.stock_failures <= 3 or cache_stats.stock_failures % 20 == 0):
                    self._update_task(
                        task_id,
                        phase="scanning",
                        progress_current=index,
                        last_symbol=symbol,
                        message=(
                            f"扫描跳过 {symbol}，失败 {cache_stats.stock_failures} 只；"
                            f"最近原因: {self._short_error(str(exc))}"
                        ),
                        cache_hits=cache_stats.cache_hits,
                        incremental_updates=cache_stats.incremental_updates,
                        full_refreshes=cache_stats.full_refreshes,
                        benchmark_cache_mode=cache_stats.benchmark_cache_mode,
                    )
                continue
            if history.empty:
                continue
            self._save_history(symbol, history)
            history_cache[symbol] = history
            stock = self._hydrate_snapshot_from_history(stock.copy(), history)
            enriched_rows.append(stock.to_dict())
            preview_result = score_stock(stock, history, benchmark)
            if task_id:
                self._save_task_item(
                    task_id=task_id,
                    symbol=symbol,
                    name=str(stock.get("name", "")),
                    cache_mode=self._last_cache_mode,
                    latest_price=float(stock.get("latest_price") or 0),
                    pct_change=float(stock.get("pct_change") or 0),
                    score=float(preview_result.score if preview_result else 0),
                )

            if task_id and (index == 1 or index % 20 == 0 or index == len(snapshot)):
                self._update_task(
                    task_id,
                    phase="scanning",
                    progress_current=index,
                    last_symbol=symbol,
                    message=self._build_progress_message(index, len(snapshot), symbol, cache_stats),
                    cache_hits=cache_stats.cache_hits,
                    incremental_updates=cache_stats.incremental_updates,
                    full_refreshes=cache_stats.full_refreshes,
                    benchmark_cache_mode=cache_stats.benchmark_cache_mode,
                )

        enriched_snapshot = pd.DataFrame(enriched_rows) if enriched_rows else snapshot
        if task_id:
            self._update_task(
                task_id,
                phase="summarizing",
                progress_current=len(snapshot),
                progress_total=len(snapshot),
                message="正在汇总结果并写入数据库",
                cache_hits=cache_stats.cache_hits,
                incremental_updates=cache_stats.incremental_updates,
                full_refreshes=cache_stats.full_refreshes,
                benchmark_cache_mode=cache_stats.benchmark_cache_mode,
            )
        enriched_snapshot = self._enrich_sector_metrics(enriched_snapshot)
        results, daily_factors, daily_scores = self._build_daily_outputs(enriched_snapshot, history_cache, benchmark)
        results.sort(key=lambda item: (item.score, item.pct_change), reverse=True)
        top_results = results[: settings.top_n]

        if task_id:
            self._update_task(task_id, phase="writing")
        self._save_snapshot(trade_date, enriched_snapshot)
        self._save_daily_factors(daily_factors)
        self._save_daily_scores(daily_scores)
        run_id = self._save_run(
            provider=provider.name,
            trade_date=trade_date,
            sample_size=len(enriched_snapshot),
            cache_stats=cache_stats,
        )

        result = {
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
            "stock_failures": cache_stats.stock_failures,
            "last_failure_symbol": cache_stats.last_failure_symbol,
            "last_failure_reason": cache_stats.last_failure_reason,
            "results": [asdict(item) for item in top_results],
            "average_score": round(sum(item.score for item in top_results) / len(top_results), 2) if top_results else 0,
        }
        if task_id:
            self._mark_task_completed(task_id, result)
        return result

    def latest_results(self) -> dict | None:
        with self.db.connect() as conn:
            latest_daily_trade_date_row = conn.execute(
                """
                SELECT MAX(trade_date) AS trade_date
                FROM daily_score
                """
            ).fetchone()
            latest_daily_trade_date = latest_daily_trade_date_row["trade_date"] if latest_daily_trade_date_row else None
            if not latest_daily_trade_date:
                return None

            run = conn.execute(
                """
                SELECT id, provider, created_at, trade_date, sample_size,
                       cache_hits, incremental_updates, full_refreshes, benchmark_cache_mode
                FROM analysis_run
                WHERE trade_date = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (latest_daily_trade_date,),
            ).fetchone()
            rows = conn.execute(
                """
                SELECT ds.symbol, ms.name, ds.score_total AS score, ms.latest_price, ms.pct_change, ms.sector,
                       ds.summary, ds.signals, ds.score_breakdown, ds.score_source, ds.review_updated_at
                FROM daily_score ds
                LEFT JOIN market_snapshot ms
                  ON ms.trade_date = ds.trade_date AND ms.symbol = ds.symbol
                WHERE ds.trade_date = ?
                ORDER BY ds.score_total DESC, ms.pct_change DESC, ds.symbol ASC
                """,
                (latest_daily_trade_date,),
            ).fetchall()

        results = []
        for row in rows:
            payload = dict(row)
            payload["signals"] = json.loads(payload["signals"])
            payload["score_breakdown"] = json.loads(payload["score_breakdown"]) if payload.get("score_breakdown") else []
            results.append(payload)

        return {
            "run_id": run["id"] if run else None,
            "provider": run["provider"] if run else settings.default_provider,
            "trade_date": latest_daily_trade_date,
            "benchmark_name": "上证指数",
            "benchmark_change": self._latest_benchmark_change(latest_daily_trade_date),
            "benchmark_cache_mode": run["benchmark_cache_mode"] if run else "unknown",
            "sample_size": len(results),
            "display_size": len(results),
            "cache_hits": run["cache_hits"] if run else 0,
            "incremental_updates": run["incremental_updates"] if run else 0,
            "full_refreshes": run["full_refreshes"] if run else 0,
            "results": results,
            "average_score": round(sum(item["score"] for item in results) / len(results), 2) if results else 0,
        }

    @staticmethod
    def backtest_config_schema() -> dict:
        return build_backtest_config_schema()

    def run_backtest(self, config: dict | None = None) -> dict:
        return self.backtest_runner.run(config)

    def recent_backtests(self, limit: int = 20) -> list[dict]:
        return self.backtest_runner.recent_runs(limit=limit)

    def backtest_detail(self, run_id: int) -> dict | None:
        return self.backtest_runner.run_detail(run_id)

    def score_trend(self, days: int = 20) -> list[dict]:
        days = max(int(days), 2)
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                WITH daily AS (
                    SELECT trade_date, COUNT(*) AS sample_size, AVG(score_total) AS avg_score
                    FROM daily_score
                    GROUP BY trade_date
                ),
                recent AS (
                    SELECT trade_date, sample_size, avg_score
                    FROM daily
                    ORDER BY trade_date DESC
                    LIMIT ?
                ),
                chronological AS (
                    SELECT trade_date, sample_size, avg_score
                    FROM recent
                    ORDER BY trade_date ASC
                )
                SELECT trade_date,
                       sample_size,
                       ROUND(avg_score, 4) AS avg_score,
                       ROUND(AVG(avg_score) OVER (ORDER BY trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), 4) AS ma5_avg_score
                FROM chronological
                ORDER BY trade_date ASC
                """,
                (days,),
            ).fetchall()
        return [dict(row) for row in rows]

    def backfill_daily_tables(
        self,
        days: int = 120,
        batch_size: int = 10,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict:
        return self._backfill_daily_tables(
            days=days,
            batch_size=batch_size,
            start_date=start_date,
            end_date=end_date,
        )

    def start_backfill_task(
        self,
        days: int = 120,
        batch_size: int = 10,
        start_date: str | None = None,
        end_date: str | None = None,
        resume_task_id: int | None = None,
    ) -> int:
        days = max(int(days), 1)
        batch_size = max(int(batch_size), 1)
        if start_date and end_date and start_date > end_date:
            raise ValueError("开始日期不能晚于结束日期")
        started_at = datetime.now().isoformat(timespec="seconds")
        with self.db.connect() as conn:
            if resume_task_id:
                row = conn.execute(
                    """
                    SELECT id, days, start_date, end_date, batch_size, progress_current, progress_total
                    FROM backfill_task
                    WHERE id = ?
                    """,
                    (resume_task_id,),
                ).fetchone()
                if not row:
                    raise ValueError(f"未找到历史回填任务 {resume_task_id}")
                days = int(row["days"])
                start_date = row["start_date"]
                end_date = row["end_date"]
                batch_size = int(row["batch_size"])
                conn.execute(
                    """
                    UPDATE backfill_task
                    SET status = 'running',
                        phase = 'pending',
                        started_at = ?,
                        finished_at = NULL,
                        resume_from_task_id = ?,
                        message = '正在恢复历史回填任务'
                    WHERE id = ?
                    """,
                    (started_at, resume_task_id, resume_task_id),
                )
                task_id = int(resume_task_id)
                start_offset = int(row["progress_current"] or 0)
            else:
                cursor = conn.execute(
                    """
                    INSERT INTO backfill_task
                    (status, phase, started_at, days, start_date, end_date, batch_size, progress_current, progress_total, message)
                    VALUES ('running', 'pending', ?, ?, ?, ?, ?, 0, 0, ?)
                    """,
                    (started_at, days, start_date, end_date, batch_size, "任务已创建，等待回填"),
                )
                task_id = int(cursor.lastrowid)
                start_offset = 0

        thread = threading.Thread(
            target=self._run_backfill_task,
            args=(task_id, days, batch_size, start_date, end_date, start_offset, resume_task_id),
            daemon=True,
        )
        thread.start()
        return task_id

    def latest_backfill_task(self) -> dict | None:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT id, status, phase, started_at, finished_at, days, start_date, end_date, batch_size,
                       progress_current, progress_total, factor_rows, score_rows,
                       last_trade_date, message, resume_from_task_id
                FROM backfill_task
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
        return self._build_backfill_task_payload(dict(row)) if row else None

    def recent_backfill_tasks(self, limit: int = 10) -> list[dict]:
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT id, status, phase, started_at, finished_at, days, start_date, end_date, batch_size,
                       progress_current, progress_total, factor_rows, score_rows,
                       last_trade_date, message, resume_from_task_id
                FROM backfill_task
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._build_backfill_task_payload(dict(row)) for row in rows]

    def backfill_task_detail(self, task_id: int) -> dict | None:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT id, status, phase, started_at, finished_at, days, start_date, end_date, batch_size,
                       progress_current, progress_total, factor_rows, score_rows,
                       last_trade_date, message, resume_from_task_id
                FROM backfill_task
                WHERE id = ?
                """,
                (task_id,),
            ).fetchone()
            batches = conn.execute(
                """
                SELECT id, task_id, batch_index, status, started_at, finished_at,
                       start_trade_date, end_trade_date, completed_dates,
                       factor_rows, score_rows, message
                FROM backfill_task_batch
                WHERE task_id = ?
                ORDER BY batch_index ASC, id ASC
                """,
                (task_id,),
            ).fetchall()
        if not row:
            return None
        payload = self._build_backfill_task_payload(dict(row))
        payload["batches"] = [dict(batch) for batch in batches]
        return payload

    def _create_backfill_batch(
        self,
        task_id: int,
        batch_index: int,
        start_trade_date: str,
        end_trade_date: str,
    ) -> int:
        with self.db.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO backfill_task_batch
                (task_id, batch_index, status, started_at, start_trade_date, end_trade_date, message)
                VALUES (?, ?, 'running', ?, ?, ?, ?)
                """,
                (
                    task_id,
                    batch_index,
                    datetime.now().isoformat(timespec="seconds"),
                    start_trade_date,
                    end_trade_date,
                    f"?? {start_trade_date} ? {end_trade_date}",
                ),
            )
            return int(cursor.lastrowid)

    def _update_backfill_batch(self, batch_id: int, **fields) -> None:
        if not fields:
            return
        assignments = ", ".join(f"{key} = ?" for key in fields.keys())
        values = list(fields.values()) + [batch_id]
        with self.db.connect() as conn:
            conn.execute(f"UPDATE backfill_task_batch SET {assignments} WHERE id = ?", values)

    def _backfill_daily_tables(
        self,
        days: int = 120,
        batch_size: int = 10,
        start_date: str | None = None,
        end_date: str | None = None,
        start_offset: int = 0,
        progress_callback=None,
    ) -> dict:
        if start_date and end_date and start_date > end_date:
            raise ValueError("开始日期不能晚于结束日期")
        target_dates = self._load_backfill_trade_dates(days=days, start_date=start_date, end_date=end_date)
        if not target_dates:
            return {
                "trade_dates": 0,
                "factor_rows": 0,
                "score_rows": 0,
                "symbols": 0,
                "batches": 0,
                "start_date": start_date,
                "end_date": end_date,
            }

        batch_size = max(int(batch_size), 1)
        start_offset = max(int(start_offset), 0)
        target_dates = target_dates[start_offset:]
        if not target_dates:
            return {
                "trade_dates": 0,
                "factor_rows": 0,
                "score_rows": 0,
                "symbols": 0,
                "batches": 0,
                "start_date": start_date,
                "end_date": end_date,
                "batch_size": batch_size,
                "completed_dates": start_offset,
            }

        effective_start_date = target_dates[0]
        effective_end_date = target_dates[-1]
        sector_map = self._load_latest_sector_map()
        benchmark = self._load_prepared_benchmark_for_backfill(effective_start_date, effective_end_date)
        benchmark_windows = self._build_benchmark_windows(benchmark, target_dates)
        sector_metrics = self._build_historical_sector_metrics(target_dates, sector_map)
        symbol_histories = self._load_prepared_symbol_histories_for_backfill(
            effective_start_date,
            effective_end_date,
            set(sector_map.keys()),
        )

        symbols_processed = 0
        total_factor_rows = 0
        total_score_rows = 0
        batches_completed = 0

        for batch_start in range(0, len(target_dates), batch_size):
            batch_dates = target_dates[batch_start : batch_start + batch_size]
            batch_end_offset = start_offset + batch_start + len(batch_dates)
            batch_index = (batch_start // batch_size) + 1
            batch_id = None
            if progress_callback:
                batch_id = progress_callback(
                    {
                        "event": "batch_started",
                        "batch_index": batch_index,
                        "start_trade_date": batch_dates[0],
                        "end_trade_date": batch_dates[-1],
                    }
                )
                progress_callback(
                    {
                        "phase": "calculating",
                        "completed_dates": start_offset + batch_start,
                        "factor_rows": total_factor_rows,
                        "score_rows": total_score_rows,
                        "last_trade_date": batch_dates[-1],
                        "message": f"正在计算 {batch_dates[0]} 至 {batch_dates[-1]} 的历史因子",
                        "batch_id": batch_id,
                    }
                )

            factor_rows: list[dict] = []
            score_rows: list[dict] = []

            for symbol, history in symbol_histories.items():
                if history.empty:
                    continue
                if batch_start == 0:
                    symbols_processed += 1
                sector = sector_map.get(symbol, "未分类")
                trade_date_labels = history["trade_date"].dt.strftime("%Y-%m-%d")
                matched_rows = history.index[trade_date_labels.isin(batch_dates)].tolist()
                for row_index in matched_rows:
                    if row_index < 59:
                        continue
                    trade_date = str(trade_date_labels.iloc[row_index])
                    benchmark_window = benchmark_windows.get(trade_date)
                    if benchmark_window is None or len(benchmark_window) < 20:
                        continue
                    metric = sector_metrics.get((trade_date, sector), {"sector_change": 0.0, "sector_up_ratio": 0.0})
                    snapshot = pd.Series(
                        {
                            "symbol": symbol,
                            "sector": sector,
                            "sector_change": metric["sector_change"],
                            "sector_up_ratio": metric["sector_up_ratio"],
                        }
                    )
                    history_window = history.iloc[: row_index + 1]
                    factor = build_daily_factor_snapshot_prepared(snapshot, history_window, benchmark_window, trade_date)
                    if factor:
                        factor_rows.append(factor)
                    score = build_daily_score_snapshot_prepared(snapshot, history_window, benchmark_window, trade_date)
                    if score:
                        score_rows.append(score)

            self._save_daily_factors(factor_rows)
            self._save_daily_scores(score_rows)
            total_factor_rows += len(factor_rows)
            total_score_rows += len(score_rows)
            batches_completed += 1

            if progress_callback:
                progress_callback(
                    {
                        "phase": "writing",
                        "completed_dates": batch_end_offset,
                        "factor_rows": total_factor_rows,
                        "score_rows": total_score_rows,
                        "last_trade_date": batch_dates[-1],
                        "message": f"已完成到 {batch_dates[-1]}，累计评分 {total_score_rows} 条",
                        "batch_id": batch_id,
                        "batch_status": "completed",
                        "batch_factor_rows": len(factor_rows),
                        "batch_score_rows": len(score_rows),
                    }
                )

        return {
            "trade_dates": len(target_dates),
            "factor_rows": total_factor_rows,
            "score_rows": total_score_rows,
            "symbols": symbols_processed,
            "start_date": effective_start_date,
            "end_date": effective_end_date,
            "batch_size": batch_size,
            "batches": batches_completed,
            "completed_dates": start_offset + len(target_dates),
        }

    def latest_task(self) -> dict | None:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT id, provider, status, phase, started_at, finished_at, progress_current, progress_total,
                       cache_hits, incremental_updates, full_refreshes, benchmark_cache_mode,
                       message, last_symbol, run_id
                FROM analysis_task
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
        return self._enrich_task_timing(dict(row)) if row else None

    def recent_tasks(self, limit: int = 5) -> list[dict]:
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT id, provider, status, phase, started_at, finished_at, progress_current, progress_total,
                       cache_hits, incremental_updates, full_refreshes, benchmark_cache_mode,
                       message, last_symbol, run_id
                FROM analysis_task
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._enrich_task_timing(dict(row)) for row in rows]

    def task_detail(self, task_id: int) -> dict | None:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT id, provider, status, phase, started_at, finished_at, progress_current, progress_total,
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
        return self._enrich_task_timing(payload)

    def stock_detail(self, symbol: str) -> dict | None:
        with self.db.connect() as conn:
            latest_daily_score = conn.execute(
                """
                SELECT trade_date, symbol, score_total, score_ma_trend, score_volume_pattern,
                       score_capital_sector, score_breakout, score_hold, score_benchmark,
                       signals, score_breakdown, summary, score_source, review_updated_at, score_version
                FROM daily_score
                WHERE symbol = ?
                ORDER BY trade_date DESC
                LIMIT 1
                """,
                (symbol,),
            ).fetchone()
            latest_daily_factor = conn.execute(
                """
                SELECT trade_date, symbol, close, pct_change, ma5, ma10, ma20, ma30, ma60,
                       vol_ma5, atr14, prior_20_high, cmf21, mfi14, sector_change, sector_up_ratio,
                       benchmark_close, benchmark_ma20, benchmark_prev_ma20
                FROM daily_factor
                WHERE symbol = ?
                ORDER BY trade_date DESC
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
                SELECT trade_date, symbol, name, latest_price, pct_change, volume, amount, sector,
                       sector_change, sector_up_ratio, main_net_inflow, main_net_inflow_ratio
                FROM market_snapshot
                WHERE trade_date = (SELECT MAX(trade_date) FROM market_snapshot)
                  AND symbol = ?
                LIMIT 1
                """,
                (symbol,),
            ).fetchone()
            sector_value = None
            if snapshot_row:
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

        snapshot_trade_date = str(snapshot_row["trade_date"])
        history_df = pd.DataFrame([dict(row) for row in history_rows])
        benchmark_df = pd.DataFrame([dict(row) for row in benchmark_rows])
        snapshot = pd.Series(dict(snapshot_row))

        detail = self._build_detail_from_daily_tables(
            latest_daily_score=latest_daily_score,
            latest_daily_factor=latest_daily_factor,
            snapshot=snapshot,
            snapshot_trade_date=snapshot_trade_date,
        )
        if detail is None:
            computed = score_stock(snapshot, history_df, benchmark_df)
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
                "score_version": None,
                "daily_factor": dict(latest_daily_factor) if latest_daily_factor else None,
            }

        detail["history"] = [dict(row) for row in history_rows]
        detail["benchmark_history"] = [dict(row) for row in benchmark_rows]
        detail["sector_members"] = [dict(row) for row in sector_rows]
        detail["history_count"] = len(detail["history"])
        detail["benchmark_count"] = len(detail["benchmark_history"])
        return detail

    def lookup_stock_score(self, symbol: str) -> dict | None:
        with self.db.connect() as conn:
            latest_daily_trade_date_row = conn.execute(
                """
                SELECT MAX(trade_date) AS trade_date
                FROM daily_score
                """
            ).fetchone()
            latest_daily_trade_date = latest_daily_trade_date_row["trade_date"] if latest_daily_trade_date_row else None
            if not latest_daily_trade_date:
                return None
            row = conn.execute(
                """
                SELECT ds.symbol, ms.name, ds.score_total AS score, ms.latest_price, ms.pct_change, ms.sector,
                       ds.summary, ds.signals, ds.score_breakdown, ds.score_source, ds.review_updated_at
                FROM daily_score ds
                LEFT JOIN market_snapshot ms
                  ON ms.trade_date = ds.trade_date AND ms.symbol = ds.symbol
                WHERE ds.trade_date = ? AND ds.symbol = ?
                LIMIT 1
                """,
                (latest_daily_trade_date, symbol),
            ).fetchone()
        if not row:
            return None
        payload = dict(row)
        payload["signals"] = json.loads(payload["signals"]) if payload.get("signals") else []
        payload["score_breakdown"] = json.loads(payload["score_breakdown"]) if payload.get("score_breakdown") else []
        return payload

    def apply_review_score(self, symbol: str, proposal: dict) -> dict | None:
        with self.db.connect() as conn:
            current_daily = conn.execute(
                """
                SELECT trade_date
                FROM daily_score
                WHERE symbol = ?
                ORDER BY trade_date DESC
                LIMIT 1
                """,
                (symbol,),
            ).fetchone()
            if not current_daily:
                return None
            conn.execute(
                """
                UPDATE daily_score
                SET score_total = ?,
                    signals = ?,
                    score_breakdown = ?,
                    summary = ?,
                    score_source = ?,
                    review_updated_at = ?
                WHERE trade_date = ? AND symbol = ?
                """,
                (
                    float(proposal["score"]),
                    json.dumps(proposal["signals"], ensure_ascii=False),
                    json.dumps(proposal["score_breakdown"], ensure_ascii=False),
                    str(proposal["summary"]),
                    "ai",
                    datetime.now().isoformat(timespec="seconds"),
                    str(current_daily["trade_date"]),
                    symbol,
                ),
            )

        return self.stock_detail(symbol)

    def _recover_stale_tasks(self) -> None:
        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE analysis_task
                SET status = 'failed',
                    phase = 'failed',
                    finished_at = ?,
                    message = '任务在应用重启前中断，请重新发起扫描'
                WHERE status = 'running'
                """,
                (datetime.now().isoformat(timespec="seconds"),),
            )

    def _recover_stale_backfill_tasks(self) -> None:
        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE backfill_task_batch
                SET status = 'failed',
                    finished_at = ?,
                    message = '历史回填批次在应用重启前中断，可重新续补'
                WHERE status = 'running'
                """,
                (datetime.now().isoformat(timespec="seconds"),),
            )
            conn.execute(
                """
                UPDATE backfill_task
                SET status = 'failed',
                    phase = 'failed',
                    finished_at = ?,
                    message = '历史回填任务在应用重启前中断，可从当前进度继续续补'
                WHERE status = 'running'
                """,
                (datetime.now().isoformat(timespec="seconds"),),
            )

    @staticmethod
    def _format_task_error(exc: Exception) -> str:
        message = str(exc)
        lower = message.lower()
        if "baostock" in lower and ("reset" in lower or "10054" in lower or "??" in message):
            return f"baostock network reset: {message}"
        if "llm api" in lower and ("reset" in lower or "10054" in lower):
            return f"llm api connection reset: {message}"
        return message

    @staticmethod
    def _short_error(message: str, max_len: int = 120) -> str:
        compact = " ".join(str(message).split())
        return compact if len(compact) <= max_len else compact[: max_len - 3] + "..."

    def _build_progress_message(self, current: int, total: int, symbol: str, cache_stats: CacheStats) -> str:
        base = f"正在扫描 {symbol}，已完成 {current}/{total}"
        if cache_stats.stock_failures <= 0:
            return base
        reason = self._short_error(cache_stats.last_failure_reason or "unknown")
        failed_symbol = cache_stats.last_failure_symbol or "-"
        return f"{base}；已跳过 {cache_stats.stock_failures} 只，最近失败 {failed_symbol}: {reason}"

    @staticmethod
    def _enrich_task_timing(task: dict) -> dict:
        if not task:
            return task
        phase = str(task.get("phase") or "")
        task["phase_label"] = StockAnalysisService._phase_label(
            phase=phase,
            finished=bool(task.get("finished_at")),
            status=str(task.get("status") or ""),
        )
        started_at = task.get("started_at")
        current = int(task.get("progress_current") or 0)
        total = int(task.get("progress_total") or 0)
        finished_at = task.get("finished_at")
        avg_seconds = None
        eta_at = None

        if started_at:
            try:
                started_dt = datetime.fromisoformat(str(started_at))
                end_dt = datetime.fromisoformat(str(finished_at)) if finished_at else datetime.now()
                elapsed_seconds = max((end_dt - started_dt).total_seconds(), 0.0)
                if current > 0:
                    avg_seconds = elapsed_seconds / current
                    if not finished_at and total > current:
                        eta_seconds = avg_seconds * (total - current)
                        eta_at = (datetime.now() + timedelta(seconds=eta_seconds)).isoformat(timespec="seconds")
            except ValueError:
                avg_seconds = None
                eta_at = None

        task["avg_item_seconds"] = round(avg_seconds, 2) if avg_seconds is not None else None
        task["avg_item_text"] = f"{avg_seconds:.2f} 秒/项" if avg_seconds is not None else "-"
        task["eta_at"] = eta_at
        task["eta_text"] = eta_at or ("已完成" if finished_at else "-")
        if not eta_at and finished_at:
            task["eta_text"] = "失败" if str(task.get("status") or "") == "failed" else "已完成"
        return task

    @staticmethod
    def _phase_label(phase: str, finished: bool, status: str) -> str:
        if status == "failed" or phase == "failed":
            return "失败"
        if status == "completed" or phase == "completed" or finished:
            return "已完成"
        labels = {
            "pending": "等待中",
            "scanning": "扫描中",
            "loading": "加载中",
            "calculating": "计算中",
            "summarizing": "汇总中",
            "writing": "写库中",
        }
        return labels.get(phase, "扫描中")

    def _update_task(self, task_id: int, **fields) -> None:
        if not fields:
            return
        assignments = ", ".join(f"{key} = ?" for key in fields.keys())
        values = list(fields.values()) + [task_id]
        last_error = None
        for attempt in range(3):
            try:
                with self.db.connect() as conn:
                    conn.execute(f"UPDATE analysis_task SET {assignments} WHERE id = ?", values)
                return
            except sqlite3.OperationalError as exc:
                last_error = exc
                message = str(exc).lower()
                if "locked" not in message and "busy" not in message:
                    raise
                time.sleep(0.5 * (attempt + 1))
        if last_error:
            raise last_error

    def _update_backfill_task(self, task_id: int, **fields) -> None:
        if not fields:
            return
        assignments = ", ".join(f"{key} = ?" for key in fields.keys())
        values = list(fields.values()) + [task_id]
        last_error = None
        for attempt in range(3):
            try:
                with self.db.connect() as conn:
                    conn.execute(f"UPDATE backfill_task SET {assignments} WHERE id = ?", values)
                return
            except sqlite3.OperationalError as exc:
                last_error = exc
                message = str(exc).lower()
                if "locked" not in message and "busy" not in message:
                    raise
                time.sleep(0.5 * (attempt + 1))
        if last_error:
            raise last_error

    def _build_backfill_task_payload(self, task: dict) -> dict:
        if not task:
            return task
        total = int(task.get("progress_total") or 0)
        current = int(task.get("progress_current") or 0)
        task["progress_percent"] = round((current / total) * 100, 1) if total else 0
        task["mode_label"] = "断点续补" if task.get("resume_from_task_id") else "普通回填"
        start_date = task.get("start_date")
        end_date = task.get("end_date")
        task["range_label"] = f"{start_date} ~ {end_date}" if start_date and end_date else (start_date or end_date or f"最近 {task.get('days') or 0} 个交易日")
        message = str(task.get("message") or "")
        if not message or self._looks_garbled(message):
            task["message"] = self._default_backfill_message(task)
        return self._enrich_task_timing(task)

    @staticmethod
    def _looks_garbled(message: str) -> bool:
        text = str(message or "")
        if not text:
            return False
        return ("?" in text and len(text.replace("?", "").strip()) < max(4, len(text) // 3)) or ("锟" in text)

    @staticmethod
    def _default_backfill_message(task: dict) -> str:
        status = str(task.get("status") or "")
        phase = str(task.get("phase") or "")
        if status == "failed" or phase == "failed":
            return "历史回填任务失败，可从当前进度继续续补"
        if status == "completed" or phase == "completed":
            return (
                f"历史回填完成，共处理 {int(task.get('progress_total') or 0)} 个交易日，"
                f"写入 {int(task.get('score_rows') or 0)} 条评分"
            )
        if phase == "loading":
            return "正在加载历史交易日与缓存数据"
        if phase == "calculating":
            return "正在计算历史因子和评分"
        if phase == "writing":
            return "正在写入 daily_factor 和 daily_score"
        return "任务已创建，等待回填"

    def _run_backfill_task(
        self,
        task_id: int,
        days: int,
        batch_size: int,
        start_date: str | None,
        end_date: str | None,
        start_offset: int,
        resume_task_id: int | None,
    ) -> None:
        try:
            with self.db.connect() as conn:
                conn.execute("DELETE FROM backfill_task_batch WHERE task_id = ?", (task_id,))
            target_dates = self._load_backfill_trade_dates(days=days, start_date=start_date, end_date=end_date)
            total_dates = len(target_dates)
            self._update_backfill_task(
                task_id,
                phase="loading",
                progress_total=total_dates,
                progress_current=min(start_offset, total_dates),
                message="正在加载历史交易日与缓存数据",
                started_at=datetime.now().isoformat(timespec="seconds"),
            )

            def progress_callback(payload: dict):
                event = str(payload.get("event") or "")
                if event == "batch_started":
                    return self._create_backfill_batch(
                        task_id=task_id,
                        batch_index=int(payload.get("batch_index") or 0),
                        start_trade_date=str(payload.get("start_trade_date") or ""),
                        end_trade_date=str(payload.get("end_trade_date") or ""),
                    )

                completed_dates = int(payload.get("completed_dates") or start_offset)
                phase = str(payload.get("phase") or "calculating")
                self._update_backfill_task(
                    task_id,
                    phase=phase,
                    progress_total=total_dates,
                    progress_current=min(completed_dates, total_dates),
                    factor_rows=int(payload.get("factor_rows") or 0),
                    score_rows=int(payload.get("score_rows") or 0),
                    last_trade_date=payload.get("last_trade_date"),
                    message=payload.get("message") or "正在回填历史因子和评分",
                )

                batch_id = payload.get("batch_id")
                if batch_id:
                    batch_fields = {
                        "status": str(payload.get("batch_status") or ("running" if phase != "writing" else "completed")),
                        "completed_dates": int(payload.get("completed_dates") or 0),
                        "factor_rows": int(payload.get("batch_factor_rows") or 0),
                        "score_rows": int(payload.get("batch_score_rows") or 0),
                        "message": payload.get("message") or "-",
                    }
                    if str(payload.get("batch_status") or "") == "completed":
                        batch_fields["finished_at"] = datetime.now().isoformat(timespec="seconds")
                    self._update_backfill_batch(int(batch_id), **batch_fields)
                return batch_id

            result = self._backfill_daily_tables(
                days=days,
                batch_size=batch_size,
                start_date=start_date,
                end_date=end_date,
                start_offset=start_offset,
                progress_callback=progress_callback,
            )
            self._update_backfill_task(
                task_id,
                status="completed",
                phase="completed",
                finished_at=datetime.now().isoformat(timespec="seconds"),
                progress_total=total_dates,
                progress_current=min(int(result.get("completed_dates") or total_dates), total_dates),
                factor_rows=int(result.get("factor_rows") or 0),
                score_rows=int(result.get("score_rows") or 0),
                last_trade_date=result.get("end_date") or result.get("last_trade_date"),
                message=(
                    f"历史回填完成，共处理 {result.get('trade_dates', 0)} 个交易日，"
                    f"写入 {result.get('score_rows', 0)} 条评分"
                ),
            )
        except Exception as exc:
            with self.db.connect() as conn:
                conn.execute(
                    """
                    UPDATE backfill_task_batch
                    SET status = 'failed',
                        finished_at = ?,
                        message = ?
                    WHERE task_id = ? AND status = 'running'
                    """,
                    (datetime.now().isoformat(timespec="seconds"), self._format_task_error(exc), task_id),
                )
            self._update_backfill_task(
                task_id,
                status="failed",
                phase="failed",
                finished_at=datetime.now().isoformat(timespec="seconds"),
                message=self._format_task_error(exc),
            )

    def _mark_task_completed(self, task_id: int, result: dict) -> None:
        self._update_task(
            task_id,
            status="completed",
            phase="completed",
            finished_at=datetime.now().isoformat(timespec="seconds"),
            message=(
                "扫描完成"
                if not result.get("stock_failures")
                else f"扫描完成，跳过 {result['stock_failures']} 只；最近失败 {result.get('last_failure_symbol') or '-'}: {self._short_error(result.get('last_failure_reason') or 'unknown')}"
            ),
            run_id=result["run_id"],
            progress_current=result["sample_size"],
            progress_total=result["sample_size"],
            cache_hits=result["cache_hits"],
            incremental_updates=result["incremental_updates"],
            full_refreshes=result["full_refreshes"],
            benchmark_cache_mode=result["benchmark_cache_mode"],
        )

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
        frame = pd.DataFrame([dict(row) for row in rows])
        return self._sanitize_benchmark_history(frame).tail(days).reset_index(drop=True)

    def _load_backfill_trade_dates(self, days: int, start_date: str | None = None, end_date: str | None = None) -> list[str]:
        with self.db.connect() as conn:
            clauses: list[str] = []
            params: list[str] = []
            if start_date:
                clauses.append("trade_date >= ?")
                params.append(start_date)
            if end_date:
                clauses.append("trade_date <= ?")
                params.append(end_date)
            where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
            rows = conn.execute(
                f"""
                SELECT DISTINCT trade_date
                FROM price_history
                {where_clause}
                ORDER BY trade_date ASC
                """,
                params,
            ).fetchall()
        trade_dates = [str(row["trade_date"]) for row in rows]
        if start_date or end_date:
            return trade_dates
        return trade_dates[-days:] if len(trade_dates) > days else trade_dates

    def _load_latest_sector_map(self) -> dict[str, str]:
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT ms.symbol, ms.sector
                FROM market_snapshot ms
                JOIN (
                    SELECT symbol, MAX(trade_date) AS latest_trade_date
                    FROM market_snapshot
                    GROUP BY symbol
                ) latest
                  ON latest.symbol = ms.symbol AND latest.latest_trade_date = ms.trade_date
                """
            ).fetchall()
        return {str(row["symbol"]): str(row["sector"] or "未分类") for row in rows}

    def _load_prepared_benchmark_for_backfill(self, start_date: str, end_date: str) -> pd.DataFrame:
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT trade_date, open, close, high, low, volume, amount
                FROM benchmark_history
                WHERE symbol = '000001' AND trade_date <= ?
                ORDER BY trade_date ASC
                """,
                (end_date,),
            ).fetchall()
        frame = pd.DataFrame([dict(row) for row in rows])
        return _prepare_history(self._sanitize_benchmark_history(frame))

    def _load_prepared_symbol_histories_for_backfill(
        self,
        start_date: str,
        end_date: str,
        symbols: set[str],
    ) -> dict[str, pd.DataFrame]:
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT symbol, trade_date, open, close, high, low, volume, amount
                FROM price_history
                WHERE trade_date <= ?
                ORDER BY symbol ASC, trade_date ASC
                """,
                (end_date,),
            ).fetchall()
        frame = pd.DataFrame([dict(row) for row in rows])
        histories: dict[str, pd.DataFrame] = {}
        if frame.empty:
            return histories
        for symbol, group in frame.groupby("symbol", sort=False):
            symbol_str = str(symbol)
            if symbols and symbol_str not in symbols:
                continue
            prepared = _prepare_history(group.reset_index(drop=True))
            if prepared.empty:
                continue
            histories[symbol_str] = prepared
        return histories

    @staticmethod
    def _build_benchmark_windows(benchmark: pd.DataFrame, target_dates: list[str]) -> dict[str, pd.DataFrame]:
        if benchmark.empty:
            return {}
        labels = benchmark["trade_date"].dt.strftime("%Y-%m-%d")
        windows: dict[str, pd.DataFrame] = {}
        for row_index in benchmark.index[labels.isin(target_dates)].tolist():
            trade_date = str(labels.iloc[row_index])
            windows[trade_date] = benchmark.iloc[: row_index + 1]
        return windows

    def _build_historical_sector_metrics(self, trade_dates: list[str], sector_map: dict[str, str]) -> dict[tuple[str, str], dict[str, float]]:
        if not trade_dates:
            return {}
        with self.db.connect() as conn:
            placeholders = ", ".join("?" for _ in trade_dates)
            rows = conn.execute(
                f"""
                SELECT symbol, trade_date, close
                FROM price_history
                WHERE trade_date IN ({placeholders})
                ORDER BY symbol ASC, trade_date ASC
                """,
                trade_dates,
            ).fetchall()
        frame = pd.DataFrame([dict(row) for row in rows])
        if frame.empty:
            return {}
        frame["symbol"] = frame["symbol"].astype(str)
        frame["sector"] = frame["symbol"].map(sector_map).fillna("未分类")
        frame["trade_date"] = frame["trade_date"].astype(str)
        frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
        frame = frame.sort_values(["symbol", "trade_date"]).reset_index(drop=True)
        frame["prev_close"] = frame.groupby("symbol")["close"].shift(1)
        frame["pct_change"] = ((frame["close"] - frame["prev_close"]) / frame["prev_close"]).fillna(0.0) * 100
        grouped = (
            frame.groupby(["trade_date", "sector"], dropna=False)
            .agg(
                sector_change=("pct_change", "mean"),
                sector_up_ratio=("pct_change", lambda s: float((s > 0).mean())),
            )
            .reset_index()
        )
        return {
            (str(row["trade_date"]), str(row["sector"])): {
                "sector_change": round(float(row["sector_change"]), 4),
                "sector_up_ratio": round(float(row["sector_up_ratio"]), 6),
            }
            for _, row in grouped.iterrows()
        }

    def _get_history_with_cache(self, provider, symbol: str, days: int, cache_stats: CacheStats, trade_date: str) -> pd.DataFrame:
        cached = self._load_cached_history(symbol, days)
        mode, history = self._resolve_history_cache(provider, symbol, days, cached, benchmark=False, target_trade_date=trade_date)
        self._last_cache_mode = mode
        if mode == "hit":
            cache_stats.cache_hits += 1
        elif mode == "incremental":
            cache_stats.incremental_updates += 1
        else:
            cache_stats.full_refreshes += 1
        return history

    def _get_benchmark_with_cache(self, provider, days: int, cache_stats: CacheStats, trade_date: str) -> pd.DataFrame:
        cached = self._load_cached_benchmark("000001", days)
        mode, history = self._resolve_history_cache(provider, "000001", days, cached, benchmark=True, target_trade_date=trade_date)
        history = self._sanitize_benchmark_history(history)
        cache_stats.benchmark_cache_mode = mode
        self._save_benchmark_history("000001", history)
        return history

    def _resolve_history_cache(
        self,
        provider,
        symbol: str,
        days: int,
        cached: pd.DataFrame,
        benchmark: bool,
        target_trade_date: str,
    ) -> tuple[str, pd.DataFrame]:
        if cached.empty:
            fresh = provider.fetch_benchmark_history(days=days) if benchmark else provider.fetch_stock_history(symbol=symbol, days=days)
            if benchmark:
                fresh = self._sanitize_benchmark_history(fresh)
            return "full", fresh

        latest_cached_date = pd.to_datetime(cached["trade_date"]).max().date()
        expected_trade_date = pd.to_datetime(target_trade_date).date()
        cache_is_fresh = latest_cached_date >= expected_trade_date
        if len(cached) >= days and cache_is_fresh:
            return "hit", cached.tail(days).reset_index(drop=True)

        if len(cached) < days:
            fresh = provider.fetch_benchmark_history(days=days) if benchmark else provider.fetch_stock_history(symbol=symbol, days=days)
            if benchmark:
                fresh = self._sanitize_benchmark_history(fresh)
            return "full", self._merge_history(cached, fresh, days)

        incremental_start = (latest_cached_date + timedelta(days=1)).isoformat()
        if benchmark:
            fresh = provider.fetch_benchmark_history(days=days)
            fresh = fresh[fresh["trade_date"] >= incremental_start].reset_index(drop=True)
            fresh = self._sanitize_benchmark_history(fresh)
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
        trade_date: str,
        sample_size: int,
        cache_stats: CacheStats,
    ) -> int:
        with self.db.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO analysis_run
                (provider, created_at, trade_date, sample_size,
                 cache_hits, incremental_updates, full_refreshes, benchmark_cache_mode)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    provider,
                    datetime.now().isoformat(timespec="seconds"),
                    trade_date,
                    sample_size,
                    cache_stats.cache_hits,
                    cache_stats.incremental_updates,
                    cache_stats.full_refreshes,
                    cache_stats.benchmark_cache_mode,
                ),
            )
            run_id = int(cursor.lastrowid)
        return run_id

    @staticmethod
    def _merge_history(cached: pd.DataFrame, incoming: pd.DataFrame, days: int) -> pd.DataFrame:
        merged = pd.concat([cached, incoming], ignore_index=True) if not cached.empty else incoming.copy()
        merged = merged.drop_duplicates(subset=["trade_date"], keep="last").sort_values("trade_date").reset_index(drop=True)
        return merged.tail(days).reset_index(drop=True)

    @staticmethod
    def _sanitize_benchmark_history(history: pd.DataFrame) -> pd.DataFrame:
        if history.empty:
            return history

        frame = history.copy()
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce")
        for column in ["open", "close", "high", "low", "volume", "amount"]:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

        frame = frame.dropna(subset=["trade_date", "close"]).sort_values("trade_date").reset_index(drop=True)
        positive = frame[frame["close"] > 0].copy()
        if positive.empty:
            return frame

        median_close = float(positive["close"].median())
        if median_close <= 0:
            return frame

        lower_bound = median_close * 0.5
        upper_bound = median_close * 1.5
        cleaned = positive[(positive["close"] >= lower_bound) & (positive["close"] <= upper_bound)].copy()
        if cleaned.empty:
            cleaned = frame

        cleaned = cleaned.reset_index(drop=True)
        cleaned["trade_date"] = cleaned["trade_date"].dt.strftime("%Y-%m-%d")
        return cleaned

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

    @staticmethod
    def _build_daily_outputs(
        snapshot: pd.DataFrame,
        history_cache: dict[str, pd.DataFrame],
        benchmark: pd.DataFrame,
    ) -> tuple[list[AnalysisResult], list[dict], list[dict]]:
        results: list[AnalysisResult] = []
        daily_factors: list[dict] = []
        daily_scores: list[dict] = []

        if snapshot.empty:
            return results, daily_factors, daily_scores

        for _, row in snapshot.iterrows():
            symbol = str(row["symbol"])
            history = history_cache.get(symbol)
            if history is None or history.empty:
                continue
            stock = pd.Series(row)
            result = score_stock(stock, history, benchmark)
            if result:
                results.append(result)
            factor = build_daily_factor_snapshot(stock, history, benchmark)
            if factor:
                daily_factors.append(factor)
            score_record = build_daily_score_snapshot(stock, history, benchmark)
            if score_record:
                daily_scores.append(score_record)

        return results, daily_factors, daily_scores

    def _save_daily_factors(self, rows: list[dict]) -> None:
        if not rows:
            return
        with self.db.connect() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO daily_factor
                (trade_date, symbol, close, pct_change, ma5, ma10, ma20, ma30, ma60,
                 vol_ma5, atr14, prior_20_high, cmf21, mfi14, sector_change, sector_up_ratio,
                 benchmark_close, benchmark_ma20, benchmark_prev_ma20)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row["trade_date"],
                        row["symbol"],
                        row["close"],
                        row["pct_change"],
                        row["ma5"],
                        row["ma10"],
                        row["ma20"],
                        row["ma30"],
                        row["ma60"],
                        row["vol_ma5"],
                        row["atr14"],
                        row["prior_20_high"],
                        row["cmf21"],
                        row["mfi14"],
                        row["sector_change"],
                        row["sector_up_ratio"],
                        row["benchmark_close"],
                        row["benchmark_ma20"],
                        row["benchmark_prev_ma20"],
                    )
                    for row in rows
                ],
            )

    def _save_daily_scores(self, rows: list[dict]) -> None:
        if not rows:
            return
        with self.db.connect() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO daily_score
                (trade_date, symbol, score_total, score_ma_trend, score_volume_pattern,
                 score_capital_sector, score_breakout, score_hold, score_benchmark,
                 signals, score_breakdown, summary, score_source, review_updated_at, score_version)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row["trade_date"],
                        row["symbol"],
                        row["score_total"],
                        row["score_ma_trend"],
                        row["score_volume_pattern"],
                        row["score_capital_sector"],
                        row["score_breakout"],
                        row["score_hold"],
                        row["score_benchmark"],
                        json.dumps(row["signals"], ensure_ascii=False),
                        json.dumps(row["score_breakdown"], ensure_ascii=False),
                        row["summary"],
                        row["score_source"],
                        row["review_updated_at"],
                        row["score_version"],
                    )
                    for row in rows
                ],
            )

    def _latest_benchmark_change(self, trade_date: str) -> float:
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT trade_date, close
                FROM benchmark_history
                WHERE symbol = '000001' AND trade_date <= ?
                ORDER BY trade_date DESC
                LIMIT 2
                """,
                (trade_date,),
            ).fetchall()
        if len(rows) < 2:
            return 0.0
        latest_close = float(rows[0]["close"])
        prev_close = float(rows[1]["close"])
        if not prev_close:
            return 0.0
        return round(((latest_close - prev_close) / prev_close) * 100, 2)

    @staticmethod
    def _build_detail_from_daily_tables(
        latest_daily_score,
        latest_daily_factor,
        snapshot: pd.Series,
        snapshot_trade_date: str,
    ) -> dict | None:
        if not latest_daily_score:
            return None

        daily_trade_date = str(latest_daily_score["trade_date"])
        if daily_trade_date != snapshot_trade_date:
            return None

        signals = json.loads(latest_daily_score["signals"]) if latest_daily_score["signals"] else []
        score_breakdown = json.loads(latest_daily_score["score_breakdown"]) if latest_daily_score["score_breakdown"] else []
        daily_factor = dict(latest_daily_factor) if latest_daily_factor else None

        return {
            "run_id": None,
            "symbol": str(snapshot["symbol"]),
            "name": str(snapshot.get("name", "")),
            "score": float(latest_daily_score["score_total"]),
            "latest_price": round(float(snapshot.get("latest_price", daily_factor["close"] if daily_factor else 0) or 0), 2),
            "pct_change": round(float(snapshot.get("pct_change", daily_factor["pct_change"] if daily_factor else 0) or 0), 2),
            "sector": str(snapshot.get("sector", "未分类") or "未分类"),
            "summary": str(latest_daily_score["summary"] or ""),
            "signals": signals,
            "score_breakdown": score_breakdown,
            "score_source": str(latest_daily_score["score_source"] or "system"),
            "review_updated_at": latest_daily_score["review_updated_at"],
            "score_version": latest_daily_score["score_version"],
            "daily_factor": daily_factor,
        }
