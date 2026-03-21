from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
import json
from pathlib import Path
import re
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
        self._recover_stale_backtest_tasks()
        self._recover_stale_backtest_compare_tasks()
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

    def backtest_templates(self) -> list[dict]:
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT id, template_key, name, description, config_json, sort_order, is_builtin, updated_at
                FROM backtest_template
                ORDER BY sort_order ASC, id ASC
                """
            ).fetchall()
        payload: list[dict] = []
        for row in rows:
            item = dict(row)
            item["config"] = json.loads(item["config_json"]) if item.get("config_json") else {}
            payload.append(item)
        return payload

    def create_backtest_template(self, name: str, description: str | None, config: dict | None = None) -> int:
        normalized = self.backtest_runner._normalize_config(dict(config or {}))
        clean_name = str(name or "").strip()
        if not clean_name:
            raise ValueError("模板名称不能为空")
        clean_description = str(description or "").strip() or None
        template_key = self._make_backtest_template_key(clean_name)
        now = datetime.now().isoformat(timespec="seconds")
        with self.db.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO backtest_template
                (template_key, name, description, config_json, sort_order, is_builtin, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 0, ?, ?)
                """,
                (
                    template_key,
                    clean_name,
                    clean_description,
                    json.dumps(normalized, ensure_ascii=False),
                    self._next_backtest_template_sort_order(conn),
                    now,
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def update_backtest_template(self, template_id: int, name: str, description: str | None, config: dict | None = None) -> None:
        normalized = self.backtest_runner._normalize_config(dict(config or {}))
        clean_name = str(name or "").strip()
        if not clean_name:
            raise ValueError("模板名称不能为空")
        clean_description = str(description or "").strip() or None
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT id, is_builtin
                FROM backtest_template
                WHERE id = ?
                """,
                (template_id,),
            ).fetchone()
            if not row:
                raise ValueError("未找到对应模板")
            if int(row["is_builtin"] or 0):
                raise ValueError("内置模板不能直接修改，请另存为自定义模板")
            conn.execute(
                """
                UPDATE backtest_template
                SET name = ?, description = ?, config_json = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    clean_name,
                    clean_description,
                    json.dumps(normalized, ensure_ascii=False),
                    datetime.now().isoformat(timespec="seconds"),
                    template_id,
                ),
            )

    def delete_backtest_template(self, template_id: int) -> None:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT id, is_builtin
                FROM backtest_template
                WHERE id = ?
                """,
                (template_id,),
            ).fetchone()
            if not row:
                raise ValueError("未找到对应模板")
            if int(row["is_builtin"] or 0):
                raise ValueError("内置模板不能删除")
            conn.execute("DELETE FROM backtest_template WHERE id = ?", (template_id,))

    def run_backtest(self, config: dict | None = None) -> dict:
        return self.backtest_runner.run(config)

    def start_backtest_task(self, config: dict | None = None) -> int:
        payload = dict(config or {})
        normalized = self.backtest_runner._normalize_config(payload)
        started_at = datetime.now().isoformat(timespec="seconds")
        with self.db.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO backtest_task
                (status, phase, started_at, progress_current, progress_total, message, config_json)
                VALUES ('running', 'pending', ?, 0, 0, ?, ?)
                """,
                (started_at, "任务已创建，等待回测启动", json.dumps(normalized, ensure_ascii=False)),
            )
            task_id = int(cursor.lastrowid)
        thread = threading.Thread(target=self._run_backtest_task, args=(task_id, normalized), daemon=True)
        thread.start()
        return task_id

    def recent_backtests(self, limit: int = 20) -> list[dict]:
        return self.backtest_runner.recent_runs(limit=limit)

    def start_backtest_compare_task(
        self,
        name: str,
        parameter_key: str,
        parameter_label: str,
        values: list[float | int],
        base_config: dict | None = None,
        secondary_parameter_key: str | None = None,
        secondary_parameter_label: str | None = None,
        secondary_values: list[float | int] | None = None,
    ) -> int:
        normalized = self.backtest_runner._normalize_config(dict(base_config or {}))
        started_at = datetime.now().isoformat(timespec="seconds")
        dimensions = [
            {
                "key": parameter_key,
                "label": parameter_label,
                "values": list(values),
            }
        ]
        if secondary_parameter_key and secondary_values:
            dimensions.append(
                {
                    "key": secondary_parameter_key,
                    "label": secondary_parameter_label or secondary_parameter_key,
                    "values": list(secondary_values),
                }
            )
        combinations = self._build_compare_combinations(dimensions)
        parameter_key_text = "|".join(item["key"] for item in dimensions)
        parameter_label_text = " / ".join(item["label"] for item in dimensions)
        with self.db.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO backtest_compare_task
                (name, status, phase, started_at, progress_current, progress_total, message,
                 parameter_key, parameter_label, values_json, base_config_json)
                VALUES ('', 'running', 'pending', ?, 0, ?, ?, ?, ?, ?, ?)
                """,
                (
                    started_at,
                    len(combinations),
                    "参数对比任务已创建，等待执行",
                    parameter_key_text,
                    parameter_label_text,
                    json.dumps({"dimensions": dimensions, "combinations": combinations}, ensure_ascii=False),
                    json.dumps(normalized, ensure_ascii=False),
                ),
            )
            task_id = int(cursor.lastrowid)
            task_name = str(name or f"{parameter_label_text} 参数对比").strip() or f"{parameter_label_text} 参数对比"
            conn.execute("UPDATE backtest_compare_task SET name = ? WHERE id = ?", (task_name, task_id))
        thread = threading.Thread(
            target=self._run_backtest_compare_task,
            args=(task_id, task_name, parameter_label_text, combinations, normalized),
            daemon=True,
        )
        thread.start()
        return task_id

    def recent_backtest_compare_tasks(self, limit: int = 10) -> list[dict]:
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM backtest_compare_task
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._build_backtest_compare_task_payload(dict(row)) for row in rows]

    def backtest_compare_task_detail(self, task_id: int) -> dict | None:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM backtest_compare_task
                WHERE id = ?
                """,
                (task_id,),
            ).fetchone()
        return self._build_backtest_compare_task_payload(dict(row)) if row else None

    def backtest_detail(self, run_id: int) -> dict | None:
        detail = self.backtest_runner.run_detail(run_id)
        if not detail or not detail.get("summary"):
            return detail
        summary = detail["summary"]
        summary["buy_execution_blocked_breakdown_label"] = self._label_execution_breakdown(
            summary.get("buy_execution_blocked_breakdown") or {}
        )
        summary["sell_execution_deferred_breakdown_label"] = self._label_execution_breakdown(
            summary.get("sell_execution_deferred_breakdown") or {}
        )
        for trade in detail.get("trades") or []:
            trade["side_label"] = self._label_trade_side(str(trade.get("side") or ""))
            trade["reason_label"] = self._label_trade_reason(str(trade.get("reason") or ""))
        self._attach_backtest_trade_returns(detail)
        self._attach_backtest_daily_pnl(detail)
        self._decorate_backtest_positions(detail)
        self._group_backtest_trades(detail)
        self._group_backtest_positions(detail)
        return detail

    def export_backtest_markdown(self, run_id: int) -> Path:
        detail = self.backtest_detail(run_id)
        if not detail:
            raise ValueError(f"未找到回测 {run_id}")

        export_dir = Path(settings.database_path).resolve().parent / "exports" / "backtests"
        export_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_name = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff_-]+", "-", str(detail.get("name") or f"run-{run_id}")).strip("-")
        filename = f"backtest_{run_id}_{safe_name or 'run'}_{timestamp}.md"
        output_path = export_dir / filename
        output_path.write_text(self._build_backtest_markdown(detail), encoding="utf-8")
        return output_path

    def latest_backtest_task(self) -> dict | None:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT id, status, phase, started_at, finished_at, progress_current, progress_total,
                       last_trade_date, message, run_id, config_json
                FROM backtest_task
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
        return self._build_backtest_task_payload(dict(row)) if row else None

    def recent_backtest_tasks(self, limit: int = 10) -> list[dict]:
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT id, status, phase, started_at, finished_at, progress_current, progress_total,
                       last_trade_date, message, run_id, config_json
                FROM backtest_task
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._build_backtest_task_payload(dict(row)) for row in rows]

    def backtest_task_detail(self, task_id: int) -> dict | None:
        with self.db.connect() as conn:
            row = conn.execute(
                """
                SELECT id, status, phase, started_at, finished_at, progress_current, progress_total,
                       last_trade_date, message, run_id, config_json
                FROM backtest_task
                WHERE id = ?
                """,
                (task_id,),
            ).fetchone()
        return self._build_backtest_task_payload(dict(row)) if row else None

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
                    message = '历史回填任务在应用重启前中断，可从当前进度继续续跑'
                WHERE status = 'running'
                """,
                (datetime.now().isoformat(timespec="seconds"),),
            )

    def _recover_stale_backtest_tasks(self) -> None:
        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE backtest_task
                SET status = 'failed',
                    phase = 'failed',
                    finished_at = ?,
                    message = '回测任务在应用重启前中断'
                WHERE status = 'running'
                """,
                (datetime.now().isoformat(timespec="seconds"),),
            )

    def _recover_stale_backtest_compare_tasks(self) -> None:
        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE backtest_compare_task
                SET status = 'failed',
                    phase = 'failed',
                    finished_at = ?,
                    message = '参数对比任务在应用重启前中断'
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

    @staticmethod
    def _label_execution_breakdown(breakdown: dict[str, int]) -> str:
        if not breakdown:
            return "-"
        labels = {
            "limit_up": "涨停开盘买不到",
            "limit_down": "跌停开盘卖不掉",
            "missing": "缺少开盘价/停牌",
        }
        return "；".join(f"{labels.get(key, key)} {value}" for key, value in breakdown.items())

    @staticmethod
    def _label_trade_side(side: str) -> str:
        labels = {
            "buy": "买入",
            "sell": "卖出",
        }
        return labels.get(side, side or "-")

    @staticmethod
    def _label_trade_reason(reason: str) -> str:
        if not reason:
            return "-"
        labels = {
            "buy_strict": "提前型严格买入",
            "buy_momentum": "提前型增强买入",
            "sell_trim": "分级止盈减仓",
            "sell_break_ma5": "跌破MA5放量清仓",
            "sell_drawdown": "高点回撤止损",
            "sell_time_stop": "时间止损",
        }
        parts = [labels.get(item.strip(), item.strip()) for item in reason.split(",") if item.strip()]
        return "、".join(parts) if parts else reason

    @staticmethod
    def _format_percent(value: float | None) -> str:
        if value is None:
            return "-"
        return f"{value * 100:.2f}%"

    @staticmethod
    def _format_amount(value: float | None) -> str:
        if value is None:
            return "-"
        return f"{value:.2f}"

    def _attach_backtest_trade_returns(self, detail: dict) -> None:
        trades = detail.get("trades") or []
        positions = detail.get("positions") or []
        latest_position_by_symbol: dict[str, dict] = {}
        for row in positions:
            symbol = str(row.get("symbol") or "")
            latest_position_by_symbol[symbol] = row

        open_entries: dict[str, list[dict]] = {}
        for trade in trades:
            trade["pnl_amount"] = None
            trade["return_rate"] = None
            trade["return_text"] = "-"
            trade["pnl_text"] = "-"
            symbol = str(trade.get("symbol") or "")
            side = str(trade.get("side") or "")
            if side == "buy":
                open_entries.setdefault(symbol, []).append(trade)
                continue

            entry_queue = open_entries.get(symbol) or []
            if not entry_queue:
                continue
            entry = entry_queue.pop(0)
            buy_cost = float(entry.get("net_amount") or 0.0)
            sell_net = float(trade.get("net_amount") or 0.0)
            pnl_amount = round(sell_net - buy_cost, 4)
            return_rate = round(pnl_amount / buy_cost, 6) if buy_cost else 0.0
            entry["pnl_amount"] = pnl_amount
            entry["return_rate"] = return_rate
            entry["return_text"] = self._format_percent(return_rate)
            entry["pnl_text"] = self._format_amount(pnl_amount)
            trade["pnl_amount"] = pnl_amount
            trade["return_rate"] = return_rate
            trade["return_text"] = self._format_percent(return_rate)
            trade["pnl_text"] = self._format_amount(pnl_amount)

        for symbol, queued in open_entries.items():
            latest_position = latest_position_by_symbol.get(symbol)
            for entry in queued:
                if not latest_position:
                    continue
                pnl_amount = round(float(latest_position.get("unrealized_pnl") or 0.0), 4)
                buy_cost = float(entry.get("net_amount") or 0.0)
                return_rate = round(pnl_amount / buy_cost, 6) if buy_cost else 0.0
                entry["pnl_amount"] = pnl_amount
                entry["return_rate"] = return_rate
                entry["return_text"] = f"{self._format_percent(return_rate)} (未实现)"
                entry["pnl_text"] = f"{self._format_amount(pnl_amount)} (未实现)"

    @staticmethod
    def _decorate_backtest_positions(detail: dict) -> None:
        position_dates_by_symbol: dict[str, list[str]] = {}
        sell_dates_by_symbol: dict[str, list[str]] = {}
        for row in detail.get("positions") or []:
            symbol = str(row.get("symbol") or "")
            position_dates_by_symbol.setdefault(symbol, []).append(str(row.get("trade_date") or ""))
        for symbol, dates in position_dates_by_symbol.items():
            position_dates_by_symbol[symbol] = sorted(dates)
        for trade in detail.get("trades") or []:
            if str(trade.get("side") or "") != "sell":
                continue
            symbol = str(trade.get("symbol") or "")
            sell_dates_by_symbol.setdefault(symbol, []).append(str(trade.get("execution_date") or ""))

        for row in detail.get("positions") or []:
            symbol = str(row.get("symbol") or "")
            trade_date = str(row.get("trade_date") or "")
            weight = float(row.get("weight") or 0.0)
            unrealized_pnl = float(row.get("unrealized_pnl") or 0.0)
            row["weight_text"] = f"{weight * 100:.2f}%"
            row["unrealized_pnl_text"] = f"{unrealized_pnl:.2f}"
            row["market_value_text"] = f"{float(row.get('market_value') or 0.0):.2f}"
            row["cost_price_text"] = f"{float(row.get('cost_price') or 0.0):.4f}"
            row["close_price_text"] = f"{float(row.get('close_price') or 0.0):.4f}"
            row["daily_pnl_text"] = f"{float(row.get('daily_pnl') or 0.0):.2f}"
            row["position_exit_label"] = ""
            later_position_dates = [item for item in position_dates_by_symbol.get(symbol, []) if item > trade_date]
            later_sell_dates = [item for item in sell_dates_by_symbol.get(symbol, []) if item > trade_date]
            if not later_position_dates and later_sell_dates:
                row["position_exit_label"] = "当日最后持仓日"

    @staticmethod
    def _attach_backtest_daily_pnl(detail: dict) -> None:
        nav_rows = detail.get("nav") or []
        prev_nav = None
        nav_daily: dict[str, float] = {}
        for row in nav_rows:
            nav_value = float(row.get("nav") or 0.0)
            daily_pnl = round(nav_value - prev_nav, 4) if prev_nav is not None else 0.0
            row["daily_pnl"] = daily_pnl
            row["daily_pnl_text"] = f"{daily_pnl:.2f}"
            nav_daily[str(row.get("trade_date") or "")] = daily_pnl
            prev_nav = nav_value

        positions_by_date: dict[str, list[dict]] = {}
        for row in detail.get("positions") or []:
            positions_by_date.setdefault(str(row.get("trade_date") or ""), []).append(row)

        trades_by_date: dict[str, list[dict]] = {}
        for row in detail.get("trades") or []:
            row["daily_impact_amount"] = None
            row["daily_impact_text"] = "-"
            trades_by_date.setdefault(str(row.get("execution_date") or ""), []).append(row)

        prev_unrealized_by_symbol: dict[str, float] = {}
        prev_market_value_by_symbol: dict[str, float] = {}
        for trade_date in sorted(set(positions_by_date.keys()) | set(trades_by_date.keys())):
            rows = positions_by_date.get(trade_date, [])
            buy_symbols_today = {
                str(trade.get("symbol") or "")
                for trade in trades_by_date.get(trade_date, [])
                if str(trade.get("side") or "") == "buy"
            }
            current_unrealized_by_symbol: dict[str, float] = {}
            current_market_value_by_symbol: dict[str, float] = {}
            for row in rows:
                symbol = str(row.get("symbol") or "")
                current_unrealized = float(row.get("unrealized_pnl") or 0.0)
                if symbol in prev_unrealized_by_symbol:
                    daily_pnl = round(current_unrealized - prev_unrealized_by_symbol[symbol], 4)
                else:
                    daily_pnl = round(current_unrealized, 4)
                row["daily_pnl"] = daily_pnl
                row["daily_pnl_text"] = f"{daily_pnl:.2f}"
                row["position_event_label"] = "当日新开仓" if symbol in buy_symbols_today else ""
                current_unrealized_by_symbol[symbol] = current_unrealized
                current_market_value_by_symbol[symbol] = float(row.get("market_value") or 0.0)

            for trade in trades_by_date.get(trade_date, []):
                symbol = str(trade.get("symbol") or "")
                side = str(trade.get("side") or "")
                if side == "buy":
                    impact = round(
                        current_market_value_by_symbol.get(symbol, 0.0) - float(trade.get("net_amount") or 0.0),
                        4,
                    )
                else:
                    impact = round(
                        float(trade.get("net_amount") or 0.0) - prev_market_value_by_symbol.get(symbol, 0.0),
                        4,
                    )
                trade["daily_impact_amount"] = impact
                trade["daily_impact_text"] = f"{impact:.2f}"

            prev_unrealized_by_symbol = current_unrealized_by_symbol
            prev_market_value_by_symbol = current_market_value_by_symbol

        detail["nav_daily_pnl"] = nav_daily

    @staticmethod
    def _group_backtest_positions(detail: dict) -> None:
        position_grouped: dict[str, list[dict]] = {}
        for row in detail.get("positions") or []:
            position_grouped.setdefault(str(row.get("trade_date") or ""), []).append(row)

        calendar: list[dict] = []
        for nav_row in detail.get("nav") or []:
            trade_date = str(nav_row.get("trade_date") or "")
            rows = position_grouped.get(trade_date, [])
            pnl = round(float(detail.get("nav_daily_pnl", {}).get(trade_date, 0.0)), 4)
            market_value = round(sum(float(item.get("market_value") or 0.0) for item in rows), 4)
            item = {
                "trade_date": trade_date,
                "position_count": len(rows),
                "daily_pnl": pnl,
                "daily_pnl_text": f"{pnl:.2f}",
                "market_value": market_value,
                "market_value_text": f"{market_value:.2f}",
                "tone": "flat",
            }
            if pnl > 0:
                item["tone"] = "up"
            elif pnl < 0:
                item["tone"] = "down"
            calendar.append(item)

        month_groups: list[dict] = []
        month_map: dict[str, dict] = {}
        for item in calendar:
            month_key = str(item["trade_date"])[:7]
            month_group = month_map.get(month_key)
            if not month_group:
                month_group = {
                    "month": month_key,
                    "days": [],
                    "realized_pnl": 0.0,
                    "realized_pnl_text": "0.00",
                    "month_end_unrealized_pnl": 0.0,
                    "month_end_unrealized_pnl_text": "0.00",
                    "net_change_pnl": 0.0,
                    "net_change_pnl_text": "0.00",
                    "tone": "flat",
                }
                month_map[month_key] = month_group
                month_groups.append(month_group)
            month_group["days"].append(item)

        realized_by_month: dict[str, float] = {}
        for trade in detail.get("trades") or []:
            if str(trade.get("side") or "") != "sell":
                continue
            pnl_amount = trade.get("pnl_amount")
            if pnl_amount is None:
                continue
            month_key = str(trade.get("execution_date") or "")[:7]
            realized_by_month[month_key] = round(realized_by_month.get(month_key, 0.0) + float(pnl_amount), 4)

        month_end_nav: dict[str, float] = {}
        for nav_row in detail.get("nav") or []:
            month_end_nav[str(nav_row.get("trade_date") or "")[:7]] = float(nav_row.get("nav") or 0.0)

        previous_month_end_nav = float(detail.get("summary", {}).get("initial_capital") or 0.0)
        for month_group in month_groups:
            last_day = month_group["days"][-1] if month_group["days"] else None
            last_day_positions = position_grouped.get(str(last_day["trade_date"]) if last_day else "", [])
            month_end_unrealized = round(
                sum(float(row.get("unrealized_pnl") or 0.0) for row in last_day_positions),
                4,
            )
            realized = float(realized_by_month.get(month_group["month"], 0.0))
            current_month_end_nav = float(month_end_nav.get(month_group["month"], previous_month_end_nav))
            net_change_pnl = round(current_month_end_nav - previous_month_end_nav, 4)
            month_group["realized_pnl"] = realized
            month_group["realized_pnl_text"] = f"{realized:.2f}"
            month_group["month_end_unrealized_pnl"] = month_end_unrealized
            month_group["month_end_unrealized_pnl_text"] = f"{month_end_unrealized:.2f}"
            month_group["net_change_pnl"] = net_change_pnl
            month_group["net_change_pnl_text"] = f"{net_change_pnl:.2f}"
            if net_change_pnl > 0:
                month_group["tone"] = "up"
            elif net_change_pnl < 0:
                month_group["tone"] = "down"
            for day in month_group["days"]:
                day["month_key"] = month_group["month"]
                day["month_realized_pnl"] = realized
                day["month_realized_pnl_text"] = month_group["realized_pnl_text"]
                day["month_end_unrealized_pnl"] = month_end_unrealized
                day["month_end_unrealized_pnl_text"] = month_group["month_end_unrealized_pnl_text"]
                day["month_net_change_pnl"] = net_change_pnl
                day["month_net_change_pnl_text"] = month_group["net_change_pnl_text"]
            previous_month_end_nav = current_month_end_nav

        detail["position_calendar"] = calendar
        detail["position_calendar_months"] = month_groups
        detail["positions_by_date"] = position_grouped
        detail["position_active_date"] = calendar[-1]["trade_date"] if calendar else ""

    @staticmethod
    def _group_backtest_trades(detail: dict) -> None:
        grouped: dict[str, list[dict]] = {}
        for row in detail.get("trades") or []:
            grouped.setdefault(str(row.get("execution_date") or ""), []).append(row)
        detail["trades_by_date"] = grouped

    @staticmethod
    def _format_backtest_task_error(exc: Exception) -> dict[str, str]:
        raw = " ".join(str(exc).split()) or exc.__class__.__name__
        lower = raw.lower()

        if "应用重启前中断" in raw:
            return {
                "summary": "回测失败：任务在应用重启前中断",
                "detail": raw,
                "hint": "这类失败通常不是策略逻辑错误，重新发起任务即可。",
            }
        if "daily_score" in lower and ("不足" in raw or "not enough" in lower):
            return {
                "summary": "回测失败：可用评分数据不足",
                "detail": raw,
                "hint": "先检查所选区间内是否已经完成 daily_score 和 daily_factor 回填。",
            }
        if "start_date" in lower or "end_date" in lower or "日期" in raw:
            return {
                "summary": "回测失败：日期区间配置无效",
                "detail": raw,
                "hint": "请检查开始日期、结束日期和最近交易日数的配置是否合理。",
            }
        if "sqlite" in lower or "database" in lower or "locked" in lower or "busy" in lower:
            return {
                "summary": "回测失败：数据库读写异常",
                "detail": raw,
                "hint": "数据库可能正被其他任务占用，稍后重试会更稳妥。",
            }
        if "json" in lower or "config" in lower or "float" in lower or "int" in lower:
            return {
                "summary": "回测失败：回测参数解析异常",
                "detail": raw,
                "hint": "请检查回测表单中的参数格式，尤其是日期、资金、费率和阈值。",
            }
        return {
            "summary": "回测失败：执行过程中出现异常",
            "detail": raw,
            "hint": "可以根据详细原因继续定位，必要时缩小区间先做小样本验证。",
        }

    def _run_backtest_task(self, task_id: int, config: dict) -> None:
        def progress_callback(payload: dict) -> None:
            self._update_backtest_task(
                task_id,
                phase=str(payload.get("phase") or "executing"),
                message=str(payload.get("message") or "正在执行回测"),
                progress_current=int(payload.get("progress_current") or 0),
                progress_total=int(payload.get("progress_total") or 0),
                last_trade_date=payload.get("last_trade_date"),
            )

        try:
            result = self.backtest_runner.run(config, progress_callback=progress_callback)
            self._update_backtest_task(
                task_id,
                status="completed",
                phase="completed",
                finished_at=datetime.now().isoformat(timespec="seconds"),
                message="回测完成",
                run_id=result["run_id"],
                progress_current=result.get("available_signal_days", 0),
                progress_total=result.get("available_signal_days", 0),
                last_trade_date=result.get("end_date"),
            )
        except Exception as exc:
            error_info = self._format_backtest_task_error(exc)
            self._update_backtest_task(
                task_id,
                status="failed",
                phase="failed",
                finished_at=datetime.now().isoformat(timespec="seconds"),
                message=error_info["detail"],
            )

    def _update_backtest_task(self, task_id: int, **fields) -> None:
        if not fields:
            return
        assignments = ", ".join(f"{key} = ?" for key in fields.keys())
        values = list(fields.values()) + [task_id]
        with self.db.connect() as conn:
            conn.execute(f"UPDATE backtest_task SET {assignments} WHERE id = ?", values)

    def _run_backtest_compare_task(
        self,
        task_id: int,
        task_name: str,
        parameter_label: str,
        combinations: list[dict],
        base_config: dict,
    ) -> None:
        run_ids: list[int] = []
        rows: list[dict] = []
        total = len(combinations)
        try:
            for index, combo in enumerate(combinations, start=1):
                combo_label = combo.get("label") or "-"
                self._update_backtest_compare_task(
                    task_id,
                    phase="executing",
                    progress_current=index - 1,
                    progress_total=total,
                    message=f"正在执行第 {index}/{total} 组：{combo_label}",
                )
                config = dict(base_config)
                parameter_map = dict(combo.get("params") or {})
                config.update(parameter_map)
                config["name"] = f"{task_name} / {combo_label}"
                result = self.backtest_runner.run(config)
                run_ids.append(int(result["run_id"]))
                rows.append(
                    {
                        "run_id": int(result["run_id"]),
                        "name": config["name"],
                        "parameter_value": combo_label,
                        "parameter_map": parameter_map,
                        "total_return": result.get("total_return"),
                        "excess_return": result.get("excess_return"),
                        "max_drawdown": result.get("max_drawdown"),
                        "trade_count": result.get("trade_count"),
                        "win_rate": result.get("win_rate"),
                        "avg_holding_days": result.get("avg_holding_days"),
                    }
                )
                self._update_backtest_compare_task(
                    task_id,
                    progress_current=index,
                    progress_total=total,
                    message=f"已完成第 {index}/{total} 组：{combo_label}",
                )

            best_total = max(rows, key=lambda item: float(item.get("total_return") or -999), default=None)
            best_excess = max(rows, key=lambda item: float(item.get("excess_return") or -999), default=None)
            summary = {
                "rows": rows,
                "best_total_return_run_id": best_total.get("run_id") if best_total else None,
                "best_excess_return_run_id": best_excess.get("run_id") if best_excess else None,
            }
            self._update_backtest_compare_task(
                task_id,
                status="completed",
                phase="completed",
                finished_at=datetime.now().isoformat(timespec="seconds"),
                progress_current=total,
                progress_total=total,
                message="参数对比完成",
                run_ids_json=json.dumps(run_ids, ensure_ascii=False),
                summary_json=json.dumps(summary, ensure_ascii=False),
            )
        except Exception as exc:
            self._update_backtest_compare_task(
                task_id,
                status="failed",
                phase="failed",
                finished_at=datetime.now().isoformat(timespec="seconds"),
                message=str(exc),
                run_ids_json=json.dumps(run_ids, ensure_ascii=False),
                summary_json=json.dumps({"rows": rows, "error": str(exc)}, ensure_ascii=False),
            )

    def _update_backtest_compare_task(self, task_id: int, **fields) -> None:
        if not fields:
            return
        assignments = ", ".join(f"{key} = ?" for key in fields.keys())
        values = list(fields.values()) + [task_id]
        with self.db.connect() as conn:
            conn.execute(f"UPDATE backtest_compare_task SET {assignments} WHERE id = ?", values)

    def _build_backtest_task_payload(self, task: dict) -> dict:
        payload = dict(task)
        payload["config"] = json.loads(payload["config_json"]) if payload.get("config_json") else {}
        payload["phase_label"] = self._phase_label(
            phase=str(payload.get("phase") or ""),
            finished=bool(payload.get("finished_at")),
            status=str(payload.get("status") or ""),
        )
        payload["phase_hint"] = self._phase_hint(
            phase=str(payload.get("phase") or ""),
            status=str(payload.get("status") or ""),
        )
        payload["error_summary"] = ""
        payload["error_detail"] = ""
        payload["error_hint"] = ""
        if str(payload.get("status") or "") == "failed":
            error_info = self._format_backtest_task_error(Exception(str(payload.get("message") or "")))
            payload["error_summary"] = error_info["summary"]
            payload["error_detail"] = error_info["detail"]
            payload["error_hint"] = error_info["hint"]
        return self._enrich_task_timing(payload)

    @staticmethod
    def _build_compare_combinations(dimensions: list[dict]) -> list[dict]:
        combinations: list[dict] = []
        if not dimensions:
            return combinations
        if len(dimensions) == 1:
            dimension = dimensions[0]
            for value in dimension.get("values") or []:
                combinations.append({
                    "label": f"{dimension['label']}={value}",
                    "params": {dimension['key']: value},
                })
            return combinations

        first = dimensions[0]
        second = dimensions[1]
        for first_value in first.get("values") or []:
            for second_value in second.get("values") or []:
                combinations.append(
                    {
                        "label": f"{first['label']}={first_value} / {second['label']}={second_value}",
                        "params": {
                            first['key']: first_value,
                            second['key']: second_value,
                        },
                    }
                )
        return combinations

    def _build_backtest_compare_task_payload(self, task: dict) -> dict:
        payload = dict(task)
        value_payload = json.loads(payload["values_json"]) if payload.get("values_json") else []
        payload["values"] = value_payload.get("dimensions", value_payload) if isinstance(value_payload, dict) else value_payload
        payload["combinations"] = value_payload.get("combinations", []) if isinstance(value_payload, dict) else []
        payload["base_config"] = json.loads(payload["base_config_json"]) if payload.get("base_config_json") else {}
        payload["run_ids"] = json.loads(payload["run_ids_json"]) if payload.get("run_ids_json") else []
        payload["summary"] = json.loads(payload["summary_json"]) if payload.get("summary_json") else {}
        payload["phase_label"] = self._phase_label(
            phase=str(payload.get("phase") or ""),
            finished=bool(payload.get("finished_at")),
            status=str(payload.get("status") or ""),
        )
        payload["phase_hint"] = self._phase_hint(
            phase=str(payload.get("phase") or ""),
            status=str(payload.get("status") or ""),
        )
        return self._enrich_task_timing(payload)

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
        task["phase_hint"] = StockAnalysisService._phase_hint(
            phase=phase,
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
        task["avg_item_text"] = f"{avg_seconds:.2f} 秒/日" if avg_seconds is not None else "-"
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
            "loading": "加载数据中",
            "preparing": "准备信号中",
            "calculating": "计算中",
            "executing": "执行交易中",
            "summarizing": "汇总结果中",
            "writing": "写库中",
        }
        return labels.get(phase, "处理中")

    @staticmethod
    def _phase_hint(phase: str, status: str) -> str:
        if status == "failed" or phase == "failed":
            return "任务已停止，请查看失败原因。"
        if status == "completed" or phase == "completed":
            return "回测结果已经落库，可以查看详情。"
        hints = {
            "pending": "任务已经创建，正在等待后台线程开始执行。",
            "loading": "正在读取交易日、评分数据、行情数据和基准数据。",
            "preparing": "正在整理每日候选股票和市场过滤条件。",
            "calculating": "正在评估买卖规则并生成候选信号。",
            "executing": "正在按交易日推进持仓、成交和净值。",
            "summarizing": "正在汇总收益、回撤、胜率和执行约束统计。",
            "writing": "正在把信号、成交、持仓和净值写入数据库。",
        }
        return hints.get(phase, "任务正在后台处理中。")

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
    def _make_backtest_template_key(name: str) -> str:
        stem = re.sub(r"[^a-z0-9]+", "-", str(name or "").strip().lower()).strip("-")
        if not stem:
            stem = "custom"
        return f"custom-{stem}-{int(time.time() * 1000)}"

    @staticmethod
    def _next_backtest_template_sort_order(conn: sqlite3.Connection) -> int:
        row = conn.execute("SELECT COALESCE(MAX(sort_order), 0) AS max_sort_order FROM backtest_template").fetchone()
        return int(row["max_sort_order"] or 0) + 10

    def _build_backtest_markdown(self, detail: dict) -> str:
        summary = detail.get("summary") or {}
        config = (build_backtest_config_schema().get("defaults") or {}) | (detail.get("config") or {})
        signal_stats = detail.get("signal_stats") or {}
        nav_rows = detail.get("nav") or []
        calendar_rows = detail.get("position_calendar") or []
        trades_by_date = detail.get("trades_by_date") or {}
        positions_by_date = detail.get("positions_by_date") or {}

        def fmt_num(value, digits: int = 2) -> str:
            if value is None:
                return "-"
            return f"{float(value):.{digits}f}"

        def fmt_pct(value, digits: int = 2) -> str:
            if value is None:
                return "-"
            return f"{float(value) * 100:.{digits}f}%"

        buy_rule_desc = {
            "buy_strict": "提前型严格买入：总分>=74，且（均线多头>=14 或 低位启动突破>=12），并满足放量上涨+缩量回调>=14、资金流入+板块强势>=10；同时仍需通过低位限制、近3日量能约束、单笔风险限制、板块5日强度排名和大盘过滤。",
            "buy_momentum": "提前型增强买入：总分>=68，且放量上涨+缩量回调>=14；同时仍需通过核心命中数、低位限制、近3日量能约束、单笔风险限制、板块5日强度排名和大盘过滤。",
        }
        sell_rule_desc = {
            "sell_trim": "分级止盈减仓：持仓盈利 `>= 8%` 且出现放量长上影时，减仓 `50%`。",
            "sell_break_ma5": "跌破MA5放量清仓：`close < MA5` 且 `volume > vol_ma5` 时清仓。",
            "sell_drawdown": "动态移动止盈：峰值盈利 `>= 10%` 时，回撤 `> 5%` 清仓；峰值盈利 `>= 18%` 时，回撤 `> 6%` 清仓；峰值盈利 `>= 30%` 时，回撤容忍度放宽到 `8%`。",
            "sell_time_stop": "时间止损：持仓 `>= 10` 天、累计收益 `< 2%`，且收盘价 `<= MA20` 时清仓。",
        }

        lines: list[str] = [
            f"# 回测报告：{detail.get('name') or f'回测 {detail.get('id') or detail.get('run_id') or ''}'}",
            "",
            f"- 回测编号：`{detail.get('id') or detail.get('run_id')}`",
            f"- 区间：`{detail.get('start_date')}` ~ `{detail.get('end_date')}`",
            f"- 状态：`{detail.get('status')}`",
            f"- 生成时间：`{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`",
            "",
            "## 算法说明",
            "",
            "- 信号生成时点：`T日收盘后`。",
            "- 买入执行时点：`T+1日开盘`。",
            "- 卖出执行时点：触发条件后的 `T+1日开盘`。",
            f"- 基准指数：`{config.get('benchmark_symbol') or '000001'}`，图表中按首日净值归一化展示。",
            f"- 最大持仓：`{config.get('max_positions', '-')}` 只。",
            "- 仓位规则：按“剩余现金 / 新开仓数量”等权分配；已持仓不因新信号调仓；不加仓。",
            "- 交易限制：不允许同一天重复买卖同一只；不考虑融资融券。",
            f"- 手续费：`{fmt_num(config.get('fee_rate'), 4)}`；滑点：`{fmt_num(config.get('slippage_rate'), 4)}`。",
            f"- 市场过滤：当全市场平均分低于 `{fmt_num(config.get('market_score_filter_min_avg'))}`、5日均值低于 `{fmt_num(config.get('market_score_filter_min_ma5'))}`，或大盘收盘价未站上 `MA20` 时，不生成新的买入信号。",
            "",
            "### 算法因子说明",
            "",
            "1. 均线多头，权重18分：",
            "   使用 5 条均线 MA5 / MA10 / MA20 / MA30 / MA60，比较 4 组相邻关系：",
            "   MA5>MA10、MA10>MA20、MA20>MA30、MA30>MA60。",
            "   系统不是简单二值判断，而是用 (短期均线-长期均线)/价格 的比例做平滑评分。",
            "   当该比例达到约 0.5% 时，该组接近满分；低于阈值按比例给分，低于 0 则记 0 分。",
            "   最终再对最近 5 个交易日做加权滑动平均，得到 0-18 的浮点分。",
            "2. 放量上涨 + 缩量回调，权重15分：",
            "   最近10个交易日里，系统会同时衡量：",
            "   - 放量上涨强度：涨幅相对 2% 的强弱，以及成交量相对 5 日均量 1.5 倍的强弱；",
            "   - 缩量整理强度：波动越小越好，且成交量越低于 5 日均量越好。",
            "   这一项同样采用平滑评分，不是“必须完美形态才给分”。",
            "3. 资金流入 + 板块强势，权重18分：",
            "   使用资金流向指标替代主力净流入占比：",
            "   - CMF(21) > 0.05 可视为资金净流入较强；",
            "   - MFI(14) > 60 代表资金买盘压力偏强，> 70 可视为更强确认；",
            "   同时结合板块涨幅是否强于大盘，以及板块内上涨家数占比是否高于 60%。",
            "4. 低位启动突破，权重22分：",
            "   当前收盘价距离近120日最低收盘价不宜过远，同时也会结合 ATR 评估是否仍处于相对低位。",
            "   在此基础上，再判断收盘价是否对前20日高点形成有效突破。",
            "   这一项现在是更高权重项，判断时要更重视“是否仍有低位安全边际”，而不是只看是否接近新高。",
            "5. 突破后未破位，权重18分：",
            "   这一项必须建立在“已经形成有效突破”的前提上；如果突破本身不成立，这一项应接近 0 分。",
            "   防守位不是固定百分比，而是结合 ATR 动态设定，以适应不同波动率股票。",
            "   同时还会考虑“突破新鲜度”：突破越新，守位信号越有效；突破时间过久，这一项权重会自然衰减。",
            "6. 大盘共振，权重10分：",
            "   大盘收盘站上 MA20，且 MA20 相比前一日继续上行。",
            "",
            "### 买入规则",
            "",
            f"- 辅助过滤：买入前 5 日振幅需 `<= {fmt_pct(config.get('buy_risk_amplitude_max'))}`，且买入前 5 日最大跌幅需 `<= {fmt_pct(config.get('buy_risk_max_drop_max'))}`。",
            f"- 板块过滤：所属板块 5 日涨幅排名需位于前 `{fmt_pct(config.get('buy_sector_rank_top_pct'))}`。",
            f"- 大盘过滤：`market_require_benchmark_above_ma20 = {bool(config.get('market_require_benchmark_above_ma20', True))}`，开启时要求大盘收盘价站上 `MA20` 才开仓。",
        ]

        enabled_buy_rules = config.get("enabled_buy_rules") or []
        for rule in enabled_buy_rules:
            lines.append(f"- `{rule}`：{buy_rule_desc.get(rule, '未定义说明')}")
        if not enabled_buy_rules:
            lines.append("- 当前未启用买入规则。")

        lines.extend(["", "### 卖出规则", ""])
        enabled_sell_rules = config.get("enabled_sell_rules") or []
        for rule in enabled_sell_rules:
            lines.append(f"- `{rule}`：{sell_rule_desc.get(rule, '未定义说明')}")
        if not enabled_sell_rules:
            lines.append("- 当前未启用卖出规则。")

        lines.extend(
            [
                "",
                "## 回测摘要",
                "",
                f"- 初始资金：`{fmt_num(summary.get('initial_capital'))}`",
                f"- 期末净值：`{fmt_num(summary.get('final_nav'))}`",
                f"- 总收益：`{fmt_pct(summary.get('total_return'))}`",
                f"- 基准收益：`{fmt_pct(summary.get('benchmark_return'))}`",
                f"- 超额收益：`{fmt_pct(summary.get('excess_return'))}`",
                f"- 最大回撤：`{fmt_pct(summary.get('max_drawdown'))}`",
                f"- 交易次数：`{summary.get('trade_count', 0)}`",
                f"- 买入次数：`{summary.get('buy_count', 0)}`",
                f"- 卖出次数：`{summary.get('sell_count', 0)}`",
                f"- 胜率：`{fmt_pct(summary.get('win_rate'))}`",
                f"- 平均持仓天数：`{fmt_num(summary.get('avg_holding_days'))}`",
                "",
                "## 配置参数",
                "",
                f"- 模板ID：`{config.get('template_id') or '-'}`",
                f"- 回看交易日：`{config.get('lookback_days', '-')}`",
                f"- 最大持仓：`{config.get('max_positions', '-')}`",
                f"- 手续费：`{fmt_num(config.get('fee_rate'), 4)}`",
                f"- 滑点：`{fmt_num(config.get('slippage_rate'), 4)}`",
                f"- 平均分过滤阈值：`{fmt_num(config.get('market_score_filter_min_avg'))}`",
                f"- 5日均值过滤阈值：`{fmt_num(config.get('market_score_filter_min_ma5'))}`",
                f"- 大盘收盘站上MA20过滤：`{bool(config.get('market_require_benchmark_above_ma20', True))}`",
                f"- 大盘MA20向上过滤：`{bool(config.get('market_require_benchmark_ma20_up', False))}`",
                f"- 买入前5日振幅上限：`{fmt_pct(config.get('buy_risk_amplitude_max'))}`",
                f"- 买入前5日最大跌幅上限：`{fmt_pct(config.get('buy_risk_max_drop_max'))}`",
                f"- 板块5日强度排名上限：`前 {fmt_pct(config.get('buy_sector_rank_top_pct'))}`",
                f"- 买入规则：`{', '.join(enabled_buy_rules) or '-'}`",
                f"- 卖出规则：`{', '.join(enabled_sell_rules) or '-'}`",
                "",
                "## 信号统计",
                "",
                f"- 总信号数：`{signal_stats.get('signal_count', 0)}`",
                f"- 买入信号数：`{signal_stats.get('selected_buy_count', 0)}`",
                f"- 买入规则命中：`{signal_stats.get('buy_rule_counts', {})}`",
                f"- 卖出规则命中：`{signal_stats.get('sell_rule_counts', {})}`",
                "",
                "## 执行约束统计",
                "",
                f"- 市场过滤拦截天数：`{summary.get('market_filter_blocked_days', 0)}`",
                f"- 市场过滤拦截信号：`{summary.get('market_filter_blocked_buy_signals', 0)}`",
                f"- 买入成交受阻：`{summary.get('buy_execution_blocked', 0)}`",
                f"- 卖出顺延次数：`{summary.get('sell_execution_deferred', 0)}`",
                f"- 买入受阻明细：`{summary.get('buy_execution_blocked_breakdown_label') or '-'}`",
                f"- 卖出顺延明细：`{summary.get('sell_execution_deferred_breakdown_label') or '-'}`",
                "",
                "## 月度净值变化",
                "",
                "| 月份 | 月度净值变化 | 月内已实现 | 月末浮盈 |",
                "| --- | ---: | ---: | ---: |",
            ]
        )

        for month in detail.get("position_calendar_months") or []:
            lines.append(
                f"| {month.get('month')} | {month.get('net_change_pnl_text')} | {month.get('realized_pnl_text')} | {month.get('month_end_unrealized_pnl_text')} |"
            )

        lines.extend(
            [
                "",
                "## 每日净值",
                "",
                "| 日期 | 策略净值 | 基准净值 | 当日收益 | 回撤 | 持仓数 |",
                "| --- | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for row in nav_rows:
            lines.append(
                f"| {row.get('trade_date')} | {fmt_num(row.get('nav'))} | {fmt_num(row.get('benchmark_nav'))} | {fmt_pct(row.get('daily_return'))} | {fmt_pct(row.get('drawdown'))} | {row.get('position_count', 0)} |"
            )

        lines.extend(["", "## 每日持仓与交易明细", ""])
        for day in calendar_rows:
            trade_date = str(day.get("trade_date") or "")
            lines.extend(
                [
                    f"### {trade_date}",
                    "",
                    f"- 当日持仓数：`{day.get('position_count', 0)}`",
                    f"- 组合当日盈亏：`{day.get('daily_pnl_text')}`",
                    f"- 当日持仓市值：`{day.get('market_value_text')}`",
                    "",
                    "#### 持仓表",
                    "",
                    "| 股票 | 数量 | 成本价 | 收盘价 | 持仓市值 | 权重 | 当日盈亏 | 浮盈亏 | 持仓天数 | 标签 |",
                    "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
                ]
            )
            positions = positions_by_date.get(trade_date, [])
            if positions:
                for row in positions:
                    labels = " / ".join(
                        [item for item in [row.get("position_event_label"), row.get("position_exit_label")] if item]
                    ) or "-"
                    lines.append(
                        f"| {row.get('symbol')} | {row.get('shares')} | {row.get('cost_price_text')} | {row.get('close_price_text')} | {row.get('market_value_text')} | {row.get('weight_text')} | {row.get('daily_pnl_text')} | {row.get('unrealized_pnl_text')} | {row.get('hold_days')} | {labels} |"
                    )
            else:
                lines.append("| - | - | - | - | - | - | - | - | - | 当日无持仓 |")

            lines.extend(
                [
                    "",
                    "#### 当日交易记录",
                    "",
                    "| 股票 | 方向 | 价格 | 数量 | 原因 | 成交对当日净值影响 |",
                    "| --- | --- | ---: | ---: | --- | ---: |",
                ]
            )
            trades = trades_by_date.get(trade_date, [])
            if trades:
                for row in trades:
                    lines.append(
                        f"| {row.get('symbol')} | {row.get('side_label') or row.get('side')} | {fmt_num(row.get('price'), 4)} | {row.get('shares')} | {row.get('reason_label') or row.get('reason')} | {row.get('daily_impact_text') or '-'} |"
                    )
            else:
                lines.append("| - | - | - | - | - | 当日无成交 |")
            lines.append("")

        return "\n".join(lines).rstrip() + "\n"

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
