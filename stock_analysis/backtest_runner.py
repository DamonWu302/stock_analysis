from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from typing import Any

from .backtest import (
    BUY_RULE_MOMENTUM,
    BUY_RULE_STRICT,
    DEFAULT_INITIAL_CAPITAL,
    SELL_RULE_BREAK_MA5,
    SELL_RULE_DRAWDOWN,
    SELL_RULE_FLIP_LOSS,
    SELL_RULE_MARKET_WEAK_DROP,
    SELL_RULE_TIME_STOP,
    SELL_RULE_TRIM,
    build_backtest_config_schema,
)
from .db import Database


@dataclass(slots=True)
class Position:
    symbol: str
    shares: float
    cost_price: float
    entry_signal_date: str
    entry_execution_date: str
    entry_cost_total: float
    peak_close: float
    trimmed: bool = False


class BacktestRunner:
    def __init__(self, db: Database):
        self.db = db

    def run(self, config: dict[str, Any] | None = None, progress_callback=None) -> dict[str, Any]:
        normalized = self._normalize_config(config or {})
        run_id = self._create_run(normalized)
        try:
            result = self._execute_run(run_id, normalized, progress_callback=progress_callback)
            self._complete_run(run_id, result)
            return result
        except Exception as exc:
            self._fail_run(run_id, str(exc))
            raise

    def recent_runs(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT id, name, status, benchmark_symbol, start_date, end_date,
                       lookback_days, max_positions, fee_rate, slippage_rate,
                       score_version, summary_json, created_at, finished_at
                FROM backtest_run
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        payload: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["summary"] = json.loads(item["summary_json"]) if item.get("summary_json") else None
            payload.append(item)
        return payload

    def run_detail(self, run_id: int) -> dict[str, Any] | None:
        with self.db.connect() as conn:
            run_row = conn.execute(
                """
                SELECT *
                FROM backtest_run
                WHERE id = ?
                """,
                (run_id,),
            ).fetchone()
            if not run_row:
                return None
            nav_rows = conn.execute(
                """
                SELECT trade_date, cash, market_value, nav, daily_return, drawdown, position_count, turnover
                FROM backtest_nav
                WHERE run_id = ?
                ORDER BY trade_date ASC
                """,
                (run_id,),
            ).fetchall()
            trade_rows = conn.execute(
                """
                SELECT id, symbol, side, signal_trade_date, execution_date, price,
                       shares, gross_amount, fee, slippage_cost, net_amount, reason
                FROM backtest_trade
                WHERE run_id = ?
                ORDER BY execution_date ASC, id ASC
                """,
                (run_id,),
            ).fetchall()
            signal_rows = conn.execute(
                """
                SELECT trade_date, symbol, score_total, rank_value, action, selected,
                       buy_rule_hits, sell_rule_hits, breakout_floor, target_position, note
                FROM backtest_signal
                WHERE run_id = ?
                ORDER BY trade_date ASC, action ASC, rank_value ASC, symbol ASC
                """,
                (run_id,),
            ).fetchall()
            position_rows = conn.execute(
                """
                SELECT trade_date, symbol, shares, cost_price, close_price,
                       market_value, weight, unrealized_pnl, hold_days
                FROM backtest_position_daily
                WHERE run_id = ?
                ORDER BY trade_date ASC, weight DESC, symbol ASC
                """,
                (run_id,),
            ).fetchall()
            benchmark_rows = []
            if nav_rows:
                benchmark_rows = conn.execute(
                    """
                    SELECT trade_date, close
                    FROM benchmark_history
                    WHERE symbol = ? AND trade_date BETWEEN ? AND ?
                    ORDER BY trade_date ASC
                    """,
                    (
                        str(run_row["benchmark_symbol"] or "000001"),
                        str(nav_rows[0]["trade_date"]),
                        str(nav_rows[-1]["trade_date"]),
                    ),
                ).fetchall()
        payload = dict(run_row)
        payload["config"] = json.loads(payload["config_json"])
        payload["summary"] = json.loads(payload["summary_json"]) if payload.get("summary_json") else None
        payload["nav"] = self._attach_benchmark_nav(nav_rows, benchmark_rows)
        payload["trades"] = [dict(row) for row in trade_rows]
        payload["signals"] = [self._deserialize_signal_row(dict(row)) for row in signal_rows]
        payload["positions"] = [dict(row) for row in position_rows]
        payload["signal_stats"] = self._build_signal_stats(payload["signals"])
        return payload

    def _normalize_config(self, config: dict[str, Any]) -> dict[str, Any]:
        schema = build_backtest_config_schema()
        defaults = dict(schema["defaults"])
        merged = defaults | {
            key: value
            for key, value in config.items()
            if value is not None and key not in {"enabled_buy_rules", "enabled_sell_rules"}
        }
        merged["enabled_buy_rules"] = list(config.get("enabled_buy_rules") or defaults["enabled_buy_rules"])
        merged["enabled_sell_rules"] = list(config.get("enabled_sell_rules") or defaults["enabled_sell_rules"])
        merged["name"] = str(merged.get("name") or defaults["name"])
        merged["benchmark_symbol"] = str(merged.get("benchmark_symbol") or defaults["benchmark_symbol"])
        merged["lookback_days"] = max(int(merged.get("lookback_days") or defaults["lookback_days"]), 2)
        merged["start_date"] = str(merged.get("start_date") or "").strip() or None
        merged["end_date"] = str(merged.get("end_date") or "").strip() or None
        if merged["start_date"] and merged["end_date"] and merged["start_date"] > merged["end_date"]:
            raise ValueError("回测开始日期不能晚于结束日期")
        merged["max_positions"] = max(int(merged.get("max_positions") or defaults["max_positions"]), 1)
        merged["max_single_position"] = min(
            max(float(merged.get("max_single_position") or defaults["max_single_position"]), 0.01),
            1.0,
        )
        merged["initial_capital"] = float(merged.get("initial_capital") or DEFAULT_INITIAL_CAPITAL)
        merged["fee_rate"] = max(float(merged.get("fee_rate") or defaults["fee_rate"]), 0.0)
        merged["slippage_rate"] = max(float(merged.get("slippage_rate") or defaults["slippage_rate"]), 0.0)
        merged["market_score_filter_min_avg"] = float(
            merged.get("market_score_filter_min_avg") or defaults["market_score_filter_min_avg"]
        )
        merged["market_score_filter_min_ma5"] = float(
            merged.get("market_score_filter_min_ma5") or defaults["market_score_filter_min_ma5"]
        )
        merged["buy_amount_min"] = float(merged.get("buy_amount_min") or defaults["buy_amount_min"])
        merged["buy_timing"] = "next_open"
        merged["sell_timing"] = "next_open"
        merged["allow_pyramiding"] = False
        merged["allow_same_day_repeat_trade"] = False
        merged["use_margin"] = False
        return merged

    def _create_run(self, config: dict[str, Any]) -> int:
        now = datetime.now().isoformat(timespec="seconds")
        with self.db.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO backtest_run
                (name, status, benchmark_symbol, start_date, end_date, lookback_days,
                 buy_timing, sell_timing, max_positions, fee_rate, slippage_rate,
                 allow_pyramiding, allow_same_day_repeat_trade, use_margin, score_version,
                 config_json, created_at)
                VALUES (?, 'running', ?, '', '', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    config["name"],
                    config["benchmark_symbol"],
                    config["lookback_days"],
                    config["buy_timing"],
                    config["sell_timing"],
                    config["max_positions"],
                    config["fee_rate"],
                    config["slippage_rate"],
                    int(config["allow_pyramiding"]),
                    int(config["allow_same_day_repeat_trade"]),
                    int(config["use_margin"]),
                    config["score_version"],
                    json.dumps(config, ensure_ascii=False),
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def _execute_run(self, run_id: int, config: dict[str, Any], progress_callback=None) -> dict[str, Any]:
        if progress_callback:
            progress_callback({"phase": "loading", "message": "正在加载回测所需历史数据"})
        trade_dates = self._load_trade_dates(
            config["lookback_days"],
            start_date=config.get("start_date"),
            end_date=config.get("end_date"),
        )
        if len(trade_dates) < 2:
            raise ValueError(f"可用于回测的 daily_score 交易日不足，当前仅有 {len(trade_dates)} 天。")

        start_date = trade_dates[0]
        end_date = trade_dates[-1]
        score_rows = self._load_score_rows(start_date, end_date)
        market_score_map = self._load_market_score_map(start_date, end_date)
        benchmark_close_map = self._load_benchmark_close_map(start_date, end_date)
        price_map = self._load_price_map(start_date, end_date)
        if progress_callback:
            progress_callback(
                {
                    "phase": "preparing",
                    "message": "正在整理每日候选股票和市场过滤条件",
                    "progress_current": 0,
                    "progress_total": len(trade_dates),
                    "last_trade_date": start_date,
                }
            )

        self._update_run_dates(run_id, start_date, end_date)

        by_date: dict[str, list[dict[str, Any]]] = {}
        for row in score_rows:
            by_date.setdefault(str(row["trade_date"]), []).append(row)
        for rows in by_date.values():
            rows.sort(key=lambda item: (float(item["score_total"]), str(item["symbol"])), reverse=True)

        pending_buys: dict[str, list[dict[str, Any]]] = {}
        pending_sells: dict[str, list[dict[str, Any]]] = {}
        positions: dict[str, Position] = {}
        cash = float(config["initial_capital"])
        initial_capital = float(config["initial_capital"])
        nav_peak = initial_capital
        nav_rows: list[tuple] = []
        signal_rows: list[tuple] = []
        trade_rows: list[tuple] = []
        position_rows: list[tuple] = []
        closed_returns: list[float] = []
        closed_holding_days: list[int] = []
        market_filter_blocked_days = 0
        market_filter_blocked_buy_signals = 0
        buy_execution_blocked = 0
        sell_execution_deferred = 0
        buy_execution_blocked_breakdown: dict[str, int] = {}
        sell_execution_deferred_breakdown: dict[str, int] = {}

        enabled_buy_rules = set(config["enabled_buy_rules"])
        enabled_sell_rules = set(config["enabled_sell_rules"])

        if progress_callback:
            progress_callback(
                {
                    "phase": "executing",
                    "message": "正在执行回测交易逻辑",
                    "progress_current": 0,
                    "progress_total": len(trade_dates),
                    "last_trade_date": start_date,
                }
            )

        for index, trade_date in enumerate(trade_dates):
            next_trade_date = trade_dates[index + 1] if index + 1 < len(trade_dates) else None
            traded_today: set[str] = set()
            daily_turnover = 0.0

            for order in pending_sells.pop(trade_date, []):
                symbol = str(order["symbol"])
                position = positions.get(symbol)
                price_row = price_map.get((trade_date, symbol))
                tradability = self._execution_tradability("sell", price_row)
                if position is None or tradability != "tradable":
                    if tradability != "tradable":
                        sell_execution_deferred_breakdown[tradability] = (
                            sell_execution_deferred_breakdown.get(tradability, 0) + 1
                        )
                    if tradability in {"limit_down", "missing"}:
                        sell_execution_deferred += 1
                    if next_trade_date:
                        pending_sells.setdefault(next_trade_date, []).append(order)
                    continue
                sell_fraction = min(max(float(order.get("fraction") or 1.0), 0.0), 1.0)
                if sell_fraction <= 0:
                    continue
                original_shares = position.shares
                shares_to_sell = round(original_shares * sell_fraction, 4)
                if shares_to_sell <= 0:
                    shares_to_sell = original_shares
                cost_portion = round(position.entry_cost_total * (shares_to_sell / max(original_shares, 0.0001)), 4)
                execution_price = round(float(price_row["open"]) * (1 - config["slippage_rate"]), 4)
                gross_amount = round(shares_to_sell * execution_price, 4)
                fee = round(gross_amount * config["fee_rate"], 4)
                net_amount = round(gross_amount - fee, 4)
                cash += net_amount
                daily_turnover += gross_amount
                holding_days = max(self._trade_date_distance(trade_dates, position.entry_execution_date, trade_date), 1)
                realized_return = (net_amount - cost_portion) / max(cost_portion, 0.01)
                closed_returns.append(realized_return)
                closed_holding_days.append(holding_days)
                trade_rows.append(
                    (
                        run_id,
                        symbol,
                        "sell",
                        order["signal_trade_date"],
                        trade_date,
                        execution_price,
                        shares_to_sell,
                        gross_amount,
                        fee,
                        round(shares_to_sell * float(price_row["open"]) * config["slippage_rate"], 4),
                        net_amount,
                        ",".join(order["rule_hits"]),
                    )
                )
                traded_today.add(symbol)
                if shares_to_sell >= original_shares - 0.0001:
                    del positions[symbol]
                else:
                    position.shares = round(original_shares - shares_to_sell, 4)
                    position.entry_cost_total = round(max(position.entry_cost_total - cost_portion, 0.0), 4)
                    position.trimmed = True

            buy_orders = [
                order
                for order in pending_buys.pop(trade_date, [])
                if order["symbol"] not in positions and order["symbol"] not in traded_today
            ]
            available_slots = max(config["max_positions"] - len(positions), 0)
            if buy_orders and available_slots > 0:
                buy_orders = buy_orders[:available_slots]
                total_equity = cash
                for symbol, position in positions.items():
                    price_row = price_map.get((trade_date, symbol), {})
                    ref_price = float(price_row.get("open") or price_row.get("close") or position.cost_price)
                    total_equity += position.shares * ref_price
                allocation = cash / len(buy_orders) if buy_orders else 0.0
                allocation_cap = total_equity * float(config["max_single_position"])
                for order in buy_orders:
                    symbol = str(order["symbol"])
                    price_row = price_map.get((trade_date, symbol))
                    tradability = self._execution_tradability("buy", price_row)
                    if tradability != "tradable":
                        buy_execution_blocked += 1
                        buy_execution_blocked_breakdown[tradability] = (
                            buy_execution_blocked_breakdown.get(tradability, 0) + 1
                        )
                        continue
                    open_price = float(price_row["open"])
                    execution_price = round(open_price * (1 + config["slippage_rate"]), 4)
                    per_share_total = execution_price * (1 + config["fee_rate"])
                    order_allocation = min(allocation, allocation_cap, cash)
                    shares = round(order_allocation / max(per_share_total, 0.01), 4)
                    if shares <= 0:
                        continue
                    gross_amount = round(shares * execution_price, 4)
                    fee = round(gross_amount * config["fee_rate"], 4)
                    net_amount = round(gross_amount + fee, 4)
                    if net_amount > cash:
                        continue
                    cash -= net_amount
                    daily_turnover += gross_amount
                    positions[symbol] = Position(
                        symbol=symbol,
                        shares=shares,
                        cost_price=execution_price,
                        entry_signal_date=order["signal_trade_date"],
                        entry_execution_date=trade_date,
                        entry_cost_total=net_amount,
                        peak_close=execution_price,
                    )
                    trade_rows.append(
                        (
                            run_id,
                            symbol,
                            "buy",
                            order["signal_trade_date"],
                            trade_date,
                            execution_price,
                            shares,
                            gross_amount,
                            fee,
                            round(shares * open_price * config["slippage_rate"], 4),
                            net_amount,
                            ",".join(order["rule_hits"]),
                        )
                    )
                    traded_today.add(symbol)

            market_value = 0.0
            daily_positions: list[tuple[str, float, float, float, int, float]] = []
            for symbol, position in positions.items():
                price_row = price_map.get((trade_date, symbol), {})
                close_price = float(price_row.get("close") or position.cost_price)
                position.peak_close = max(float(position.peak_close), close_price)
                value = round(position.shares * close_price, 4)
                market_value += value
                daily_positions.append(
                    (
                        symbol,
                        position.shares,
                        position.cost_price,
                        close_price,
                        self._trade_date_distance(trade_dates, position.entry_execution_date, trade_date),
                        round(value - position.entry_cost_total, 4),
                    )
                )

            for symbol, shares, cost_price, close_price, hold_days, unrealized_pnl in daily_positions:
                value = round(shares * close_price, 4)
                weight = round(value / market_value, 6) if market_value else 0.0
                position_rows.append(
                    (
                        run_id,
                        trade_date,
                        symbol,
                        shares,
                        cost_price,
                        close_price,
                        value,
                        weight,
                        unrealized_pnl,
                        hold_days,
                    )
                )

            nav = round(cash + market_value, 4)
            nav_peak = max(nav_peak, nav)
            drawdown = round((nav_peak - nav) / nav_peak, 6) if nav_peak else 0.0
            prev_nav = nav_rows[-1][4] if nav_rows else initial_capital
            daily_return = round((nav - prev_nav) / prev_nav, 6) if prev_nav else 0.0
            turnover = round(daily_turnover / max(prev_nav, 0.01), 6)
            nav_rows.append(
                (run_id, trade_date, round(cash, 4), round(market_value, 4), nav, daily_return, drawdown, len(positions), turnover)
            )

            daily_rows = by_date.get(trade_date, [])
            if not next_trade_date:
                continue

            available_slots = max(config["max_positions"] - len(positions), 0)
            buy_candidates: list[dict[str, Any]] = []
            market_filter = market_score_map.get(trade_date, {})
            market_filter_passed = self._passes_market_filter(market_filter, config)
            blocked_today = False
            for rank_value, row in enumerate(daily_rows, start=1):
                symbol = str(row["symbol"])
                breakout_floor = self._breakout_floor(row)
                if symbol in positions:
                    sell_decision = self._evaluate_sell_rules(
                        row,
                        position=positions[symbol],
                        current_close=float(row["close"]),
                        hold_days=self._trade_date_distance(trade_dates, positions[symbol].entry_execution_date, trade_date),
                        market_filter=market_filter,
                        enabled_sell_rules=enabled_sell_rules,
                        config=config,
                    )
                    if sell_decision:
                        pending_sells.setdefault(next_trade_date, []).append(
                            {
                                "symbol": symbol,
                                "signal_trade_date": trade_date,
                                "rule_hits": sell_decision["rule_hits"],
                                "fraction": sell_decision["fraction"],
                            }
                        )
                        signal_rows.append(
                            (
                                run_id,
                                trade_date,
                                symbol,
                                float(row["score_total"]),
                                rank_value,
                                "sell",
                                1,
                                None,
                                json.dumps(sell_decision["rule_hits"], ensure_ascii=False),
                                breakout_floor,
                                round(1.0 - float(sell_decision["fraction"]), 6),
                                "",
                            )
                        )
                    continue

                buy_hits = self._evaluate_buy_rules(row, enabled_buy_rules=enabled_buy_rules, config=config)
                if not buy_hits:
                    continue
                if not market_filter_passed:
                    blocked_today = True
                    market_filter_blocked_buy_signals += 1
                    continue
                buy_candidates.append(
                    {
                        "symbol": symbol,
                        "score_total": float(row["score_total"]),
                        "rank_value": rank_value,
                        "buy_rule_hits": buy_hits,
                        "breakout_floor": breakout_floor,
                    }
                )

            if blocked_today:
                market_filter_blocked_days += 1

            if buy_candidates and available_slots > 0:
                selected_buys = buy_candidates[:available_slots]
                for candidate in buy_candidates:
                    selected = 1 if candidate in selected_buys else 0
                    signal_rows.append(
                        (
                            run_id,
                            trade_date,
                            candidate["symbol"],
                            candidate["score_total"],
                            candidate["rank_value"],
                            "buy",
                            selected,
                            json.dumps(candidate["buy_rule_hits"], ensure_ascii=False),
                            None,
                            candidate["breakout_floor"],
                            round(min(1 / config["max_positions"], float(config["max_single_position"])), 6),
                            "",
                        )
                    )
                for candidate in selected_buys:
                    pending_buys.setdefault(next_trade_date, []).append(
                        {
                            "symbol": candidate["symbol"],
                            "signal_trade_date": trade_date,
                            "rule_hits": candidate["buy_rule_hits"],
                        }
                    )
            if progress_callback and (index == 0 or (index + 1) % 5 == 0 or index + 1 == len(trade_dates)):
                progress_callback(
                    {
                        "phase": "executing",
                        "message": f"正在执行回测，已完成 {index + 1}/{len(trade_dates)} 个交易日",
                        "progress_current": index + 1,
                        "progress_total": len(trade_dates),
                        "last_trade_date": trade_date,
                    }
                )

        if progress_callback:
            progress_callback(
                {
                    "phase": "summarizing",
                    "message": "正在汇总收益、回撤和执行约束统计",
                    "progress_current": len(trade_dates),
                    "progress_total": len(trade_dates),
                    "last_trade_date": end_date,
                }
            )
        benchmark_return = self._benchmark_return(benchmark_close_map, start_date, end_date)
        final_nav = nav_rows[-1][4] if nav_rows else initial_capital
        total_return = round((final_nav - initial_capital) / initial_capital, 6) if initial_capital else 0.0
        max_drawdown = max((row[6] for row in nav_rows), default=0.0)
        summary = {
            "run_id": run_id,
            "start_date": start_date,
            "end_date": end_date,
            "initial_capital": initial_capital,
            "final_nav": round(final_nav, 4),
            "total_return": total_return,
            "benchmark_return": benchmark_return,
            "excess_return": round(total_return - benchmark_return, 6),
            "max_drawdown": round(max_drawdown, 6),
            "trade_count": len(trade_rows),
            "buy_count": sum(1 for row in trade_rows if row[2] == "buy"),
            "sell_count": sum(1 for row in trade_rows if row[2] == "sell"),
            "signal_count": len(signal_rows),
            "selected_buy_signal_count": sum(1 for row in signal_rows if row[6] == 1 and row[5] == "buy"),
            "market_filter_blocked_days": market_filter_blocked_days,
            "market_filter_blocked_buy_signals": market_filter_blocked_buy_signals,
            "buy_execution_blocked": buy_execution_blocked,
            "sell_execution_deferred": sell_execution_deferred,
            "buy_execution_blocked_breakdown": buy_execution_blocked_breakdown,
            "sell_execution_deferred_breakdown": sell_execution_deferred_breakdown,
            "market_score_filter_min_avg": config["market_score_filter_min_avg"],
            "market_score_filter_min_ma5": config["market_score_filter_min_ma5"],
            "win_rate": round(sum(1 for value in closed_returns if value > 0) / len(closed_returns), 6) if closed_returns else 0.0,
            "avg_holding_days": round(sum(closed_holding_days) / len(closed_holding_days), 2) if closed_holding_days else 0.0,
            "available_signal_days": len(trade_dates),
        }
        if progress_callback:
            progress_callback(
                {
                    "phase": "writing",
                    "message": "正在写入回测结果到数据库",
                    "progress_current": len(trade_dates),
                    "progress_total": len(trade_dates),
                    "last_trade_date": end_date,
                }
            )
        self._persist_run_rows(run_id, signal_rows, trade_rows, position_rows, nav_rows)
        return summary

    def _load_trade_dates(self, lookback_days: int, start_date: str | None = None, end_date: str | None = None) -> list[str]:
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
                FROM daily_score
                {where_clause}
                ORDER BY trade_date ASC
                """,
                params,
            ).fetchall()
        trade_dates = [str(row["trade_date"]) for row in rows]
        if start_date or end_date or len(trade_dates) <= lookback_days:
            return trade_dates
        return trade_dates[-lookback_days:]

    def _load_score_rows(self, start_date: str, end_date: str) -> list[dict[str, Any]]:
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                WITH latest_sector AS (
                    SELECT ms.symbol, ms.sector
                    FROM market_snapshot ms
                    JOIN (
                        SELECT symbol, MAX(trade_date) AS latest_trade_date
                        FROM market_snapshot
                        GROUP BY symbol
                    ) latest
                      ON latest.symbol = ms.symbol AND latest.latest_trade_date = ms.trade_date
                ),
                sector_base AS (
                    SELECT ph.trade_date,
                           ph.symbol,
                           ls.sector,
                           ph.close,
                           LAG(ph.close, 5) OVER (PARTITION BY ph.symbol ORDER BY ph.trade_date) AS close_5d_ago
                    FROM price_history ph
                    LEFT JOIN latest_sector ls ON ls.symbol = ph.symbol
                    WHERE ph.trade_date BETWEEN ? AND ?
                ),
                sector_return AS (
                    SELECT trade_date,
                           COALESCE(sector, '未分类') AS sector,
                           AVG((close / NULLIF(close_5d_ago, 0)) - 1.0) AS sector_return_5d
                    FROM sector_base
                    WHERE close_5d_ago IS NOT NULL
                    GROUP BY trade_date, COALESCE(sector, '未分类')
                ),
                sector_rank AS (
                    SELECT trade_date,
                           sector,
                           sector_return_5d,
                           PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY sector_return_5d DESC) AS sector_rank_pct
                    FROM sector_return
                )
                SELECT ds.trade_date, ds.symbol, ds.score_total, ds.score_ma_trend, ds.score_volume_pattern,
                       ds.score_capital_sector, ds.score_breakout, ds.score_hold, ds.score_benchmark,
                       df.close, df.ma5, df.ma10, df.ma20, df.atr14, df.prior_20_high, df.cmf21,
                       df.mfi14, df.vol_ma5, df.pct_change, ph.open, ph.high, ph.low, ph.volume, ph.amount,
                       LAG(ds.score_total) OVER (PARTITION BY ds.symbol ORDER BY ds.trade_date) AS prev_score_total,
                       LAG(df.mfi14) OVER (PARTITION BY ds.symbol ORDER BY ds.trade_date) AS prev_mfi14,
                       LAG(df.close, 20) OVER (PARTITION BY ds.symbol ORDER BY ds.trade_date) AS close_20d_ago,
                       LAG(df.pct_change, 1) OVER (PARTITION BY ds.symbol ORDER BY ds.trade_date) AS pct_change_lag1,
                       LAG(df.pct_change, 2) OVER (PARTITION BY ds.symbol ORDER BY ds.trade_date) AS pct_change_lag2,
                       LAG(ph.volume, 1) OVER (PARTITION BY ds.symbol ORDER BY ds.trade_date) AS volume_lag1,
                       LAG(ph.volume, 2) OVER (PARTITION BY ds.symbol ORDER BY ds.trade_date) AS volume_lag2,
                       LAG(ph.amount, 1) OVER (PARTITION BY ds.symbol ORDER BY ds.trade_date) AS amount_lag1,
                       LAG(ph.amount, 2) OVER (PARTITION BY ds.symbol ORDER BY ds.trade_date) AS amount_lag2,
                       LAG(df.vol_ma5, 1) OVER (PARTITION BY ds.symbol ORDER BY ds.trade_date) AS vol_ma5_lag1,
                       LAG(df.vol_ma5, 2) OVER (PARTITION BY ds.symbol ORDER BY ds.trade_date) AS vol_ma5_lag2,
                       MAX(ph.high) OVER (PARTITION BY ds.symbol ORDER BY ds.trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS risk_high_5d,
                       MIN(ph.low) OVER (PARTITION BY ds.symbol ORDER BY ds.trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS risk_low_5d,
                       MAX(ph.close) OVER (PARTITION BY ds.symbol ORDER BY ds.trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS risk_close_high_5d,
                       MIN(ph.close) OVER (PARTITION BY ds.symbol ORDER BY ds.trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS risk_close_low_5d,
                       sector_rank.sector_rank_pct,
                       sector_rank.sector_return_5d
                FROM daily_score ds
                JOIN daily_factor df
                  ON df.trade_date = ds.trade_date AND df.symbol = ds.symbol
                LEFT JOIN price_history ph
                  ON ph.trade_date = ds.trade_date AND ph.symbol = ds.symbol
                LEFT JOIN latest_sector ls
                  ON ls.symbol = ds.symbol
                LEFT JOIN sector_rank
                  ON sector_rank.trade_date = ds.trade_date AND sector_rank.sector = COALESCE(ls.sector, '未分类')
                WHERE ds.trade_date BETWEEN ? AND ?
                ORDER BY ds.trade_date ASC, ds.score_total DESC, ds.symbol ASC
                """,
                (start_date, end_date, start_date, end_date),
            ).fetchall()
        return [dict(row) for row in rows]

    def _load_market_score_map(self, start_date: str, end_date: str) -> dict[str, dict[str, float]]:
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                WITH stats AS (
                    SELECT trade_date, AVG(score_total) AS avg_score
                    FROM daily_score
                    WHERE trade_date BETWEEN ? AND ?
                    GROUP BY trade_date
                ),
                benchmark_base AS (
                    SELECT trade_date, close
                    FROM benchmark_history
                    WHERE symbol = '000001' AND trade_date BETWEEN ? AND ?
                ),
                benchmark_ma AS (
                    SELECT trade_date,
                           close,
                           AVG(close) OVER (
                               ORDER BY trade_date
                               ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                           ) AS ma20
                    FROM benchmark_base
                ),
                benchmark_ratio AS (
                    SELECT trade_date,
                           close / NULLIF(ma20, 0) AS close_ma20_ratio,
                           ma20 / NULLIF(LAG(ma20) OVER (ORDER BY trade_date), 0) AS ma20_ratio
                    FROM benchmark_ma
                )
                SELECT stats.trade_date,
                       stats.avg_score,
                       AVG(stats.avg_score) OVER (
                           ORDER BY stats.trade_date
                           ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                       ) AS ma5_avg_score,
                       benchmark_ratio.ma20_ratio,
                       benchmark_ratio.close_ma20_ratio
                FROM stats
                LEFT JOIN benchmark_ratio
                  ON benchmark_ratio.trade_date = stats.trade_date
                ORDER BY stats.trade_date ASC
                """,
                (start_date, end_date, start_date, end_date),
            ).fetchall()
        return {
            str(row["trade_date"]): {
                "avg_score": round(float(row["avg_score"]), 4) if row["avg_score"] is not None else 0.0,
                "ma5_avg_score": round(float(row["ma5_avg_score"]), 4) if row["ma5_avg_score"] is not None else 0.0,
                "benchmark_ma20_ratio": round(float(row["ma20_ratio"]), 6) if row["ma20_ratio"] is not None else 0.0,
                "benchmark_close_ma20_ratio": round(float(row["close_ma20_ratio"]), 6) if row["close_ma20_ratio"] is not None else 0.0,
            }
            for row in rows
        }

    def _load_benchmark_close_map(self, start_date: str, end_date: str) -> dict[str, float]:
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT trade_date, close
                FROM benchmark_history
                WHERE symbol = '000001' AND trade_date BETWEEN ? AND ?
                ORDER BY trade_date ASC
                """,
                (start_date, end_date),
            ).fetchall()
        return {str(row["trade_date"]): float(row["close"]) for row in rows}

    def _load_price_map(self, start_date: str, end_date: str) -> dict[tuple[str, str], dict[str, float | None]]:
        with self.db.connect() as conn:
            rows = conn.execute(
                """
                SELECT symbol,
                       trade_date,
                       open,
                       close,
                       high,
                       low,
                       LAG(close) OVER (PARTITION BY symbol ORDER BY trade_date) AS prev_close
                FROM price_history
                WHERE trade_date BETWEEN ? AND ?
                """,
                (start_date, end_date),
            ).fetchall()
        return {
            (str(row["trade_date"]), str(row["symbol"])): {
                "open": float(row["open"]) if row["open"] is not None else None,
                "close": float(row["close"]) if row["close"] is not None else None,
                "high": float(row["high"]) if row["high"] is not None else None,
                "low": float(row["low"]) if row["low"] is not None else None,
                "prev_close": float(row["prev_close"]) if row["prev_close"] is not None else None,
            }
            for row in rows
        }

    def _update_run_dates(self, run_id: int, start_date: str, end_date: str) -> None:
        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE backtest_run
                SET start_date = ?, end_date = ?
                WHERE id = ?
                """,
                (start_date, end_date, run_id),
            )

    def _persist_run_rows(
        self,
        run_id: int,
        signal_rows: list[tuple],
        trade_rows: list[tuple],
        position_rows: list[tuple],
        nav_rows: list[tuple],
    ) -> None:
        with self.db.connect() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO backtest_signal
                (run_id, trade_date, symbol, score_total, rank_value, action, selected,
                 buy_rule_hits, sell_rule_hits, breakout_floor, target_position, note)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                signal_rows,
            )
            conn.executemany(
                """
                INSERT INTO backtest_trade
                (run_id, symbol, side, signal_trade_date, execution_date, price, shares,
                 gross_amount, fee, slippage_cost, net_amount, reason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                trade_rows,
            )
            conn.executemany(
                """
                INSERT OR REPLACE INTO backtest_position_daily
                (run_id, trade_date, symbol, shares, cost_price, close_price,
                 market_value, weight, unrealized_pnl, hold_days)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                position_rows,
            )
            conn.executemany(
                """
                INSERT OR REPLACE INTO backtest_nav
                (run_id, trade_date, cash, market_value, nav, daily_return, drawdown, position_count, turnover)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                nav_rows,
            )

    def _complete_run(self, run_id: int, summary: dict[str, Any]) -> None:
        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE backtest_run
                SET status = 'completed',
                    summary_json = ?,
                    finished_at = ?
                WHERE id = ?
                """,
                (json.dumps(summary, ensure_ascii=False), datetime.now().isoformat(timespec="seconds"), run_id),
            )

    def _fail_run(self, run_id: int, error_message: str) -> None:
        with self.db.connect() as conn:
            conn.execute(
                """
                UPDATE backtest_run
                SET status = 'failed',
                    summary_json = ?,
                    finished_at = ?
                WHERE id = ?
                """,
                (
                    json.dumps({"error": error_message}, ensure_ascii=False),
                    datetime.now().isoformat(timespec="seconds"),
                    run_id,
                ),
            )

    @staticmethod
    def _trade_date_distance(trade_dates: list[str], start_date: str, end_date: str) -> int:
        try:
            start_index = trade_dates.index(start_date)
            end_index = trade_dates.index(end_date)
        except ValueError:
            return 0
        return max(end_index - start_index, 0)

    @staticmethod
    def _breakout_floor(row: dict[str, Any]) -> float | None:
        prior_20_high = row.get("prior_20_high")
        atr14 = row.get("atr14")
        if prior_20_high is None or atr14 is None:
            return None
        return round(max(float(prior_20_high) - 1.2 * float(atr14), float(prior_20_high) * 0.96), 4)

    def _evaluate_buy_rules(self, row: dict[str, Any], enabled_buy_rules: set[str], config: dict[str, Any]) -> list[str]:
        hits: list[str] = []
        core_scores = {
            "score_ma_trend": float(row.get("score_ma_trend") or 0.0),
            "score_volume_pattern": float(row.get("score_volume_pattern") or 0.0),
            "score_capital_sector": float(row.get("score_capital_sector") or 0.0),
            "score_breakout": float(row.get("score_breakout") or 0.0),
            "score_hold": float(row.get("score_hold") or 0.0),
            "score_benchmark": float(row.get("score_benchmark") or 0.0),
        }
        core_hits = sum(
            [
                core_scores["score_ma_trend"] >= 18.0,
                core_scores["score_volume_pattern"] >= 12.0,
                core_scores["score_capital_sector"] >= 10.0,
                core_scores["score_breakout"] >= 10.0,
                core_scores["score_hold"] >= 8.0,
                core_scores["score_benchmark"] >= 6.0,
            ]
        )
        if core_hits < int(config["buy_min_core_hits"]):
            return hits

        current_close = float(row.get("close") or 0.0)
        prior_20_high = row.get("prior_20_high")
        close_20d_ago = row.get("close_20d_ago")
        is_low_position = False
        if prior_20_high is not None and float(prior_20_high) > 0:
            is_low_position = current_close <= float(prior_20_high) * float(config["buy_low_position_high_ratio_max"])
        gain_20d_ok = False
        if close_20d_ago is not None and float(close_20d_ago) > 0:
            gain_20d = (current_close / float(close_20d_ago) - 1.0) * 100.0
            gain_20d_ok = gain_20d <= float(config["buy_20d_gain_max"])
        if not (is_low_position or gain_20d_ok):
            return hits

        lookback = max(int(config["buy_recent_stall_lookback"]), 1)
        stall_pct_max = float(config["buy_recent_stall_pct_max"])
        stall_volume_multiple = float(config["buy_recent_stall_volume_multiple"])
        recent_windows = [
            (row.get("pct_change"), row.get("volume"), row.get("vol_ma5")),
            (row.get("pct_change_lag1"), row.get("volume_lag1"), row.get("vol_ma5_lag1")),
            (row.get("pct_change_lag2"), row.get("volume_lag2"), row.get("vol_ma5_lag2")),
        ][:lookback]
        has_recent_volume_stall = False
        for pct_change, volume, vol_ma5 in recent_windows:
            if pct_change is None or volume is None or vol_ma5 is None:
                continue
            if float(vol_ma5) <= 0:
                continue
            if float(pct_change) < stall_pct_max and float(volume) > float(vol_ma5) * stall_volume_multiple:
                has_recent_volume_stall = True
                break
        if has_recent_volume_stall:
            return hits

        risk_high_5d = row.get("risk_high_5d")
        risk_low_5d = row.get("risk_low_5d")
        risk_close_high_5d = row.get("risk_close_high_5d")
        risk_close_low_5d = row.get("risk_close_low_5d")
        recent_amplitude = None
        if risk_high_5d is not None and risk_low_5d is not None and float(risk_low_5d) > 0:
            recent_amplitude = (float(risk_high_5d) - float(risk_low_5d)) / float(risk_low_5d)
        recent_max_drop = None
        if risk_close_high_5d is not None and risk_close_low_5d is not None and float(risk_close_high_5d) > 0:
            recent_max_drop = (float(risk_close_high_5d) - float(risk_close_low_5d)) / float(risk_close_high_5d)
        if recent_amplitude is None or recent_max_drop is None:
            return hits
        if (
            recent_amplitude > float(config["buy_risk_amplitude_max"])
            or recent_max_drop > float(config["buy_risk_max_drop_max"])
        ):
            return hits
        sector_rank_pct = row.get("sector_rank_pct")
        if sector_rank_pct is None:
            return hits
        if float(sector_rank_pct) > float(config["buy_sector_rank_top_pct"]):
            return hits
        trade_amount = row.get("amount")
        amount_lag1 = row.get("amount_lag1")
        amount_lag2 = row.get("amount_lag2")
        if trade_amount is None or amount_lag1 is None or amount_lag2 is None:
            return hits
        amount_3d_avg = (float(trade_amount) + float(amount_lag1) + float(amount_lag2)) / 3.0
        if amount_3d_avg < float(config["buy_amount_min"]):
            return hits

        if BUY_RULE_STRICT in enabled_buy_rules:
            if (
                float(row["score_total"]) >= float(config["buy_strict_score_total"])
                and (
                    float(row["score_ma_trend"]) >= float(config["buy_strict_score_ma_trend"])
                    or float(row["score_breakout"]) >= float(config["buy_strict_score_breakout"])
                )
                and float(row["score_volume_pattern"]) >= float(config["buy_strict_score_volume_pattern"])
                and float(row["score_capital_sector"]) >= float(config["buy_strict_score_capital_sector"])
            ):
                hits.append(BUY_RULE_STRICT)
        if BUY_RULE_MOMENTUM in enabled_buy_rules:
            if (
                float(row["score_total"]) >= float(config["buy_momentum_score_total"])
                and float(row["score_volume_pattern"]) >= float(config["buy_momentum_score_volume_pattern"])
            ):
                hits.append(BUY_RULE_MOMENTUM)
        return hits

    @staticmethod
    def _execution_tradability(side: str, price_row: dict[str, float | None] | None) -> str:
        if not price_row:
            return "missing"
        open_price = price_row.get("open")
        prev_close = price_row.get("prev_close")
        if open_price is None:
            return "missing"
        if prev_close is None or prev_close <= 0:
            return "tradable"
        upper_limit = prev_close * 1.098
        lower_limit = prev_close * 0.902
        if side == "buy" and float(open_price) >= upper_limit:
            return "limit_up"
        if side == "sell" and float(open_price) <= lower_limit:
            return "limit_down"
        return "tradable"

    @staticmethod
    def _passes_market_filter(market_filter: dict[str, float], config: dict[str, Any]) -> bool:
        if not market_filter:
            return False
        return (
            float(market_filter.get("avg_score") or 0.0) >= float(config["market_score_filter_min_avg"])
            and float(market_filter.get("ma5_avg_score") or 0.0) >= float(config["market_score_filter_min_ma5"])
            and (
                not bool(config.get("market_require_benchmark_ma20_up", True))
                or float(market_filter.get("benchmark_ma20_ratio") or 0.0) > 1.0
            )
            and (
                not bool(config.get("market_require_benchmark_above_ma20", True))
                or float(market_filter.get("benchmark_close_ma20_ratio") or 0.0) > 1.0
            )
        )

    def _evaluate_sell_rules(
        self,
        row: dict[str, Any],
        position: Position,
        current_close: float,
        hold_days: int,
        market_filter: dict[str, Any],
        enabled_sell_rules: set[str],
        config: dict[str, Any],
    ) -> dict[str, Any] | None:
        position_return = (current_close - position.cost_price) / max(position.cost_price, 0.01)
        ma5 = row.get("ma5")
        volume = float(row.get("volume") or 0.0)
        vol_ma5 = float(row.get("vol_ma5") or 0.0)
        current_hold_days = max(int(hold_days), 0)
        ma10 = row.get("ma10")
        ma20 = row.get("ma20")
        pct_change = float(row.get("pct_change") or 0.0)
        market_avg_score = float(market_filter.get("avg_score") or 0.0)

        drawdown_from_peak = 0.0
        if position.peak_close > 0:
            drawdown_from_peak = (float(position.peak_close) - current_close) / float(position.peak_close)
        peak_return = (float(position.peak_close) - position.cost_price) / max(position.cost_price, 0.01)
        is_flip_loss = (
            SELL_RULE_FLIP_LOSS in enabled_sell_rules
            and peak_return > 0
            and position_return < 0
        )

        is_break_ma5 = (
            SELL_RULE_BREAK_MA5 in enabled_sell_rules
            and ma5 is not None
            and current_close < float(ma5)
            and vol_ma5 > 0
            and volume > vol_ma5 * float(config["sell_break_ma5_volume_multiple"])
        )
        drawdown_limit = None
        if peak_return >= float(config["sell_drawdown_profit_threshold_high"]):
            drawdown_limit = float(config["sell_drawdown_threshold_high"])
        elif peak_return >= float(config["sell_drawdown_profit_threshold_mid"]):
            drawdown_limit = float(config["sell_drawdown_threshold_mid"])
        elif peak_return >= float(config["sell_drawdown_profit_threshold"]):
            drawdown_limit = float(config["sell_drawdown_threshold"])
        is_drawdown_stop = SELL_RULE_DRAWDOWN in enabled_sell_rules and drawdown_limit is not None and drawdown_from_peak > drawdown_limit
        is_time_stop = (
            SELL_RULE_TIME_STOP in enabled_sell_rules
            and current_hold_days >= int(config["sell_time_stop_days"])
            and position_return < float(config["sell_time_stop_return_threshold"])
            and ma20 is not None
            and current_close <= float(ma20)
        )
        is_market_weak_drop = (
            SELL_RULE_MARKET_WEAK_DROP in enabled_sell_rules
            and market_avg_score < float(config["sell_market_score_threshold"])
            and pct_change <= float(config["sell_market_drop_threshold"])
        )
        if is_drawdown_stop or is_break_ma5 or is_time_stop or is_flip_loss or is_market_weak_drop:
            rule_hits: list[str] = []
            if is_drawdown_stop:
                rule_hits.append(SELL_RULE_DRAWDOWN)
            if is_break_ma5:
                rule_hits.append(SELL_RULE_BREAK_MA5)
            if is_time_stop:
                rule_hits.append(SELL_RULE_TIME_STOP)
            if is_flip_loss:
                rule_hits.append(SELL_RULE_FLIP_LOSS)
            if is_market_weak_drop:
                rule_hits.append(SELL_RULE_MARKET_WEAK_DROP)
            return {"fraction": 1.0, "rule_hits": rule_hits}

        if SELL_RULE_TRIM not in enabled_sell_rules or position.trimmed:
            return None
        if position_return < float(config["sell_trim_profit_threshold"]):
            return None

        open_price = float(row.get("open") or current_close)
        high_price = float(row.get("high") or current_close)
        low_price = float(row.get("low") or current_close)
        upper_shadow = high_price - max(open_price, current_close)
        full_range = max(high_price - low_price, 0.01)
        is_long_upper_shadow = (
            upper_shadow > 0
            and upper_shadow / max(current_close, 0.01) >= float(config["sell_trim_upper_shadow_ratio"])
            and upper_shadow / full_range >= 0.35
            and vol_ma5 > 0
            and volume > vol_ma5 * float(config["sell_trim_volume_multiple"])
        )
        if is_long_upper_shadow:
            trim_hits: list[str] = [SELL_RULE_TRIM]
            return {"fraction": float(config["sell_trim_fraction"]), "rule_hits": trim_hits}
        return None

    @staticmethod
    def _deserialize_signal_row(row: dict[str, Any]) -> dict[str, Any]:
        payload = dict(row)
        payload["buy_rule_hits"] = json.loads(payload["buy_rule_hits"]) if payload.get("buy_rule_hits") else []
        payload["sell_rule_hits"] = json.loads(payload["sell_rule_hits"]) if payload.get("sell_rule_hits") else []
        return payload

    @staticmethod
    def _attach_benchmark_nav(nav_rows: list[Any], benchmark_rows: list[Any]) -> list[dict[str, Any]]:
        nav_payload = [dict(row) for row in nav_rows]
        if not nav_payload:
            return nav_payload
        benchmark_map = {str(row["trade_date"]): float(row["close"]) for row in benchmark_rows if row["close"] is not None}
        start_trade_date = str(nav_payload[0]["trade_date"])
        base_close = benchmark_map.get(start_trade_date)
        base_nav = float(nav_payload[0]["nav"])
        for row in nav_payload:
            trade_date = str(row["trade_date"])
            benchmark_close = benchmark_map.get(trade_date)
            row["benchmark_close"] = benchmark_close
            row["benchmark_nav"] = (
                round((benchmark_close / base_close) * base_nav, 4)
                if benchmark_close is not None and base_close
                else None
            )
        return nav_payload

    @staticmethod
    def _build_signal_stats(signal_rows: list[dict[str, Any]]) -> dict[str, Any]:
        buy_rule_counts: dict[str, int] = {}
        sell_rule_counts: dict[str, int] = {}
        selected_buy_count = 0
        for row in signal_rows:
            if row.get("action") == "buy" and int(row.get("selected") or 0) == 1:
                selected_buy_count += 1
            for rule_id in row.get("buy_rule_hits") or []:
                buy_rule_counts[rule_id] = buy_rule_counts.get(rule_id, 0) + 1
            for rule_id in row.get("sell_rule_hits") or []:
                sell_rule_counts[rule_id] = sell_rule_counts.get(rule_id, 0) + 1
        return {
            "buy_rule_counts": buy_rule_counts,
            "sell_rule_counts": sell_rule_counts,
            "selected_buy_count": selected_buy_count,
            "signal_count": len(signal_rows),
        }

    @staticmethod
    def _benchmark_return(benchmark_close_map: dict[str, float], start_date: str, end_date: str) -> float:
        start_close = benchmark_close_map.get(start_date)
        end_close = benchmark_close_map.get(end_date)
        if start_close is None or end_close is None or not start_close:
            return 0.0
        return round((end_close - start_close) / start_close, 6)
