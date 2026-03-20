from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterator


SCHEMA = """
CREATE TABLE IF NOT EXISTS price_history (
    symbol TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    open REAL,
    close REAL,
    high REAL,
    low REAL,
    volume REAL,
    amount REAL,
    PRIMARY KEY (symbol, trade_date)
);

CREATE TABLE IF NOT EXISTS benchmark_history (
    symbol TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    open REAL,
    close REAL,
    high REAL,
    low REAL,
    volume REAL,
    amount REAL,
    PRIMARY KEY (symbol, trade_date)
);

CREATE TABLE IF NOT EXISTS market_snapshot (
    trade_date TEXT NOT NULL,
    symbol TEXT NOT NULL,
    name TEXT,
    latest_price REAL,
    pct_change REAL,
    volume REAL,
    amount REAL,
    sector TEXT,
    sector_change REAL,
    sector_up_ratio REAL,
    main_net_inflow REAL,
    main_net_inflow_ratio REAL,
    PRIMARY KEY (trade_date, symbol)
);

CREATE TABLE IF NOT EXISTS analysis_run (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL,
    created_at TEXT NOT NULL,
    trade_date TEXT,
    sample_size INTEGER NOT NULL,
    cache_hits INTEGER NOT NULL DEFAULT 0,
    incremental_updates INTEGER NOT NULL DEFAULT 0,
    full_refreshes INTEGER NOT NULL DEFAULT 0,
    benchmark_cache_mode TEXT
);

CREATE TABLE IF NOT EXISTS analysis_task (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL,
    status TEXT NOT NULL,
    phase TEXT NOT NULL DEFAULT 'pending',
    started_at TEXT NOT NULL,
    finished_at TEXT,
    progress_current INTEGER NOT NULL DEFAULT 0,
    progress_total INTEGER NOT NULL DEFAULT 0,
    cache_hits INTEGER NOT NULL DEFAULT 0,
    incremental_updates INTEGER NOT NULL DEFAULT 0,
    full_refreshes INTEGER NOT NULL DEFAULT 0,
    benchmark_cache_mode TEXT,
    message TEXT,
    last_symbol TEXT,
    run_id INTEGER
);

CREATE TABLE IF NOT EXISTS analysis_task_item (
    task_id INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    name TEXT,
    cache_mode TEXT,
    latest_price REAL,
    pct_change REAL,
    score REAL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (task_id, symbol),
    FOREIGN KEY (task_id) REFERENCES analysis_task(id)
);

CREATE TABLE IF NOT EXISTS daily_factor (
    trade_date TEXT NOT NULL,
    symbol TEXT NOT NULL,
    close REAL,
    pct_change REAL,
    ma5 REAL,
    ma10 REAL,
    ma20 REAL,
    ma30 REAL,
    ma60 REAL,
    vol_ma5 REAL,
    atr14 REAL,
    prior_20_high REAL,
    cmf21 REAL,
    mfi14 REAL,
    sector_change REAL,
    sector_up_ratio REAL,
    benchmark_close REAL,
    benchmark_ma20 REAL,
    benchmark_prev_ma20 REAL,
    PRIMARY KEY (trade_date, symbol)
);

CREATE TABLE IF NOT EXISTS daily_score (
    trade_date TEXT NOT NULL,
    symbol TEXT NOT NULL,
    score_total REAL NOT NULL,
    score_ma_trend REAL,
    score_volume_pattern REAL,
    score_capital_sector REAL,
    score_breakout REAL,
    score_hold REAL,
    score_benchmark REAL,
    signals TEXT,
    score_breakdown TEXT,
    summary TEXT,
    score_source TEXT NOT NULL DEFAULT 'system',
    review_updated_at TEXT,
    score_version TEXT NOT NULL,
    PRIMARY KEY (trade_date, symbol)
);

CREATE TABLE IF NOT EXISTS backtest_run (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft',
    benchmark_symbol TEXT NOT NULL,
    start_date TEXT NOT NULL,
    end_date TEXT NOT NULL,
    lookback_days INTEGER NOT NULL,
    buy_timing TEXT NOT NULL,
    sell_timing TEXT NOT NULL,
    max_positions INTEGER NOT NULL,
    fee_rate REAL NOT NULL,
    slippage_rate REAL NOT NULL,
    allow_pyramiding INTEGER NOT NULL DEFAULT 0,
    allow_same_day_repeat_trade INTEGER NOT NULL DEFAULT 0,
    use_margin INTEGER NOT NULL DEFAULT 0,
    score_version TEXT NOT NULL,
    config_json TEXT NOT NULL,
    summary_json TEXT,
    created_at TEXT NOT NULL,
    finished_at TEXT
);

CREATE TABLE IF NOT EXISTS backtest_signal (
    run_id INTEGER NOT NULL,
    trade_date TEXT NOT NULL,
    symbol TEXT NOT NULL,
    score_total REAL,
    rank_value INTEGER,
    action TEXT NOT NULL,
    selected INTEGER NOT NULL DEFAULT 0,
    buy_rule_hits TEXT,
    sell_rule_hits TEXT,
    breakout_floor REAL,
    target_position REAL,
    note TEXT,
    PRIMARY KEY (run_id, trade_date, symbol, action),
    FOREIGN KEY (run_id) REFERENCES backtest_run(id)
);

CREATE TABLE IF NOT EXISTS backtest_trade (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    signal_trade_date TEXT NOT NULL,
    execution_date TEXT NOT NULL,
    price REAL NOT NULL,
    shares REAL NOT NULL,
    gross_amount REAL NOT NULL,
    fee REAL NOT NULL,
    slippage_cost REAL NOT NULL,
    net_amount REAL NOT NULL,
    reason TEXT,
    FOREIGN KEY (run_id) REFERENCES backtest_run(id)
);

CREATE TABLE IF NOT EXISTS backtest_position_daily (
    run_id INTEGER NOT NULL,
    trade_date TEXT NOT NULL,
    symbol TEXT NOT NULL,
    shares REAL NOT NULL,
    cost_price REAL NOT NULL,
    close_price REAL NOT NULL,
    market_value REAL NOT NULL,
    weight REAL NOT NULL,
    unrealized_pnl REAL NOT NULL,
    hold_days INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, trade_date, symbol),
    FOREIGN KEY (run_id) REFERENCES backtest_run(id)
);

CREATE TABLE IF NOT EXISTS backtest_nav (
    run_id INTEGER NOT NULL,
    trade_date TEXT NOT NULL,
    cash REAL NOT NULL,
    market_value REAL NOT NULL,
    nav REAL NOT NULL,
    daily_return REAL NOT NULL,
    drawdown REAL NOT NULL,
    position_count INTEGER NOT NULL,
    turnover REAL NOT NULL,
    PRIMARY KEY (run_id, trade_date),
    FOREIGN KEY (run_id) REFERENCES backtest_run(id)
);

CREATE TABLE IF NOT EXISTS backtest_task (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    status TEXT NOT NULL,
    phase TEXT NOT NULL DEFAULT 'pending',
    started_at TEXT NOT NULL,
    finished_at TEXT,
    progress_current INTEGER NOT NULL DEFAULT 0,
    progress_total INTEGER NOT NULL DEFAULT 0,
    last_trade_date TEXT,
    message TEXT,
    run_id INTEGER,
    config_json TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES backtest_run(id)
);

CREATE TABLE IF NOT EXISTS backtest_template (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    template_key TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    description TEXT,
    config_json TEXT NOT NULL,
    sort_order INTEGER NOT NULL DEFAULT 0,
    is_builtin INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS backfill_task (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    status TEXT NOT NULL,
    phase TEXT NOT NULL DEFAULT 'pending',
    started_at TEXT NOT NULL,
    finished_at TEXT,
    days INTEGER NOT NULL,
    start_date TEXT,
    end_date TEXT,
    batch_size INTEGER NOT NULL,
    progress_current INTEGER NOT NULL DEFAULT 0,
    progress_total INTEGER NOT NULL DEFAULT 0,
    factor_rows INTEGER NOT NULL DEFAULT 0,
    score_rows INTEGER NOT NULL DEFAULT 0,
    last_trade_date TEXT,
    message TEXT,
    resume_from_task_id INTEGER
);

CREATE TABLE IF NOT EXISTS backfill_task_batch (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER NOT NULL,
    batch_index INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    started_at TEXT NOT NULL,
    finished_at TEXT,
    start_trade_date TEXT,
    end_trade_date TEXT,
    completed_dates INTEGER NOT NULL DEFAULT 0,
    factor_rows INTEGER NOT NULL DEFAULT 0,
    score_rows INTEGER NOT NULL DEFAULT 0,
    message TEXT,
    FOREIGN KEY (task_id) REFERENCES backfill_task(id)
);
"""


class Database:
    def __init__(self, db_path: Path):
        self.db_path = db_path

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout = 30000")
            yield conn
            conn.commit()
        finally:
            conn.close()

    def initialize(self) -> None:
        with self.connect() as conn:
            conn.executescript(SCHEMA)
            self._migrate(conn)
            self._seed_backtest_templates(conn)

    @staticmethod
    def _migrate(conn: sqlite3.Connection) -> None:
        conn.execute("DROP TABLE IF EXISTS analysis_result")
        Database._rebuild_analysis_run(conn)
        Database._ensure_columns(
            conn,
            "analysis_run",
            {
                "trade_date": "TEXT",
                "cache_hits": "INTEGER NOT NULL DEFAULT 0",
                "incremental_updates": "INTEGER NOT NULL DEFAULT 0",
                "full_refreshes": "INTEGER NOT NULL DEFAULT 0",
                "benchmark_cache_mode": "TEXT",
            },
        )
        Database._ensure_columns(
            conn,
            "analysis_task",
            {
                "provider": "TEXT NOT NULL DEFAULT 'baostock'",
                "status": "TEXT NOT NULL DEFAULT 'running'",
                "phase": "TEXT NOT NULL DEFAULT 'pending'",
                "started_at": "TEXT",
                "finished_at": "TEXT",
                "progress_current": "INTEGER NOT NULL DEFAULT 0",
                "progress_total": "INTEGER NOT NULL DEFAULT 0",
                "cache_hits": "INTEGER NOT NULL DEFAULT 0",
                "incremental_updates": "INTEGER NOT NULL DEFAULT 0",
                "full_refreshes": "INTEGER NOT NULL DEFAULT 0",
                "benchmark_cache_mode": "TEXT",
                "message": "TEXT",
                "last_symbol": "TEXT",
                "run_id": "INTEGER",
            },
        )
        conn.execute(
            """
            UPDATE analysis_task
            SET phase = CASE
                WHEN status = 'completed' THEN 'completed'
                WHEN status = 'failed' THEN 'failed'
                ELSE phase
            END
            WHERE phase IS NULL OR phase = '' OR phase = 'pending'
            """
        )
        Database._ensure_columns(
            conn,
            "daily_score",
            {
                "score_breakdown": "TEXT",
                "score_source": "TEXT NOT NULL DEFAULT 'system'",
                "review_updated_at": "TEXT",
            },
        )
        Database._ensure_columns(
            conn,
            "backfill_task",
            {
                "phase": "TEXT NOT NULL DEFAULT 'pending'",
                "days": "INTEGER NOT NULL DEFAULT 120",
                "start_date": "TEXT",
                "end_date": "TEXT",
                "batch_size": "INTEGER NOT NULL DEFAULT 10",
                "progress_current": "INTEGER NOT NULL DEFAULT 0",
                "progress_total": "INTEGER NOT NULL DEFAULT 0",
                "factor_rows": "INTEGER NOT NULL DEFAULT 0",
                "score_rows": "INTEGER NOT NULL DEFAULT 0",
                "last_trade_date": "TEXT",
                "message": "TEXT",
                "resume_from_task_id": "INTEGER",
            },
        )
        Database._ensure_columns(
            conn,
            "backfill_task_batch",
            {
                "status": "TEXT NOT NULL DEFAULT 'running'",
                "started_at": "TEXT",
                "finished_at": "TEXT",
                "start_trade_date": "TEXT",
                "end_trade_date": "TEXT",
                "completed_dates": "INTEGER NOT NULL DEFAULT 0",
                "factor_rows": "INTEGER NOT NULL DEFAULT 0",
                "score_rows": "INTEGER NOT NULL DEFAULT 0",
                "message": "TEXT",
            },
        )
        Database._ensure_columns(
            conn,
            "backtest_task",
            {
                "phase": "TEXT NOT NULL DEFAULT 'pending'",
                "progress_current": "INTEGER NOT NULL DEFAULT 0",
                "progress_total": "INTEGER NOT NULL DEFAULT 0",
                "last_trade_date": "TEXT",
                "message": "TEXT",
                "run_id": "INTEGER",
                "config_json": "TEXT NOT NULL DEFAULT '{}'",
            },
        )
        conn.execute(
            """
            UPDATE backfill_task
            SET phase = CASE
                WHEN status = 'completed' THEN 'completed'
                WHEN status = 'failed' THEN 'failed'
                ELSE phase
            END
            WHERE phase IS NULL OR phase = ''
            """
        )
        conn.execute(
            """
            UPDATE backtest_task
            SET phase = CASE
                WHEN status = 'completed' THEN 'completed'
                WHEN status = 'failed' THEN 'failed'
                ELSE phase
            END
            WHERE phase IS NULL OR phase = ''
            """
        )
        Database._cleanup_benchmark_history(conn)

    @staticmethod
    def _seed_backtest_templates(conn: sqlite3.Connection) -> None:
        from .backtest import build_backtest_templates

        now = datetime.now().isoformat(timespec="seconds")
        for item in build_backtest_templates():
            conn.execute(
                """
                INSERT INTO backtest_template
                (template_key, name, description, config_json, sort_order, is_builtin, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 1, ?, ?)
                ON CONFLICT(template_key) DO UPDATE SET
                    name = excluded.name,
                    description = excluded.description,
                    config_json = excluded.config_json,
                    sort_order = excluded.sort_order,
                    updated_at = excluded.updated_at
                """,
                (
                    item["template_key"],
                    item["name"],
                    item.get("description"),
                    json.dumps(item.get("config") or {}, ensure_ascii=False),
                    int(item.get("sort_order") or 0),
                    now,
                    now,
                ),
            )

    @staticmethod
    def _ensure_columns(conn: sqlite3.Connection, table_name: str, columns: dict[str, str]) -> None:
        existing = {
            row["name"]
            for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
        for column_name, column_def in columns.items():
            if column_name not in existing:
                try:
                    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")
                except sqlite3.OperationalError as exc:
                    if "duplicate column name" not in str(exc).lower():
                        raise

    @staticmethod
    def _rebuild_analysis_run(conn: sqlite3.Connection) -> None:
        existing = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(analysis_run)").fetchall()
        }
        expected = {
            "id",
            "provider",
            "created_at",
            "trade_date",
            "sample_size",
            "cache_hits",
            "incremental_updates",
            "full_refreshes",
            "benchmark_cache_mode",
        }
        if existing == expected:
            return

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS analysis_run_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                provider TEXT NOT NULL,
                created_at TEXT NOT NULL,
                trade_date TEXT,
                sample_size INTEGER NOT NULL,
                cache_hits INTEGER NOT NULL DEFAULT 0,
                incremental_updates INTEGER NOT NULL DEFAULT 0,
                full_refreshes INTEGER NOT NULL DEFAULT 0,
                benchmark_cache_mode TEXT
            )
            """
        )
        trade_date_expr = "trade_date" if "trade_date" in existing else "NULL"
        conn.execute(
            f"""
            INSERT INTO analysis_run_new
            (id, provider, created_at, trade_date, sample_size,
             cache_hits, incremental_updates, full_refreshes, benchmark_cache_mode)
            SELECT id,
                   provider,
                   created_at,
                   {trade_date_expr},
                   sample_size,
                   COALESCE(cache_hits, 0),
                   COALESCE(incremental_updates, 0),
                   COALESCE(full_refreshes, 0),
                   benchmark_cache_mode
            FROM analysis_run
            """
        )
        conn.execute("DROP TABLE analysis_run")
        conn.execute("ALTER TABLE analysis_run_new RENAME TO analysis_run")

    @staticmethod
    def _cleanup_benchmark_history(conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            DELETE FROM benchmark_history
            WHERE symbol = '000001' AND close IS NOT NULL AND close < 1000
            """
        )
