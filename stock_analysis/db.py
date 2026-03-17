from __future__ import annotations

import sqlite3
from contextlib import contextmanager
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
    benchmark_symbol TEXT,
    benchmark_name TEXT,
    benchmark_change REAL,
    sample_size INTEGER NOT NULL,
    cache_hits INTEGER NOT NULL DEFAULT 0,
    incremental_updates INTEGER NOT NULL DEFAULT 0,
    full_refreshes INTEGER NOT NULL DEFAULT 0,
    benchmark_cache_mode TEXT
);

CREATE TABLE IF NOT EXISTS analysis_result (
    run_id INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    name TEXT,
    score REAL NOT NULL,
    latest_price REAL,
    pct_change REAL,
    sector TEXT,
    summary TEXT,
    signals TEXT,
    score_breakdown TEXT,
    score_source TEXT NOT NULL DEFAULT 'system',
    review_updated_at TEXT,
    PRIMARY KEY (run_id, symbol),
    FOREIGN KEY (run_id) REFERENCES analysis_run(id)
);

CREATE TABLE IF NOT EXISTS analysis_task (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL,
    status TEXT NOT NULL,
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
"""


class Database:
    def __init__(self, db_path: Path):
        self.db_path = db_path

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def initialize(self) -> None:
        with self.connect() as conn:
            conn.executescript(SCHEMA)
            self._migrate(conn)

    @staticmethod
    def _migrate(conn: sqlite3.Connection) -> None:
        Database._ensure_columns(
            conn,
            "analysis_run",
            {
                "cache_hits": "INTEGER NOT NULL DEFAULT 0",
                "incremental_updates": "INTEGER NOT NULL DEFAULT 0",
                "full_refreshes": "INTEGER NOT NULL DEFAULT 0",
                "benchmark_cache_mode": "TEXT",
            },
        )
        Database._ensure_columns(
            conn,
            "analysis_result",
            {
                "score_breakdown": "TEXT",
                "score_source": "TEXT NOT NULL DEFAULT 'system'",
                "review_updated_at": "TEXT",
            },
        )
        Database._ensure_columns(
            conn,
            "analysis_task",
            {
                "provider": "TEXT NOT NULL DEFAULT 'baostock'",
                "status": "TEXT NOT NULL DEFAULT 'running'",
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
        Database._cleanup_benchmark_history(conn)

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
    def _cleanup_benchmark_history(conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            DELETE FROM benchmark_history
            WHERE symbol = '000001' AND close IS NOT NULL AND close < 1000
            """
        )
