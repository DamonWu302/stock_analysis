from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)


@dataclass(slots=True)
class Settings:
    database_path: Path = DATA_DIR / "stock_analysis.db"
    default_provider: str = os.getenv("STOCK_PROVIDER", "baostock")
    analysis_limit: int = int(os.getenv("ANALYSIS_LIMIT", "0"))
    history_days: int = int(os.getenv("HISTORY_DAYS", "180"))
    top_n: int = int(os.getenv("TOP_N_RESULTS", "50"))
    scan_batch_size: int = int(os.getenv("SCAN_BATCH_SIZE", "200"))
    disable_system_proxy: bool = os.getenv("DISABLE_SYSTEM_PROXY", "1") == "1"
    akshare_proxy: str | None = os.getenv("AKSHARE_PROXY") or None
    llm_api_base: str = os.getenv("LLM_API_BASE", "https://api.openai.com/v1")
    llm_api_key: str | None = os.getenv("LLM_API_KEY") or None
    llm_model: str = os.getenv("LLM_MODEL", "gpt-4o-mini")
    llm_timeout_seconds: int = int(os.getenv("LLM_TIMEOUT_SECONDS", "180"))
    llm_connect_timeout_seconds: int = int(os.getenv("LLM_CONNECT_TIMEOUT_SECONDS", "20"))
    llm_max_retries: int = int(os.getenv("LLM_MAX_RETRIES", "3"))


settings = Settings()
