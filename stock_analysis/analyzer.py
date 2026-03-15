from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(slots=True)
class AnalysisResult:
    symbol: str
    name: str
    score: float
    latest_price: float
    pct_change: float
    sector: str
    summary: str
    signals: list[str]
    score_breakdown: list[dict[str, Any]]


def _prepare_history(history: pd.DataFrame) -> pd.DataFrame:
    df = history.copy()
    for column in ["open", "close", "high", "low", "volume", "amount"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df.sort_values("trade_date").reset_index(drop=True)
    df["pct_change"] = df["close"].pct_change().fillna(0)

    for window in [5, 10, 20, 30, 60, 120]:
        df[f"ma{window}"] = df["close"].rolling(window).mean()
    df["vol_ma5"] = df["volume"].rolling(5).mean()
    return df


def _clip_ratio(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, float(value)))


def _ratio_over(value: float, target: float) -> float:
    if target <= 0:
        return 0.0
    return _clip_ratio(value / target)


def _ratio_under(value: float, ceiling: float) -> float:
    if ceiling <= 0:
        return 0.0
    return _clip_ratio(1 - max(value, 0) / ceiling)


def _safe_mean(*values: float) -> float:
    usable = [value for value in values if pd.notna(value)]
    if not usable:
        return 0.0
    return sum(usable) / len(usable)


def _range_ratio(value: float, low: float, high: float) -> float:
    if high <= low:
        return 0.0
    return _clip_ratio((value - low) / (high - low))


def _score_item(label: str, weight: float, progress: float, threshold: float, comment: str) -> dict[str, Any]:
    normalized = _clip_ratio(progress)
    return {
        "label": label,
        "matched": normalized >= threshold,
        "weight": weight,
        "score": round(weight * normalized, 2),
        "progress": round(normalized, 4),
        "threshold": threshold,
        "comment": comment,
    }


def build_score_breakdown(snapshot: pd.Series, history: pd.DataFrame, benchmark_history: pd.DataFrame) -> list[dict[str, Any]]:
    if history.empty or len(history) < 60:
        return []

    df = _prepare_history(history)
    benchmark = _prepare_history(benchmark_history)
    latest = df.iloc[-1]
    prev = df.iloc[-2]
    benchmark_latest = benchmark.iloc[-1]
    benchmark_prev = benchmark.iloc[-2]

    short_ma_progress = _safe_mean(
        1.0 if latest["ma5"] > latest["ma10"] else _ratio_over(latest["ma5"], latest["ma10"]),
        1.0 if latest["ma10"] > latest["ma20"] else _ratio_over(latest["ma10"], latest["ma20"]),
    )
    long_ma_gate = _range_ratio(float(latest["ma20"] / latest["ma60"]), 0.995, 1.01)
    price_above_ma20 = _ratio_over(latest["close"], latest["ma20"])
    ma20_slope = 1.0 if latest["ma20"] > prev["ma20"] else _ratio_over(latest["ma20"], prev["ma20"])
    ma_progress = (0.25 * short_ma_progress + 0.2 * price_above_ma20 + 0.15 * ma20_slope + 0.4 * long_ma_gate)
    ma_comment = (
        f"短中期均线强度 {short_ma_progress:.0%}，MA20/MA60 关系 {float(latest['ma20'] / latest['ma60']):.3f}，"
        f"长期均线门槛强度 {long_ma_gate:.0%}"
    )

    recent = df.tail(10).copy()
    recent["up_gain_score"] = (recent["pct_change"] / 0.02).clip(lower=0, upper=1)
    recent["up_volume_score"] = (recent["volume"] / (recent["vol_ma5"] * 1.5)).clip(lower=0, upper=1)
    recent["up_score"] = recent[["up_gain_score", "up_volume_score"]].min(axis=1)
    recent["pullback_pct_score"] = (1 - recent["pct_change"].abs() / 0.02).clip(lower=0, upper=1)
    recent["pullback_volume_score"] = (1 - (recent["volume"] / recent["vol_ma5"] - 0.8) / 0.8).clip(lower=0, upper=1)
    recent["pullback_score"] = recent[["pullback_pct_score", "pullback_volume_score"]].min(axis=1)
    volume_progress = _safe_mean(float(recent["up_score"].max()), float(recent["pullback_score"].max()))
    volume_comment = f"近10日放量上涨强度 {recent['up_score'].max():.0%}，缩量整理强度 {recent['pullback_score'].max():.0%}"

    inflow_ratio = float(snapshot.get("main_net_inflow_ratio", 0) or 0)
    sector_change = float(snapshot.get("sector_change", 0) or 0)
    sector_up_ratio = float(snapshot.get("sector_up_ratio", 0) or 0)
    benchmark_pct = float(benchmark_latest["pct_change"] * 100)
    capital_progress = _safe_mean(
        _ratio_over(inflow_ratio, 0.05),
        1.0 if sector_change > benchmark_pct else _ratio_over(sector_change - benchmark_pct + 0.5, 1.0),
        _ratio_over(sector_up_ratio, 0.6),
    )
    capital_comment = f"主力净流入占比 {inflow_ratio:.2%}，板块涨幅 {sector_change:.2f}% ，上涨占比 {sector_up_ratio:.0%}"

    rolling_low = float(df["close"].tail(120).min())
    prior_20_high = float(df["high"].rolling(20).max().shift(1).iloc[-1])
    low_position = (latest["close"] - rolling_low) / rolling_low if rolling_low else 1.0
    breakout_distance = (latest["close"] / prior_20_high) if prior_20_high and pd.notna(prior_20_high) else 0.0
    low_component = _ratio_under(low_position, 0.3)
    breakout_component = _range_ratio(breakout_distance, 0.985, 1.015)
    breakout_progress = 0.45 * low_component + 0.55 * breakout_component
    breakout_comment = (
        f"距120日低点 {low_position:.2%}，相对前20日高点 {breakout_distance:.3f} 倍；"
        f"突破确认强度 {breakout_component:.0%}"
    )

    recent_after_breakout = df.tail(4)
    breakout_floor = prior_20_high * 0.98 if prior_20_high and pd.notna(prior_20_high) else None
    if breakout_floor:
        hold_ratio = float(recent_after_breakout["low"].min() / breakout_floor)
        breakout_validity = _range_ratio(breakout_distance, 1.0, 1.03)
        hold_support = _range_ratio(hold_ratio, 0.995, 1.01)
        hold_progress = breakout_validity * hold_support
    else:
        hold_progress = 0.0
        hold_ratio = 0.0
        breakout_validity = 0.0
        hold_support = 0.0
    hold_comment = (
        f"突破成立强度 {breakout_validity:.0%}，最近4日最低价相对防守位强度 {hold_ratio:.3f} 倍，"
        f"守位强度 {hold_support:.0%}"
    )

    benchmark_progress = _safe_mean(
        _ratio_over(float(benchmark_latest["close"]), float(benchmark_latest["ma20"])),
        1.0 if benchmark_latest["ma20"] > benchmark_prev["ma20"] else _ratio_over(float(benchmark_latest["ma20"]), float(benchmark_prev["ma20"])),
    )
    benchmark_comment = f"指数收盘相对 MA20 为 {float(benchmark_latest['close']) / float(benchmark_latest['ma20']):.2f} 倍"

    return [
        _score_item("均线多头", 24, ma_progress, 0.85, ma_comment),
        _score_item("放量上涨+缩量回调", 20, volume_progress, 0.8, volume_comment),
        _score_item("资金流入+板块强势", 18, capital_progress, 0.8, capital_comment),
        _score_item("低位启动突破", 16, breakout_progress, 0.8, breakout_comment),
        _score_item("突破后未破位", 12, hold_progress, 0.85, hold_comment),
        _score_item("大盘共振", 10, benchmark_progress, 0.8, benchmark_comment),
    ]


def score_stock(snapshot: pd.Series, history: pd.DataFrame, benchmark_history: pd.DataFrame) -> AnalysisResult | None:
    if history.empty or len(history) < 60:
        return None

    df = _prepare_history(history)
    latest = df.iloc[-1]
    breakdown = build_score_breakdown(snapshot, history, benchmark_history)
    score = round(sum(item["score"] for item in breakdown), 2)
    signals = [item["label"] for item in breakdown if item["matched"]]
    summary = " / ".join(signals) if signals else "暂未命中核心信号"

    return AnalysisResult(
        symbol=str(snapshot["symbol"]),
        name=str(snapshot["name"]),
        score=score,
        latest_price=round(float(snapshot.get("latest_price", latest["close"]) or latest["close"]), 2),
        pct_change=round(float(snapshot.get("pct_change", latest["pct_change"] * 100) or latest["pct_change"] * 100), 2),
        sector=str(snapshot.get("sector", "未分类") or "未分类"),
        summary=summary,
        signals=signals,
        score_breakdown=breakdown,
    )
