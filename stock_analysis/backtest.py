from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from .analyzer import SCORE_VERSION


BUY_RULE_STRICT = "buy_strict"
BUY_RULE_MOMENTUM = "buy_momentum"

SELL_RULE_STOP = "sell_stop"
SELL_RULE_TREND = "sell_trend"
SELL_RULE_CAPITAL = "sell_capital"
SELL_RULE_STALE = "sell_stale"

DEFAULT_INITIAL_CAPITAL = 1_000_000.0


@dataclass(slots=True)
class BacktestDefaults:
    name: str = "120日策略回测"
    benchmark_symbol: str = "000001"
    lookback_days: int = 120
    start_date: str | None = None
    end_date: str | None = None
    market_score_filter_min_avg: float = 41.0
    market_score_filter_min_ma5: float = 41.0
    max_positions: int = 4
    initial_capital: float = DEFAULT_INITIAL_CAPITAL
    fee_rate: float = 0.001
    slippage_rate: float = 0.001
    buy_timing: str = "next_open"
    sell_timing: str = "next_open"
    allow_pyramiding: bool = False
    allow_same_day_repeat_trade: bool = False
    use_margin: bool = False
    score_version: str = SCORE_VERSION
    enabled_buy_rules: list[str] = field(default_factory=lambda: [BUY_RULE_STRICT, BUY_RULE_MOMENTUM])
    enabled_sell_rules: list[str] = field(
        default_factory=lambda: [SELL_RULE_STOP, SELL_RULE_TREND, SELL_RULE_CAPITAL, SELL_RULE_STALE]
    )


def build_backtest_config_schema() -> dict[str, Any]:
    defaults = BacktestDefaults()
    return {
        "defaults": asdict(defaults),
        "buy_rules": [
            {
                "id": BUY_RULE_STRICT,
                "label": "严格买入",
                "enabled": True,
                "description": "总分、趋势、突破、资金三项同时达标时触发主买点。",
                "conditions": [
                    {"field": "score_total", "operator": ">=", "value": 80.0},
                    {"field": "score_ma_trend", "operator": ">=", "value": 18.0},
                    {"field": "score_breakout", "operator": ">=", "value": 12.8},
                    {"field": "score_capital_sector", "operator": ">=", "value": 14.4},
                ],
                "logic": "all",
            },
            {
                "id": BUY_RULE_MOMENTUM,
                "label": "增强买入",
                "enabled": True,
                "description": "总分略低但量价形态极强时触发右侧买点。",
                "conditions": [
                    {"field": "score_total", "operator": ">=", "value": 75.0},
                    {"field": "score_volume_pattern", "operator": ">=", "value": 20.0},
                ],
                "logic": "all",
            },
        ],
        "sell_rules": [
            {
                "id": SELL_RULE_STOP,
                "label": "硬止损",
                "enabled": True,
                "description": "跌破突破防守位时，视为突破失败。",
                "conditions": [
                    {
                        "field": "close_vs_breakout_floor",
                        "operator": "<",
                        "value": 1.0,
                        "formula": "close < max(prior_20_high - 1.2*ATR14, prior_20_high*0.96)",
                    }
                ],
                "logic": "all",
            },
            {
                "id": SELL_RULE_TREND,
                "label": "趋势转弱",
                "enabled": True,
                "description": "短趋势走平或跌破关键均线时离场。",
                "conditions": [
                    {"field": "ma5_vs_ma10", "operator": "<=", "value": 1.0},
                    {"field": "close_vs_ma10", "operator": "<", "value": 1.0},
                ],
                "logic": "any",
            },
            {
                "id": SELL_RULE_CAPITAL,
                "label": "资金撤退",
                "enabled": True,
                "description": "CMF 转负时，视为资金开始撤退。",
                "conditions": [
                    {"field": "cmf21", "operator": "<", "value": 0.0},
                ],
                "logic": "all",
            },
            {
                "id": SELL_RULE_STALE,
                "label": "新鲜度失效",
                "enabled": True,
                "description": "突破后迟迟不走强时，切换到更有活力的标的。",
                "conditions": [
                    {"field": "score_hold_ratio", "operator": "<", "value": 0.5},
                    {"field": "position_return", "operator": "<", "value": 0.05},
                ],
                "logic": "all",
            },
        ],
        "portfolio_rules": {
            "max_positions": defaults.max_positions,
            "allocation": "equal_cash_on_new_entries",
            "description": "按剩余现金 / 新开仓数量等权分配；已持仓不因新信号调仓，只在卖出后补新票。",
            "restrictions": [
                "不加仓",
                "不允许同一天重复买卖同一只",
                "不考虑融资融券",
                "所有成交默认按下一交易日开盘价",
            ],
        },
        "market_filter_rules": {
            "enabled": True,
            "min_avg_score": defaults.market_score_filter_min_avg,
            "min_ma5_avg_score": defaults.market_score_filter_min_ma5,
            "description": "当全市场评分温度低于阈值时，不再生成新的买入信号。",
        },
        "execution_rules": {
            "signal_time": "T日收盘后",
            "buy_execution": "T+1日开盘",
            "sell_execution": "触发条件后的 T+1 日开盘",
            "benchmark_symbol": defaults.benchmark_symbol,
            "initial_capital": defaults.initial_capital,
            "fee_rate": defaults.fee_rate,
            "slippage_rate": defaults.slippage_rate,
        },
    }


def default_backtest_config() -> dict[str, Any]:
    return build_backtest_config_schema()["defaults"]
