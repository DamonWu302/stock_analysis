from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from .analyzer import SCORE_VERSION


BUY_RULE_STRICT = "buy_strict"
BUY_RULE_MOMENTUM = "buy_momentum"

SELL_RULE_TRIM = "sell_trim"
SELL_RULE_BREAK_MA5 = "sell_break_ma5"
SELL_RULE_DRAWDOWN = "sell_drawdown"
SELL_RULE_TIME_STOP = "sell_time_stop"
SELL_RULE_FLIP_LOSS = "sell_flip_loss"
SELL_RULE_MARKET_WEAK_DROP = "sell_market_weak_drop"

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
    market_require_benchmark_ma20_up: bool = False
    market_require_benchmark_above_ma20: bool = True
    max_positions: int = 4
    max_single_position: float = 0.30
    initial_capital: float = DEFAULT_INITIAL_CAPITAL
    fee_rate: float = 0.001
    slippage_rate: float = 0.001
    buy_strict_score_total: float = 74.0
    buy_strict_score_ma_trend: float = 14.0
    buy_strict_score_breakout: float = 12.0
    buy_strict_score_capital_sector: float = 10.0
    buy_strict_score_volume_pattern: float = 14.0
    buy_momentum_score_total: float = 68.0
    buy_momentum_score_volume_pattern: float = 14.0
    buy_min_core_hits: int = 4
    buy_low_position_high_ratio_max: float = 0.95
    buy_20d_gain_max: float = 15.0
    buy_recent_stall_lookback: int = 3
    buy_recent_stall_pct_max: float = 1.0
    buy_recent_stall_volume_multiple: float = 1.2
    buy_risk_amplitude_max: float = 0.20
    buy_risk_max_drop_max: float = 0.12
    buy_sector_rank_top_pct: float = 0.50
    buy_amount_min: float = 250_000_000.0
    sell_trim_profit_threshold: float = 0.08
    sell_trim_fraction: float = 0.5
    sell_trim_upper_shadow_ratio: float = 0.03
    sell_trim_volume_multiple: float = 1.2
    sell_break_ma5_volume_multiple: float = 1.0
    sell_drawdown_profit_threshold: float = 0.10
    sell_drawdown_threshold: float = 0.05
    sell_drawdown_profit_threshold_mid: float = 0.18
    sell_drawdown_threshold_mid: float = 0.06
    sell_drawdown_profit_threshold_high: float = 0.30
    sell_drawdown_threshold_high: float = 0.08
    sell_time_stop_days: int = 10
    sell_time_stop_return_threshold: float = 0.02
    sell_market_score_threshold: float = 35.0
    sell_market_drop_threshold: float = -3.0
    buy_timing: str = "next_open"
    sell_timing: str = "next_open"
    allow_pyramiding: bool = False
    allow_same_day_repeat_trade: bool = False
    use_margin: bool = False
    score_version: str = SCORE_VERSION
    enabled_buy_rules: list[str] = field(default_factory=lambda: [BUY_RULE_STRICT, BUY_RULE_MOMENTUM])
    enabled_sell_rules: list[str] = field(
        default_factory=lambda: [
            SELL_RULE_TRIM,
            SELL_RULE_BREAK_MA5,
            SELL_RULE_DRAWDOWN,
            SELL_RULE_TIME_STOP,
            SELL_RULE_FLIP_LOSS,
            SELL_RULE_MARKET_WEAK_DROP,
        ]
    )


def build_backtest_config_schema() -> dict[str, Any]:
    defaults = BacktestDefaults()
    return {
        "defaults": asdict(defaults),
        "compare_fields": [
            {"id": "buy_strict_score_total", "label": "严格买入总分阈值", "type": "float"},
            {"id": "buy_momentum_score_total", "label": "增强买入总分阈值", "type": "float"},
            {"id": "buy_min_core_hits", "label": "买入最少核心命中数", "type": "int"},
            {"id": "buy_20d_gain_max", "label": "买入20日涨幅上限", "type": "float"},
            {"id": "market_score_filter_min_avg", "label": "市场平均分过滤阈值", "type": "float"},
            {"id": "market_score_filter_min_ma5", "label": "市场5日均值过滤阈值", "type": "float"},
            {"id": "max_positions", "label": "最大持仓数", "type": "int"},
            {"id": "max_single_position", "label": "单票仓位上限", "type": "float"},
            {"id": "sell_market_score_threshold", "label": "弱市清仓平均分阈值", "type": "float"},
            {"id": "sell_market_drop_threshold", "label": "弱市清仓跌幅阈值", "type": "float"},
            {"id": "fee_rate", "label": "手续费", "type": "float"},
            {"id": "slippage_rate", "label": "滑点", "type": "float"},
        ],
        "buy_rules": [
            {
                "id": BUY_RULE_STRICT,
                "label": "提前型严格买入",
                "enabled": True,
                "description": "总分达到 74 分以上，且均线多头/低位突破满足其一，同时量价与资金板块共振达标，再叠加低位和近3日量能过滤。",
                "conditions": [
                    {"field": "score_total", "operator": ">=", "value": defaults.buy_strict_score_total},
                    {
                        "field": "score_ma_trend_or_breakout",
                        "operator": "either",
                        "value": f"均线多头 >= {defaults.buy_strict_score_ma_trend} 或 低位启动突破 >= {defaults.buy_strict_score_breakout}",
                    },
                    {"field": "score_volume_pattern", "operator": ">=", "value": defaults.buy_strict_score_volume_pattern},
                    {"field": "score_capital_sector", "operator": ">=", "value": defaults.buy_strict_score_capital_sector},
                    {"field": "core_hits", "operator": ">=", "value": defaults.buy_min_core_hits},
                    {
                        "field": "low_position_gate",
                        "operator": "=",
                        "value": 1.0,
                        "formula": f"close <= prior_20_high * {defaults.buy_low_position_high_ratio_max} or 20日涨幅 <= {defaults.buy_20d_gain_max}%",
                    },
                    {
                        "field": "recent_volume_stall",
                        "operator": "=",
                        "value": 0.0,
                        "formula": f"近{defaults.buy_recent_stall_lookback}日内不存在 涨幅 < {defaults.buy_recent_stall_pct_max}% 且 成交量 > 5日均量 {defaults.buy_recent_stall_volume_multiple} 倍",
                    },
                    {
                        "field": "buy_risk_limit",
                        "operator": "=",
                        "value": 1.0,
                        "formula": f"买入前5日振幅 <= {defaults.buy_risk_amplitude_max:.0%} 且 最大跌幅 <= {defaults.buy_risk_max_drop_max:.0%}",
                    },
                    {
                        "field": "sector_rank_gate",
                        "operator": "<=",
                        "value": defaults.buy_sector_rank_top_pct,
                        "formula": f"所属板块5日涨幅排名需位于前 {defaults.buy_sector_rank_top_pct:.0%}",
                    },
                    {
                    "field": "amount_gate",
                    "operator": ">=",
                    "value": defaults.buy_amount_min,
                    "formula": f"近3日平均成交额 >= {defaults.buy_amount_min / 100000000:.1f} 亿元",
                },
                ],
                "logic": "all",
            },
            {
                "id": BUY_RULE_MOMENTUM,
                "label": "提前型增强买入",
                "enabled": True,
                "description": "保留动量买点，但同样要求核心指标、低位约束和近3日量能环境不过热。",
                "conditions": [
                    {"field": "score_total", "operator": ">=", "value": defaults.buy_momentum_score_total},
                    {"field": "score_volume_pattern", "operator": ">=", "value": defaults.buy_momentum_score_volume_pattern},
                    {"field": "core_hits", "operator": ">=", "value": defaults.buy_min_core_hits},
                    {
                        "field": "low_position_gate",
                        "operator": "=",
                        "value": 1.0,
                        "formula": f"close <= prior_20_high * {defaults.buy_low_position_high_ratio_max} or 20日涨幅 <= {defaults.buy_20d_gain_max}%",
                    },
                    {
                        "field": "recent_volume_stall",
                        "operator": "=",
                        "value": 0.0,
                        "formula": f"近{defaults.buy_recent_stall_lookback}日内不存在 涨幅 < {defaults.buy_recent_stall_pct_max}% 且 成交量 > 5日均量 {defaults.buy_recent_stall_volume_multiple} 倍",
                    },
                    {
                        "field": "buy_risk_limit",
                        "operator": "=",
                        "value": 1.0,
                        "formula": f"买入前5日振幅 <= {defaults.buy_risk_amplitude_max:.0%} 且 最大跌幅 <= {defaults.buy_risk_max_drop_max:.0%}",
                    },
                    {
                        "field": "sector_rank_gate",
                        "operator": "<=",
                        "value": defaults.buy_sector_rank_top_pct,
                        "formula": f"所属板块5日涨幅排名需位于前 {defaults.buy_sector_rank_top_pct:.0%}",
                    },
                    {
                    "field": "amount_gate",
                    "operator": ">=",
                    "value": defaults.buy_amount_min,
                    "formula": f"近3日平均成交额 >= {defaults.buy_amount_min / 100000000:.1f} 亿元",
                },
                ],
                "logic": "all",
            },
        ],
        "sell_rules": [
            {
                "id": SELL_RULE_TRIM,
                "label": "分级止盈减仓",
                "enabled": True,
                "description": "持仓盈利达到阈值后，如出现放量长上影，先减仓 50% 锁定利润，作为辅助止盈。",
                "conditions": [
                    {
                        "field": "position_return",
                        "operator": ">=",
                        "value": defaults.sell_trim_profit_threshold,
                    },
                    {
                        "field": "long_upper_shadow_signal",
                        "operator": "=",
                        "value": 1.0,
                        "formula": "放量长上影",
                    }
                ],
                "logic": "all",
            },
            {
                "id": SELL_RULE_BREAK_MA5,
                "label": "跌破 MA5 放量清仓",
                "enabled": True,
                "description": "收盘跌破 MA5 且成交量高于 MA5 量能时，直接清仓。",
                "conditions": [
                    {"field": "close_vs_ma5", "operator": "<", "value": 1.0},
                    {"field": "volume_vs_vol_ma5", "operator": ">", "value": defaults.sell_break_ma5_volume_multiple},
                ],
                "logic": "all",
            },
            {
                "id": SELL_RULE_DRAWDOWN,
                "label": "高点回撤止损",
                "enabled": True,
                "description": "盈利达到 10% 后启用移动止盈；若阶段利润达到 20%，则放宽回撤容忍度。",
                "conditions": [
                    {
                        "field": "dynamic_trailing_stop",
                        "operator": "=",
                        "value": 1.0,
                        "formula": f"峰值盈利 >= {defaults.sell_drawdown_profit_threshold:.0%} 时回撤>{defaults.sell_drawdown_threshold:.0%}；峰值盈利 >= {defaults.sell_drawdown_profit_threshold_mid:.0%} 时回撤>{defaults.sell_drawdown_threshold_mid:.0%}；峰值盈利 >= {defaults.sell_drawdown_profit_threshold_high:.0%} 时回撤>{defaults.sell_drawdown_threshold_high:.0%}",
                    },
                ],
                "logic": "all",
            },
            {
                "id": SELL_RULE_TIME_STOP,
                "label": "时间止损",
                "enabled": True,
                "description": "持仓 10 天后若收益仍弱，且跌到 MA20 下方，直接清仓提升资金周转。",
                "conditions": [
                    {"field": "hold_days", "operator": ">=", "value": defaults.sell_time_stop_days},
                    {"field": "position_return", "operator": "<", "value": defaults.sell_time_stop_return_threshold},
                    {"field": "close_vs_ma20", "operator": "<=", "value": 1.0},
                ],
                "logic": "all",
            },
            {
                "id": SELL_RULE_FLIP_LOSS,
                "label": "盈转亏卖出",
                "enabled": True,
                "description": "持仓曾经处于盈利状态，但当前收盘已跌回成本线下方时，直接清仓。",
                "conditions": [
                    {"field": "peak_return", "operator": ">", "value": 0.0},
                    {"field": "position_return", "operator": "<", "value": 0.0},
                ],
                "logic": "all",
            },
            {
                "id": SELL_RULE_MARKET_WEAK_DROP,
                "label": "弱市大跌清仓",
                "enabled": True,
                "description": "当市场平均分低于阈值，且个股当日跌幅达到设定值时，直接清仓。",
                "conditions": [
                    {"field": "market_avg_score", "operator": "<", "value": defaults.sell_market_score_threshold},
                    {"field": "pct_change", "operator": "<=", "value": defaults.sell_market_drop_threshold},
                ],
                "logic": "all",
            },
        ],
        "portfolio_rules": {
            "max_positions": defaults.max_positions,
            "max_single_position": defaults.max_single_position,
            "allocation": "equal_cash_on_new_entries",
            "description": "按剩余现金 / 新开仓数量等权分配，并受单票仓位上限约束；已持仓不因新信号调仓，只在卖出后补新票。",
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


def build_backtest_templates() -> list[dict[str, Any]]:
    return [
        {
            "template_key": "balanced_default",
            "name": "均衡默认",
            "description": "提前买点 + 五档卖出，适合先跑全量评估。",
            "sort_order": 10,
            "config": {
                "name": "均衡默认模板",
                "lookback_days": 120,
                "max_positions": 4,
                "max_single_position": 0.30,
                "market_score_filter_min_avg": 41.0,
                "market_score_filter_min_ma5": 41.0,
                "enabled_buy_rules": [BUY_RULE_STRICT, BUY_RULE_MOMENTUM],
                "enabled_sell_rules": [
                    SELL_RULE_TRIM,
                    SELL_RULE_BREAK_MA5,
                    SELL_RULE_DRAWDOWN,
                    SELL_RULE_TIME_STOP,
                    SELL_RULE_FLIP_LOSS,
                    SELL_RULE_MARKET_WEAK_DROP,
                ],
            },
        },
        {
            "template_key": "strict_trend",
            "name": "严格趋势",
            "description": "只做提前型严格买点，止盈和风控更完整。",
            "sort_order": 20,
            "config": {
                "name": "严格趋势模板",
                "lookback_days": 120,
                "max_positions": 3,
                "max_single_position": 0.30,
                "market_score_filter_min_avg": 45.0,
                "market_score_filter_min_ma5": 45.0,
                "enabled_buy_rules": [BUY_RULE_STRICT],
                "enabled_sell_rules": [
                    SELL_RULE_TRIM,
                    SELL_RULE_BREAK_MA5,
                    SELL_RULE_DRAWDOWN,
                    SELL_RULE_TIME_STOP,
                    SELL_RULE_FLIP_LOSS,
                    SELL_RULE_MARKET_WEAK_DROP,
                ],
            },
        },
        {
            "template_key": "momentum_attack",
            "name": "动量进攻",
            "description": "偏向提前型增强买点，更强调快速止盈和放量撤退。",
            "sort_order": 30,
            "config": {
                "name": "动量进攻模板",
                "lookback_days": 120,
                "max_positions": 4,
                "max_single_position": 0.30,
                "market_score_filter_min_avg": 43.0,
                "market_score_filter_min_ma5": 43.0,
                "enabled_buy_rules": [BUY_RULE_MOMENTUM],
                "enabled_sell_rules": [
                    SELL_RULE_TRIM,
                    SELL_RULE_BREAK_MA5,
                    SELL_RULE_DRAWDOWN,
                    SELL_RULE_TIME_STOP,
                    SELL_RULE_FLIP_LOSS,
                    SELL_RULE_MARKET_WEAK_DROP,
                ],
            },
        },
        {
            "template_key": "defensive_low_exposure",
            "name": "防守低仓",
            "description": "更少持仓、更高过滤，适合弱势环境。",
            "sort_order": 40,
            "config": {
                "name": "防守低仓模板",
                "lookback_days": 120,
                "max_positions": 2,
                "max_single_position": 0.30,
                "market_score_filter_min_avg": 46.0,
                "market_score_filter_min_ma5": 46.0,
                "enabled_buy_rules": [BUY_RULE_STRICT],
                "enabled_sell_rules": [
                    SELL_RULE_TRIM,
                    SELL_RULE_BREAK_MA5,
                    SELL_RULE_DRAWDOWN,
                    SELL_RULE_TIME_STOP,
                    SELL_RULE_FLIP_LOSS,
                    SELL_RULE_MARKET_WEAK_DROP,
                ],
            },
        },
        {
            "template_key": "return_priority",
            "name": "收益优先",
            "description": "基于第三轮最优结果，偏向收益最大化，接受适度波动换取更高上行空间。",
            "sort_order": 50,
            "config": {
                "name": "收益优先模板",
                "lookback_days": 120,
                "max_positions": 3,
                "max_single_position": 0.50,
                "market_score_filter_min_avg": 35.0,
                "market_score_filter_min_ma5": 36.0,
                "buy_strict_score_total": 72.0,
                "buy_momentum_score_total": 60.0,
                "buy_min_core_hits": 4,
                "buy_amount_min": 550000000.0,
                "sell_drawdown_threshold": 0.07,
                "sell_drawdown_threshold_mid": 0.05,
                "sell_market_score_threshold": 37.0,
                "sell_market_drop_threshold": -3.5,
                "enabled_buy_rules": [BUY_RULE_STRICT, BUY_RULE_MOMENTUM],
                "enabled_sell_rules": [
                    SELL_RULE_DRAWDOWN,
                    SELL_RULE_TIME_STOP,
                    SELL_RULE_FLIP_LOSS,
                ],
            },
        },
        {
            "template_key": "steady_default",
            "name": "稳健默认",
            "description": "基于第四轮最优结果，强调更高稳定性和更均衡的收益回撤表现。",
            "sort_order": 60,
            "config": {
                "name": "稳健默认模板",
                "lookback_days": 120,
                "max_positions": 4,
                "max_single_position": 0.30,
                "market_score_filter_min_avg": 36.0,
                "market_score_filter_min_ma5": 36.0,
                "buy_strict_score_total": 70.0,
                "buy_momentum_score_total": 60.0,
                "buy_min_core_hits": 3,
                "buy_amount_min": 550000000.0,
                "sell_drawdown_threshold": 0.07,
                "sell_drawdown_threshold_mid": 0.06,
                "sell_market_score_threshold": 36.0,
                "sell_market_drop_threshold": -3.5,
                "enabled_buy_rules": [BUY_RULE_STRICT, BUY_RULE_MOMENTUM],
                "enabled_sell_rules": [
                    SELL_RULE_DRAWDOWN,
                    SELL_RULE_TIME_STOP,
                    SELL_RULE_FLIP_LOSS,
                ],
            },
        },
    ]


def default_backtest_config() -> dict[str, Any]:
    return build_backtest_config_schema()["defaults"]
