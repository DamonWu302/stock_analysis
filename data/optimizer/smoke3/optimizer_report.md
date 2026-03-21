# 参数优化报告：backtest-optimizer-v1

- 生成时间：`2026-03-21 21:11:20`
- 搜索方法：`random`
- 结果数量：`2`

## 约束条件

- 最少交易数：`5`
- 最大回撤上限：`0.25`
- 最低胜率：`0.25`

## 前十结果

### 1. Trial #1

- run_id：`89`
- 是否通过约束：`True`
- 总收益：`-0.010858`
- 超额收益：`-0.029308`
- 最大回撤：`0.086175`
- 胜率：`0.454545`
- 交易次数：`22`
- 参数：`{"market_score_filter_min_avg": 38, "market_score_filter_min_ma5": 33, "buy_strict_score_total": 72, "buy_momentum_score_total": 70, "buy_min_core_hits": 3, "buy_amount_min": 250000000.0, "max_single_position": 0.25, "sell_market_score_threshold": 38, "sell_market_drop_threshold": -4.0}`

### 2. Trial #2

- run_id：`90`
- 是否通过约束：`True`
- 总收益：`-0.022234`
- 超额收益：`-0.040684`
- 最大回撤：`0.045327`
- 胜率：`0.363636`
- 交易次数：`22`
- 参数：`{"market_score_filter_min_avg": 38, "market_score_filter_min_ma5": 38, "buy_strict_score_total": 72, "buy_momentum_score_total": 72, "buy_min_core_hits": 3, "buy_amount_min": 200000000.0, "max_single_position": 0.2, "sell_market_score_threshold": 34, "sell_market_drop_threshold": -3.5}`
