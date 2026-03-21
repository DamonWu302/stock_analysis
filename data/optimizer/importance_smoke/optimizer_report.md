# 参数优化报告：backtest-optimizer-v2

- 生成时间：`2026-03-21 21:31:46`
- 搜索阶段数：`2`
- 结果数量：`5`

## 约束条件

- 最少交易数：`5`
- 最大回撤上限：`0.25`
- 最低胜率：`0.25`

## 前十结果

### 1. Trial #4

- 所属阶段：`refine`
- run_id：`120`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`0.046637`
- 超额收益：`0.028187`
- 最大回撤：`0.055287`
- 胜率：`0.384615`
- 交易次数：`25`
- 参数：`{"market_score_filter_min_avg": 38, "market_score_filter_min_ma5": 37, "buy_strict_score_total": 72, "buy_momentum_score_total": 68, "buy_min_core_hits": 4, "buy_amount_min": 400000000.0, "max_single_position": 0.3, "sell_market_score_threshold": 34, "sell_market_drop_threshold": -2.5}`

### 2. Trial #3

- 所属阶段：`coarse`
- run_id：`119`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`0.019258`
- 超额收益：`0.000808`
- 最大回撤：`0.078493`
- 胜率：`0.4`
- 交易次数：`10`
- 参数：`{"market_score_filter_min_avg": 37, "market_score_filter_min_ma5": 37, "buy_strict_score_total": 72, "buy_momentum_score_total": 68, "buy_min_core_hits": 5, "buy_amount_min": 400000000.0, "max_single_position": 0.35, "sell_market_score_threshold": 34, "sell_market_drop_threshold": -2.5}`

### 3. Trial #5

- 所属阶段：`refine`
- run_id：`121`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`0.008881`
- 超额收益：`-0.009569`
- 最大回撤：`0.036127`
- 胜率：`0.428571`
- 交易次数：`14`
- 参数：`{"market_score_filter_min_avg": 38, "market_score_filter_min_ma5": 33, "buy_strict_score_total": 74, "buy_momentum_score_total": 72, "buy_min_core_hits": 4, "buy_amount_min": 250000000.0, "max_single_position": 0.3, "sell_market_score_threshold": 38, "sell_market_drop_threshold": -4.0}`

### 4. Trial #1

- 所属阶段：`coarse`
- run_id：`117`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`-0.010858`
- 超额收益：`-0.029308`
- 最大回撤：`0.086175`
- 胜率：`0.454545`
- 交易次数：`22`
- 参数：`{"market_score_filter_min_avg": 38, "market_score_filter_min_ma5": 33, "buy_strict_score_total": 72, "buy_momentum_score_total": 70, "buy_min_core_hits": 3, "buy_amount_min": 250000000.0, "max_single_position": 0.25, "sell_market_score_threshold": 38, "sell_market_drop_threshold": -4.0}`

### 5. Trial #2

- 所属阶段：`coarse`
- run_id：`118`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`-0.022234`
- 超额收益：`-0.040684`
- 最大回撤：`0.045327`
- 胜率：`0.363636`
- 交易次数：`22`
- 参数：`{"market_score_filter_min_avg": 38, "market_score_filter_min_ma5": 38, "buy_strict_score_total": 72, "buy_momentum_score_total": 72, "buy_min_core_hits": 3, "buy_amount_min": 200000000.0, "max_single_position": 0.2, "sell_market_score_threshold": 34, "sell_market_drop_threshold": -3.5}`
