# 参数优化报告：backtest-optimizer-v2

- 生成时间：`2026-03-21 21:38:33`
- 搜索阶段数：`2`
- 结果数量：`3`

## 约束条件

- 最少交易数：`5`
- 最大回撤上限：`0.25`
- 最低胜率：`0.25`

## 前十结果

### 1. Trial #3

- 所属阶段：`refine`
- run_id：`124`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`0.035991`
- 超额收益：`0.017541`
- 最大回撤：`0.065152`
- 胜率：`0.357143`
- 交易次数：`28`
- 参数：`{"market_score_filter_min_avg": 38, "market_score_filter_min_ma5": 33, "buy_strict_score_total": 74, "buy_momentum_score_total": 68, "buy_min_core_hits": 4, "buy_amount_min": 200000000.0, "max_single_position": 0.25, "sell_market_score_threshold": 38, "sell_market_drop_threshold": -3.5}`

### 2. Trial #1

- 所属阶段：`coarse`
- run_id：`122`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`-0.010858`
- 超额收益：`-0.029308`
- 最大回撤：`0.086175`
- 胜率：`0.454545`
- 交易次数：`22`
- 参数：`{"market_score_filter_min_avg": 38, "market_score_filter_min_ma5": 33, "buy_strict_score_total": 72, "buy_momentum_score_total": 70, "buy_min_core_hits": 3, "buy_amount_min": 250000000.0, "max_single_position": 0.25, "sell_market_score_threshold": 38, "sell_market_drop_threshold": -4.0}`

### 3. Trial #2

- 所属阶段：`coarse`
- run_id：`123`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`-0.022234`
- 超额收益：`-0.040684`
- 最大回撤：`0.045327`
- 胜率：`0.363636`
- 交易次数：`22`
- 参数：`{"market_score_filter_min_avg": 38, "market_score_filter_min_ma5": 38, "buy_strict_score_total": 72, "buy_momentum_score_total": 72, "buy_min_core_hits": 3, "buy_amount_min": 200000000.0, "max_single_position": 0.2, "sell_market_score_threshold": 34, "sell_market_drop_threshold": -3.5}`

## 参数重要性统计

- 目标字段：`total_return`
- 统计参数数：`8`

### 1. buy_momentum_score_total

- 重要性分数：`0.058225`
- 最优取值：`68`
- 最优平均目标值：`0.035991`
- 最差取值：`72`
- 最差平均目标值：`-0.022234`
- 分桶数：`3`

### 2. buy_min_core_hits

- 重要性分数：`0.052537`
- 最优取值：`4`
- 最优平均目标值：`0.035991`
- 最差取值：`3`
- 最差平均目标值：`-0.016546`
- 分桶数：`2`

### 3. buy_strict_score_total

- 重要性分数：`0.052537`
- 最优取值：`74`
- 最优平均目标值：`0.035991`
- 最差取值：`72`
- 最差平均目标值：`-0.016546`
- 分桶数：`2`

### 4. market_score_filter_min_ma5

- 重要性分数：`0.0348`
- 最优取值：`33`
- 最优平均目标值：`0.012567`
- 最差取值：`38`
- 最差平均目标值：`-0.022234`
- 分桶数：`2`

### 5. max_single_position

- 重要性分数：`0.0348`
- 最优取值：`0.25`
- 最优平均目标值：`0.012567`
- 最差取值：`0.2`
- 最差平均目标值：`-0.022234`
- 分桶数：`2`

### 6. sell_market_score_threshold

- 重要性分数：`0.0348`
- 最优取值：`38`
- 最优平均目标值：`0.012567`
- 最差取值：`34`
- 最差平均目标值：`-0.022234`
- 分桶数：`2`

### 7. buy_amount_min

- 重要性分数：`0.017737`
- 最优取值：`200000000.0`
- 最优平均目标值：`0.006879`
- 最差取值：`250000000.0`
- 最差平均目标值：`-0.010858`
- 分桶数：`2`

### 8. sell_market_drop_threshold

- 重要性分数：`0.017737`
- 最优取值：`-3.5`
- 最优平均目标值：`0.006879`
- 最差取值：`-4.0`
- 最差平均目标值：`-0.010858`
- 分桶数：`2`
