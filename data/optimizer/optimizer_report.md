# 参数优化报告：backtest-optimizer-v2

- 生成时间：`2026-03-22 01:14:34`
- 搜索阶段数：`2`
- 结果数量：`5`

## 约束条件

- 最少交易数：`5`
- 最大回撤上限：`0.25`
- 最低胜率：`0.25`

## 前十结果

### 1. Trial #7

- 所属阶段：`coarse`
- run_id：`147`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`0.014026`
- 超额收益：`-0.004424`
- 最大回撤：`0.035273`
- 胜率：`0.5`
- 交易次数：`12`
- 参数：`{"market_score_filter_min_avg": 38, "market_score_filter_min_ma5": 44, "enable_buy_momentum": false, "enable_sell_time_stop": true, "buy_strict_score_total": 70, "buy_momentum_score_total": 58, "buy_min_core_hits": 1, "buy_amount_min": 400000000.0, "max_single_position": 0.25, "sell_market_score_threshold": 38, "sell_market_drop_threshold": -3.5}`

### 2. Trial #14

- 所属阶段：`refine`
- run_id：`152`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`-0.002244`
- 超额收益：`-0.020694`
- 最大回撤：`0.049467`
- 胜率：`0.333333`
- 交易次数：`18`
- 参数：`{"market_score_filter_min_avg": 39, "market_score_filter_min_ma5": 44, "enable_buy_momentum": true, "enable_sell_time_stop": false, "buy_strict_score_total": 72, "buy_momentum_score_total": 58, "buy_min_core_hits": 2, "buy_amount_min": 400000000.0, "max_single_position": 0.3, "sell_market_score_threshold": 37, "sell_market_drop_threshold": -4.0}`

### 3. Trial #15

- 所属阶段：`refine`
- run_id：`153`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`-0.003639`
- 超额收益：`-0.022089`
- 最大回撤：`0.033295`
- 胜率：`0.444444`
- 交易次数：`18`
- 参数：`{"market_score_filter_min_avg": 46, "market_score_filter_min_ma5": 44, "enable_buy_momentum": true, "enable_sell_time_stop": true, "buy_strict_score_total": 64, "buy_momentum_score_total": 62, "buy_min_core_hits": 2, "buy_amount_min": 250000000.0, "max_single_position": 0.2, "sell_market_score_threshold": 31, "sell_market_drop_threshold": -4.0}`

### 4. Trial #11

- 所属阶段：`coarse`
- run_id：`149`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`-0.009702`
- 超额收益：`-0.028152`
- 最大回撤：`0.062223`
- 胜率：`0.333333`
- 交易次数：`18`
- 参数：`{"market_score_filter_min_avg": 45, "market_score_filter_min_ma5": 44, "enable_buy_momentum": true, "enable_sell_time_stop": true, "buy_strict_score_total": 66, "buy_momentum_score_total": 64, "buy_min_core_hits": 1, "buy_amount_min": 200000000.0, "max_single_position": 0.2, "sell_market_score_threshold": 32, "sell_market_drop_threshold": -3.5}`

### 5. Trial #1

- 所属阶段：`coarse`
- run_id：`142`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`-0.010154`
- 超额收益：`-0.028604`
- 最大回撤：`0.109893`
- 胜率：`0.357143`
- 交易次数：`54`
- 参数：`{"market_score_filter_min_avg": 36, "market_score_filter_min_ma5": 33, "enable_buy_momentum": false, "enable_sell_time_stop": true, "buy_strict_score_total": 56, "buy_momentum_score_total": 54, "buy_min_core_hits": 1, "buy_amount_min": 400000000.0, "max_single_position": 0.2, "sell_market_score_threshold": 39, "sell_market_drop_threshold": -2.5}`

## 参数重要性统计

- 目标字段：`total_return`
- 统计参数数：`11`

### 1. buy_strict_score_total

- 重要性分数：`0.047568`
- 最优取值：`70`
- 最优平均目标值：`0.014026`
- 最差取值：`76`
- 最差平均目标值：`-0.033542`
- 分桶数：`10`

### 2. market_score_filter_min_avg

- 重要性分数：`0.040555`
- 最优取值：`38`
- 最优平均目标值：`0.007013`
- 最差取值：`40`
- 最差平均目标值：`-0.033542`
- 分桶数：`7`

### 3. buy_momentum_score_total

- 重要性分数：`0.035066`
- 最优取值：`58`
- 最优平均目标值：`0.003927`
- 最差取值：`52`
- 最差平均目标值：`-0.031138`
- 分桶数：`8`

### 4. market_score_filter_min_ma5

- 重要性分数：`0.033542`
- 最优取值：`40`
- 最优平均目标值：`0.0`
- 最差取值：`42`
- 最差平均目标值：`-0.033542`
- 分桶数：`8`

### 5. max_single_position

- 重要性分数：`0.031674`
- 最优取值：`0.25`
- 最优平均目标值：`-0.001868`
- 最差取值：`0.35`
- 最差平均目标值：`-0.033542`
- 分桶数：`4`

### 6. buy_min_core_hits

- 重要性分数：`0.031599`
- 最优取值：`1`
- 最优平均目标值：`-0.001943`
- 最差取值：`4`
- 最差平均目标值：`-0.033542`
- 分桶数：`5`

### 7. sell_market_drop_threshold

- 重要性分数：`0.027667`
- 最优取值：`-3.5`
- 最优平均目标值：`0.001081`
- 最差取值：`-3.0`
- 最差平均目标值：`-0.026586`
- 分桶数：`4`

### 8. sell_market_score_threshold

- 重要性分数：`0.021117`
- 最优取值：`34`
- 最优平均目标值：`0.0`
- 最差取值：`40`
- 最差平均目标值：`-0.021117`
- 分桶数：`8`

### 9. buy_amount_min

- 重要性分数：`0.018285`
- 最优取值：`400000000.0`
- 最优平均目标值：`0.000543`
- 最差取值：`300000000.0`
- 最差平均目标值：`-0.017742`
- 分桶数：`5`

### 10. enable_buy_momentum

- 重要性分数：`0.015446`
- 最优取值：`False`
- 最优平均目标值：`-0.000803`
- 最差取值：`True`
- 最差平均目标值：`-0.016249`
- 分桶数：`2`
