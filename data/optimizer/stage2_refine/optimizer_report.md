# 参数优化报告：backtest-stage2-refine

- 生成时间：`2026-03-22 01:55:17`
- 搜索阶段数：`2`
- 结果数量：`15`

## 约束条件

- 最少交易数：`8`
- 最大回撤上限：`0.2`
- 最低胜率：`0.3`

## 前十结果

### 1. Trial #44

- 所属阶段：`coarse`
- run_id：`231`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`0.189468`
- 超额收益：`0.171018`
- 最大回撤：`0.068947`
- 胜率：`0.428571`
- 交易次数：`28`
- 参数：`{"enable_buy_momentum": true, "enable_sell_trim": false, "enable_sell_break_ma5": false, "enable_sell_market_weak_drop": false, "enable_sell_time_stop": false, "market_score_filter_min_avg": 36, "market_score_filter_min_ma5": 36, "buy_strict_score_total": 76, "buy_momentum_score_total": 68, "buy_min_core_hits": 3, "buy_amount_min": 300000000.0, "max_single_position": 0.3, "sell_drawdown_threshold": 0.04, "sell_drawdown_threshold_mid": 0.05, "sell_time_stop_days": 11, "sell_market_score_threshold": 36, "sell_market_drop_threshold": -4.0}`

### 2. Trial #46

- 所属阶段：`coarse`
- run_id：`233`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`0.186375`
- 超额收益：`0.167925`
- 最大回撤：`0.053092`
- 胜率：`0.833333`
- 交易次数：`24`
- 参数：`{"enable_buy_momentum": true, "enable_sell_trim": false, "enable_sell_break_ma5": false, "enable_sell_market_weak_drop": true, "enable_sell_time_stop": false, "market_score_filter_min_avg": 38, "market_score_filter_min_ma5": 33, "buy_strict_score_total": 74, "buy_momentum_score_total": 68, "buy_min_core_hits": 3, "buy_amount_min": 200000000.0, "max_single_position": 0.25, "sell_drawdown_threshold": 0.04, "sell_drawdown_threshold_mid": 0.08, "sell_time_stop_days": 11, "sell_market_score_threshold": 38, "sell_market_drop_threshold": -2.0}`

### 3. Trial #23

- 所属阶段：`coarse`
- run_id：`210`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`0.134052`
- 超额收益：`0.115602`
- 最大回撤：`0.060825`
- 胜率：`0.428571`
- 交易次数：`28`
- 参数：`{"enable_buy_momentum": true, "enable_sell_trim": false, "enable_sell_break_ma5": false, "enable_sell_market_weak_drop": false, "enable_sell_time_stop": false, "market_score_filter_min_avg": 34, "market_score_filter_min_ma5": 34, "buy_strict_score_total": 72, "buy_momentum_score_total": 68, "buy_min_core_hits": 4, "buy_amount_min": 300000000.0, "max_single_position": 0.3, "sell_drawdown_threshold": 0.04, "sell_drawdown_threshold_mid": 0.07, "sell_time_stop_days": 10, "sell_market_score_threshold": 38, "sell_market_drop_threshold": -2.0}`

### 4. Trial #62

- 所属阶段：`refine`
- run_id：`249`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`0.133112`
- 超额收益：`0.114662`
- 最大回撤：`0.072734`
- 胜率：`0.416667`
- 交易次数：`25`
- 参数：`{"enable_buy_momentum": true, "enable_sell_trim": false, "enable_sell_break_ma5": false, "enable_sell_market_weak_drop": false, "enable_sell_time_stop": false, "market_score_filter_min_avg": 35, "market_score_filter_min_ma5": 35, "buy_strict_score_total": 72, "buy_momentum_score_total": 68, "buy_min_core_hits": 4, "buy_amount_min": 250000000.0, "max_single_position": 0.3, "sell_drawdown_threshold": 0.05, "sell_drawdown_threshold_mid": 0.07, "sell_time_stop_days": 10, "sell_market_score_threshold": 37, "sell_market_drop_threshold": -2.0}`

### 5. Trial #34

- 所属阶段：`coarse`
- run_id：`221`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`0.125542`
- 超额收益：`0.107092`
- 最大回撤：`0.06681`
- 胜率：`0.384615`
- 交易次数：`26`
- 参数：`{"enable_buy_momentum": true, "enable_sell_trim": false, "enable_sell_break_ma5": false, "enable_sell_market_weak_drop": true, "enable_sell_time_stop": false, "market_score_filter_min_avg": 34, "market_score_filter_min_ma5": 36, "buy_strict_score_total": 74, "buy_momentum_score_total": 66, "buy_min_core_hits": 4, "buy_amount_min": 350000000.0, "max_single_position": 0.3, "sell_drawdown_threshold": 0.05, "sell_drawdown_threshold_mid": 0.08, "sell_time_stop_days": 10, "sell_market_score_threshold": 35, "sell_market_drop_threshold": -2.5}`

### 6. Trial #61

- 所属阶段：`refine`
- run_id：`248`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`0.123198`
- 超额收益：`0.104748`
- 最大回撤：`0.073006`
- 胜率：`0.461538`
- 交易次数：`26`
- 参数：`{"enable_buy_momentum": true, "enable_sell_trim": false, "enable_sell_break_ma5": false, "enable_sell_market_weak_drop": true, "enable_sell_time_stop": false, "market_score_filter_min_avg": 34, "market_score_filter_min_ma5": 33, "buy_strict_score_total": 72, "buy_momentum_score_total": 66, "buy_min_core_hits": 4, "buy_amount_min": 350000000.0, "max_single_position": 0.25, "sell_drawdown_threshold": 0.04, "sell_drawdown_threshold_mid": 0.08, "sell_time_stop_days": 10, "sell_market_score_threshold": 37, "sell_market_drop_threshold": -2.5}`

### 7. Trial #56

- 所属阶段：`refine`
- run_id：`243`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`0.100973`
- 超额收益：`0.082523`
- 最大回撤：`0.077414`
- 胜率：`0.583333`
- 交易次数：`24`
- 参数：`{"enable_buy_momentum": true, "enable_sell_trim": false, "enable_sell_break_ma5": false, "enable_sell_market_weak_drop": true, "enable_sell_time_stop": true, "market_score_filter_min_avg": 38, "market_score_filter_min_ma5": 33, "buy_strict_score_total": 74, "buy_momentum_score_total": 68, "buy_min_core_hits": 3, "buy_amount_min": 200000000.0, "max_single_position": 0.3, "sell_drawdown_threshold": 0.05, "sell_drawdown_threshold_mid": 0.07, "sell_time_stop_days": 11, "sell_market_score_threshold": 37, "sell_market_drop_threshold": -2.0}`

### 8. Trial #28

- 所属阶段：`coarse`
- run_id：`215`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`0.095889`
- 超额收益：`0.077439`
- 最大回撤：`0.081079`
- 胜率：`0.411765`
- 交易次数：`34`
- 参数：`{"enable_buy_momentum": true, "enable_sell_trim": false, "enable_sell_break_ma5": false, "enable_sell_market_weak_drop": false, "enable_sell_time_stop": true, "market_score_filter_min_avg": 34, "market_score_filter_min_ma5": 34, "buy_strict_score_total": 72, "buy_momentum_score_total": 66, "buy_min_core_hits": 3, "buy_amount_min": 350000000.0, "max_single_position": 0.35, "sell_drawdown_threshold": 0.04, "sell_drawdown_threshold_mid": 0.08, "sell_time_stop_days": 11, "sell_market_score_threshold": 38, "sell_market_drop_threshold": -2.0}`

### 9. Trial #63

- 所属阶段：`refine`
- run_id：`250`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`0.095019`
- 超额收益：`0.076569`
- 最大回撤：`0.054808`
- 胜率：`0.470588`
- 交易次数：`34`
- 参数：`{"enable_buy_momentum": true, "enable_sell_trim": false, "enable_sell_break_ma5": false, "enable_sell_market_weak_drop": false, "enable_sell_time_stop": false, "market_score_filter_min_avg": 34, "market_score_filter_min_ma5": 34, "buy_strict_score_total": 74, "buy_momentum_score_total": 68, "buy_min_core_hits": 3, "buy_amount_min": 300000000.0, "max_single_position": 0.25, "sell_drawdown_threshold": 0.05, "sell_drawdown_threshold_mid": 0.07, "sell_time_stop_days": 11, "sell_market_score_threshold": 37, "sell_market_drop_threshold": -2.0}`

### 10. Trial #58

- 所属阶段：`refine`
- run_id：`245`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`0.08205`
- 超额收益：`0.0636`
- 最大回撤：`0.075057`
- 胜率：`0.6`
- 交易次数：`20`
- 参数：`{"enable_buy_momentum": true, "enable_sell_trim": false, "enable_sell_break_ma5": false, "enable_sell_market_weak_drop": false, "enable_sell_time_stop": false, "market_score_filter_min_avg": 37, "market_score_filter_min_ma5": 34, "buy_strict_score_total": 72, "buy_momentum_score_total": 68, "buy_min_core_hits": 4, "buy_amount_min": 200000000.0, "max_single_position": 0.3, "sell_drawdown_threshold": 0.05, "sell_drawdown_threshold_mid": 0.08, "sell_time_stop_days": 12, "sell_market_score_threshold": 38, "sell_market_drop_threshold": -2.0}`

## 参数重要性统计

- 目标字段：`total_return`
- 统计参数数：`14`

### 1. buy_momentum_score_total

- 重要性分数：`0.060085`
- 最优取值：`68`
- 最优平均目标值：`0.060956`
- 最差取值：`72`
- 最差平均目标值：`0.000871`
- 分桶数：`4`

### 2. market_score_filter_min_ma5

- 重要性分数：`0.05749`
- 最优取值：`33`
- 最优平均目标值：`0.045413`
- 最差取值：`37`
- 最差平均目标值：`-0.012077`
- 分桶数：`6`

### 3. sell_time_stop_days

- 重要性分数：`0.0544`
- 最优取值：`11`
- 最优平均目标值：`0.048187`
- 最差取值：`8`
- 最差平均目标值：`-0.006213`
- 分桶数：`5`

### 4. sell_market_drop_threshold

- 重要性分数：`0.052234`
- 最优取值：`-2.0`
- 最优平均目标值：`0.049709`
- 最差取值：`-3.5`
- 最差平均目标值：`-0.002525`
- 分桶数：`5`

### 5. market_score_filter_min_avg

- 重要性分数：`0.05204`
- 最优取值：`38`
- 最优平均目标值：`0.060336`
- 最差取值：`33`
- 最差平均目标值：`0.008297`
- 分桶数：`6`

### 6. sell_drawdown_threshold_mid

- 重要性分数：`0.050172`
- 最优取值：`0.07`
- 最优平均目标值：`0.049867`
- 最差取值：`0.06`
- 最差平均目标值：`-0.000305`
- 分桶数：`4`

### 7. sell_drawdown_threshold

- 重要性分数：`0.048047`
- 最优取值：`0.04`
- 最优平均目标值：`0.052105`
- 最差取值：`0.07`
- 最差平均目标值：`0.004058`
- 分桶数：`4`

### 8. buy_amount_min

- 重要性分数：`0.039085`
- 最优取值：`300000000.0`
- 最优平均目标值：`0.055979`
- 最差取值：`350000000.0`
- 最差平均目标值：`0.016894`
- 分桶数：`4`

### 9. max_single_position

- 重要性分数：`0.03746`
- 最优取值：`0.3`
- 最优平均目标值：`0.040647`
- 最差取值：`0.35`
- 最差平均目标值：`0.003187`
- 分桶数：`3`

### 10. sell_market_score_threshold

- 重要性分数：`0.033825`
- 最优取值：`38`
- 最优平均目标值：`0.051103`
- 最差取值：`35`
- 最差平均目标值：`0.017278`
- 分桶数：`6`
