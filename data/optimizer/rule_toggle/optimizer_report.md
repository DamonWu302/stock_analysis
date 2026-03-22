# 参数优化报告：backtest-rule-toggle-optimizer

- 生成时间：`2026-03-22 01:22:51`
- 搜索阶段数：`1`
- 结果数量：`12`

## 约束条件

- 最少交易数：`5`
- 最大回撤上限：`0.25`
- 最低胜率：`0.25`

## 前十结果

### 1. Trial #14

- 所属阶段：`rule-toggle`
- run_id：`169`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`0.14056`
- 超额收益：`0.12211`
- 最大回撤：`0.067876`
- 胜率：`0.384615`
- 交易次数：`26`
- 参数：`{"enable_buy_momentum": true, "enable_sell_trim": false, "enable_sell_time_stop": false, "enable_sell_market_weak_drop": true, "enable_sell_break_ma5": false}`

### 2. Trial #16

- 所属阶段：`rule-toggle`
- run_id：`171`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`0.133112`
- 超额收益：`0.114662`
- 最大回撤：`0.072734`
- 胜率：`0.416667`
- 交易次数：`25`
- 参数：`{"enable_buy_momentum": true, "enable_sell_trim": false, "enable_sell_time_stop": false, "enable_sell_market_weak_drop": false, "enable_sell_break_ma5": false}`

### 3. Trial #9

- 所属阶段：`rule-toggle`
- run_id：`164`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`0.130355`
- 超额收益：`0.111905`
- 最大回撤：`0.065856`
- 胜率：`0.333333`
- 交易次数：`30`
- 参数：`{"enable_buy_momentum": true, "enable_sell_trim": false, "enable_sell_time_stop": true, "enable_sell_market_weak_drop": true, "enable_sell_break_ma5": true}`

### 4. Trial #13

- 所属阶段：`rule-toggle`
- run_id：`168`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`0.130355`
- 超额收益：`0.111905`
- 最大回撤：`0.065856`
- 胜率：`0.333333`
- 交易次数：`30`
- 参数：`{"enable_buy_momentum": true, "enable_sell_trim": false, "enable_sell_time_stop": false, "enable_sell_market_weak_drop": true, "enable_sell_break_ma5": true}`

### 5. Trial #11

- 所属阶段：`rule-toggle`
- run_id：`166`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`0.129301`
- 超额收益：`0.110851`
- 最大回撤：`0.066727`
- 胜率：`0.333333`
- 交易次数：`30`
- 参数：`{"enable_buy_momentum": true, "enable_sell_trim": false, "enable_sell_time_stop": true, "enable_sell_market_weak_drop": false, "enable_sell_break_ma5": true}`

### 6. Trial #15

- 所属阶段：`rule-toggle`
- run_id：`170`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`0.128669`
- 超额收益：`0.110219`
- 最大回撤：`0.06725`
- 胜率：`0.333333`
- 交易次数：`30`
- 参数：`{"enable_buy_momentum": true, "enable_sell_trim": false, "enable_sell_time_stop": false, "enable_sell_market_weak_drop": false, "enable_sell_break_ma5": true}`

### 7. Trial #10

- 所属阶段：`rule-toggle`
- run_id：`165`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`0.117151`
- 超额收益：`0.098701`
- 最大回撤：`0.065996`
- 胜率：`0.357143`
- 交易次数：`28`
- 参数：`{"enable_buy_momentum": true, "enable_sell_trim": false, "enable_sell_time_stop": true, "enable_sell_market_weak_drop": true, "enable_sell_break_ma5": false}`

### 8. Trial #12

- 所属阶段：`rule-toggle`
- run_id：`167`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`0.11612`
- 超额收益：`0.09767`
- 最大回撤：`0.066857`
- 胜率：`0.357143`
- 交易次数：`28`
- 参数：`{"enable_buy_momentum": true, "enable_sell_trim": false, "enable_sell_time_stop": true, "enable_sell_market_weak_drop": false, "enable_sell_break_ma5": false}`

### 9. Trial #1

- 所属阶段：`rule-toggle`
- run_id：`156`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`0.101516`
- 超额收益：`0.083066`
- 最大回撤：`0.06617`
- 胜率：`0.375`
- 交易次数：`31`
- 参数：`{"enable_buy_momentum": true, "enable_sell_trim": true, "enable_sell_time_stop": true, "enable_sell_market_weak_drop": true, "enable_sell_break_ma5": true}`

### 10. Trial #5

- 所属阶段：`rule-toggle`
- run_id：`160`
- 状态：`completed`
- 是否通过约束：`True`
- 总收益：`0.101516`
- 超额收益：`0.083066`
- 最大回撤：`0.06617`
- 胜率：`0.375`
- 交易次数：`31`
- 参数：`{"enable_buy_momentum": true, "enable_sell_trim": true, "enable_sell_time_stop": false, "enable_sell_market_weak_drop": true, "enable_sell_break_ma5": true}`

## 参数重要性统计

- 目标字段：`total_return`
- 统计参数数：`5`

### 1. enable_buy_momentum

- 重要性分数：`0.1071`
- 最优取值：`True`
- 最优平均目标值：`0.108504`
- 最差取值：`False`
- 最差平均目标值：`0.001403`
- 分桶数：`2`

### 2. enable_sell_trim

- 重要性分数：`0.019699`
- 最优取值：`False`
- 最优平均目标值：`0.064803`
- 最差取值：`True`
- 最差平均目标值：`0.045104`
- 分桶数：`2`

### 3. enable_sell_break_ma5

- 重要性分数：`0.013444`
- 最优取值：`False`
- 最优平均目标值：`0.061676`
- 最差取值：`True`
- 最差平均目标值：`0.048232`
- 分桶数：`2`

### 4. enable_sell_market_weak_drop

- 重要性分数：`0.002787`
- 最优取值：`True`
- 最优平均目标值：`0.056347`
- 最差取值：`False`
- 最差平均目标值：`0.05356`
- 分桶数：`2`

### 5. enable_sell_time_stop

- 重要性分数：`0.001875`
- 最优取值：`True`
- 最优平均目标值：`0.055891`
- 最差取值：`False`
- 最差平均目标值：`0.054016`
- 分桶数：`2`
