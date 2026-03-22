# 参数优化报告：backtest-optimizer-v2

- 生成时间：`2026-03-22 01:08:51`
- 搜索阶段数：`1`
- 结果数量：`4`

## 约束条件

- 最少交易数：`5`
- 最大回撤上限：`0.25`
- 最低胜率：`0.25`

## 前十结果

### 1. Trial #3

- 所属阶段：`toggle-smoke`
- run_id：`140`
- 状态：`completed`
- 是否通过约束：`False`
- 总收益：`-0.013383`
- 超额收益：`-0.031833`
- 最大回撤：`0.013537`
- 胜率：`0.0`
- 交易次数：`2`
- 参数：`{"enable_buy_momentum": false, "enable_sell_time_stop": true}`

### 2. Trial #4

- 所属阶段：`toggle-smoke`
- run_id：`141`
- 状态：`completed`
- 是否通过约束：`False`
- 总收益：`-0.013383`
- 超额收益：`-0.031833`
- 最大回撤：`0.013537`
- 胜率：`0.0`
- 交易次数：`2`
- 参数：`{"enable_buy_momentum": false, "enable_sell_time_stop": false}`

### 3. Trial #1

- 所属阶段：`toggle-smoke`
- run_id：`138`
- 状态：`completed`
- 是否通过约束：`False`
- 总收益：`-0.045142`
- 超额收益：`-0.063592`
- 最大回撤：`0.079058`
- 胜率：`0.230769`
- 交易次数：`26`
- 参数：`{"enable_buy_momentum": true, "enable_sell_time_stop": true}`

### 4. Trial #2

- 所属阶段：`toggle-smoke`
- run_id：`139`
- 状态：`completed`
- 是否通过约束：`False`
- 总收益：`-0.045142`
- 超额收益：`-0.063592`
- 最大回撤：`0.079058`
- 胜率：`0.230769`
- 交易次数：`26`
- 参数：`{"enable_buy_momentum": true, "enable_sell_time_stop": false}`

## 参数重要性统计

- 目标字段：`total_return`
- 统计参数数：`2`

### 1. enable_buy_momentum

- 重要性分数：`0.031759`
- 最优取值：`False`
- 最优平均目标值：`-0.013383`
- 最差取值：`True`
- 最差平均目标值：`-0.045142`
- 分桶数：`2`

### 2. enable_sell_time_stop

- 重要性分数：`0.0`
- 最优取值：`True`
- 最优平均目标值：`-0.029263`
- 最差取值：`True`
- 最差平均目标值：`-0.029263`
- 分桶数：`2`
