# 参数优化 LLM 记忆

- 更新时间：`2026-03-22 00:52:48`
- 稳定参数总数：`6`，其中 `fix=0`，`narrow=3`

## 判定规则

- `fixed_count`：历史复盘中被建议固定的次数
- `narrow_count`：历史复盘中被建议收窄范围的次数
- `important_count`：历史优化中被识别为重要参数的次数
- `stability_score = fixed_count * 2 + narrow_count * 1.5 + important_count * 1`
- `stability_score >= 2` 时进入稳定参数集合
- `recommended_action = keep / narrow / fix`
- 当 `fixed_count >= 3` 且固定值出现明确共识时，优先使用 `fix`
- 当 `fixed_count >= 2` 或 `narrow_count >= 2` 时，优先使用 `narrow`

## 稳定参数

- buy_min_core_hits: 稳定分=4.0，建议动作=narrow，fixed=2，narrow=0，important=0，固定值=4
- market_score_filter_min_avg: 稳定分=4.0，建议动作=narrow，fixed=0，narrow=2，important=1
- buy_strict_score_total: 稳定分=4.0，建议动作=narrow，fixed=0，narrow=2，important=1
- buy_momentum_score_total: 稳定分=2.5，建议动作=keep，fixed=0，narrow=1，important=1
- sell_market_score_threshold: 稳定分=2.5，建议动作=keep，fixed=0，narrow=1，important=1
- market_score_filter_min_ma5: 稳定分=2.5，建议动作=keep，fixed=0，narrow=1，important=1

## 历史条目

- 2026-03-21 22:19:33 run_id=106 summary=本轮优化中最佳试验获得14.53%总收益和12.69%超额收益，最大回撤5.46%，表现良好但基于10个有限样本。
- 2026-03-22 00:52:48 run_id=127 summary=本轮优化最佳试验总收益13.99%，超额收益12.14%，最大回撤7.58%，表现良好但基于有限样本需谨慎。
