# 参数优化 LLM 记忆

- 更新时间：`2026-03-21 23:52:11`
- 稳定参数总数：`1`，其中 `fix=0`，`narrow=0`

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

- buy_min_core_hits: 稳定分=2.0，建议动作=keep，fixed=1，narrow=0，important=0

## 历史条目

- 2026-03-21 22:19:33 run_id=106 summary=本轮优化中最佳试验获得14.53%总收益和12.69%超额收益，最大回撤5.46%，表现良好但基于10个有限样本。
