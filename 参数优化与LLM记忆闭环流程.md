# 参数优化与 LLM 记忆闭环流程

## 1. 目标

这套流程的目标是：

- 先用程序化回测优化器批量搜索参数组合
- 再用大模型对结果做结构化复盘
- 把多轮复盘中的“稳定参数”沉淀到本地记忆
- 将 LLM 建议和本地记忆一起并入下一轮配置
- 逐轮缩小搜索空间，提升收益率与风险收益比

这是一条“程序搜索为主，LLM 辅助解释和缩圈”的闭环，而不是让大模型直接代替回测引擎。

## 2. 整体闭环

```mermaid
flowchart TD
    A["回测参数配置"] --> B["参数优化器<br/>粗搜/细搜/多阶段搜索"]
    B --> C["优化结果产物<br/>csv/json/md"]
    C --> D["LLM 复盘"]
    D --> E["结构化建议<br/>fixed / narrow / relax"]
    E --> F["本地记忆更新<br/>stable params"]
    F --> G["生成下一轮配置"]
    G --> B
```

## 3. 主要文件

### 参数优化器

- [optimizer.sample.json](/D:/codex/stock_analysis/optimizer.sample.json)
  - 参数优化示例配置
- [optimize_backtest.py](/D:/codex/stock_analysis/scripts/optimize_backtest.py)
  - 参数优化入口脚本
- [optimizer.py](/D:/codex/stock_analysis/stock_analysis/optimizer.py)
  - 优化主流程
- [optimizer_space.py](/D:/codex/stock_analysis/stock_analysis/optimizer_space.py)
  - 参数采样、网格和多阶段空间生成
- [optimizer_report.py](/D:/codex/stock_analysis/stock_analysis/optimizer_report.py)
  - 结果导出和 Markdown 报告

### LLM 复盘与记忆

- [optimizer_llm.py](/D:/codex/stock_analysis/stock_analysis/optimizer_llm.py)
  - LLM 复盘主逻辑
- [review_optimizer_with_llm.py](/D:/codex/stock_analysis/scripts/review_optimizer_with_llm.py)
  - LLM 复盘入口脚本
- [optimizer_llm_prompt.md](/D:/codex/stock_analysis/optimizer_llm_prompt.md)
  - LLM 提示词模板

### 输出目录

默认优化输出目录通常是：

- [data/optimizer](/D:/codex/stock_analysis/data/optimizer)

常见产物包括：

- `backtest_trials.csv`
- `best_params.json`
- `importance.json`
- `optimizer_report.md`
- `next_round_config.json`
- `optimizer_llm_review.json`
- `optimizer_llm_review.md`
- `optimizer_llm_memory.json`
- `optimizer_llm_memory.md`
- `optimizer_llm_next_round_config.json`

## 4. 第一步：运行参数优化器

### 作用

先由程序化优化器跑一轮参数搜索，得到最优组合、重要性统计和下一轮基础配置。

### 推荐命令

```powershell
python D:\codex\stock_analysis\scripts\optimize_backtest.py D:\codex\stock_analysis\optimizer.sample.json
```

### 输入

- 基础回测配置
- 参数范围
- 搜索方法
- 约束条件
- 优化目标

### 输出

- 回测明细结果
- 最优参数组合
- 参数重要性统计
- 自动生成的下一轮配置

## 5. 第二步：运行 LLM 复盘

### 作用

让大模型基于本轮优化结果做结构化分析，不直接替代回测，而是做：

- 结果解释
- 风险提示
- 参数固定建议
- 参数缩圈建议
- 下一轮搜索重点建议

### 推荐命令

```powershell
python D:\codex\stock_analysis\scripts\review_optimizer_with_llm.py D:\codex\stock_analysis\data\optimizer --top-n 5
```

### 读取内容

- `best_params.json`
- `importance.json`
- `next_round_config.json`
- `optimizer_report.md`
- 本地记忆 `optimizer_llm_memory.json`

### 输出内容

- `optimizer_llm_review.json`
- `optimizer_llm_review.md`
- `optimizer_llm_next_round_config.json`

## 6. 第三步：本地记忆如何工作

### 记忆文件

- [optimizer_llm_memory.json](/D:/codex/stock_analysis/data/optimizer/optimizer_llm_memory.json)
- [optimizer_llm_memory.md](/D:/codex/stock_analysis/data/optimizer/optimizer_llm_memory.md)

### 记忆内容

每次 LLM 复盘后，系统会记录：

- 本轮总结
- 最优 trial / run
- 固定参数
- 收窄参数
- 放宽参数
- 重要参数

### 稳定参数识别逻辑

系统会从多轮历史条目中统计：

- `fixed_count`
- `narrow_count`
- `important_count`

并计算：

- `stability_score`
- `recommended_action`
- `consensus_fixed_value`

### 当前稳定参数行为

稳定参数不再只是备注，而是真正参与两层逻辑：

1. 提示词权重
- LLM 会被明确要求优先保留稳定参数
- 如果要改稳定参数，必须给出反证理由

2. 下一轮配置合并
- 如果当前 review 没主动调整某个稳定参数
- 系统会自动施加记忆偏置
- `fix` 型稳定参数会自动固定
- `narrow` 型稳定参数会自动收紧搜索范围

## 7. 第四步：如何生成下一轮配置

下一轮配置来源于三层叠加：

1. 优化器自动生成的基础 `next_round_config.json`
2. LLM review 的建议
   - `fixed_params`
   - `narrow_params`
   - `relax_params`
3. 本地记忆中的稳定参数偏置

最终输出：

- [optimizer_llm_next_round_config.json](/D:/codex/stock_analysis/data/optimizer/optimizer_llm_next_round_config.json)

这个文件可以直接作为下一轮优化器输入。

## 8. 第五步：复用已有 review 更新记忆和配置

如果不想再次请求大模型，可以直接复用已有 review：

```powershell
python D:\codex\stock_analysis\scripts\review_optimizer_with_llm.py D:\codex\stock_analysis\data\optimizer --reuse-review
```

这个模式会：

- 读取已有 `optimizer_llm_review.json`
- 重新应用到下一轮配置
- 更新本地记忆

适合在你修改记忆逻辑、稳定参数逻辑后重新生成配置。

## 9. 第六步：继续下一轮优化

使用合并后的下一轮配置继续跑：

```powershell
python D:\codex\stock_analysis\scripts\optimize_backtest.py D:\codex\stock_analysis\data\optimizer\optimizer_llm_next_round_config.json
```

这样就形成了真正的闭环：

- 优化器搜索
- LLM 复盘
- 本地记忆积累
- 下一轮配置缩圈
- 再优化

## 10. 推荐使用节奏

### 第一轮

- 参数范围稍大
- 先粗搜
- 看收益、回撤、交易数

### 第二轮

- 接入 LLM 建议
- 开始缩圈
- 观察稳定参数是否出现

### 第三轮及以后

- 稳定参数逐渐形成
- 优先固定强共识参数
- 把搜索资源集中在不稳定参数

## 11. 当前设计原则

### 1. 程序优先

所有收益、回撤、交易数都来自真实回测，不让 LLM 替代计算。

### 2. LLM 做研究助手

LLM 负责：

- 解释结果
- 提供微调建议
- 识别稳定参数
- 帮助缩小搜索空间

### 3. 本地记忆持续积累

历史复盘不会丢失，会逐步形成：

- 稳定参数
- 共识固定值
- 更可靠的下一轮缩圈建议

## 12. 当前可以直接使用的命令

### 跑一轮优化

```powershell
python D:\codex\stock_analysis\scripts\optimize_backtest.py D:\codex\stock_analysis\optimizer.sample.json
```

### 跑一轮 LLM 复盘

```powershell
python D:\codex\stock_analysis\scripts\review_optimizer_with_llm.py D:\codex\stock_analysis\data\optimizer --top-n 5
```

### 复用已有 review

```powershell
python D:\codex\stock_analysis\scripts\review_optimizer_with_llm.py D:\codex\stock_analysis\data\optimizer --reuse-review
```

### 用 LLM 合并后的配置继续优化

```powershell
python D:\codex\stock_analysis\scripts\optimize_backtest.py D:\codex\stock_analysis\data\optimizer\optimizer_llm_next_round_config.json
```

## 13. 后续可继续增强的方向

- 稳定参数达到连续多轮后，自动从搜索空间移除
- 让 LLM 直接生成“第二轮配置解释报告”
- 多轮优化完成后，生成策略演化时间线
- 把稳定参数和不稳定参数分别做可视化

## 14. 稳定参数判定规则明细

这一节专门说明本地记忆里 `stable_params` 是怎么计算出来的。

### 14.1 数据来源

每次 LLM 复盘完成后，系统会把这轮结果写入一条历史 entry。

每条 entry 里目前会记录：

- `fixed_param_names`
- `fixed_param_values`
- `narrow_param_names`
- `relax_param_names`
- `important_params`

其中：

- `fixed_param_names`
  - 来自 LLM 输出的 `fixed_params[].name`
- `fixed_param_values`
  - 来自 LLM 输出的 `fixed_params[].value`
  - 用于统计某个参数是否反复被固定到同一个值
- `narrow_param_names`
  - 来自 LLM 输出的 `narrow_params[].name`
- `important_params`
  - 来自优化器重要性统计中排名靠前的参数

### 14.2 三个核心计数

对于每个参数，系统会在多轮 entry 里累计三个计数：

#### `fixed_count`

表示这个参数在历史复盘中，被 LLM 明确建议“固定”的次数。

统计规则：

- 每一轮中，如果该参数出现在 `fixed_params` 里
- 则该轮为它的 `fixed_count + 1`

这个计数代表：

- LLM 对这个参数的“确定性”有多高
- 越高说明越倾向于不再搜索它

#### `narrow_count`

表示这个参数在历史复盘中，被 LLM 建议“收窄搜索范围”的次数。

统计规则：

- 每一轮中，如果该参数出现在 `narrow_params` 里
- 则该轮为它的 `narrow_count + 1`

这个计数代表：

- 这个参数虽然未必应该彻底固定
- 但其有效区间在多轮中持续收缩

#### `important_count`

表示这个参数在历史优化中，多次出现在“重要参数”列表里的次数。

统计规则：

- 每一轮中，如果该参数进入该轮 `important_params`
- 则该轮为它的 `important_count + 1`

这个计数代表：

- 该参数对收益、回撤或目标函数确实有较强影响
- 它不是无关参数

### 14.3 固定值共识统计

如果某个参数进入了 `fixed_params`，系统还会记录它的具体固定值：

- `fixed_param_values[name] = value`

随后会统计：

- 这个参数被固定过哪些值
- 每个值分别出现了几次

这一步的作用是得到：

- `consensus_fixed_value`

即：

- 在历史固定建议中，出现次数最多的固定值

这个值用于判断：

- 该参数是否已经形成“固定值共识”

### 14.4 稳定分 `stability_score`

系统当前使用下面的加权公式：

```text
stability_score = fixed_count * 2 + narrow_count * 1.5 + important_count * 1
```

含义是：

- `fixed_count` 权重最高
  - 因为“建议固定”比“建议收窄”更强
- `narrow_count` 次之
  - 因为它体现了有效区间持续收缩
- `important_count` 最低
  - 因为“重要”不一定等于“稳定”

### 14.5 进入稳定参数的最低门槛

当前规则是：

```text
stability_score >= 2
```

满足这个条件，参数就会被写入 `stable_params`。

这意味着下面几类情况都可能进入：

- `fixed_count = 1`
  - 分数 `2.0`
- `narrow_count = 2`
  - 分数 `3.0`
- `important_count = 2`
  - 分数 `2.0`
- 多种计数组合叠加

所以“稳定参数”并不一定意味着“已经完全锁定”，而是表示：

- 它已经在历史中出现了稳定信号

### 14.6 推荐动作 `recommended_action`

每个稳定参数都会进一步给出一个推荐动作。

当前有三种：

#### `keep`

表示：

- 这个参数已有一定稳定性
- 但还不够强，不建议自动固定或强收缩
- 在提示词里会提高优先级，但在下一轮配置里不强制动作

典型情况：

- 只出现过 1 次固定
- 或只被判定为重要，但还未形成足够共识

#### `narrow`

表示：

- 这个参数已经表现出较明显稳定性
- 适合继续收窄搜索范围

触发条件主要包括：

- `fixed_count >= 2`
- 或 `narrow_count >= 2`

在下一轮配置里，这类参数会被自动进一步收紧区间。

#### `fix`

表示：

- 这个参数已经形成较强共识
- 适合直接固定，不再继续大范围搜索

当前触发条件：

- `fixed_count >= 3`
- 且某个固定值至少重复出现 `2` 次以上

也就是说：

- 不仅多轮建议固定
- 而且固定值本身也趋于一致

这时系统会把：

- `consensus_fixed_value`

作为自动固定值，直接写入下一轮配置。

### 14.7 `consensus_fixed_value` 的作用

如果一个参数被多次固定到相同值，比如：

- 第 1 轮固定到 `4`
- 第 2 轮固定到 `4`
- 第 3 轮固定到 `4`

那么：

- `consensus_fixed_value = 4`

在推荐动作为 `fix` 时：

- 下一轮配置会直接写成单值搜索

例如：

```json
{
  "buy_min_core_hits": {
    "type": "int",
    "values": [4]
  }
}
```

### 14.8 稳定参数如何参与提示词

在发送给 LLM 的提示词中，稳定参数不是简单展示，而是带规则说明：

- 稳定参数优先保留
- 如果要修改稳定参数，必须说明反证依据
- 下一轮搜索空间优先收缩其他不稳定参数

同时还会给出每个稳定参数的：

- `stability_score`
- `recommended_action`
- `fixed_count / narrow_count / important_count`
- `consensus_fixed_value`

这样 LLM 看到的不是一句“这个参数很重要”，而是一组可解释的历史证据。

### 14.9 稳定参数如何参与下一轮配置

在合并 `LLM review -> next_round_config` 时，系统会先看这轮 review 是否已经修改了该参数。

#### 情况 A：这轮 review 已经主动改了它

那么以这轮 review 为准。

#### 情况 B：这轮 review 没有改它

系统会按 `recommended_action` 自动施加偏置：

- `fix`
  - 直接固定为 `consensus_fixed_value`
- `narrow`
  - 自动把现有搜索区间收紧
- `keep`
  - 只在提示词层加权，不自动改配置

### 14.10 为什么要这样设计

这样设计的好处是：

- 不会让单轮 LLM 建议完全覆盖历史经验
- 也不会一刀切把历史稳定参数全部锁死
- 形成“本轮结果 + 历史记忆”的折中机制

简单说：

- 程序负责算结果
- LLM 负责解释结果
- 记忆负责保留跨轮共识

### 14.11 当前注意点

目前稳定参数机制已经可用，但还处在第一版：

- 历史轮数较少时，稳定参数还不会很多
- 前几轮更像“建立记忆”
- 当轮数积累后，稳定参数的作用会越来越明显

后续还可以继续增强：

- 给近期轮次更高权重
- 区分“长期稳定”和“近期稳定”
- 对不同参数类型使用不同稳定分公式
- 达到连续多轮强共识后，自动移出搜索空间
