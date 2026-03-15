# A股分析系统

这是一个面向 A 股场景的股票分析系统，包含数据采集、历史缓存、评分计算、任务状态页、结果页、个股详情页和 AI 复盘对话。

系统当前已经支持从真实历史数据生成评分结果，并在前端展示完整的评分拆解、K 线、成交量、板块信息和 AI 复盘建议。

## 当前能力

- 支持 `baostock`、`akshare`、`mock` 三种数据源
- 支持沪深主板全量扫描，自动去掉 `ST`、创业板、科创板
- 个股历史数据和上证指数历史数据支持增量缓存
- 支持后台扫描任务、任务状态页、进度条和缓存明细
- 评分结果页支持分页、筛选、排序
- 结果页支持按股票代码查询单只股票评分详情，不受前 50 条展示限制
- 个股详情页支持：
  - K 线图
  - 成交量
  - `MA5 / MA10 / MA20 / MA30 / MA60`
  - 同板块股票
  - 详细评分拆解
  - AI 多轮对话
  - AI 复盘评分建议并回写系统

## 目录结构

```text
stock_analysis/
├─ app.py
├─ requirements.txt
├─ README.md
├─ .gitignore
├─ data/
├─ static/
│  ├─ styles.css
│  └─ favicon.svg
├─ templates/
│  ├─ index.html
│  ├─ results.html
│  ├─ status.html
│  └─ detail.html
└─ stock_analysis/
   ├─ analyzer.py
   ├─ charts.py
   ├─ chat.py
   ├─ config.py
   ├─ data_source.py
   ├─ db.py
   ├─ sample_data.py
   ├─ service.py
   └─ web.py
```

## 评分逻辑

当前系统拆成 6 条主评分项，总分 100 分：

1. `均线多头`，24 分
2. `放量上涨 + 缩量回调`，20 分
3. `资金流入 + 板块强势`，18 分
4. `低位启动突破`，16 分
5. `突破后未破位`，12 分
6. `大盘共振`，10 分

### 最近的评分逻辑更新

- `均线多头` 已改成 5 条均线的相邻关系评分：
  - `MA5 > MA10`
  - `MA10 > MA20`
  - `MA20 > MA30`
  - `MA30 > MA60`
- 这 4 组关系每组满分 6 分，总分 24 分
- 采用平滑评分，不再是简单命中即满分
- 对最近 5 个交易日做加权滑动平均，减少单日噪音

- `低位启动突破` 已加入：
  - 相对近 120 日低点的位置判断
  - `ATR14` 视角下的相对低位判断
  - 对前 20 日高点的突破确认强度

- `突破后未破位` 已加入：
  - 基于 `ATR` 的动态防守位
  - 突破新鲜度衰减
  - 与“突破成立强度”联动，避免假突破拿高分

### 评分展示

详情页会显示每条规则的：

- 是否命中
- 实际得分
- 满分
- 评分解释

## AI 复盘能力

个股详情页已经集成 AI 对话，支持：

- 多轮上下文
- 快捷问题按钮
- 流式输出
- Markdown 渲染
- 删除历史 / 新建对话

### AI 复盘评分

“判断复盘”会把以下信息提供给模型：

- 当前系统评分标准
- 当前系统评分拆解
- 个股最近交易数据
- 上证指数最近交易数据
- 个股和指数的最新交易日
- 个股关键计算字段

模型会返回两部分：

1. 文字分析
2. 结构化评分提案 JSON

如果模型认为当前数据足够，会生成“可更新评分提案”；
如果模型认为关键字段不足或数据过旧，会返回“建议先刷新数据”的原因说明。

用户确认后，可以把 AI 复盘评分回写到系统中，详情页会显示 `AI 评分` 标记。

## 页面说明

### 首页 `/`

- 启动全量扫描
- 查看最近任务
- 查看缓存统计
- 跳转评分结果页

### 结果页 `/results`

- 按评分、涨跌幅、板块筛选
- 按评分、涨跌幅、现价、代码排序
- 分页浏览
- 按股票代码查询单只个股评分详情

### 状态页 `/status/<task_id>`

- 查看扫描进度
- 查看最近几次任务
- 查看每只股票的缓存模式：
  - `hit`
  - `incremental`
  - `full`

### 详情页 `/stocks/<symbol>`

- K 线 + 成交量
- 多条均线
- 命中信号与评分拆解
- 同板块股票
- AI 对话与 AI 复盘

## 数据缓存

系统默认使用 SQLite 做缓存，主要表包括：

- `price_history`
- `benchmark_history`
- `market_snapshot`
- `analysis_run`
- `analysis_result`
- `analysis_task`
- `analysis_task_item`

### 增量缓存策略

- 个股历史：优先读本地缓存，不足则回补或增量更新
- 上证指数历史：同样支持缓存命中 / 增量更新 / 完整回补
- 首次全量扫描较慢，后续扫描会快很多

## 运行方式

安装依赖：

```bash
pip install -r requirements.txt
```

启动服务：

```bash
python app.py
```

默认访问：

```text
http://127.0.0.1:5000
```

## 环境变量

### 数据源

```bash
set STOCK_PROVIDER=baostock
python app.py
```

### 大模型对话

支持 OpenAI 兼容接口，例如 OpenAI、DeepSeek 等。

```bash
set LLM_API_BASE=https://api.deepseek.com/v1
set LLM_API_KEY=你的Key
set LLM_MODEL=deepseek-chat
python app.py
```

常用变量：

- `LLM_API_BASE`
- `LLM_API_KEY`
- `LLM_MODEL`

## 仓库说明

仓库已经加入 `.gitignore`，默认忽略：

- `data/*.db`
- `__pycache__/`
- `.venv/`
- `.tmp/`

这样可以避免把 SQLite 数据库、缓存文件和虚拟环境继续提交到 Git。

## 建议的下一步

- 给结果页增加导出 CSV
- 给任务页增加预计剩余时间
- 给评分拆解增加可视化进度条
- 增加可配置策略参数页
- 增加历史回测和收益对比
