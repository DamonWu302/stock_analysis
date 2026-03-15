# A股分析系统

这是一个从零搭建的股票分析系统示例，目标是把 A 股行情获取、策略计算、SQLite 缓存和前端展示串成一个完整流程。

## 功能概览

- 自动获取市场快照与个股历史行情
- 使用多因子规则为股票打分
- 将历史行情、市场快照和分析结果缓存到 SQLite
- 用 Flask 渲染分析看板
- 支持 `mock` 演示数据和 `akshare` 真实数据

## 项目结构

```text
stock_analysis/
├─ app.py
├─ requirements.txt
├─ data/
├─ static/
├─ templates/
└─ stock_analysis/
   ├─ analyzer.py
   ├─ config.py
   ├─ data_source.py
   ├─ db.py
   ├─ sample_data.py
   ├─ service.py
   └─ web.py
```

## 策略逻辑

当前实现按照你提供的方案，拆成 6 类信号：

1. 均线多头排列
2. 放量上涨后出现缩量回调
3. 主力资金流入且板块强于大盘
4. 相对低位启动并突破近 20 日高点
5. 突破后 3 日内没有明显跌破
6. 大盘处于 MA20 上方且继续走强

每个信号都有独立权重，最终累加形成总分，并按得分排序展示。

## 运行方式

先安装依赖：

```bash
pip install -r requirements.txt
```

启动项目：

```bash
python app.py
```

默认访问：

```text
http://127.0.0.1:5000
```

## 数据源切换

页面里可以直接切换数据源：

- `mock`：本地模拟数据，适合先跑通页面和流程
- `akshare`：真实 A 股数据，依赖本地已安装 AKShare 且网络可用

也可以通过环境变量指定默认数据源：

```bash
set STOCK_PROVIDER=akshare
python app.py
```

## 下一步建议

- 把 AKShare 的板块映射与主力资金流接口补完整
- 增加定时任务入口，每日自动跑批
- 增加 K 线图和个股详情页
- 补充回测模块和策略参数配置
