from __future__ import annotations

from collections.abc import Iterator
from typing import Any
import json

import pandas as pd
import requests

from .config import settings


class StockChatService:
    def chat(self, detail: dict[str, Any], user_message: str, history: list[dict[str, str]] | None = None) -> str:
        if not settings.llm_api_key:
            raise RuntimeError("未配置 LLM_API_KEY，暂时无法使用股票对话功能。")

        prompt = self._build_prompt(detail, user_message, history or [])
        response = requests.post(
            f"{settings.llm_api_base.rstrip('/')}/chat/completions",
            headers={
                "Authorization": f"Bearer {settings.llm_api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": settings.llm_model,
                "messages": prompt,
                "temperature": 0.4,
            },
            timeout=settings.llm_timeout_seconds,
        )
        if not response.ok:
            try:
                error_payload = response.json()
            except Exception:
                error_payload = response.text
            raise RuntimeError(f"模型接口请求失败：HTTP {response.status_code}，返回内容：{error_payload}")

        payload = response.json()
        return payload["choices"][0]["message"]["content"].strip()

    def stream_chat(
        self,
        detail: dict[str, Any],
        user_message: str,
        history: list[dict[str, str]] | None = None,
    ) -> Iterator[str]:
        if not settings.llm_api_key:
            raise RuntimeError("未配置 LLM_API_KEY，暂时无法使用股票对话功能。")

        prompt = self._build_prompt(detail, user_message, history or [])
        with requests.post(
            f"{settings.llm_api_base.rstrip('/')}/chat/completions",
            headers={
                "Authorization": f"Bearer {settings.llm_api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": settings.llm_model,
                "messages": prompt,
                "temperature": 0.4,
                "stream": True,
            },
            timeout=settings.llm_timeout_seconds,
            stream=True,
        ) as response:
            if not response.ok:
                try:
                    error_payload = response.json()
                except Exception:
                    error_payload = response.text
                raise RuntimeError(f"模型接口请求失败：HTTP {response.status_code}，返回内容：{error_payload}")

            for raw_line in response.iter_lines(decode_unicode=True):
                if not raw_line:
                    continue
                line = raw_line.strip()
                if not line.startswith("data:"):
                    continue
                data = line[5:].strip()
                if data == "[DONE]":
                    break
                try:
                    payload = json.loads(data)
                except json.JSONDecodeError:
                    continue
                delta = payload.get("choices", [{}])[0].get("delta", {})
                content = delta.get("content")
                if content:
                    yield content

    @staticmethod
    def _build_prompt(detail: dict[str, Any], user_message: str, history: list[dict[str, str]]) -> list[dict[str, str]]:
        signals = "、".join(detail.get("signals", [])) or "无明显信号"
        sector_members = detail.get("sector_members", [])[:5]
        sector_text = "\n".join(
            f"- {item['symbol']} {item['name']} 现价:{item['latest_price']} 涨跌幅:{item['pct_change']}%"
            for item in sector_members
        ) or "暂无同板块样本"
        kline_history = detail.get("history", [])[-20:]
        history_text = "\n".join(
            f"- {item['trade_date']} O:{item['open']} H:{item['high']} L:{item['low']} C:{item['close']} V:{item['volume']}"
            for item in kline_history
        )
        ma_text = StockChatService._build_ma_summary(detail.get("history", []))
        trend_text = StockChatService._build_trend_summary(detail.get("history", []))
        benchmark_history = detail.get("benchmark_history", [])[-20:]
        benchmark_text = "\n".join(
            f"- {item['trade_date']} O:{item['open']} H:{item['high']} L:{item['low']} C:{item['close']} V:{item['volume']}"
            for item in benchmark_history
        ) or "暂无指数数据"
        benchmark_ma_text = StockChatService._build_ma_summary(detail.get("benchmark_history", []))
        benchmark_trend_text = StockChatService._build_trend_summary(detail.get("benchmark_history", []))
        benchmark_change = StockChatService._build_latest_change(detail.get("benchmark_history", []))
        latest_trade_date = detail.get("history", [])[-1]["trade_date"] if detail.get("history") else "未知"
        benchmark_trade_date = detail.get("benchmark_history", [])[-1]["trade_date"] if detail.get("benchmark_history") else "未知"
        score_breakdown = detail.get("score_breakdown", [])
        score_breakdown_text = "\n".join(
            f"- {item['label']}: {'已命中' if item['matched'] else '未命中'}，当前系统得分 {item['score']}/{item['weight']}"
            for item in score_breakdown
        ) or "暂无拆解数据"
        stock_key_metrics = StockChatService._build_stock_key_metrics(detail.get("history", []))
        benchmark_key_metrics = StockChatService._build_benchmark_key_metrics(detail.get("benchmark_history", []))

        system = (
            "你是股票分析助手。请基于给定股票信息做简洁、结构清晰的中文回答。"
            "回答优先使用以下结构：1. 结论 2. 依据 3. 风险提示 4. 可继续观察点。"
            "不要承诺收益，不要给出绝对化投资建议，可以提示风险和不确定性。"
            "你还要结合本系统的评分算法来判断系统结论是否合理，而不是只做泛泛的技术分析。"
        )
        algorithm_text = """
本系统当前评分算法如下，请把它当作固定前置规则来理解：
1. 均线多头，权重24分：
   使用 5 条均线 MA5 / MA10 / MA20 / MA30 / MA60，比较 4 组相邻关系：
   MA5>MA10、MA10>MA20、MA20>MA30、MA30>MA60。
   每组满分 6 分，总分 24 分。
   系统不是简单二值判断，而是用 (短期均线-长期均线)/价格 的比例做平滑评分：
   当该比例达到约 0.5% 时，该组拿满分；低于阈值按比例给分，低于 0 则记 0 分。
   最终再对最近 5 个交易日做加权滑动平均，得到 0-24 的浮点分。
   因此即使某一天没有形成标准多头，若最近几天均线关系持续改善，也可能保留部分分数。
2. 放量上涨 + 缩量回调，权重20分：
   最近10个交易日里，至少出现一次涨幅大于2%且成交量大于5日均量1.5倍的放量上涨，
   同时也至少出现一次跌幅不超过2%且成交量小于5日均量0.8倍的缩量整理。
3. 资金流入 + 板块强势，权重18分：
   使用资金流向指标替代主力净流入占比：
   - CMF(21) > 0.05 可视为资金净流入较强；
   - MFI(14) > 60 代表资金买盘压力偏强，> 70 可视为更强确认；
   同时要求板块涨幅强于大盘，并且板块内上涨家数占比 > 60%。
4. 低位启动突破，权重16分：
   当前收盘价距离近120日最低收盘价不宜过远，同时也会结合 ATR 评估是否仍处于相对低位。
   在此基础上，再判断收盘价是否对前20日高点形成明确突破。
   仅仅接近前20日高点可以拿到少量平滑分，但如果没有实际突破，不应给高分；
   一旦已经明确站上前20日高点，应显著提高该项得分。
5. 突破后未破位，权重12分：
   这一项必须建立在“已经形成有效突破”的前提上；如果突破本身不成立，这一项应接近 0 分。
   防守位不是固定 98%，而是结合 ATR 动态设定，以适应不同波动率股票。
   同时还会考虑“突破新鲜度”：突破越新，守位信号越有效；突破时间过久，这一项权重会自然衰减。
6. 大盘共振，权重10分：
   大盘收盘站上 MA20，且 MA20 相比前一日继续上行。

补充说明：
- 上面 6 个主条件合计 100 分。
- “板块走强”属于辅助观察信号，会展示在结果里，但不再额外加分，避免总分超过 100。
- 本系统现在采用平滑评分，不是“命中就满分、未命中就零分”。
- 每一项都会根据接近程度给 0 到该项权重之间的连续分值；只有达到较强阈值时，才会被标记为“命中信号”。

请在回答里明确区分：
- 系统命中了哪些条件
- 哪些条件虽然未命中，但是否接近命中
- 系统当前判断是偏合理、部分合理还是明显偏乐观/偏保守

如果用户在做“判断复盘”或要求你重算评分，请在正文分析后额外输出一个 ```json 代码块，格式固定为：
{
  "action": "propose_review_update" 或 "needs_refresh",
  "data_sufficient": true,
  "effective_trade_date": "YYYY-MM-DD",
  "benchmark_trade_date": "YYYY-MM-DD",
  "score": 0-100 的数字,
  "summary": "一句话总结",
  "signals": ["命中信号1", "命中信号2"],
  "score_breakdown": [
    {"label": "均线多头", "matched": true, "weight": 24, "score": 24, "comment": "简短理由"}
  ],
  "refresh_reason": ""
}

要求：
- 如果当前数据足以分析，就返回 action=propose_review_update。
- 如果你认为数据过旧、缺关键字段或不足以下结论，就返回 action=needs_refresh，并说明 refresh_reason。
- effective_trade_date 必须使用当前给你的最新交易日，不要臆造更新日期。
- score_breakdown 必须严格沿用本系统的 6 条评分项和权重，总分按你自己的判断重算。
- score_breakdown 里的 score 可以是小数，应该体现平滑度，而不是简单地只给 0 或满分。
""".strip()
        context = f"""
股票代码: {detail['symbol']}
股票名称: {detail['name']}
评分: {detail['score']}
评分来源: {detail.get('score_source', 'system')}
现价: {detail['latest_price']}
涨跌幅: {detail['pct_change']}%
板块: {detail['sector']}
命中信号: {signals}
摘要: {detail['summary']}
当前系统评分拆解:
{score_breakdown_text}
个股关键计算字段:
{stock_key_metrics}
大盘关键计算字段:
{benchmark_key_metrics}
个股数据最新交易日: {latest_trade_date}
大盘数据最新交易日: {benchmark_trade_date}
均线概况: {ma_text}
趋势概况: {trend_text}
大盘指数: 上证指数(000001)
大盘涨跌幅: {benchmark_change}
大盘均线概况: {benchmark_ma_text}
大盘趋势概况: {benchmark_trend_text}

最近20个交易日:
{history_text}

上证指数最近20个交易日:
{benchmark_text}

同板块样本:
{sector_text}
""".strip()

        messages: list[dict[str, str]] = [
            {"role": "system", "content": system},
            {"role": "system", "content": algorithm_text},
            {"role": "system", "content": context},
        ]
        for item in history[-8:]:
            role = item.get("role", "")
            content = item.get("content", "").strip()
            if role in {"user", "assistant"} and content:
                messages.append({"role": role, "content": content})
        messages.append({"role": "user", "content": user_message})
        return messages

    @staticmethod
    def _build_ma_summary(history: list[dict[str, Any]]) -> str:
        if not history:
            return "暂无数据"
        df = pd.DataFrame(history).copy()
        if df.empty or "close" not in df.columns:
            return "暂无数据"
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.dropna(subset=["close"])
        if df.empty:
            return "暂无数据"
        values = []
        for window in [5, 10, 20, 30]:
            series = df["close"].rolling(window).mean().dropna()
            values.append(f"MA{window}:{series.iloc[-1]:.2f}" if not series.empty else f"MA{window}:--")
        return " ".join(values)

    @staticmethod
    def _build_trend_summary(history: list[dict[str, Any]]) -> str:
        if not history:
            return "暂无趋势数据"
        df = pd.DataFrame(history).copy()
        for column in ["close", "volume"]:
            df[column] = pd.to_numeric(df[column], errors="coerce")
        df = df.dropna(subset=["close"])
        if len(df) < 20:
            return "历史数据不足，无法完整判断趋势"
        latest_close = df["close"].iloc[-1]
        ma5 = df["close"].rolling(5).mean().iloc[-1]
        ma20 = df["close"].rolling(20).mean().iloc[-1]
        volume_ratio = df["volume"].iloc[-1] / max(df["volume"].rolling(5).mean().iloc[-1], 1)
        trend = "偏强" if latest_close > ma5 > ma20 else "偏弱"
        return f"收盘位于MA5/MA20关系: {trend}; 最新量比(近5日): {volume_ratio:.2f}"

    @staticmethod
    def _build_latest_change(history: list[dict[str, Any]]) -> str:
        if not history or len(history) < 2:
            return "暂无数据"
        df = pd.DataFrame(history).copy()
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.dropna(subset=["close"])
        if len(df) < 2:
            return "暂无数据"
        change = df["close"].pct_change().iloc[-1] * 100
        return f"{change:.2f}%"

    @staticmethod
    def _build_stock_key_metrics(history: list[dict[str, Any]]) -> str:
        if not history:
            return "暂无数据"
        df = pd.DataFrame(history).copy()
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df["high"] = pd.to_numeric(df["high"], errors="coerce")
        df["low"] = pd.to_numeric(df["low"], errors="coerce")
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = df.dropna(subset=["close", "high", "low", "volume"]).sort_values("trade_date").reset_index(drop=True)
        if df.empty:
            return "暂无数据"
        for window in [20, 30, 60]:
            df[f"ma{window}"] = df["close"].rolling(window).mean()
        df["cmf21"] = StockChatService._compute_cmf(df, window=21)
        df["mfi14"] = StockChatService._compute_mfi(df, window=14)
        latest = df.iloc[-1]
        low_120 = df["close"].tail(120).min() if len(df) >= 1 else None
        prior_20_high = df["high"].rolling(20).max().shift(1).iloc[-1] if len(df) >= 21 else None
        return (
            f"- 最新收盘价: {StockChatService._fmt_number(latest['close'])}\n"
            f"- MA20: {StockChatService._fmt_number(latest['ma20'])}\n"
            f"- MA30: {StockChatService._fmt_number(latest['ma30'])}\n"
            f"- MA60: {StockChatService._fmt_number(latest['ma60'])}\n"
            f"- CMF(21): {StockChatService._fmt_percent(latest['cmf21'])}\n"
            f"- MFI(14): {StockChatService._fmt_number(latest['mfi14'])}\n"
            f"- 近120日最低收盘价: {StockChatService._fmt_number(low_120)}\n"
            f"- 前20日高点(不含当日): {StockChatService._fmt_number(prior_20_high)}"
        )

    @staticmethod
    def _build_benchmark_key_metrics(history: list[dict[str, Any]]) -> str:
        if not history:
            return "暂无数据"
        df = pd.DataFrame(history).copy()
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df["trade_date"] = pd.to_datetime(df["trade_date"])
        df = df.dropna(subset=["close"]).sort_values("trade_date").reset_index(drop=True)
        if len(df) < 2:
            return "暂无数据"
        df["ma20"] = df["close"].rolling(20).mean()
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        latest_ma20 = latest["ma20"] if pd.notna(latest["ma20"]) else None
        prev_ma20 = prev["ma20"] if pd.notna(prev["ma20"]) else None
        return (
            f"- 最新收盘价: {StockChatService._fmt_number(latest['close'])}\n"
            f"- 最新 MA20: {StockChatService._fmt_number(latest_ma20)}\n"
            f"- 前一日 MA20: {StockChatService._fmt_number(prev_ma20)}"
        )

    @staticmethod
    def _fmt_number(value: Any) -> str:
        if value is None or pd.isna(value):
            return "暂无数据"
        return f"{float(value):.2f}"

    @staticmethod
    def _fmt_percent(value: Any) -> str:
        if value is None or pd.isna(value):
            return "暂无数据"
        return f"{float(value):.2%}"

    @staticmethod
    def _compute_cmf(df: pd.DataFrame, window: int = 21) -> pd.Series:
        price_span = (df["high"] - df["low"]).replace(0, pd.NA)
        multiplier = ((df["close"] - df["low"]) - (df["high"] - df["close"])) / price_span
        multiplier = multiplier.fillna(0.0)
        money_flow_volume = multiplier * df["volume"]
        volume_sum = df["volume"].rolling(window).sum().replace(0, pd.NA)
        return (money_flow_volume.rolling(window).sum() / volume_sum).fillna(0.0)

    @staticmethod
    def _compute_mfi(df: pd.DataFrame, window: int = 14) -> pd.Series:
        typical_price = (df["high"] + df["low"] + df["close"]) / 3
        raw_money_flow = typical_price * df["volume"]
        price_delta = typical_price.diff()
        positive_flow = raw_money_flow.where(price_delta > 0, 0.0)
        negative_flow = raw_money_flow.where(price_delta < 0, 0.0).abs()
        positive_sum = positive_flow.rolling(window).sum()
        negative_sum = negative_flow.rolling(window).sum()
        money_ratio = positive_sum / negative_sum.replace(0, pd.NA)
        mfi = 100 - (100 / (1 + money_ratio))
        return mfi.fillna(50.0)
