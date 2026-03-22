from __future__ import annotations

import html

import pandas as pd


def build_candlestick_svg(history: pd.DataFrame, width: int = 900, height: int = 360) -> str:
    df = history.copy().tail(60).reset_index(drop=True)
    for column in ["open", "close", "high", "low", "volume"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df = df.dropna(subset=["open", "close", "high", "low"])
    if df.empty:
        return "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 900 360'><text x='24' y='48'>No chart data</text></svg>"

    df["pct_change"] = df["close"].pct_change().fillna(0) * 100
    for window in [5, 10, 20, 30, 60]:
        df[f"ma{window}"] = df["close"].rolling(window).mean()

    padding = 28
    chart_width = width - padding * 2
    chart_height = height - padding * 2
    volume_height = chart_height * 0.24
    volume_top = height - padding - volume_height
    price_height = chart_height - volume_height - 22
    price_top = padding + 10
    min_price = float(df[["low", "ma5", "ma10", "ma20", "ma30", "ma60"]].min(numeric_only=True).min())
    max_price = float(df[["high", "ma5", "ma10", "ma20", "ma30", "ma60"]].max(numeric_only=True).max())
    price_range = max(max_price - min_price, 0.01)
    max_volume = max(float(df["volume"].max()) if "volume" in df.columns else 0.0, 1.0)
    step_x = chart_width / max(len(df), 1)
    candle_width = max(step_x * 0.58, 3)

    def y_scale(price: float) -> float:
        return price_top + (max_price - price) / price_range * price_height

    def volume_y_scale(volume: float) -> float:
        return volume_top + volume_height - (volume / max_volume) * volume_height

    parts = [
        f"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 {width} {height}' role='img' aria-label='K线图'>",
        """<style>
        .candle-tip{opacity:0;pointer-events:none;transition:opacity .15s ease}
        .candle-group:hover .candle-tip{opacity:1}
        .candle-tip-box{fill:rgba(31,41,51,.92);stroke:rgba(255,255,255,.16);stroke-width:1}
        .candle-tip-text{fill:#fff;font-size:11px}
        </style>""",
        "<rect width='100%' height='100%' fill='#fffaf4' rx='20' />",
        f"<text x='{padding}' y='20' fill='#6b7280' font-size='12'>最近60个交易日 K 线</text>",
        _build_ma_label(df, "ma5", "MA5", "#b45309", padding + 180, 20),
        _build_ma_label(df, "ma10", "MA10", "#2563eb", padding + 275, 20),
        _build_ma_label(df, "ma20", "MA20", "#7c3aed", padding + 380, 20),
        _build_ma_label(df, "ma30", "MA30", "#dc2626", padding + 490, 20),
        _build_ma_label(df, "ma60", "MA60", "#0f766e", padding + 600, 20),
        f"<line x1='{padding}' x2='{width - padding}' y1='{volume_top - 10:.2f}' y2='{volume_top - 10:.2f}' stroke='rgba(31,41,51,.08)' stroke-width='1' />",
        f"<text x='{padding}' y='{volume_top - 14:.2f}' fill='#6b7280' font-size='12'>成交量</text>",
    ]

    for index, row in df.iterrows():
        x = padding + index * step_x + step_x / 2
        open_price = float(row["open"])
        close_price = float(row["close"])
        high_price = float(row["high"])
        low_price = float(row["low"])
        pct_change = float(row["pct_change"])
        volume = float(row.get("volume", 0) or 0)
        trade_date = str(row.get("trade_date", ""))
        color = "#b91c1c" if close_price >= open_price else "#0f766e"
        wick_top = y_scale(high_price)
        wick_bottom = y_scale(low_price)
        body_top = y_scale(max(open_price, close_price))
        body_bottom = y_scale(min(open_price, close_price))
        body_height = max(body_bottom - body_top, 1.2)
        volume_top_y = volume_y_scale(volume)
        volume_bar_height = max(volume_top + volume_height - volume_top_y, 1.2)
        tip_x = min(max(x - 70, 10), width - 150)
        tip_y = max(wick_top - 66, 28)
        parts.append("<g class='candle-group'>")
        parts.append(f"<line x1='{x:.2f}' x2='{x:.2f}' y1='{wick_top:.2f}' y2='{wick_bottom:.2f}' stroke='{color}' stroke-width='1.4' />")
        parts.append(
            f"<rect x='{x - candle_width / 2:.2f}' y='{body_top:.2f}' width='{candle_width:.2f}' "
            f"height='{body_height:.2f}' fill='{color}' rx='1.5' />"
        )
        parts.append(
            f"<rect x='{x - candle_width / 2:.2f}' y='{volume_top_y:.2f}' width='{candle_width:.2f}' "
            f"height='{volume_bar_height:.2f}' fill='{color}' opacity='0.45' rx='1.5' />"
        )
        parts.append(f"<g class='candle-tip' transform='translate({tip_x:.2f},{tip_y:.2f})'>")
        parts.append("<rect class='candle-tip-box' width='140' height='68' rx='10' />")
        parts.append(f"<text class='candle-tip-text' x='10' y='16'>{html.escape(trade_date)}</text>")
        parts.append(f"<text class='candle-tip-text' x='10' y='31'>O {open_price:.2f} H {high_price:.2f}</text>")
        parts.append(f"<text class='candle-tip-text' x='10' y='46'>L {low_price:.2f} C {close_price:.2f} {pct_change:.2f}%</text>")
        parts.append(f"<text class='candle-tip-text' x='10' y='61'>V {volume:,.0f}</text>")
        parts.append("</g>")
        parts.append("</g>")

    parts.append(_build_ma_path(df, "ma5", "#b45309", padding, step_x, y_scale))
    parts.append(_build_ma_path(df, "ma10", "#2563eb", padding, step_x, y_scale))
    parts.append(_build_ma_path(df, "ma20", "#7c3aed", padding, step_x, y_scale))
    parts.append(_build_ma_path(df, "ma30", "#dc2626", padding, step_x, y_scale))
    parts.append(_build_ma_path(df, "ma60", "#0f766e", padding, step_x, y_scale))

    last_close = float(df.iloc[-1]["close"])
    parts.append(
        f"<text x='{width - 170}' y='20' fill='#6b7280' font-size='12'>收盘: {html.escape(f'{last_close:.2f}')}</text>"
    )
    parts.append("</svg>")
    return "".join(parts)


def build_score_trend_svg(rows: list[dict], width: int = 900, height: int = 280) -> str:
    if not rows:
        return "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 900 280'><text x='24' y='48'>No chart data</text></svg>"

    df = pd.DataFrame(rows).copy()
    df["avg_score"] = pd.to_numeric(df["avg_score"], errors="coerce")
    df["ma5_avg_score"] = pd.to_numeric(df["ma5_avg_score"], errors="coerce")
    df = df.dropna(subset=["avg_score"]).reset_index(drop=True)
    if df.empty:
        return "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 900 280'><text x='24' y='48'>No chart data</text></svg>"

    padding = 28
    chart_width = width - padding * 2
    chart_height = height - padding * 2
    min_value = float(df[["avg_score", "ma5_avg_score"]].min(numeric_only=True).min())
    max_value = float(df[["avg_score", "ma5_avg_score"]].max(numeric_only=True).max())
    value_range = max(max_value - min_value, 0.01)
    step_x = chart_width / max(len(df) - 1, 1)

    def y_scale(value: float) -> float:
        return padding + (max_value - value) / value_range * chart_height

    def build_path(column: str, color: str) -> str:
        points: list[str] = []
        for index, value in enumerate(df[column]):
            if pd.isna(value):
                continue
            x = padding + index * step_x
            y = y_scale(float(value))
            points.append(f"{x:.2f},{y:.2f}")
        if len(points) < 2:
            return ""
        return f"<polyline fill='none' stroke='{color}' stroke-width='2.2' points='{' '.join(points)}' />"

    grid_lines: list[str] = []
    for step in range(5):
        y = padding + chart_height * step / 4
        value = max_value - (value_range * step / 4)
        grid_lines.append(
            f"<line x1='{padding}' x2='{width - padding}' y1='{y:.2f}' y2='{y:.2f}' stroke='rgba(31,41,51,.08)' stroke-width='1' />"
        )
        grid_lines.append(
            f"<text x='{padding - 2}' y='{y - 6:.2f}' fill='#6b7280' font-size='11' text-anchor='end'>{value:.1f}</text>"
        )

    labels: list[str] = []
    for index, row in enumerate(df.to_dict("records")):
        if index not in {0, len(df) // 2, len(df) - 1}:
            continue
        x = padding + index * step_x
        labels.append(
            f"<text x='{x:.2f}' y='{height - 8}' fill='#6b7280' font-size='11' text-anchor='middle'>{html.escape(str(row['trade_date'])[5:])}</text>"
        )

    holding_bands: list[str] = []
    band_start_index: int | None = None
    for index, row in enumerate(df.to_dict("records")):
        is_holding = int(row.get("position_count") or 0) > 0
        if is_holding and band_start_index is None:
            band_start_index = index
        is_band_end = band_start_index is not None and (not is_holding or index == len(df) - 1)
        if not is_band_end:
            continue
        end_index = index if is_holding and index == len(df) - 1 else index - 1
        if end_index >= band_start_index:
            start_x = padding + band_start_index * step_x
            end_x = padding + end_index * step_x
            band_width = max(end_x - start_x + max(step_x, 6), 6)
            holding_bands.append(
                f"<rect x='{start_x:.2f}' y='{padding:.2f}' width='{band_width:.2f}' height='{chart_height:.2f}' "
                "fill='rgba(180, 83, 9, 0.08)' rx='8' />"
            )
        band_start_index = None

    latest_avg = float(df.iloc[-1]["avg_score"])
    latest_ma5 = float(df.iloc[-1]["ma5_avg_score"]) if not pd.isna(df.iloc[-1]["ma5_avg_score"]) else latest_avg
    parts = [
        f"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 {width} {height}' role='img' aria-label='每日平均分走势'>",
        "<rect width='100%' height='100%' fill='#fffaf4' rx='20' />",
        f"<text x='{padding}' y='20' fill='#6b7280' font-size='12'>最近 {len(df)} 个交易日每日平均分走势</text>",
        f"<text x='{padding + 240}' y='20' fill='#b45309' font-size='12'>平均分 {latest_avg:.2f}</text>",
        f"<text x='{padding + 360}' y='20' fill='#0f766e' font-size='12'>5日均线 {latest_ma5:.2f}</text>",
        *grid_lines,
        build_path("avg_score", "#b45309"),
        build_path("ma5_avg_score", "#0f766e"),
        *labels,
        "</svg>",
    ]
    return "".join(parts)


def build_strategy_vs_benchmark_svg(rows: list[dict], width: int = 860, height: int = 240) -> str:
    if not rows:
        return "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 860 240'><text x='24' y='48'>No chart data</text></svg>"

    df = pd.DataFrame(rows).copy()
    df["strategy_return"] = pd.to_numeric(df["strategy_return"], errors="coerce").fillna(0.0)
    df["benchmark_return"] = pd.to_numeric(df["benchmark_return"], errors="coerce").fillna(0.0)
    df["excess_return"] = df["strategy_return"] - df["benchmark_return"]
    df = df.reset_index(drop=True)
    if df.empty:
        return "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 860 240'><text x='24' y='48'>No chart data</text></svg>"

    padding = 28
    chart_width = width - padding * 2
    chart_height = height - padding * 2
    min_value = float(df[["strategy_return", "benchmark_return", "excess_return"]].min(numeric_only=True).min())
    max_value = float(df[["strategy_return", "benchmark_return", "excess_return"]].max(numeric_only=True).max())
    min_value = min(min_value, 0.0)
    max_value = max(max_value, 0.0)
    value_range = max(max_value - min_value, 0.01)
    step_x = chart_width / max(len(df) - 1, 1)

    def y_scale(value: float) -> float:
        return padding + (max_value - value) / value_range * chart_height

    def build_path(column: str, color: str, dash: str = "") -> str:
        points: list[str] = []
        for index, value in enumerate(df[column]):
            x = padding + index * step_x
            y = y_scale(float(value))
            points.append(f"{x:.2f},{y:.2f}")
        dash_attr = f" stroke-dasharray='{dash}'" if dash else ""
        return (
            f"<polyline fill='none' stroke='{color}' stroke-width='3' stroke-linecap='round' "
            f"stroke-linejoin='round'{dash_attr} points='{' '.join(points)}' />"
        )

    parts = [
        f"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 {width} {height}' role='img' aria-label='策略收益对比图'>",
        "<rect width='100%' height='100%' fill='#fffaf4' rx='20' />",
        f"<text x='{padding}' y='20' fill='#6b7280' font-size='12'>策略收益 vs 基准收益 vs 超额收益</text>",
    ]

    zero_y = y_scale(0.0)
    parts.append(
        f"<line x1='{padding}' x2='{width - padding}' y1='{zero_y:.2f}' y2='{zero_y:.2f}' stroke='rgba(31,41,51,.12)' stroke-width='1' />"
    )

    for index, row in df.iterrows():
        x = padding + index * step_x
        label = html.escape(str(row.get("trade_date") or ""))
        parts.append(f"<circle cx='{x:.2f}' cy='{y_scale(float(row['strategy_return'])):.2f}' r='3.5' fill='#ea580c' />")
        parts.append(f"<circle cx='{x:.2f}' cy='{y_scale(float(row['benchmark_return'])):.2f}' r='3' fill='#2563eb' />")
        parts.append(f"<circle cx='{x:.2f}' cy='{y_scale(float(row['excess_return'])):.2f}' r='2.8' fill='#0f766e' />")
        if index in {0, len(df) - 1}:
            parts.append(f"<text x='{x:.2f}' y='{height - 10}' text-anchor='middle' fill='#6b7280' font-size='11'>{label}</text>")

    parts.append(build_path("strategy_return", "#ea580c"))
    parts.append(build_path("benchmark_return", "#2563eb", "6 5"))
    parts.append(build_path("excess_return", "#0f766e", "3 4"))
    parts.append(f"<text x='{padding + 170}' y='20' fill='#ea580c' font-size='12'>策略</text>")
    parts.append(f"<text x='{padding + 220}' y='20' fill='#2563eb' font-size='12'>基准</text>")
    parts.append(f"<text x='{padding + 270}' y='20' fill='#0f766e' font-size='12'>超额</text>")
    parts.append("</svg>")
    return "".join(parts)


def build_nav_svg(rows: list[dict], trades: list[dict] | None = None, width: int = 900, height: int = 280) -> str:
    if not rows:
        return "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 900 280'><text x='24' y='48'>No chart data</text></svg>"

    df = pd.DataFrame(rows).copy()
    df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
    if "benchmark_nav" in df.columns:
        df["benchmark_nav"] = pd.to_numeric(df["benchmark_nav"], errors="coerce")
    df["drawdown"] = pd.to_numeric(df.get("drawdown"), errors="coerce")
    df = df.dropna(subset=["nav"]).reset_index(drop=True)
    if df.empty:
        return "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 900 280'><text x='24' y='48'>No chart data</text></svg>"

    padding = 28
    chart_width = width - padding * 2
    chart_height = height - padding * 2
    min_value = float(df[["nav", "benchmark_nav"]].min(numeric_only=True).min()) if "benchmark_nav" in df.columns else float(df["nav"].min())
    max_value = float(df[["nav", "benchmark_nav"]].max(numeric_only=True).max()) if "benchmark_nav" in df.columns else float(df["nav"].max())
    value_range = max(max_value - min_value, 0.01)
    step_x = chart_width / max(len(df) - 1, 1)

    def y_scale(value: float) -> float:
        return padding + (max_value - value) / value_range * chart_height

    points: list[str] = []
    for index, value in enumerate(df["nav"]):
        x = padding + index * step_x
        y = y_scale(float(value))
        points.append(f"{x:.2f},{y:.2f}")

    benchmark_points: list[str] = []
    excess_points: list[str] = []
    if "benchmark_nav" in df.columns:
        base_nav = float(df.iloc[0]["nav"])
        for index, value in enumerate(df["benchmark_nav"]):
            if pd.isna(value):
                continue
            x = padding + index * step_x
            y = y_scale(float(value))
            benchmark_points.append(f"{x:.2f},{y:.2f}")
            excess_value = float(df.iloc[index]["nav"]) - float(value) + base_nav
            excess_y = y_scale(excess_value)
            excess_points.append(f"{x:.2f},{excess_y:.2f}")

    records = df.to_dict("records")

    grid_lines: list[str] = []
    for step in range(5):
        y = padding + chart_height * step / 4
        value = max_value - (value_range * step / 4)
        grid_lines.append(
            f"<line x1='{padding}' x2='{width - padding}' y1='{y:.2f}' y2='{y:.2f}' stroke='rgba(31,41,51,.08)' stroke-width='1' />"
        )
        grid_lines.append(
            f"<text x='{padding - 2}' y='{y - 6:.2f}' fill='#6b7280' font-size='11' text-anchor='end'>{value:,.0f}</text>"
        )

    labels: list[str] = []
    for index, row in enumerate(records):
        if index not in {0, len(df) // 2, len(df) - 1}:
            continue
        x = padding + index * step_x
        labels.append(
            f"<text x='{x:.2f}' y='{height - 8}' fill='#6b7280' font-size='11' text-anchor='middle'>{html.escape(str(row['trade_date'])[5:])}</text>"
        )

    holding_bands: list[str] = []
    band_start_index: int | None = None
    for index, row in enumerate(records):
        is_holding = int(row.get("position_count") or 0) > 0
        if is_holding and band_start_index is None:
            band_start_index = index
        is_band_end = band_start_index is not None and (not is_holding or index == len(df) - 1)
        if not is_band_end:
            continue
        end_index = index if is_holding and index == len(df) - 1 else index - 1
        if end_index >= band_start_index:
            start_x = padding + band_start_index * step_x
            end_x = padding + end_index * step_x
            band_width = max(end_x - start_x + max(step_x, 6), 6)
            holding_bands.append(
                f"<rect x='{start_x:.2f}' y='{padding:.2f}' width='{band_width:.2f}' height='{chart_height:.2f}' "
                "fill='rgba(180, 83, 9, 0.08)' rx='8' />"
            )
        band_start_index = None

    latest_nav = float(df.iloc[-1]["nav"])
    latest_drawdown = float(df.iloc[-1]["drawdown"]) if "drawdown" in df.columns and not pd.isna(df.iloc[-1]["drawdown"]) else 0.0
    latest_benchmark = (
        float(df.iloc[-1]["benchmark_nav"])
        if "benchmark_nav" in df.columns and not pd.isna(df.iloc[-1]["benchmark_nav"])
        else None
    )
    latest_excess = latest_nav - latest_benchmark if latest_benchmark is not None else None
    date_to_trades: dict[str, list[dict]] = {}
    if trades:
        for trade in trades:
            trade_date = str(trade.get("execution_date") or "")
            if not trade_date:
                continue
            date_to_trades.setdefault(trade_date, []).append(trade)
    trade_markers: list[str] = []
    if trades:
        date_to_index = {str(row["trade_date"]): index for index, row in enumerate(records)}
        buy_y = height - 28
        sell_y = height - 14
        for trade in trades:
            trade_date = str(trade.get("execution_date") or "")
            index = date_to_index.get(trade_date)
            if index is None:
                continue
            x = padding + index * step_x
            side = str(trade.get("side") or "")
            if side == "buy":
                trade_markers.append(
                    f"<circle cx='{x:.2f}' cy='{buy_y:.2f}' r='4' fill='#b91c1c' />"
                    f"<text x='{x:.2f}' y='{buy_y - 8:.2f}' fill='#b91c1c' font-size='10' text-anchor='middle'>B</text>"
                )
            elif side == "sell":
                trade_markers.append(
                    f"<circle cx='{x:.2f}' cy='{sell_y:.2f}' r='4' fill='#0f766e' />"
                    f"<text x='{x:.2f}' y='{sell_y - 8:.2f}' fill='#0f766e' font-size='10' text-anchor='middle'>S</text>"
                )
    hover_layers: list[str] = []
    for index, row in enumerate(records):
        x = padding + index * step_x
        hotspot_x = x - max(step_x / 2, 8)
        hotspot_width = max(step_x, 16)
        nav_value = float(row["nav"])
        benchmark_value = row.get("benchmark_nav")
        benchmark_text = f"{float(benchmark_value):,.2f}" if benchmark_value is not None and not pd.isna(benchmark_value) else "--"
        excess_text = f"{(nav_value - float(benchmark_value)) if benchmark_value is not None and not pd.isna(benchmark_value) else 0:,.2f}"
        holding_text = "持仓中" if int(row.get("position_count") or 0) > 0 else "空仓"
        day_trades = date_to_trades.get(str(row["trade_date"]), [])
        trade_text = " / ".join("买入" if str(item.get("side")) == "buy" else "卖出" for item in day_trades) if day_trades else "无交易"
        tip_width = 172
        tip_height = 88
        tip_x = min(max(x - tip_width / 2, 10), width - tip_width - 10)
        tip_y = max(padding + 6, y_scale(nav_value) - tip_height - 14)
        hover_layers.append(
            "<g class='nav-hotspot'>"
            f"<rect x='{hotspot_x:.2f}' y='{padding:.2f}' width='{hotspot_width:.2f}' height='{chart_height:.2f}' fill='transparent' />"
            f"<line x1='{x:.2f}' x2='{x:.2f}' y1='{padding:.2f}' y2='{padding + chart_height:.2f}' stroke='rgba(180,83,9,.18)' stroke-width='1.2' stroke-dasharray='4 4' class='nav-guide' />"
            f"<circle cx='{x:.2f}' cy='{y_scale(nav_value):.2f}' r='4.5' fill='#b45309' stroke='#fffaf4' stroke-width='1.5' class='nav-guide' />"
            f"<g class='nav-tooltip' transform='translate({tip_x:.2f},{tip_y:.2f})'>"
            f"<rect width='{tip_width}' height='{tip_height}' rx='14' fill='rgba(26,32,44,.94)' stroke='rgba(255,255,255,.12)' stroke-width='1' />"
            f"<text x='12' y='18' fill='#fff7ed' font-size='11' font-weight='700'>{html.escape(str(row['trade_date']))}</text>"
            f"<text x='12' y='35' fill='#fed7aa' font-size='11'>策略净值 {nav_value:,.2f}</text>"
            f"<text x='12' y='50' fill='#bfdbfe' font-size='11'>基准净值 {benchmark_text}</text>"
            f"<text x='12' y='65' fill='#99f6e4' font-size='11'>超额 {excess_text}</text>"
            f"<text x='12' y='80' fill='#e5e7eb' font-size='11'>{holding_text} · {trade_text}</text>"
            "</g>"
            "</g>"
        )
    parts = [
        f"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 {width} {height}' role='img' aria-label='回测净值曲线'>",
        """<style>
        .nav-tooltip,.nav-guide{opacity:0;pointer-events:none;transition:opacity .16s ease}
        .nav-hotspot:hover .nav-tooltip,.nav-hotspot:hover .nav-guide{opacity:1}
        </style>""",
        "<rect width='100%' height='100%' fill='#fffaf4' rx='20' />",
        f"<text x='{padding}' y='20' fill='#6b7280' font-size='12'>回测区间净值 vs 基准</text>",
        f"<text x='{padding + 180}' y='20' fill='#b45309' font-size='12'>策略 {latest_nav:,.2f}</text>",
        (
            f"<text x='{padding + 320}' y='20' fill='#2563eb' font-size='12'>基准 {latest_benchmark:,.2f}</text>"
            if latest_benchmark is not None
            else ""
        ),
        (
            f"<text x='{padding + 470}' y='20' fill='#0f766e' font-size='12'>超额 {latest_excess:,.2f}</text>"
            if latest_excess is not None
            else ""
        ),
        f"<text x='{padding + 620}' y='20' fill='#7c3aed' font-size='12'>回撤 {latest_drawdown * 100:.2f}%</text>",
        f"<text x='{padding + 760}' y='20' fill='#92400e' font-size='12'>阴影=持仓区间</text>",
        *holding_bands,
        *grid_lines,
        f"<polyline fill='none' stroke='#b45309' stroke-width='2.2' points='{' '.join(points)}' />",
        (
            f"<polyline fill='none' stroke='#2563eb' stroke-width='2' stroke-dasharray='6 4' points='{' '.join(benchmark_points)}' />"
            if benchmark_points
            else ""
        ),
        (
            f"<polyline fill='none' stroke='#0f766e' stroke-width='1.8' stroke-dasharray='2 4' points='{' '.join(excess_points)}' />"
            if excess_points
            else ""
        ),
        *hover_layers,
        *labels,
        *trade_markers,
        "</svg>",
    ]
    return "".join(parts)


def _build_ma_path(df: pd.DataFrame, column: str, color: str, padding: int, step_x: float, y_scale) -> str:
    points: list[str] = []
    for index, value in enumerate(df[column]):
        if pd.isna(value):
            continue
        x = padding + index * step_x + step_x / 2
        y = y_scale(float(value))
        points.append(f"{x:.2f},{y:.2f}")
    if len(points) < 2:
        return ""
    return f"<polyline fill='none' stroke='{color}' stroke-width='1.8' points='{' '.join(points)}' />"


def _build_ma_label(df: pd.DataFrame, column: str, label: str, color: str, x: int, y: int) -> str:
    latest = df[column].dropna()
    value = f"{float(latest.iloc[-1]):.2f}" if not latest.empty else "--"
    return f"<text x='{x}' y='{y}' fill='{color}' font-size='12'>{html.escape(label)} {html.escape(value)}</text>"
