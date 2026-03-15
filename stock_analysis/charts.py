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
