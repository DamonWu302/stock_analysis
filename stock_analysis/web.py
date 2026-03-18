from __future__ import annotations

from datetime import datetime
from pathlib import Path

from flask import Flask, Response, jsonify, redirect, render_template, request, stream_with_context, url_for
import json
import pandas as pd

from .charts import build_candlestick_svg
from .chat import StockChatService
from .config import settings
from .service import StockAnalysisService


def create_app() -> Flask:
    base_dir = Path(__file__).resolve().parent.parent
    app = Flask(
        __name__,
        template_folder=str(base_dir / "templates"),
        static_folder=str(base_dir / "static"),
    )
    service = StockAnalysisService()
    chat_service = StockChatService()

    @app.template_filter("fmt_dt")
    def format_datetime(value):
        if value in (None, "", "-"):
            return "-"
        if value == "已完成":
            return value
        try:
            return datetime.fromisoformat(str(value)).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            return str(value)

    @app.get("/")
    def index():
        return render_template(
            "index.html",
            data=service.latest_results(),
            task=service.latest_task(),
            recent_tasks=service.recent_tasks(),
            defaults=settings,
            error=request.args.get("error"),
        )

    @app.get("/results")
    def results():
        data = service.latest_results()
        if not data:
            return redirect(url_for("index", error="还没有评分结果，请先执行一次扫描"))

        page = max(int(request.args.get("page", "1")), 1)
        per_page = 20
        min_score = float(request.args.get("min_score", "0") or 0)
        min_pct_change = float(request.args.get("min_pct_change", "-100") or -100)
        sector = request.args.get("sector", "").strip()
        sort_by = request.args.get("sort_by", "score")
        sort_dir = request.args.get("sort_dir", "desc")
        query_symbol = request.args.get("query_symbol", "").strip()

        filtered_results = list(data["results"])
        if min_score > 0:
            filtered_results = [row for row in filtered_results if float(row["score"]) >= min_score]
        if min_pct_change > -100:
            filtered_results = [row for row in filtered_results if float(row["pct_change"]) >= min_pct_change]
        if sector:
            filtered_results = [row for row in filtered_results if row["sector"] == sector]

        sort_fields = {
            "score": "score",
            "pct_change": "pct_change",
            "latest_price": "latest_price",
            "symbol": "symbol",
        }
        sort_key = sort_fields.get(sort_by, "score")
        reverse = sort_dir != "asc"
        filtered_results.sort(key=lambda row: row[sort_key], reverse=reverse)

        total_items = len(filtered_results)
        total_pages = max((total_items + per_page - 1) // per_page, 1)
        page = min(page, total_pages)
        start = (page - 1) * per_page
        end = start + per_page

        paged = dict(data)
        paged["results"] = filtered_results[start:end]
        paged["page"] = page
        paged["per_page"] = per_page
        paged["total_items"] = total_items
        paged["total_pages"] = total_pages
        paged["has_prev"] = page > 1
        paged["has_next"] = page < total_pages
        paged["prev_page"] = page - 1
        paged["next_page"] = page + 1
        paged["filters"] = {
            "min_score": min_score,
            "min_pct_change": min_pct_change,
            "sector": sector,
            "sort_by": sort_by,
            "sort_dir": sort_dir,
            "query_symbol": query_symbol,
        }
        paged["sector_options"] = sorted({row["sector"] for row in data["results"] if row["sector"]})
        paged["page_numbers"] = _build_page_numbers(page, total_pages)
        paged["searched_stock"] = service.lookup_stock_score(query_symbol) if query_symbol else None
        return render_template("results.html", data=paged)

    @app.get("/api/backtest/config")
    def backtest_config():
        return jsonify(service.backtest_config_schema())

    @app.get("/api/backtest/runs")
    def backtest_runs():
        limit = max(int(request.args.get("limit", "20")), 1)
        return jsonify({"runs": service.recent_backtests(limit=limit)})

    @app.get("/api/backtest/runs/<int:run_id>")
    def backtest_run_detail(run_id: int):
        detail = service.backtest_detail(run_id)
        if not detail:
            return jsonify({"error": f"未找到回测 {run_id}"}), 404
        return jsonify(detail)

    @app.post("/api/backtest/runs")
    def create_backtest_run():
        payload = request.get_json(silent=True) or {}
        if not isinstance(payload, dict):
            return jsonify({"error": "回测配置格式不正确"}), 400
        try:
            result = service.run_backtest(payload)
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500
        return jsonify(result)

    @app.post("/api/daily/backfill")
    def backfill_daily_tables():
        payload = request.get_json(silent=True) or {}
        days = int(payload.get("days", 120) or 120)
        try:
            result = service.backfill_daily_tables(days=days)
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500
        return jsonify(result)

    @app.post("/analyze")
    def analyze():
        provider = request.form.get("provider", settings.default_provider)
        try:
            task_id = service.start_background_run(provider_name=provider, limit=0)
            return redirect(url_for("task_status", task_id=task_id))
        except Exception as exc:
            return redirect(url_for("index", error=str(exc)))

    @app.get("/status")
    def latest_status():
        task = service.latest_task()
        if not task:
            return redirect(url_for("index", error="还没有任务记录"))
        return redirect(url_for("task_status", task_id=task["id"]))

    @app.get("/status/<int:task_id>")
    def task_status(task_id: int):
        task = service.task_detail(task_id)
        if not task:
            return redirect(url_for("index", error=f"未找到任务 {task_id}"))
        return render_template("status.html", task=task, recent_tasks=service.recent_tasks())

    @app.get("/stocks/<symbol>")
    def stock_detail(symbol: str):
        detail = service.stock_detail(symbol)
        if not detail:
            return redirect(url_for("index", error=f"未找到股票 {symbol} 的分析结果"))

        return_to = request.args.get("return_to", "results")
        page = request.args.get("page", "1")
        min_score = request.args.get("min_score", "0")
        min_pct_change = request.args.get("min_pct_change", "-100")
        sector = request.args.get("sector", "")
        sort_by = request.args.get("sort_by", "score")
        sort_dir = request.args.get("sort_dir", "desc")

        if return_to == "results":
            back_url = url_for(
                "results",
                page=page,
                min_score=min_score,
                min_pct_change=min_pct_change,
                sector=sector,
                sort_by=sort_by,
                sort_dir=sort_dir,
            )
            back_label = f"返回评分结果第 {page} 页"
        else:
            back_url = url_for("index")
            back_label = "返回首页"

        detail["chart_svg"] = build_candlestick_svg(pd.DataFrame(detail["history"]))
        detail["chat_enabled"] = bool(settings.llm_api_key)
        return render_template("detail.html", detail=detail, back_url=back_url, back_label=back_label)

    @app.post("/api/stocks/<symbol>/chat")
    def stock_chat(symbol: str):
        payload = request.get_json(silent=True) or {}
        message = str(payload.get("message", "")).strip()
        history = payload.get("history", [])
        if not message:
            return jsonify({"error": "请输入问题"}), 400

        detail = service.stock_detail(symbol)
        if not detail:
            return jsonify({"error": f"未找到股票 {symbol} 的分析结果"}), 404

        try:
            reply = chat_service.chat(detail, message, history=history if isinstance(history, list) else [])
            return jsonify({"reply": reply})
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    @app.post("/api/stocks/<symbol>/chat/stream")
    def stock_chat_stream(symbol: str):
        payload = request.get_json(silent=True) or {}
        message = str(payload.get("message", "")).strip()
        history = payload.get("history", [])
        if not message:
            return jsonify({"error": "请输入问题"}), 400

        detail = service.stock_detail(symbol)
        if not detail:
            return jsonify({"error": f"未找到股票 {symbol} 的分析结果"}), 404

        def generate():
            chunks: list[str] = []
            try:
                for part in chat_service.stream_chat(detail, message, history=history if isinstance(history, list) else []):
                    chunks.append(part)
                    yield json.dumps({"type": "chunk", "content": part}, ensure_ascii=False) + "\n"
                yield json.dumps({"type": "done", "content": "".join(chunks)}, ensure_ascii=False) + "\n"
            except Exception as exc:
                yield json.dumps({"type": "error", "error": str(exc)}, ensure_ascii=False) + "\n"

        return Response(stream_with_context(generate()), mimetype="application/x-ndjson")

    @app.post("/api/stocks/<symbol>/apply-review")
    def apply_stock_review(symbol: str):
        payload = request.get_json(silent=True) or {}
        proposal = payload.get("proposal", {})
        if not isinstance(proposal, dict):
            return jsonify({"error": "评分提案格式不正确"}), 400

        required_fields = {"score", "summary", "signals", "score_breakdown"}
        if not required_fields.issubset(proposal.keys()):
            return jsonify({"error": "评分提案缺少必要字段"}), 400

        try:
            updated = service.apply_review_score(symbol, proposal)
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500
        if not updated:
            return jsonify({"error": f"未找到股票 {symbol} 的分析结果"}), 404
        return jsonify({
            "message": "AI 评分已更新到系统",
            "score": updated["score"],
            "score_source": updated.get("score_source", "ai"),
        })

    return app


def _build_page_numbers(page: int, total_pages: int) -> list[int]:
    start = max(page - 2, 1)
    end = min(page + 2, total_pages)
    if end - start < 4:
        if start == 1:
            end = min(5, total_pages)
        elif end == total_pages:
            start = max(total_pages - 4, 1)
    return list(range(start, end + 1))
