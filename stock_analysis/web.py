from __future__ import annotations

from datetime import datetime
from pathlib import Path

from flask import Flask, Response, jsonify, redirect, render_template, request, stream_with_context, url_for
import json
import pandas as pd

from .charts import build_candlestick_svg, build_nav_svg, build_score_trend_svg, build_strategy_vs_benchmark_svg
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

    def build_backtest_payload(form) -> dict:
        schema_defaults = dict(service.backtest_config_schema().get("defaults") or {})
        raw_template_id = (form.get("template_id") or "").strip()
        template_id = int(raw_template_id) if raw_template_id else 0
        template_config: dict[str, object] = {}
        if template_id:
            for item in service.backtest_templates():
                if int(item["id"]) == template_id:
                    template_config = dict(item.get("config") or {})
                    break
        base = dict(schema_defaults)
        base.update(template_config)

        def parse_int(name: str, default: int | None = None) -> int:
            if default is None:
                default = int(base.get(name, 0) or 0)
            value = (form.get(name) or "").strip()
            if value == "":
                return int(default)
            return int(value)

        def parse_float(name: str, default: float | None = None) -> float:
            if default is None:
                default = float(base.get(name, 0.0) or 0.0)
            value = (form.get(name) or "").strip()
            if value == "":
                return float(default)
            return float(value)

        def parse_bool(name: str, default: bool | None = None) -> bool:
            if default is None:
                default = bool(base.get(name, False))
            values = form.getlist(name)
            if not values:
                return default
            value = values[-1]
            return str(value).strip().lower() in {"1", "true", "on", "yes"}

        def parse_rule_list(name: str) -> list[str]:
            values = [str(item).strip() for item in form.getlist(name) if str(item).strip()]
            if values:
                return values
            base_values = base.get(name) or []
            if isinstance(base_values, list):
                return [str(item).strip() for item in base_values if str(item).strip()]
            return []

        return {
            "template_id": template_id or None,
            "name": (form.get("name") or "").strip() or None,
            "start_date": (form.get("start_date") or "").strip() or None,
            "end_date": (form.get("end_date") or "").strip() or None,
            "lookback_days": parse_int("lookback_days", 120),
            "max_positions": parse_int("max_positions"),
            "max_single_position": parse_float("max_single_position"),
            "initial_capital": parse_float("initial_capital"),
            "fee_rate": parse_float("fee_rate"),
            "slippage_rate": parse_float("slippage_rate"),
            "market_score_filter_min_avg": parse_float("market_score_filter_min_avg"),
            "market_score_filter_min_ma5": parse_float("market_score_filter_min_ma5"),
            "market_require_benchmark_above_ma20": parse_bool("market_require_benchmark_above_ma20"),
            "market_require_benchmark_ma20_up": parse_bool("market_require_benchmark_ma20_up"),
            "buy_strict_score_total": parse_float("buy_strict_score_total"),
            "buy_strict_score_ma_trend": parse_float("buy_strict_score_ma_trend"),
            "buy_strict_score_breakout": parse_float("buy_strict_score_breakout"),
            "buy_strict_score_capital_sector": parse_float("buy_strict_score_capital_sector"),
            "buy_strict_score_volume_pattern": parse_float("buy_strict_score_volume_pattern"),
            "buy_momentum_score_total": parse_float("buy_momentum_score_total"),
            "buy_momentum_score_volume_pattern": parse_float("buy_momentum_score_volume_pattern"),
            "buy_min_core_hits": parse_int("buy_min_core_hits"),
            "buy_low_position_high_ratio_max": parse_float("buy_low_position_high_ratio_max"),
            "buy_20d_gain_max": parse_float("buy_20d_gain_max"),
            "buy_recent_stall_lookback": parse_int("buy_recent_stall_lookback"),
            "buy_recent_stall_pct_max": parse_float("buy_recent_stall_pct_max"),
            "buy_recent_stall_volume_multiple": parse_float("buy_recent_stall_volume_multiple"),
            "buy_risk_amplitude_max": parse_float("buy_risk_amplitude_max"),
            "buy_risk_max_drop_max": parse_float("buy_risk_max_drop_max"),
            "buy_sector_rank_top_pct": parse_float("buy_sector_rank_top_pct"),
            "buy_amount_min": parse_float("buy_amount_min"),
            "sell_trim_profit_threshold": parse_float("sell_trim_profit_threshold"),
            "sell_trim_fraction": parse_float("sell_trim_fraction"),
            "sell_trim_upper_shadow_ratio": parse_float("sell_trim_upper_shadow_ratio"),
            "sell_trim_volume_multiple": parse_float("sell_trim_volume_multiple"),
            "sell_break_ma5_volume_multiple": parse_float("sell_break_ma5_volume_multiple"),
            "sell_drawdown_profit_threshold": parse_float("sell_drawdown_profit_threshold"),
            "sell_drawdown_threshold": parse_float("sell_drawdown_threshold"),
            "sell_drawdown_profit_threshold_mid": parse_float("sell_drawdown_profit_threshold_mid"),
            "sell_drawdown_threshold_mid": parse_float("sell_drawdown_threshold_mid"),
            "sell_drawdown_profit_threshold_high": parse_float("sell_drawdown_profit_threshold_high"),
            "sell_drawdown_threshold_high": parse_float("sell_drawdown_threshold_high"),
            "sell_time_stop_days": parse_int("sell_time_stop_days"),
            "sell_time_stop_return_threshold": parse_float("sell_time_stop_return_threshold"),
            "sell_market_score_threshold": parse_float("sell_market_score_threshold"),
            "sell_market_drop_threshold": parse_float("sell_market_drop_threshold"),
            "enabled_buy_rules": parse_rule_list("enabled_buy_rules"),
            "enabled_sell_rules": parse_rule_list("enabled_sell_rules"),
        }

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
        score_trend = service.score_trend(20)
        return render_template(
            "index.html",
            data=service.latest_results(),
            task=service.latest_task(),
            recent_tasks=service.recent_tasks(),
            backfill_task=service.latest_backfill_task(),
            score_trend=score_trend,
            score_trend_svg=build_score_trend_svg(score_trend),
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

    @app.get("/backtests")
    def backtests():
        runs = service.recent_backtests(limit=20)
        tasks = service.recent_backtest_tasks(limit=10)
        compare_tasks = service.recent_backtest_compare_tasks(limit=10)
        score_trend = service.score_trend(20)
        schema = service.backtest_config_schema()
        templates = service.backtest_templates()
        selected_template_id = request.args.get("template_id", type=int)
        selected_template = None
        if templates:
            selected_template = next((item for item in templates if item["id"] == selected_template_id), templates[0])
            selected_template_id = int(selected_template["id"])
        defaults = dict(schema["defaults"])
        if selected_template:
            selected_config = dict(selected_template.get("config") or {})
            for key, value in selected_config.items():
                defaults[key] = value
        return render_template(
            "backtests.html",
            runs=runs,
            tasks=tasks,
            score_trend=score_trend,
            score_trend_svg=build_score_trend_svg(score_trend),
            schema=schema,
            defaults=defaults,
            templates=templates,
            compare_tasks=compare_tasks,
            selected_template=selected_template,
            selected_template_id=selected_template_id,
            error=request.args.get("error"),
            success=request.args.get("success"),
        )

    @app.get("/strategy")
    def strategy_dashboard():
        template_key = (request.args.get("template") or "return_priority").strip() or "return_priority"
        trade_date = (request.args.get("trade_date") or "").strip() or None
        plan = service.strategy_plan(trade_date=trade_date, template_key=template_key)
        positions = service.strategy_positions(trade_date=trade_date, template_key=template_key)
        performance_rows = service.strategy_performance_series(template_key=template_key, days=30)
        available_dates = service.strategy_plan_dates(template_key=template_key, limit=60)
        templates = service.backtest_templates()
        strategy_templates = [
            item
            for item in templates
            if str(item.get("template_key") or "") in {"return_priority", "steady_default"}
        ]
        sell_actions = []
        trim_actions = []
        hold_actions = []
        if plan:
            sell_actions = [item for item in plan.get("position_actions", []) if str(item.get("action") or "") == "sell"]
            trim_actions = [item for item in plan.get("position_actions", []) if str(item.get("action") or "") == "trim"]
            hold_actions = [item for item in plan.get("position_actions", []) if str(item.get("action") or "") == "hold"]
        return render_template(
            "strategy.html",
            plan=plan,
            positions=positions,
            strategy_templates=strategy_templates,
            selected_template_key=template_key,
            selected_trade_date=(plan or {}).get("trade_date") or trade_date or "",
            available_dates=available_dates,
            buy_candidates=(plan or {}).get("buy_candidates", []),
            sell_actions=sell_actions,
            trim_actions=trim_actions,
            hold_actions=hold_actions,
            performance_rows=performance_rows,
            performance_svg=build_strategy_vs_benchmark_svg(performance_rows),
            success=request.args.get("success"),
            error=request.args.get("error"),
        )

    @app.post("/strategy/generate")
    def generate_strategy_dashboard():
        template_key = (request.form.get("template_key") or "return_priority").strip() or "return_priority"
        trade_date = (request.form.get("trade_date") or "").strip() or None
        try:
            plan = service.generate_strategy_plan(trade_date=trade_date, template_key=template_key)
        except Exception as exc:
            return redirect(url_for("strategy_dashboard", template=template_key, error=str(exc)))
        return redirect(
            url_for(
                "strategy_dashboard",
                template=template_key,
                success=f"已生成 {plan['trade_date']} 的策略执行计划",
            )
        )

    @app.post("/strategy/signals/<int:signal_id>/status")
    def update_strategy_signal(signal_id: int):
        template_key = (request.form.get("template_key") or "return_priority").strip() or "return_priority"
        status = (request.form.get("execution_status") or "").strip().lower()
        note = (request.form.get("execution_note") or "").strip()
        try:
            signal = service.update_strategy_signal_status(signal_id=signal_id, execution_status=status, execution_note=note)
        except Exception as exc:
            return redirect(url_for("strategy_dashboard", template=template_key, error=str(exc)))
        return redirect(
            url_for(
                "strategy_dashboard",
                template=signal.get("template_key") or template_key,
                success=f"{signal['symbol']} 的{signal['signal_type']}信号已更新为{signal['execution_status_label']}",
            )
        )

    @app.get("/backtests/<int:run_id>")
    def backtest_detail_page(run_id: int):
        detail = service.backtest_detail(run_id)
        if not detail:
            return redirect(url_for("backtests"))
        score_trend = service.score_trend(20)
        return render_template(
            "backtest_detail.html",
            detail=detail,
            score_trend=score_trend,
            score_trend_svg=build_score_trend_svg(score_trend),
            nav_svg=build_nav_svg(detail.get("nav") or [], detail.get("trades") or []),
            success=request.args.get("success"),
            error=request.args.get("error"),
            export_path=request.args.get("export_path"),
        )

    @app.post("/backtests/<int:run_id>/rerun")
    def rerun_backtest_form(run_id: int):
        payload = build_backtest_payload(request.form)
        try:
            task_id = service.start_backtest_task(payload)
        except Exception as exc:
            return redirect(url_for("backtest_detail_page", run_id=run_id, error=str(exc)))
        return redirect(url_for("backtest_task_page", task_id=task_id, success="已按当前参数创建重算任务"))

    @app.post("/backtests/<int:run_id>/export-md")
    def export_backtest_markdown(run_id: int):
        try:
            output_path = service.export_backtest_markdown(run_id)
        except Exception as exc:
            return redirect(url_for("backtest_detail_page", run_id=run_id, error=str(exc)))
        return redirect(
            url_for(
                "backtest_detail_page",
                run_id=run_id,
                success="回测明细已导出为 Markdown 文档",
                export_path=str(output_path),
            )
        )

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

    @app.get("/api/backtest/tasks")
    def backtest_tasks():
        limit = max(int(request.args.get("limit", "10")), 1)
        return jsonify({"tasks": service.recent_backtest_tasks(limit=limit)})

    @app.get("/api/backtest/tasks/<int:task_id>")
    def backtest_task_detail(task_id: int):
        detail = service.backtest_task_detail(task_id)
        if not detail:
            return jsonify({"error": f"未找到回测任务 {task_id}"}), 404
        return jsonify(detail)

    @app.get("/backtests/tasks/<int:task_id>")
    def backtest_task_page(task_id: int):
        detail = service.backtest_task_detail(task_id)
        if not detail:
            return redirect(url_for("backtests", error=f"未找到回测任务 {task_id}"))
        return render_template("backtest_task_status.html", task=detail, success=request.args.get("success"))

    @app.post("/backtests/compare")
    def create_backtest_compare_task_form():
        form = request.form
        payload = build_backtest_payload(form)
        parameter_key = (form.get("compare_parameter_key") or "").strip()
        raw_values = (form.get("compare_values") or "").strip()
        secondary_parameter_key = (form.get("compare_parameter_key_secondary") or "").strip()
        raw_secondary_values = (form.get("compare_values_secondary") or "").strip()
        compare_name = (form.get("compare_name") or "").strip()
        compare_fields = {item["id"]: item for item in service.backtest_config_schema().get("compare_fields", [])}
        if parameter_key not in compare_fields:
            return redirect(url_for("backtests", error="未选择有效的主对比参数"))

        def parse_values(field_key: str, raw_text: str):
            values = []
            for chunk in [item.strip() for item in raw_text.split(",") if item.strip()]:
                if compare_fields[field_key]["type"] == "int":
                    values.append(int(float(chunk)))
                else:
                    values.append(float(chunk))
            return values

        try:
            values = parse_values(parameter_key, raw_values)
            secondary_values = parse_values(secondary_parameter_key, raw_secondary_values) if secondary_parameter_key and raw_secondary_values else []
        except ValueError:
            return redirect(url_for("backtests", error="对比参数值格式不正确，请使用英文逗号分隔数字"))
        if len(values) < 2:
            return redirect(url_for("backtests", error="主参数至少提供两组值用于对比"))
        if secondary_parameter_key and secondary_parameter_key == parameter_key:
            return redirect(url_for("backtests", error="第二参数不能与主参数重复"))
        if secondary_parameter_key and len(secondary_values) < 2:
            return redirect(url_for("backtests", error="第二参数至少提供两组值用于网格对比"))
        try:
            task_id = service.start_backtest_compare_task(
                name=compare_name,
                parameter_key=parameter_key,
                parameter_label=str(compare_fields[parameter_key]["label"]),
                values=values,
                base_config=payload,
                secondary_parameter_key=secondary_parameter_key or None,
                secondary_parameter_label=str(compare_fields[secondary_parameter_key]["label"]) if secondary_parameter_key else None,
                secondary_values=secondary_values or None,
            )
        except Exception as exc:
            return redirect(url_for("backtests", error=str(exc)))
        return redirect(url_for("backtest_compare_task_page", task_id=task_id))

    @app.get("/backtests/compare/tasks/<int:task_id>")
    def backtest_compare_task_page(task_id: int):
        detail = service.backtest_compare_task_detail(task_id)
        if not detail:
            return redirect(url_for("backtests", error=f"未找到参数对比任务 {task_id}"))
        compare_charts = []
        for row in (detail.get("summary") or {}).get("rows", [])[:12]:
            run_detail = service.backtest_detail(int(row.get("run_id")))
            if not run_detail:
                continue
            compare_charts.append(
                {
                    "run_id": int(row.get("run_id")),
                    "name": row.get("name"),
                    "parameter_value": row.get("parameter_value"),
                    "total_return": row.get("total_return"),
                    "excess_return": row.get("excess_return"),
                    "nav_svg": build_nav_svg(run_detail.get("nav") or [], run_detail.get("trades") or []),
                }
            )
        return render_template(
            "backtest_compare_task_status.html",
            task=detail,
            compare_charts=compare_charts,
            success=request.args.get("success"),
        )

    @app.get("/api/backtests/compare/tasks/<int:task_id>")
    def backtest_compare_task_detail_api(task_id: int):
        detail = service.backtest_compare_task_detail(task_id)
        if not detail:
            return jsonify({"error": f"未找到参数对比任务 {task_id}"}), 404
        return jsonify(detail)

    @app.post("/backtests")
    def create_backtest_run_form():
        form = request.form
        payload = build_backtest_payload(form)
        try:
            task_id = service.start_backtest_task(payload)
        except Exception as exc:
            return redirect(url_for("backtests", error=str(exc)))
        return redirect(url_for("backtest_task_page", task_id=task_id, success="回测任务已创建"))

    @app.post("/backtests/templates")
    def create_backtest_template_form():
        form = request.form
        payload = build_backtest_payload(form)
        name = (form.get("template_name") or "").strip()
        description = (form.get("template_description") or "").strip() or None
        try:
            template_id = service.create_backtest_template(name=name, description=description, config=payload)
        except Exception as exc:
            return redirect(url_for("backtests", error=str(exc)))
        return redirect(url_for("backtests", template_id=template_id, success="已保存自定义模板"))

    @app.post("/backtests/templates/<int:template_id>/update")
    def update_backtest_template_form(template_id: int):
        form = request.form
        payload = build_backtest_payload(form)
        name = (form.get("template_name") or "").strip()
        description = (form.get("template_description") or "").strip() or None
        try:
            service.update_backtest_template(template_id=template_id, name=name, description=description, config=payload)
        except Exception as exc:
            return redirect(url_for("backtests", template_id=template_id, error=str(exc)))
        return redirect(url_for("backtests", template_id=template_id, success="模板已更新"))

    @app.post("/backtests/templates/<int:template_id>/delete")
    def delete_backtest_template_form(template_id: int):
        try:
            service.delete_backtest_template(template_id)
        except Exception as exc:
            return redirect(url_for("backtests", template_id=template_id, error=str(exc)))
        return redirect(url_for("backtests", success="模板已删除"))

    @app.post("/api/daily/backfill")
    def backfill_daily_tables():
        payload = request.get_json(silent=True) or {}
        days = int(payload.get("days", 120) or 120)
        batch_size = int(payload.get("batch_size", 10) or 10)
        start_date = str(payload.get("start_date", "") or "").strip() or None
        end_date = str(payload.get("end_date", "") or "").strip() or None
        try:
            result = service.backfill_daily_tables(days=days, batch_size=batch_size, start_date=start_date, end_date=end_date)
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500
        return jsonify(result)

    @app.post("/api/daily/backfill/tasks")
    def create_backfill_task_api():
        payload = request.get_json(silent=True) or {}
        days = int(payload.get("days", 120) or 120)
        batch_size = int(payload.get("batch_size", 10) or 10)
        start_date = str(payload.get("start_date", "") or "").strip() or None
        end_date = str(payload.get("end_date", "") or "").strip() or None
        resume_task_id = payload.get("resume_task_id")
        try:
            task_id = service.start_backfill_task(days=days, batch_size=batch_size, start_date=start_date, end_date=end_date, resume_task_id=resume_task_id)
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500
        return jsonify({"task_id": task_id, "task": service.backfill_task_detail(task_id)})

    @app.get("/api/daily/backfill/tasks")
    def backfill_tasks_api():
        limit = max(int(request.args.get("limit", "10")), 1)
        return jsonify({"tasks": service.recent_backfill_tasks(limit=limit)})

    @app.get("/api/daily/backfill/tasks/<int:task_id>")
    def backfill_task_detail_api(task_id: int):
        task = service.backfill_task_detail(task_id)
        if not task:
            return jsonify({"error": f"未找到历史回填任务 {task_id}"}), 404
        return jsonify(task)

    @app.post("/backfill")
    def start_backfill_task():
        days = int(request.form.get("days", "120") or 120)
        batch_size = int(request.form.get("batch_size", "10") or 10)
        start_date = request.form.get("start_date", "").strip() or None
        end_date = request.form.get("end_date", "").strip() or None
        resume_task_id = request.form.get("resume_task_id")
        try:
            task_id = service.start_backfill_task(
                days=days,
                batch_size=batch_size,
                start_date=start_date,
                end_date=end_date,
                resume_task_id=int(resume_task_id) if resume_task_id else None,
            )
        except Exception as exc:
            return redirect(url_for("backfill_latest_status", error=str(exc)))
        return redirect(url_for("backfill_task_status", task_id=task_id))

    @app.get("/backfill/status")
    def backfill_latest_status():
        task = service.latest_backfill_task()
        if not task:
            return render_template(
                "backfill_status.html",
                task=None,
                recent_tasks=[],
                defaults={"days": 120, "batch_size": 10, "start_date": "", "end_date": ""},
                error=request.args.get("error"),
            )
        return redirect(url_for("backfill_task_status", task_id=task["id"]))

    @app.get("/backfill/status/<int:task_id>")
    def backfill_task_status(task_id: int):
        task = service.backfill_task_detail(task_id)
        if not task:
            return redirect(url_for("backfill_latest_status", error=f"未找到历史回填任务 {task_id}"))
        return render_template(
            "backfill_status.html",
            task=task,
            recent_tasks=service.recent_backfill_tasks(),
            defaults={"days": task["days"], "batch_size": task["batch_size"], "start_date": task.get("start_date") or "", "end_date": task.get("end_date") or ""},
            error=request.args.get("error"),
        )

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
