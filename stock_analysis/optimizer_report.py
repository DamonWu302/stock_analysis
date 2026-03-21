from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from .optimizer_types import OptimizerConfig, TrialResult


def write_trials_csv(path: Path, trials: list[TrialResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    all_keys: set[str] = {
        "trial_id",
        "stage_name",
        "run_id",
        "status",
        "passed_constraints",
        "error",
    }
    for trial in trials:
        row = {
            "trial_id": trial.trial_id,
            "stage_name": trial.stage_name,
            "run_id": trial.run_id,
            "status": trial.status,
            "passed_constraints": int(trial.passed_constraints),
            "error": trial.error or "",
        }
        for key, value in sorted(trial.config.items()):
            row[f"cfg_{key}"] = value
        for key, value in sorted((trial.summary or {}).items()):
            row[f"summary_{key}"] = value
        all_keys.update(row.keys())
        rows.append(row)
    fieldnames = sorted(all_keys)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_best_json(path: Path, config: OptimizerConfig, trials: list[TrialResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "name": config.name,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "best_trials": [
            {
                "trial_id": trial.trial_id,
                "stage_name": trial.stage_name,
                "run_id": trial.run_id,
                "status": trial.status,
                "config": trial.config,
                "summary": trial.summary,
                "passed_constraints": trial.passed_constraints,
                "error": trial.error,
            }
            for trial in trials
        ],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_summary_md(
    path: Path,
    config: OptimizerConfig,
    trials: list[TrialResult],
    importance_payload: dict[str, Any] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# 参数优化报告：{config.name}",
        "",
        f"- 生成时间：`{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`",
        f"- 搜索阶段数：`{len(config.search.stages) or 1}`",
        f"- 结果数量：`{len(trials)}`",
        "",
        "## 约束条件",
        "",
        f"- 最少交易数：`{config.constraints.min_trade_count}`",
        f"- 最大回撤上限：`{config.constraints.max_drawdown_lte}`",
        f"- 最低胜率：`{config.constraints.min_win_rate}`",
        "",
        "## 前十结果",
        "",
    ]
    for index, trial in enumerate(trials[:10], start=1):
        summary = trial.summary or {}
        lines.extend(
            [
                f"### {index}. Trial #{trial.trial_id}",
                "",
                f"- 所属阶段：`{trial.stage_name}`",
                f"- run_id：`{trial.run_id}`",
                f"- 状态：`{trial.status}`",
                f"- 是否通过约束：`{trial.passed_constraints}`",
                f"- 总收益：`{summary.get('total_return', 0)}`",
                f"- 超额收益：`{summary.get('excess_return', 0)}`",
                f"- 最大回撤：`{summary.get('max_drawdown', 0)}`",
                f"- 胜率：`{summary.get('win_rate', 0)}`",
                f"- 交易次数：`{summary.get('trade_count', 0)}`",
                f"- 参数：`{json.dumps(trial.config, ensure_ascii=False)}`",
                "",
            ]
        )

    if importance_payload and importance_payload.get("parameters"):
        lines.extend(
            [
                "## 参数重要性统计",
                "",
                f"- 目标字段：`{importance_payload.get('objective')}`",
                f"- 统计参数数：`{importance_payload.get('parameter_count')}`",
                "",
            ]
        )
        for index, item in enumerate((importance_payload.get("parameters") or [])[:10], start=1):
            lines.extend(
                [
                    f"### {index}. {item.get('name')}",
                    "",
                    f"- 重要性分数：`{item.get('importance_score')}`",
                    f"- 最优取值：`{item.get('best_value')}`",
                    f"- 最优平均目标值：`{item.get('best_avg_objective')}`",
                    f"- 最差取值：`{item.get('worst_value')}`",
                    f"- 最差平均目标值：`{item.get('worst_avg_objective')}`",
                    f"- 分桶数：`{item.get('bucket_count')}`",
                    "",
                ]
            )

    path.write_text("\n".join(lines), encoding="utf-8")


def write_importance_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_next_round_config(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
