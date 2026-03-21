from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

from .config import settings


DEFAULT_PROMPT_TEMPLATE = """你是一名量化研究复盘助手。

你的任务不是直接选出“最高收益参数”，而是基于给定的回测优化结果，给出下一轮参数微调建议。

请遵守以下原则：
1. 综合考虑总收益、超额收益、最大回撤、交易次数、胜率。
2. 不要迷信样本过少的偶然最优结果。
3. 优先输出可执行的下一轮搜索建议，而不是空泛描述。
4. 明确指出哪些参数建议固定、哪些参数建议收窄范围、哪些参数建议放宽或移除。
5. 如果发现某些参数对结果影响很弱，也要指出。

请只输出一个 JSON 对象，不要输出额外解释，格式如下：
{
  "summary": "一句话总结本轮优化表现",
  "best_pattern": [
    "观察1",
    "观察2"
  ],
  "fixed_params": [
    {"name": "参数名", "reason": "为什么建议固定", "value": "建议值"}
  ],
  "narrow_params": [
    {"name": "参数名", "reason": "为什么建议缩窄", "current_range": "当前范围", "suggested_range": "建议范围"}
  ],
  "relax_params": [
    {"name": "参数名", "reason": "为什么建议放宽", "current_range": "当前范围", "suggested_range": "建议范围"}
  ],
  "disable_or_enable_rules": [
    {"name": "规则名", "action": "enable/disable/keep", "reason": "理由"}
  ],
  "risk_notes": [
    "风险提示1",
    "风险提示2"
  ],
  "next_round_focus": [
    "下一轮重点1",
    "下一轮重点2"
  ]
}
"""


class OptimizerLLMReviewService:
    def __init__(self, prompt_template_path: str | Path | None = None):
        self.prompt_template_path = Path(prompt_template_path) if prompt_template_path else None

    def review(self, output_dir: str | Path, top_n: int = 10) -> dict[str, Any]:
        output_path = Path(output_dir)
        best_payload = self._read_json(output_path / "best_params.json")
        importance_payload = self._read_json(output_path / "importance.json", default={})
        next_round_payload = self._read_json(output_path / "next_round_config.json", default={})
        report_text = self._read_text(output_path / "optimizer_report.md", default="")

        top_trials = list((best_payload.get("best_trials") or [])[:top_n])
        prompt = self._build_prompt(
            best_payload=best_payload,
            importance_payload=importance_payload,
            next_round_payload=next_round_payload,
            report_text=report_text,
            top_trials=top_trials,
        )
        review_payload = self._call_llm(prompt)
        merged_config = self._merge_review_into_next_round(
            best_payload=best_payload,
            existing_next_round=next_round_payload,
            review_payload=review_payload,
            output_dir=output_path,
        )

        wrapped = {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "output_dir": str(output_path),
            "model": settings.llm_model,
            "top_n": top_n,
            "review": review_payload,
        }
        json_path = output_path / "optimizer_llm_review.json"
        md_path = output_path / "optimizer_llm_review.md"
        merged_path = output_path / "optimizer_llm_next_round_config.json"
        json_path.write_text(json.dumps(wrapped, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(self._render_markdown(wrapped), encoding="utf-8")
        merged_path.write_text(json.dumps(merged_config, ensure_ascii=False, indent=2), encoding="utf-8")
        return {
            "json_path": str(json_path),
            "md_path": str(md_path),
            "merged_config_path": str(merged_path),
            "review": wrapped,
        }

    def apply_existing_review(self, output_dir: str | Path) -> dict[str, Any]:
        output_path = Path(output_dir)
        review_wrapper = self._read_json(output_path / "optimizer_llm_review.json")
        if not review_wrapper:
            raise RuntimeError("未找到 optimizer_llm_review.json，无法应用现有 LLM 建议。")
        best_payload = self._read_json(output_path / "best_params.json")
        next_round_payload = self._read_json(output_path / "next_round_config.json", default={})
        merged_config = self._merge_review_into_next_round(
            best_payload=best_payload,
            existing_next_round=next_round_payload,
            review_payload=review_wrapper.get("review") or {},
            output_dir=output_path,
        )
        merged_path = output_path / "optimizer_llm_next_round_config.json"
        merged_path.write_text(json.dumps(merged_config, ensure_ascii=False, indent=2), encoding="utf-8")
        return {"merged_config_path": str(merged_path), "merged_config": merged_config}

    def _build_prompt(
        self,
        best_payload: dict[str, Any],
        importance_payload: dict[str, Any],
        next_round_payload: dict[str, Any],
        report_text: str,
        top_trials: list[dict[str, Any]],
    ) -> list[dict[str, str]]:
        system = self._load_prompt_template()
        compact_report = report_text[:12000]
        compact_next_round = json.dumps(next_round_payload, ensure_ascii=False, indent=2)[:12000]
        compact_best = json.dumps(top_trials, ensure_ascii=False, indent=2)
        compact_importance = json.dumps(importance_payload, ensure_ascii=False, indent=2)
        user = f"""
以下是本轮参数优化结果，请输出结构化 JSON 复盘建议。

## 最优试验摘要
{compact_best}

## 参数重要性统计
{compact_importance}

## 自动生成的下一轮配置
{compact_next_round}

## 本轮 Markdown 报告摘要
{compact_report}
""".strip()
        return [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ]

    def _call_llm(self, messages: list[dict[str, str]]) -> dict[str, Any]:
        if not settings.llm_api_key:
            raise RuntimeError("未配置 LLM_API_KEY，无法执行优化器 LLM 复盘。")
        response = requests.post(
            f"{settings.llm_api_base.rstrip('/')}/chat/completions",
            headers={
                "Authorization": f"Bearer {settings.llm_api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": settings.llm_model,
                "messages": messages,
                "temperature": 0.2,
                "response_format": {"type": "json_object"},
            },
            timeout=settings.llm_timeout_seconds,
        )
        if not response.ok:
            try:
                payload = response.json()
            except Exception:
                payload = response.text
            raise RuntimeError(f"优化器 LLM 复盘失败: HTTP {response.status_code}, payload: {payload}")
        content = response.json()["choices"][0]["message"]["content"].strip()
        try:
            return json.loads(content)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"优化器 LLM 返回了非 JSON 内容: {content[:500]}") from exc

    def _merge_review_into_next_round(
        self,
        best_payload: dict[str, Any],
        existing_next_round: dict[str, Any],
        review_payload: dict[str, Any],
        output_dir: Path,
    ) -> dict[str, Any]:
        merged = self._build_base_next_round(best_payload, existing_next_round, output_dir)
        for item in review_payload.get("fixed_params") or []:
            name = str(item.get("name") or "").strip()
            if not name:
                continue
            value = self._parse_scalar_value(item.get("value"))
            self._apply_fixed_param(merged, name, value)
        for item in review_payload.get("narrow_params") or []:
            name = str(item.get("name") or "").strip()
            if not name:
                continue
            value_range = self._parse_range(item.get("suggested_range"))
            if value_range is not None:
                self._apply_range_param(merged, name, value_range)
        for item in review_payload.get("relax_params") or []:
            name = str(item.get("name") or "").strip()
            if not name:
                continue
            value_range = self._parse_range(item.get("suggested_range"))
            if value_range is not None:
                self._apply_range_param(merged, name, value_range)
        merged["name"] = f"{merged.get('name', 'optimizer-next-round')}-llm"
        merged["llm_review_applied_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        merged["llm_review_summary"] = review_payload.get("summary") or ""
        return merged

    def _build_base_next_round(
        self,
        best_payload: dict[str, Any],
        existing_next_round: dict[str, Any],
        output_dir: Path,
    ) -> dict[str, Any]:
        if existing_next_round:
            payload = dict(existing_next_round)
            payload["output_dir"] = str(output_dir)
            return payload

        best_trial = ((best_payload.get("best_trials") or [])[:1] or [None])[0]
        best_config = dict((best_trial or {}).get("config") or {})
        return {
            "name": f"{best_payload.get('name', 'optimizer')}-next-round",
            "workspace": str(output_dir.parent.parent),
            "output_dir": str(output_dir),
            "base_config": {
                "name": "LLM 下一轮优化",
                "enabled_buy_rules": ["buy_strict", "buy_momentum"],
                "enabled_sell_rules": [
                    "sell_trim",
                    "sell_break_ma5",
                    "sell_drawdown",
                    "sell_time_stop",
                    "sell_flip_loss",
                    "sell_market_weak_drop",
                ],
            },
            "search": {
                "method": "random",
                "trials": 20,
                "seed": 42,
                "top_k": 10,
                "progress_every": 5,
                "checkpoint_every": 5,
                "stages": [
                    {
                        "name": "llm-refine",
                        "method": "random",
                        "trials": 20,
                        "seed": 42,
                        "top_k": 10,
                        "source_top_n": 5,
                        "radius_steps": 1,
                        "relations": [],
                        "param_space": {
                            key: {"type": self._guess_param_type(value), "values": [value]}
                            for key, value in best_config.items()
                        },
                    }
                ],
            },
            "constraints": {
                "min_trade_count": 5,
                "max_drawdown_lte": 0.25,
                "min_win_rate": 0.25,
            },
            "objective": {
                "primary": "total_return",
                "mode": "max",
                "secondary": [
                    {"field": "excess_return", "mode": "max"},
                    {"field": "max_drawdown", "mode": "min"},
                ],
            },
            "report": {"save_csv": True, "save_json": True, "save_md": True},
        }

    def _apply_fixed_param(self, payload: dict[str, Any], name: str, value: Any) -> None:
        for stage in payload.get("search", {}).get("stages") or []:
            param_space = stage.setdefault("param_space", {})
            spec = param_space.get(name)
            spec_type = self._guess_param_type(value) if spec is None else str(spec.get("type") or self._guess_param_type(value))
            param_space[name] = {"type": spec_type, "values": [value]}

    def _apply_range_param(self, payload: dict[str, Any], name: str, value_range: tuple[float, float]) -> None:
        lower, upper = value_range
        for stage in payload.get("search", {}).get("stages") or []:
            param_space = stage.setdefault("param_space", {})
            spec = param_space.get(name)
            if spec is None:
                spec_type = "float"
                step = 1
            else:
                spec_type = str(spec.get("type") or "float")
                step = spec.get("step", 1 if spec_type == "int" else 0.01)
            if spec_type == "int":
                lower = int(round(lower))
                upper = int(round(upper))
                step = int(step or 1)
            param_space[name] = {
                "type": spec_type,
                "min": lower,
                "max": upper,
                "step": step,
            }

    @staticmethod
    def _guess_param_type(value: Any) -> str:
        if isinstance(value, bool):
            return "bool"
        if isinstance(value, int) and not isinstance(value, bool):
            return "int"
        return "float"

    @staticmethod
    def _parse_scalar_value(value: Any) -> Any:
        if isinstance(value, (int, float, bool)):
            return value
        text = str(value).strip()
        if text.lower() in {"true", "false"}:
            return text.lower() == "true"
        compact = text.replace(",", "").replace(" ", "")
        multiplier = 1.0
        if compact.lower().endswith("m"):
            multiplier = 1_000_000.0
            compact = compact[:-1]
        elif compact.lower().endswith("b"):
            multiplier = 1_000_000_000.0
            compact = compact[:-1]
        try:
            number = float(compact) * multiplier
        except ValueError:
            return text
        if number.is_integer():
            return int(number)
        return number

    @staticmethod
    def _parse_range(value: Any) -> tuple[float, float] | None:
        text = str(value or "").strip()
        if not text:
            return None
        normalized = text.replace(",", "")
        pattern = re.compile(
            r"^\s*(-?\d+(?:\.\d+)?)\s*(?:to|TO|~|—|–|-)\s*(-?\d+(?:\.\d+)?)\s*$"
        )
        match = pattern.match(normalized)
        if match:
            left = float(match.group(1))
            right = float(match.group(2))
            return (left, right) if left <= right else (right, left)
        matches = re.findall(r"-?\d+(?:\.\d+)?", normalized)
        if len(matches) < 2:
            return None
        left = float(matches[0])
        right = float(matches[1])
        return (left, right) if left <= right else (right, left)

    def _load_prompt_template(self) -> str:
        if self.prompt_template_path and self.prompt_template_path.exists():
            return self.prompt_template_path.read_text(encoding="utf-8")
        return DEFAULT_PROMPT_TEMPLATE

    @staticmethod
    def _read_json(path: Path, default: dict[str, Any] | None = None) -> dict[str, Any]:
        if not path.exists():
            return default or {}
        return json.loads(path.read_text(encoding="utf-8"))

    @staticmethod
    def _read_text(path: Path, default: str = "") -> str:
        if not path.exists():
            return default
        return path.read_text(encoding="utf-8")

    @staticmethod
    def _render_markdown(payload: dict[str, Any]) -> str:
        review = payload.get("review") or {}
        lines = [
            "# 参数优化 LLM 复盘",
            "",
            f"- 生成时间：`{payload.get('generated_at')}`",
            f"- 模型：`{payload.get('model')}`",
            f"- 输出目录：`{payload.get('output_dir')}`",
            "",
            "## 总结",
            "",
            str(review.get("summary") or "-"),
            "",
        ]
        lines.extend(_markdown_section("最佳模式", review.get("best_pattern")))
        lines.extend(_markdown_kv_section("建议固定参数", review.get("fixed_params")))
        lines.extend(_markdown_kv_section("建议收窄参数", review.get("narrow_params")))
        lines.extend(_markdown_kv_section("建议放宽参数", review.get("relax_params")))
        lines.extend(_markdown_kv_section("规则开关建议", review.get("disable_or_enable_rules")))
        lines.extend(_markdown_section("风险提示", review.get("risk_notes")))
        lines.extend(_markdown_section("下一轮重点", review.get("next_round_focus")))
        return "\n".join(lines)


def _markdown_section(title: str, items: Any) -> list[str]:
    lines = [f"## {title}", ""]
    if not items:
        return lines + ["- 暂无", ""]
    for item in items:
        lines.append(f"- {item}")
    lines.append("")
    return lines


def _markdown_kv_section(title: str, items: Any) -> list[str]:
    lines = [f"## {title}", ""]
    if not items:
        return lines + ["- 暂无", ""]
    for item in items:
        if isinstance(item, dict):
            lines.append(f"- {json.dumps(item, ensure_ascii=False)}")
        else:
            lines.append(f"- {item}")
    lines.append("")
    return lines
