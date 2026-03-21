from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

from .config import settings


DEFAULT_PROMPT_TEMPLATE = """你是一名量化研究复盘助手。
你的任务不是直接选出“最高收益参数”，而是基于给定的回测优化结果，输出下一轮参数微调建议。

请严格遵守以下原则：
1. 综合考虑总收益、超额收益、最大回撤、交易次数、胜率、持仓周期，不能只追求单一高收益。
2. 对样本过少、交易次数过低、明显偶然的最优结果保持警惕。
3. 优先给出下一轮可执行的参数缩圈建议，而不是泛泛点评。
4. 历史上多轮重复出现的“稳定参数”具有更高优先级。除非本轮结果出现明显反证，否则优先保持稳定参数不变，或仅做很小范围微调。
5. 如果你建议修改稳定参数，必须在 reason 里明确说明“为什么本轮证据足以推翻历史稳定结论”。
6. 如果某些参数在多轮中持续被固定、持续进入重要参数、或持续被缩窄，应该优先把搜索空间集中到其他更不稳定的参数上。
7. 输出必须是一个 JSON 对象，不要输出额外解释。

输出格式如下：
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
        memory_payload = self._load_memory(output_path)

        top_trials = list((best_payload.get("best_trials") or [])[:top_n])
        prompt = self._build_prompt(
            best_payload=best_payload,
            importance_payload=importance_payload,
            next_round_payload=next_round_payload,
            memory_payload=memory_payload,
            report_text=report_text,
            top_trials=top_trials,
        )
        review_payload = self._call_llm(prompt)
        merged_config = self._merge_review_into_next_round(
            best_payload=best_payload,
            existing_next_round=next_round_payload,
            review_payload=review_payload,
            memory_payload=memory_payload,
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
        memory_state = self._update_memory(
            output_dir=output_path,
            memory_payload=memory_payload,
            best_payload=best_payload,
            importance_payload=importance_payload,
            review_wrapper=wrapped,
        )
        return {
            "json_path": str(json_path),
            "md_path": str(md_path),
            "merged_config_path": str(merged_path),
            "memory_json_path": str(output_path / "optimizer_llm_memory.json"),
            "memory_md_path": str(output_path / "optimizer_llm_memory.md"),
            "memory": memory_state,
            "review": wrapped,
        }

    def apply_existing_review(self, output_dir: str | Path) -> dict[str, Any]:
        output_path = Path(output_dir)
        review_wrapper = self._read_json(output_path / "optimizer_llm_review.json")
        if not review_wrapper:
            raise RuntimeError("未找到 optimizer_llm_review.json，无法应用现有 LLM 建议。")
        best_payload = self._read_json(output_path / "best_params.json")
        next_round_payload = self._read_json(output_path / "next_round_config.json", default={})
        importance_payload = self._read_json(output_path / "importance.json", default={})
        memory_payload = self._load_memory(output_path)
        merged_config = self._merge_review_into_next_round(
            best_payload=best_payload,
            existing_next_round=next_round_payload,
            review_payload=review_wrapper.get("review") or {},
            memory_payload=memory_payload,
            output_dir=output_path,
        )
        merged_path = output_path / "optimizer_llm_next_round_config.json"
        merged_path.write_text(json.dumps(merged_config, ensure_ascii=False, indent=2), encoding="utf-8")
        memory_state = self._update_memory(
            output_dir=output_path,
            memory_payload=memory_payload,
            best_payload=best_payload,
            importance_payload=importance_payload,
            review_wrapper=review_wrapper,
        )
        return {
            "merged_config_path": str(merged_path),
            "memory_json_path": str(output_path / "optimizer_llm_memory.json"),
            "memory_md_path": str(output_path / "optimizer_llm_memory.md"),
            "merged_config": merged_config,
            "memory": memory_state,
        }

    def _build_prompt(
        self,
        best_payload: dict[str, Any],
        importance_payload: dict[str, Any],
        next_round_payload: dict[str, Any],
        memory_payload: dict[str, Any],
        report_text: str,
        top_trials: list[dict[str, Any]],
    ) -> list[dict[str, str]]:
        system = self._load_prompt_template()
        compact_report = report_text[:12000]
        compact_next_round = json.dumps(next_round_payload, ensure_ascii=False, indent=2)[:12000]
        compact_best = json.dumps(top_trials, ensure_ascii=False, indent=2)
        compact_importance = json.dumps(importance_payload, ensure_ascii=False, indent=2)
        memory_text = self._build_memory_context(memory_payload)
        user = f"""
以下是本轮参数优化结果，请输出结构化 JSON 复盘建议。

## 历史记忆与稳定参数
{memory_text}

## 本轮最优试验摘要
{compact_best}

## 参数重要性统计
{compact_importance}

## 当前自动生成的下一轮配置
{compact_next_round}

## 本轮 Markdown 报告摘要
{compact_report}
""".strip()
        return [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ]

    def _build_memory_context(self, memory_payload: dict[str, Any]) -> str:
        entries = list(memory_payload.get("entries") or [])[-5:]
        stable_params = self._normalize_stable_params(memory_payload.get("stable_params") or {})
        if not entries and not stable_params:
            return "暂无历史记忆。请基于本轮结果给出第一轮复盘建议。"

        lines = [
            "请将稳定参数视为高优先级保留对象：",
            "- 若稳定参数没有出现明显反证，优先保持不变。",
            "- 若建议修改稳定参数，必须在 reason 中说明反证依据。",
            "- 下一轮搜索空间应优先收缩不稳定参数，而不是频繁改动稳定参数。",
            "",
        ]
        if stable_params:
            lines.append("历史稳定参数：")
            for name, detail in list(stable_params.items())[:12]:
                action = detail.get("recommended_action") or "keep"
                score = detail.get("stability_score", 0)
                counts = (
                    f"fixed={detail.get('fixed_count', 0)}, "
                    f"narrow={detail.get('narrow_count', 0)}, "
                    f"important={detail.get('important_count', 0)}"
                )
                consensus = detail.get("consensus_fixed_value")
                consensus_text = f"，历史固定值={consensus}" if consensus is not None else ""
                lines.append(
                    f"- {name}: 稳定分={score}，建议动作={action}，{counts}{consensus_text}"
                )
        else:
            lines.append("历史稳定参数：暂无。")

        lines.append("")
        lines.append("最近几轮复盘结论：")
        for item in entries:
            summary = item.get("summary") or ""
            best_run = item.get("best_run_id")
            fixed_names = ", ".join(item.get("fixed_param_names") or []) or "无"
            narrow_names = ", ".join(item.get("narrow_param_names") or []) or "无"
            important_names = ", ".join(item.get("important_params") or []) or "无"
            lines.append(f"- {item.get('generated_at')} run_id={best_run} summary={summary}")
            lines.append(
                f"  固定参数: {fixed_names}; 收窄参数: {narrow_names}; 重要参数: {important_names}"
            )
        return "\n".join(lines)

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
            raise RuntimeError(
                f"优化器 LLM 复盘失败: HTTP {response.status_code}, payload: {payload}"
            )
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
        memory_payload: dict[str, Any],
        output_dir: Path,
    ) -> dict[str, Any]:
        merged = self._build_base_next_round(best_payload, existing_next_round, output_dir)
        stable_params = self._normalize_stable_params(memory_payload.get("stable_params") or {})
        touched_names: set[str] = set()

        for item in review_payload.get("fixed_params") or []:
            name = str(item.get("name") or "").strip()
            if not name:
                continue
            value = self._parse_scalar_value(item.get("value"))
            self._apply_fixed_param(merged, name, value)
            touched_names.add(name)
        for item in review_payload.get("narrow_params") or []:
            name = str(item.get("name") or "").strip()
            if not name:
                continue
            value_range = self._parse_range(item.get("suggested_range"))
            if value_range is not None:
                self._apply_range_param(merged, name, value_range)
                touched_names.add(name)
        for item in review_payload.get("relax_params") or []:
            name = str(item.get("name") or "").strip()
            if not name:
                continue
            value_range = self._parse_range(item.get("suggested_range"))
            if value_range is not None:
                self._apply_range_param(merged, name, value_range)
                touched_names.add(name)

        self._apply_stable_memory_bias(merged, stable_params, touched_names)

        merged["name"] = f"{merged.get('name', 'optimizer-next-round')}-llm"
        merged["llm_review_applied_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        merged["llm_review_summary"] = review_payload.get("summary") or ""
        merged["llm_memory_bias_applied"] = bool(stable_params)
        merged["llm_stable_params_considered"] = sorted(stable_params.keys())
        return merged

    def _apply_stable_memory_bias(
        self,
        payload: dict[str, Any],
        stable_params: dict[str, dict[str, Any]],
        touched_names: set[str],
    ) -> None:
        for name, detail in stable_params.items():
            if name in touched_names:
                continue
            action = detail.get("recommended_action") or "keep"
            consensus = detail.get("consensus_fixed_value")
            if action == "fix" and consensus is not None:
                self._apply_fixed_param(payload, name, consensus)
            elif action == "narrow":
                self._tighten_existing_range(payload, name)

    def _tighten_existing_range(self, payload: dict[str, Any], name: str) -> None:
        for stage in payload.get("search", {}).get("stages") or []:
            spec = (stage.get("param_space") or {}).get(name)
            if not isinstance(spec, dict):
                continue
            if "values" in spec or "min" not in spec or "max" not in spec:
                continue
            lower = spec["min"]
            upper = spec["max"]
            if not isinstance(lower, (int, float)) or not isinstance(upper, (int, float)):
                continue
            center = (lower + upper) / 2
            half = (upper - lower) / 2
            tightened_half = half * 0.7
            if isinstance(spec.get("step"), int) or str(spec.get("type")) == "int":
                new_min = int(round(center - tightened_half))
                new_max = int(round(center + tightened_half))
                if new_min == new_max:
                    spec["values"] = [new_min]
                    for key in ("min", "max", "step"):
                        spec.pop(key, None)
                else:
                    spec["min"] = min(new_min, new_max)
                    spec["max"] = max(new_min, new_max)
            else:
                new_min = center - tightened_half
                new_max = center + tightened_half
                spec["min"] = min(new_min, new_max)
                spec["max"] = max(new_min, new_max)

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
            spec_type = (
                self._guess_param_type(value)
                if spec is None
                else str(spec.get("type") or self._guess_param_type(value))
            )
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
        pattern = re.compile(r"^\s*(-?\d+(?:\.\d+)?)\s*(?:to|TO|~|—|–|-)\s*(-?\d+(?:\.\d+)?)\s*$")
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

    def _load_memory(self, output_dir: Path) -> dict[str, Any]:
        path = output_dir / "optimizer_llm_memory.json"
        if not path.exists():
            return {"entries": [], "stable_params": {}}
        return json.loads(path.read_text(encoding="utf-8"))

    def _update_memory(
        self,
        output_dir: Path,
        memory_payload: dict[str, Any],
        best_payload: dict[str, Any],
        importance_payload: dict[str, Any],
        review_wrapper: dict[str, Any],
    ) -> dict[str, Any]:
        state = {
            "entries": list(memory_payload.get("entries") or []),
            "stable_params": self._normalize_stable_params(memory_payload.get("stable_params") or {}),
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        review = review_wrapper.get("review") or {}
        top_trial = ((best_payload.get("best_trials") or [])[:1] or [None])[0] or {}
        signature = self._memory_signature(review_wrapper, top_trial)
        if not any(item.get("signature") == signature for item in state["entries"]):
            entry = {
                "signature": signature,
                "generated_at": review_wrapper.get("generated_at") or state["updated_at"],
                "summary": review.get("summary") or "",
                "best_run_id": top_trial.get("run_id"),
                "best_trial_id": top_trial.get("trial_id"),
                "best_total_return": ((top_trial.get("summary") or {}).get("total_return")),
                "fixed_param_names": [
                    str(item.get("name"))
                    for item in (review.get("fixed_params") or [])
                    if item.get("name")
                ],
                "fixed_param_values": {
                    str(item.get("name")): self._parse_scalar_value(item.get("value"))
                    for item in (review.get("fixed_params") or [])
                    if item.get("name")
                },
                "narrow_param_names": [
                    str(item.get("name"))
                    for item in (review.get("narrow_params") or [])
                    if item.get("name")
                ],
                "relax_param_names": [
                    str(item.get("name"))
                    for item in (review.get("relax_params") or [])
                    if item.get("name")
                ],
                "important_params": [
                    str(item.get("name"))
                    for item in (importance_payload.get("parameters") or [])[:5]
                    if item.get("name")
                ],
            }
            state["entries"].append(entry)
        state["entries"] = state["entries"][-20:]
        state["stable_params"] = self._rebuild_stable_params(state["entries"])
        memory_json_path = output_dir / "optimizer_llm_memory.json"
        memory_md_path = output_dir / "optimizer_llm_memory.md"
        memory_json_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        memory_md_path.write_text(self._render_memory_markdown(state), encoding="utf-8")
        return state

    @staticmethod
    def _memory_signature(review_wrapper: dict[str, Any], top_trial: dict[str, Any]) -> str:
        payload = {
            "summary": (review_wrapper.get("review") or {}).get("summary"),
            "generated_at": review_wrapper.get("generated_at"),
            "best_run_id": top_trial.get("run_id"),
            "best_trial_id": top_trial.get("trial_id"),
        }
        text = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        return hashlib.sha1(text.encode("utf-8")).hexdigest()

    def _normalize_stable_params(self, raw: dict[str, Any]) -> dict[str, dict[str, Any]]:
        normalized: dict[str, dict[str, Any]] = {}
        for name, value in (raw or {}).items():
            if isinstance(value, dict):
                normalized[name] = {
                    "fixed_count": int(value.get("fixed_count", 0)),
                    "narrow_count": int(value.get("narrow_count", 0)),
                    "important_count": int(value.get("important_count", 0)),
                    "stability_score": float(value.get("stability_score", 0)),
                    "recommended_action": value.get("recommended_action") or "keep",
                    "consensus_fixed_value": value.get("consensus_fixed_value"),
                }
            else:
                labels = list(value or [])
                normalized[name] = {
                    "fixed_count": 0,
                    "narrow_count": 0,
                    "important_count": 0,
                    "stability_score": float(len(labels)),
                    "recommended_action": "keep",
                    "consensus_fixed_value": None,
                    "legacy_labels": labels,
                }
        return normalized

    @staticmethod
    def _rebuild_stable_params(entries: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        counts: dict[str, dict[str, Any]] = {}
        for entry in entries:
            fixed_values = entry.get("fixed_param_values") or {}
            for name in entry.get("fixed_param_names") or []:
                item = counts.setdefault(
                    name,
                    {"fixed_count": 0, "narrow_count": 0, "important_count": 0, "fixed_values": {}},
                )
                item["fixed_count"] += 1
                if name in fixed_values:
                    value = fixed_values[name]
                    value_key = json.dumps(value, ensure_ascii=False, sort_keys=True)
                    item["fixed_values"][value_key] = item["fixed_values"].get(value_key, 0) + 1
            for name in entry.get("narrow_param_names") or []:
                item = counts.setdefault(
                    name,
                    {"fixed_count": 0, "narrow_count": 0, "important_count": 0, "fixed_values": {}},
                )
                item["narrow_count"] += 1
            for name in entry.get("important_params") or []:
                item = counts.setdefault(
                    name,
                    {"fixed_count": 0, "narrow_count": 0, "important_count": 0, "fixed_values": {}},
                )
                item["important_count"] += 1

        stable: dict[str, dict[str, Any]] = {}
        for name, counter in counts.items():
            fixed_count = int(counter.get("fixed_count", 0))
            narrow_count = int(counter.get("narrow_count", 0))
            important_count = int(counter.get("important_count", 0))
            stability_score = fixed_count * 2 + narrow_count * 1.5 + important_count * 1
            if stability_score < 2:
                continue

            recommended_action = "keep"
            fixed_values = counter.get("fixed_values") or {}
            consensus_fixed_value = None
            if fixed_values:
                consensus_key, consensus_count = max(fixed_values.items(), key=lambda item: item[1])
                consensus_fixed_value = json.loads(consensus_key)
                if fixed_count >= 3 and consensus_count >= 2:
                    recommended_action = "fix"
                elif fixed_count >= 2 or narrow_count >= 2:
                    recommended_action = "narrow"
            elif narrow_count >= 2:
                recommended_action = "narrow"

            stable[name] = {
                "fixed_count": fixed_count,
                "narrow_count": narrow_count,
                "important_count": important_count,
                "stability_score": round(stability_score, 2),
                "recommended_action": recommended_action,
                "consensus_fixed_value": consensus_fixed_value,
            }
        return stable

    def _render_memory_markdown(self, memory_payload: dict[str, Any]) -> str:
        stable_params = self._normalize_stable_params(memory_payload.get("stable_params") or {})
        stable_count = len(stable_params)
        fix_count = sum(1 for item in stable_params.values() if item.get("recommended_action") == "fix")
        narrow_count = sum(1 for item in stable_params.values() if item.get("recommended_action") == "narrow")
        lines = [
            "# 参数优化 LLM 记忆",
            "",
            f"- 更新时间：`{memory_payload.get('updated_at', '')}`",
            f"- 稳定参数总数：`{stable_count}`，其中 `fix={fix_count}`，`narrow={narrow_count}`",
            "",
            "## 判定规则",
            "",
            "- `fixed_count`：历史复盘中被建议固定的次数",
            "- `narrow_count`：历史复盘中被建议收窄范围的次数",
            "- `important_count`：历史优化中被识别为重要参数的次数",
            "- `stability_score = fixed_count * 2 + narrow_count * 1.5 + important_count * 1`",
            "- `stability_score >= 2` 时进入稳定参数集合",
            "- `recommended_action = keep / narrow / fix`",
            "- 当 `fixed_count >= 3` 且固定值出现明确共识时，优先使用 `fix`",
            "- 当 `fixed_count >= 2` 或 `narrow_count >= 2` 时，优先使用 `narrow`",
            "",
            "## 稳定参数",
            "",
        ]
        if stable_params:
            for name, detail in stable_params.items():
                consensus = detail.get("consensus_fixed_value")
                consensus_text = f"，固定值={consensus}" if consensus is not None else ""
                lines.append(
                    f"- {name}: 稳定分={detail.get('stability_score')}"
                    f"，建议动作={detail.get('recommended_action')}"
                    f"，fixed={detail.get('fixed_count', 0)}"
                    f"，narrow={detail.get('narrow_count', 0)}"
                    f"，important={detail.get('important_count', 0)}"
                    f"{consensus_text}"
                )
        else:
            lines.append("- 暂无")
        lines.extend(["", "## 历史条目", ""])
        entries = memory_payload.get("entries") or []
        if not entries:
            lines.append("- 暂无")
        else:
            for item in entries[-10:]:
                lines.append(
                    f"- {item.get('generated_at')} run_id={item.get('best_run_id')} "
                    f"summary={item.get('summary')}"
                )
        lines.append("")
        return "\n".join(lines)

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
