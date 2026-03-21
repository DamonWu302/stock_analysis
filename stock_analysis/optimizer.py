from __future__ import annotations

import json
import random
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .backtest_runner import BacktestRunner
from .config import settings
from .db import Database
from .optimizer_report import write_best_json, write_summary_md, write_trials_csv
from .optimizer_report import write_importance_json, write_next_round_config
from .optimizer_space import build_candidate_values, build_grid_trials, sample_random_trials
from .optimizer_types import (
    ConstraintSpec,
    ObjectiveField,
    ObjectiveSpec,
    OptimizerConfig,
    RelationRule,
    ReportSpec,
    SearchParamSpec,
    SearchSpec,
    StageSpec,
    TrialResult,
)


def _load_relations(payload: dict[str, Any]) -> list[RelationRule]:
    return [RelationRule(**item) for item in (payload.get("relations") or [])]


def _load_param_space(payload: dict[str, Any]) -> dict[str, SearchParamSpec]:
    return {
        name: SearchParamSpec(**spec)
        for name, spec in (payload.get("param_space") or {}).items()
    }


def load_optimizer_config(path: str | Path) -> OptimizerConfig:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    search_payload = payload.get("search") or {}
    secondary = [
        ObjectiveField(**item)
        for item in (payload.get("objective") or {}).get("secondary", [])
    ]
    stages = [
        StageSpec(
            name=str(item.get("name") or f"stage-{index + 1}"),
            method=str(item.get("method") or "random"),
            trials=int(item.get("trials") or 100),
            seed=int(item.get("seed") or search_payload.get("seed") or 42),
            top_k=int(item.get("top_k") or search_payload.get("top_k") or 20),
            param_space=_load_param_space(item),
            relations=_load_relations(item),
            source_top_n=max(int(item.get("source_top_n") or 5), 1),
            radius_steps=max(int(item.get("radius_steps") or 1), 1),
        )
        for index, item in enumerate(search_payload.get("stages") or [])
    ]
    return OptimizerConfig(
        name=str(payload.get("name") or "backtest-optimizer"),
        workspace=str(payload.get("workspace") or Path(path).resolve().parent),
        output_dir=str(payload.get("output_dir") or (Path(path).resolve().parent / "data" / "optimizer")),
        base_config=dict(payload.get("base_config") or {}),
        search=SearchSpec(
            method=str(search_payload.get("method") or "random"),
            trials=int(search_payload.get("trials") or 100),
            seed=int(search_payload.get("seed") or 42),
            top_k=int(search_payload.get("top_k") or 20),
            progress_every=max(int(search_payload.get("progress_every") or 10), 1),
            checkpoint_every=max(int(search_payload.get("checkpoint_every") or 20), 1),
            param_space=_load_param_space(search_payload),
            relations=_load_relations(search_payload),
            stages=stages,
        ),
        constraints=ConstraintSpec(**(payload.get("constraints") or {})),
        objective=ObjectiveSpec(
            primary=str((payload.get("objective") or {}).get("primary") or "total_return"),
            mode=str((payload.get("objective") or {}).get("mode") or "max"),
            secondary=secondary,
        ),
        report=ReportSpec(**(payload.get("report") or {})),
    )


class BacktestOptimizer:
    def __init__(self, config: OptimizerConfig):
        self.config = config
        self.runner = BacktestRunner(Database(settings.database_path))

    def run(self) -> dict[str, Any]:
        stage_specs = self._build_stage_specs()
        trials: list[TrialResult] = []
        output_dir = Path(self.config.output_dir)
        csv_path = output_dir / "backtest_trials.csv"
        json_path = output_dir / "best_params.json"
        md_path = output_dir / "optimizer_report.md"
        importance_path = output_dir / "importance.json"
        next_round_path = output_dir / "next_round_config.json"
        best_so_far: TrialResult | None = None
        trial_id = 1
        total_expected = self._estimate_total_trials(stage_specs)
        previous_stage_ranked: list[TrialResult] = []

        for stage_index, stage in enumerate(stage_specs, start=1):
            trial_configs = self._build_stage_trial_configs(stage, previous_stage_ranked)
            print(
                f"[optimizer] stage={stage.name} ({stage_index}/{len(stage_specs)}) trials={len(trial_configs)}",
                flush=True,
            )
            stage_trials: list[TrialResult] = []
            for stage_offset, params in enumerate(trial_configs, start=1):
                relation_error = self._validate_relations(params, stage.relations)
                if relation_error:
                    result = TrialResult(
                        trial_id=trial_id,
                        stage_name=stage.name,
                        config=params,
                        summary=None,
                        run_id=None,
                        status="skipped",
                        error=relation_error,
                        passed_constraints=False,
                    )
                else:
                    trial_config = dict(self.config.base_config)
                    trial_config.update(params)
                    trial_config["name"] = f"{self.config.name}-{stage.name}-{stage_offset:03d}"
                    try:
                        summary = self.runner.run(trial_config)
                        result = TrialResult(
                            trial_id=trial_id,
                            stage_name=stage.name,
                            config=params,
                            summary=summary,
                            run_id=summary.get("run_id"),
                            status="completed",
                            passed_constraints=self._passes_constraints(summary),
                        )
                    except Exception as exc:
                        result = TrialResult(
                            trial_id=trial_id,
                            stage_name=stage.name,
                            config=params,
                            summary=None,
                            run_id=None,
                            status="failed",
                            error=str(exc),
                            passed_constraints=False,
                        )
                trials.append(result)
                stage_trials.append(result)
                best_so_far = self._choose_better(best_so_far, result)
                if (
                    trial_id == 1
                    or trial_id % self.config.search.progress_every == 0
                    or trial_id == total_expected
                ):
                    self._print_progress(
                        index=trial_id,
                        total=total_expected,
                        current=result,
                        best=best_so_far,
                        stage_name=stage.name,
                    )
                if trial_id % self.config.search.checkpoint_every == 0 or trial_id == total_expected:
                    self._write_outputs(
                        csv_path=csv_path,
                        json_path=json_path,
                        md_path=md_path,
                        importance_path=importance_path,
                        next_round_path=next_round_path,
                        trials=trials,
                    )
                trial_id += 1
            previous_stage_ranked = self._rank_trials(stage_trials)

        ranked = self._rank_trials(trials)
        return {
            "trial_count": len(trials),
            "ranked_count": len(ranked),
            "best_count": min(len(ranked), self.config.search.top_k),
            "output_dir": str(output_dir),
            "best_trials": ranked[: self.config.search.top_k],
            "csv_path": str(csv_path),
            "json_path": str(json_path),
            "md_path": str(md_path),
            "importance_path": str(importance_path),
            "next_round_path": str(next_round_path),
            "skipped_count": sum(1 for item in trials if item.status == "skipped"),
            "failed_count": sum(1 for item in trials if item.status == "failed"),
            "completed_count": sum(1 for item in trials if item.status == "completed"),
        }

    def _build_stage_specs(self) -> list[StageSpec]:
        if not self.config.search.stages:
            return [
                StageSpec(
                name="single-stage",
                method=self.config.search.method,
                trials=self.config.search.trials,
                seed=self.config.search.seed,
                top_k=self.config.search.top_k,
                param_space=self.config.search.param_space,
                relations=self.config.search.relations,
                )
            ]
        return list(self.config.search.stages)

    def _estimate_total_trials(self, stage_specs: list[StageSpec]) -> int:
        return sum(max(int(stage.trials), 0) for stage in stage_specs)

    def _build_stage_trial_configs(self, stage: StageSpec, previous_stage_ranked: list[TrialResult]) -> list[dict[str, Any]]:
        if stage.method == "refine":
            return self._build_refine_trial_configs(stage, previous_stage_ranked)
        return self._build_trial_configs(stage)

    def _build_trial_configs(self, stage: StageSpec) -> list[dict[str, Any]]:
        if stage.method == "grid":
            return build_grid_trials(stage.param_space, limit=stage.trials)
        return sample_random_trials(stage.param_space, trials=stage.trials, seed=stage.seed)

    def _build_refine_trial_configs(self, stage: StageSpec, ranked_trials: list[TrialResult]) -> list[dict[str, Any]]:
        seeds = ranked_trials[: stage.source_top_n]
        if not seeds:
            return self._build_trial_configs(
                StageSpec(
                    name=stage.name,
                    method="random",
                    trials=stage.trials,
                    seed=stage.seed,
                    top_k=stage.top_k,
                    param_space=stage.param_space,
                    relations=stage.relations,
                    source_top_n=stage.source_top_n,
                    radius_steps=stage.radius_steps,
                )
            )
        rng = random.Random(stage.seed)
        results: list[dict[str, Any]] = []
        seen: set[tuple[tuple[str, Any], ...]] = set()
        per_seed = max(stage.trials // max(len(seeds), 1), 1)
        for seed_trial in seeds:
            for _ in range(per_seed):
                payload: dict[str, Any] = {}
                for name, spec in stage.param_space.items():
                    pool = build_candidate_values(spec)
                    seed_value = seed_trial.config.get(name, self.config.base_config.get(name))
                    if seed_value not in pool:
                        payload[name] = rng.choice(pool)
                        continue
                    seed_index = pool.index(seed_value)
                    left = max(0, seed_index - stage.radius_steps)
                    right = min(len(pool), seed_index + stage.radius_steps + 1)
                    payload[name] = rng.choice(pool[left:right])
                key = tuple(sorted(payload.items()))
                if key in seen:
                    continue
                seen.add(key)
                results.append(payload)
                if len(results) >= stage.trials:
                    return results
        return results

    def _passes_constraints(self, summary: dict[str, Any]) -> bool:
        constraints = self.config.constraints
        if int(summary.get("trade_count") or 0) < int(constraints.min_trade_count or 0):
            return False
        if constraints.max_drawdown_lte is not None and float(summary.get("max_drawdown") or 0.0) > float(constraints.max_drawdown_lte):
            return False
        if constraints.min_win_rate is not None and float(summary.get("win_rate") or 0.0) < float(constraints.min_win_rate):
            return False
        return True

    def _rank_trials(self, trials: list[TrialResult]) -> list[TrialResult]:
        valid = [trial for trial in trials if trial.status == "completed" and trial.summary]
        constrained = [trial for trial in valid if trial.passed_constraints]
        target = constrained or valid
        return sorted(target, key=self._trial_sort_key, reverse=False)

    def _write_outputs(
        self,
        csv_path: Path,
        json_path: Path,
        md_path: Path,
        importance_path: Path,
        next_round_path: Path,
        trials: list[TrialResult],
    ) -> None:
        ranked = self._rank_trials(trials)
        importance_payload = self._build_importance_payload(trials)
        if self.config.report.save_csv:
            write_trials_csv(csv_path, trials)
        if self.config.report.save_json:
            write_best_json(json_path, self.config, ranked[: self.config.search.top_k])
        if self.config.report.save_md:
            write_summary_md(
                md_path,
                self.config,
                ranked[: self.config.search.top_k],
                importance_payload=importance_payload,
            )
        write_importance_json(importance_path, importance_payload)
        next_round_payload = self._build_next_round_payload(ranked)
        write_next_round_config(next_round_path, next_round_payload)

    def _trial_sort_key(self, trial: TrialResult):
        summary = trial.summary or {}
        keys: list[float] = []
        keys.append(self._score_value(summary.get(self.config.objective.primary), self.config.objective.mode))
        for field in self.config.objective.secondary:
            keys.append(self._score_value(summary.get(field.field), field.mode))
        keys.append(float(trial.trial_id))
        return tuple(keys)

    def _choose_better(self, current_best: TrialResult | None, candidate: TrialResult) -> TrialResult | None:
        if candidate.status != "completed" or not candidate.summary:
            return current_best
        if current_best is None:
            return candidate
        ranked = sorted([current_best, candidate], key=self._trial_sort_key, reverse=False)
        return ranked[0]

    def _print_progress(
        self,
        index: int,
        total: int,
        current: TrialResult,
        best: TrialResult | None,
        stage_name: str,
    ) -> None:
        progress = (index / max(total, 1)) * 100.0
        parts = [
            f"[optimizer] {index}/{total}",
            f"{progress:.1f}%",
            f"stage={stage_name}",
            f"status={current.status}",
        ]
        if current.run_id is not None:
            parts.append(f"run_id={current.run_id}")
        if current.summary:
            parts.append(f"total_return={float(current.summary.get('total_return') or 0.0):.4f}")
            parts.append(f"drawdown={float(current.summary.get('max_drawdown') or 0.0):.4f}")
            parts.append(f"trades={int(current.summary.get('trade_count') or 0)}")
        elif current.error:
            parts.append(f"reason={current.error}")
        if best and best.summary:
            parts.append(
                "best="
                f"trial#{best.trial_id}/run#{best.run_id}"
                f"/ret={float(best.summary.get('total_return') or 0.0):.4f}"
            )
        print(" ".join(parts), flush=True)

    def _validate_relations(self, params: dict[str, Any], relations: list[RelationRule]) -> str | None:
        if not relations:
            return None
        merged = dict(self.config.base_config)
        merged.update(params)
        for rule in relations:
            left = merged.get(rule.left)
            right = merged.get(rule.right) if rule.right else rule.value
            if not self._compare_relation(left, rule.operator, right):
                if rule.message:
                    return rule.message
                target = rule.right if rule.right else rule.value
                return f"relation failed: {rule.left} {rule.operator} {target}"
        return None

    @staticmethod
    def _compare_relation(left: Any, operator: str, right: Any) -> bool:
        if operator in {"==", "!=", "<", "<=", ">", ">="}:
            if left is None or right is None:
                return False
            if operator == "==":
                return left == right
            if operator == "!=":
                return left != right
            try:
                left_num = float(left)
                right_num = float(right)
            except (TypeError, ValueError):
                return False
            if operator == "<":
                return left_num < right_num
            if operator == "<=":
                return left_num <= right_num
            if operator == ">":
                return left_num > right_num
            if operator == ">=":
                return left_num >= right_num
        raise ValueError(f"unsupported relation operator: {operator}")

    @staticmethod
    def _score_value(value: Any, mode: str) -> float:
        numeric = float(value or 0.0)
        if mode == "max":
            return -numeric
        return numeric

    def _build_importance_payload(self, trials: list[TrialResult]) -> dict[str, Any]:
        completed = [trial for trial in trials if trial.status == "completed" and trial.summary]
        if not completed:
            return {"generated_at": None, "objective": self.config.objective.primary, "parameters": []}
        objective = self.config.objective.primary
        parameters: list[dict[str, Any]] = []
        all_params = sorted({key for trial in completed for key in trial.config.keys()})
        for name in all_params:
            buckets: dict[str, list[float]] = {}
            for trial in completed:
                if name not in trial.config:
                    continue
                value = trial.config[name]
                score = float((trial.summary or {}).get(objective) or 0.0)
                buckets.setdefault(str(value), []).append(score)
            if len(buckets) < 2:
                continue
            averages = {key: sum(values) / len(values) for key, values in buckets.items()}
            best_value = max(averages.items(), key=lambda item: item[1])
            worst_value = min(averages.items(), key=lambda item: item[1])
            parameters.append(
                {
                    "name": name,
                    "importance_score": round(best_value[1] - worst_value[1], 6),
                    "best_value": best_value[0],
                    "best_avg_objective": round(best_value[1], 6),
                    "worst_value": worst_value[0],
                    "worst_avg_objective": round(worst_value[1], 6),
                    "bucket_count": len(buckets),
                }
            )
        parameters.sort(key=lambda item: item["importance_score"], reverse=True)
        return {
            "generated_at": self._now_text(),
            "objective": objective,
            "parameter_count": len(parameters),
            "parameters": parameters,
        }

    def _build_next_round_payload(self, ranked: list[TrialResult]) -> dict[str, Any]:
        top_trials = ranked[: max(min(self.config.search.top_k, 5), 1)]
        payload = self._config_to_payload()
        if not top_trials:
            return payload
        narrowed_stages = []
        stages = self.config.search.stages or [
            StageSpec(
                name="single-stage",
                method=self.config.search.method,
                trials=self.config.search.trials,
                seed=self.config.search.seed,
                top_k=self.config.search.top_k,
                param_space=self.config.search.param_space,
                relations=self.config.search.relations,
            )
        ]
        for stage in stages:
            narrowed_space = {}
            for name, spec in stage.param_space.items():
                narrowed_space[name] = self._narrow_param_spec(spec, top_trials, name)
            narrowed_stages.append(
                {
                    "name": stage.name,
                    "method": stage.method,
                    "trials": stage.trials,
                    "seed": stage.seed,
                    "top_k": stage.top_k,
                    "source_top_n": stage.source_top_n,
                    "radius_steps": stage.radius_steps,
                    "relations": [asdict(rule) for rule in stage.relations],
                    "param_space": narrowed_space,
                }
            )
        payload["name"] = f"{self.config.name}-next-round"
        payload["search"]["stages"] = narrowed_stages
        payload["search"]["method"] = "random"
        payload["generated_from_top_trials"] = [
            {"trial_id": trial.trial_id, "run_id": trial.run_id, "stage_name": trial.stage_name}
            for trial in top_trials
        ]
        payload["generated_at"] = self._now_text()
        return payload

    def _narrow_param_spec(self, spec: SearchParamSpec, top_trials: list[TrialResult], name: str) -> dict[str, Any]:
        if spec.values:
            picked = []
            for trial in top_trials:
                value = trial.config.get(name)
                if value in spec.values and value not in picked:
                    picked.append(value)
            return {"type": spec.type, "values": picked or list(spec.values)}
        if spec.min is None or spec.max is None or spec.step in (None, 0):
            return {"type": spec.type, "min": spec.min, "max": spec.max, "step": spec.step}
        pool = build_candidate_values(spec)
        picked_values = [trial.config.get(name) for trial in top_trials if trial.config.get(name) in pool]
        if not picked_values:
            return {"type": spec.type, "min": spec.min, "max": spec.max, "step": spec.step}
        indices = sorted(pool.index(value) for value in picked_values)
        left = max(0, indices[0] - 1)
        right = min(len(pool) - 1, indices[-1] + 1)
        new_min = pool[left]
        new_max = pool[right]
        return {
            "type": spec.type,
            "min": new_min,
            "max": new_max,
            "step": spec.step,
        }

    def _config_to_payload(self) -> dict[str, Any]:
        payload = {
            "name": self.config.name,
            "workspace": self.config.workspace,
            "output_dir": self.config.output_dir,
            "base_config": dict(self.config.base_config),
            "search": {
                "method": self.config.search.method,
                "trials": self.config.search.trials,
                "seed": self.config.search.seed,
                "top_k": self.config.search.top_k,
                "progress_every": self.config.search.progress_every,
                "checkpoint_every": self.config.search.checkpoint_every,
                "param_space": {
                    name: self._param_spec_to_payload(spec)
                    for name, spec in self.config.search.param_space.items()
                },
                "relations": [asdict(rule) for rule in self.config.search.relations],
            },
            "constraints": asdict(self.config.constraints),
            "objective": {
                "primary": self.config.objective.primary,
                "mode": self.config.objective.mode,
                "secondary": [asdict(field) for field in self.config.objective.secondary],
            },
            "report": asdict(self.config.report),
        }
        if self.config.search.stages:
            payload["search"]["stages"] = [
                {
                    "name": stage.name,
                    "method": stage.method,
                    "trials": stage.trials,
                    "seed": stage.seed,
                    "top_k": stage.top_k,
                    "source_top_n": stage.source_top_n,
                    "radius_steps": stage.radius_steps,
                    "relations": [asdict(rule) for rule in stage.relations],
                    "param_space": {
                        name: self._param_spec_to_payload(spec)
                        for name, spec in stage.param_space.items()
                    },
                }
                for stage in self.config.search.stages
            ]
        return payload

    @staticmethod
    def _param_spec_to_payload(spec: SearchParamSpec) -> dict[str, Any]:
        payload = {"type": spec.type}
        if spec.values:
            payload["values"] = list(spec.values)
        else:
            payload["min"] = spec.min
            payload["max"] = spec.max
            payload["step"] = spec.step
        return payload

    @staticmethod
    def _now_text() -> str:
        from datetime import datetime

        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
