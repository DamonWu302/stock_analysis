from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class SearchParamSpec:
    type: str
    min: float | int | None = None
    max: float | int | None = None
    step: float | int | None = None
    values: list[Any] = field(default_factory=list)


@dataclass(slots=True)
class ConstraintSpec:
    min_trade_count: int = 0
    max_drawdown_lte: float | None = None
    min_win_rate: float | None = None


@dataclass(slots=True)
class ObjectiveField:
    field: str
    mode: str


@dataclass(slots=True)
class ObjectiveSpec:
    primary: str
    mode: str = "max"
    secondary: list[ObjectiveField] = field(default_factory=list)


@dataclass(slots=True)
class ReportSpec:
    save_csv: bool = True
    save_json: bool = True
    save_md: bool = True


@dataclass(slots=True)
class RelationRule:
    left: str
    operator: str
    right: str | None = None
    value: float | int | bool | None = None
    message: str = ""


@dataclass(slots=True)
class StageSpec:
    name: str
    method: str = "random"
    trials: int = 100
    seed: int = 42
    top_k: int = 20
    param_space: dict[str, SearchParamSpec] = field(default_factory=dict)
    relations: list[RelationRule] = field(default_factory=list)
    source_top_n: int = 5
    radius_steps: int = 1


@dataclass(slots=True)
class SearchSpec:
    method: str = "random"
    trials: int = 100
    seed: int = 42
    top_k: int = 20
    progress_every: int = 10
    checkpoint_every: int = 20
    param_space: dict[str, SearchParamSpec] = field(default_factory=dict)
    relations: list[RelationRule] = field(default_factory=list)
    stages: list[StageSpec] = field(default_factory=list)


@dataclass(slots=True)
class OptimizerConfig:
    name: str
    workspace: str
    output_dir: str
    base_config: dict[str, Any]
    search: SearchSpec
    constraints: ConstraintSpec
    objective: ObjectiveSpec
    report: ReportSpec


@dataclass(slots=True)
class TrialResult:
    trial_id: int
    stage_name: str
    config: dict[str, Any]
    summary: dict[str, Any] | None
    run_id: int | None
    status: str
    error: str | None = None
    passed_constraints: bool = False
