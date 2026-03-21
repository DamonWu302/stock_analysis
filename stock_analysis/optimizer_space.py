from __future__ import annotations

import itertools
import math
import random
from typing import Any

from .optimizer_types import SearchParamSpec


def build_candidate_values(spec: SearchParamSpec) -> list[Any]:
    if spec.values:
        return list(spec.values)
    if spec.min is None or spec.max is None or spec.step in (None, 0):
        raise ValueError(f"参数范围定义不完整: {spec}")
    values: list[Any] = []
    current = float(spec.min)
    end = float(spec.max)
    step = float(spec.step)
    while current <= end + 1e-9:
        if spec.type == "int":
            values.append(int(round(current)))
        elif spec.type == "bool":
            values.append(bool(round(current)))
        else:
            values.append(round(current, 6))
        current += step
    return values


def sample_random_trials(param_space: dict[str, SearchParamSpec], trials: int, seed: int) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    pools = {name: build_candidate_values(spec) for name, spec in param_space.items()}
    results: list[dict[str, Any]] = []
    seen: set[tuple[tuple[str, Any], ...]] = set()
    max_unique = math.prod(max(len(values), 1) for values in pools.values()) if pools else 1
    target = min(trials, max_unique)
    while len(results) < target:
        payload = {name: rng.choice(values) for name, values in pools.items()}
        key = tuple(sorted(payload.items()))
        if key in seen:
            continue
        seen.add(key)
        results.append(payload)
    return results


def build_grid_trials(param_space: dict[str, SearchParamSpec], limit: int | None = None) -> list[dict[str, Any]]:
    pools = {name: build_candidate_values(spec) for name, spec in param_space.items()}
    ordered_names = list(pools.keys())
    ordered_values = [pools[name] for name in ordered_names]
    results: list[dict[str, Any]] = []
    for combo in itertools.product(*ordered_values):
        results.append(dict(zip(ordered_names, combo)))
        if limit is not None and len(results) >= limit:
            break
    return results
