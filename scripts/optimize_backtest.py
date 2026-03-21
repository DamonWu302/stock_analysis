from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stock_analysis.optimizer import BacktestOptimizer, load_optimizer_config


def main() -> None:
    parser = argparse.ArgumentParser(description="运行回测参数优化器")
    parser.add_argument("config", help="优化器配置 JSON 文件路径")
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    config = load_optimizer_config(config_path)
    optimizer = BacktestOptimizer(config)
    result = optimizer.run()

    print(f"trial_count={result['trial_count']}")
    print(f"completed_count={result['completed_count']}")
    print(f"skipped_count={result['skipped_count']}")
    print(f"failed_count={result['failed_count']}")
    print(f"ranked_count={result['ranked_count']}")
    print(f"best_count={result['best_count']}")
    print(f"output_dir={result['output_dir']}")
    print(f"csv_path={result['csv_path']}")
    print(f"json_path={result['json_path']}")
    print(f"md_path={result['md_path']}")
    print(f"importance_path={result['importance_path']}")
    print(f"next_round_path={result['next_round_path']}")
    for index, trial in enumerate(result["best_trials"][:5], start=1):
        summary = trial.summary or {}
        print(
            f"top{index}: trial={trial.trial_id} run_id={trial.run_id} "
            f"total_return={summary.get('total_return', 0)} "
            f"max_drawdown={summary.get('max_drawdown', 0)} "
            f"trade_count={summary.get('trade_count', 0)}"
        )


if __name__ == "__main__":
    main()
