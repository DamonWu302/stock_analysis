from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stock_analysis.optimizer_llm import OptimizerLLMReviewService


def main() -> None:
    parser = argparse.ArgumentParser(description="使用大模型复盘参数优化结果")
    parser.add_argument("output_dir", help="优化器输出目录")
    parser.add_argument("--top-n", type=int, default=10, help="送入大模型的前 N 组最优结果")
    parser.add_argument("--reuse-review", action="store_true", help="不重新请求模型，直接复用已有 review 文件生成合并配置")
    parser.add_argument(
        "--prompt-template",
        default=str(ROOT / "optimizer_llm_prompt.md"),
        help="提示词模板路径",
    )
    args = parser.parse_args()

    service = OptimizerLLMReviewService(prompt_template_path=args.prompt_template)
    if args.reuse_review:
        result = service.apply_existing_review(args.output_dir)
        print(f"merged_config_path={result['merged_config_path']}")
        print(f"memory_json_path={result['memory_json_path']}")
        print(f"memory_md_path={result['memory_md_path']}")
    else:
        result = service.review(args.output_dir, top_n=max(args.top_n, 1))
        print(f"json_path={result['json_path']}")
        print(f"md_path={result['md_path']}")
        print(f"merged_config_path={result['merged_config_path']}")
        print(f"memory_json_path={result['memory_json_path']}")
        print(f"memory_md_path={result['memory_md_path']}")


if __name__ == "__main__":
    main()
