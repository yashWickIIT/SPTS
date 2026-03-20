"""
evaluate.py
-----------
Automated research-ready evaluation for SPTS vs Baseline text-to-SQL.

Usage:
    python evaluate.py                          # run with defaults
    python evaluate.py --limit 10               # quick smoke test (10 questions)
    python evaluate.py --dataset data/my.json   # custom dataset
    python evaluate.py --output results/        # custom output directory
    python evaluate.py --delay 1.5              # seconds between LLM calls

Outputs (in --output directory):
    evaluation_log.json         per-question detail (SQL, results, errors)
    final_thesis_metrics.json   thesis-ready summary metrics
"""

import argparse
import json
import os
import sys
import time

# â”€â”€ Import path: works from project root (python evaluate.py) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from backend.grounding import ground_query
from backend.text_to_sql import baseline_text_to_sql, fix_sql_with_llm, spts_text_to_sql
from backend.database import execute_sql
from metrics_calculator import categorize_sqlite_error, compare_execution_results

try:
    from tqdm import tqdm
except ImportError:

    def tqdm(iterable, desc="", total=None):
        print(f"Starting: {desc}")
        for i, item in enumerate(iterable, 1):
            if i % 5 == 0 or (total and i == total):
                print(f"  Processed {i}/{total or '?'}...")
            yield item


ERROR_BUCKETS = [
    "Schema Error (No such table/column)",
    "Syntax Error",
    "Ambiguous Column Reference",
    "Invalid Function",
    "Execution Error (Other)",
]


def _empty_error_counts():
    return {k: 0 for k in ERROR_BUCKETS}


def evaluate(dataset_path: str, output_dir: str, limit: int | None, delay: float):
    if not os.path.exists(dataset_path):
        print(
            f"\nERROR: Dataset not found at '{dataset_path}'.\n"
            "Please provide a JSON file with question/SQL pairs using --dataset.\n"
            "Expected format:\n"
            '  [{"question": "How many schools?", "SQL": "SELECT COUNT(*) FROM schools"}, ...]\n'
        )
        sys.exit(1)

    with open(dataset_path, encoding="utf-8") as f:
        dataset = json.load(f)

    if not dataset:
        print("Dataset is empty.")
        sys.exit(1)

    if limit:
        dataset = dataset[:limit]

    os.makedirs(output_dir, exist_ok=True)

    total = len(dataset)
    results_log = []
    base_va = base_ex = spts_va = spts_ex = 0
    base_errors = _empty_error_counts()
    spts_errors = _empty_error_counts()

    print(f"\nRunning evaluation on {total} questions  (delay={delay}s between calls)")
    print(f"Dataset : {dataset_path}")
    print(f"Output  : {output_dir}\n")

    for item in tqdm(dataset, desc="Evaluating", total=total):
        question = item.get("question", "").strip()
        gold_sql = item.get("SQL", "").strip()
        if not question or not gold_sql:
            continue

        gold_res = execute_sql(gold_sql)

        base_response = baseline_text_to_sql(question)
        base_sql = base_response["sql"]
        base_res = execute_sql(base_sql)
        base_valid = base_res.get("success", False)
        base_accurate = compare_execution_results(gold_res, base_res)

        if base_valid:
            base_va += 1
        else:
            cat = categorize_sqlite_error(base_res.get("error"))
            base_errors[cat] = base_errors.get(cat, 0) + 1

        if base_accurate:
            base_ex += 1

        time.sleep(delay)

        # â”€â”€ SPTS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        _, mappings = ground_query(question)
        spts_response = spts_text_to_sql(question, mappings=mappings)
        spts_sql = spts_response["sql"]
        spts_res = execute_sql(spts_sql)

        # One auto-correction pass (mirrors production /query behaviour)
        if not spts_res.get("success", False):
            error_msg = spts_res.get("error", "Unknown error")
            fixed_sql = fix_sql_with_llm(
                question, spts_sql, error_msg, mappings=mappings
            )
            if "-- API Error:" not in fixed_sql:
                spts_sql = fixed_sql
                spts_res = execute_sql(spts_sql)

        spts_valid = spts_res.get("success", False)
        spts_accurate = compare_execution_results(gold_res, spts_res)

        if spts_valid:
            spts_va += 1
        else:
            cat = categorize_sqlite_error(spts_res.get("error"))
            spts_errors[cat] = spts_errors.get(cat, 0) + 1

        if spts_accurate:
            spts_ex += 1

        time.sleep(delay)

        results_log.append(
            {
                "question": question,
                "gold_sql": gold_sql,
                "difficulty": item.get("difficulty", "unknown"),
                "baseline": {
                    "generated_sql": base_sql,
                    "valid": base_valid,
                    "accurate": base_accurate,
                    "latency_ms": base_response["rationale"].get("latency_ms"),
                    "token_usage": base_response["rationale"].get("token_usage"),
                    "error": base_res.get("error") if not base_valid else None,
                },
                "spts": {
                    "generated_sql": spts_sql,
                    "valid": spts_valid,
                    "accurate": spts_accurate,
                    "latency_ms": spts_response["rationale"].get("latency_ms"),
                    "token_usage": spts_response["rationale"].get("token_usage"),
                    "mappings": mappings,
                    "error": spts_res.get("error") if not spts_valid else None,
                },
            }
        )

    base_va_rate = (base_va / total) * 100
    base_ex_rate = (base_ex / total) * 100
    spts_va_rate = (spts_va / total) * 100
    spts_ex_rate = (spts_ex / total) * 100

    metrics_payload = {
        "dataset": dataset_path,
        "dataset_size": total,
        "metrics": {
            "VA": {
                "baseline": round(base_va_rate, 2),
                "spts": round(spts_va_rate, 2),
                "improvement": round(spts_va_rate - base_va_rate, 2),
            },
            "EX": {
                "baseline": round(base_ex_rate, 2),
                "spts": round(spts_ex_rate, 2),
                "improvement": round(spts_ex_rate - base_ex_rate, 2),
            },
        },
        "errors": {
            "baseline": base_errors,
            "spts": spts_errors,
        },
    }

    log_path = os.path.join(output_dir, "evaluation_log.json")
    metrics_path = os.path.join(output_dir, "final_thesis_metrics.json")

    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(results_log, f, indent=4, ensure_ascii=False)

    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(metrics_payload, f, indent=4, ensure_ascii=False)

    w = 80
    print("\n" + "=" * w)
    print(f"  SPTS THESIS EVALUATION  (N={total})")
    print("=" * w)
    print(f"  {'Metric':<32} {'Baseline':>10}   {'SPTS':>10}   {'Delta':>10}")
    print("  " + "-" * (w - 2))
    print(
        f"  {'Valid SQL Rate (VA)':<32} {base_va_rate:>9.2f}%  {spts_va_rate:>9.2f}%  {spts_va_rate - base_va_rate:>+9.2f}%"
    )
    print(
        f"  {'Execution Accuracy (EX)':<32} {base_ex_rate:>9.2f}%  {spts_ex_rate:>9.2f}%  {spts_ex_rate - base_ex_rate:>+9.2f}%"
    )
    print()
    print(
        f"  {'Error Category':<32} {'Baseline':>10}   {'SPTS':>10}   {'Reduction':>10}"
    )
    print("  " + "-" * (w - 2))
    for cat in ERROR_BUCKETS:
        b = base_errors.get(cat, 0)
        s = spts_errors.get(cat, 0)
        print(f"  {cat:<32} {b:>10}   {s:>10}   {b - s:>+10}")
    print("=" * w)
    print(f"\n  Logs  -> {log_path}")
    print(f"  Metrics -> {metrics_path}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Run SPTS automated evaluation against a gold SQL dataset."
    )
    parser.add_argument(
        "--dataset",
        default=os.path.join("data", "bird_dev_sample.json"),
        help="Path to evaluation dataset JSON (default: data/bird_dev_sample.json)",
    )
    parser.add_argument(
        "--output",
        default=".",
        help="Directory to write evaluation_log.json and final_thesis_metrics.json (default: project root)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max questions to evaluate (omit for full dataset)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=2.0,
        help="Seconds to sleep between LLM calls to avoid rate limiting (default: 2.0)",
    )
    args = parser.parse_args()
    evaluate(args.dataset, args.output, args.limit, args.delay)


if __name__ == "__main__":
    main()
