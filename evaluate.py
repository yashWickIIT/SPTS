import os
import sys
import json
import time

# Append backend directory so we can import the modules
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

from backend.grounding import ground_query
from backend.text_to_sql import baseline_text_to_sql, spts_text_to_sql
from backend.database import execute_sql
from metrics_calculator import compare_execution_results, categorize_sqlite_error

try:
    from tqdm import tqdm
except ImportError:
    print("tqdm not installed, falling back to basic progress prints.")
    def tqdm(iterable, desc="", total=None):
        print(f"Starting: {desc}")
        for i, item in enumerate(iterable, 1):
            if i % 5 == 0 or (total and i == total):
                print(f"Processed {i}/{total or '?'}...")
            yield item

# Removed the old parse_results_for_comparison since compare_execution_results handles normalization now

def evaluate():
    data_path = os.path.join(os.path.dirname(__file__), "data", "bird_dev_sample.json")
    if not os.path.exists(data_path):
        print(f"Error: Dataset not found at {data_path}. Please create it to run the evaluation.")
        return

    with open(data_path, "r", encoding="utf-8") as f:
        dataset = json.load(f)

    if not dataset:
        print("Dataset is empty.")
        return

    total_questions = len(dataset)
    results_log = []

    # Counters
    base_va_count = 0
    base_ex_count = 0
    spts_va_count = 0
    spts_ex_count = 0
    
    # Error classification counters
    base_errors = {
        "Schema Error (No such table/column)": 0,
        "Syntax Error": 0,
        "Ambiguous Column Reference": 0,
        "Invalid Function": 0,
        "Execution Error (Other)": 0
    }
    spts_errors = {
        "Schema Error (No such table/column)": 0,
        "Syntax Error": 0,
        "Ambiguous Column Reference": 0,
        "Invalid Function": 0,
        "Execution Error (Other)": 0
    }

    print(f"Starting evaluation on {total_questions} questions...")

    for item in tqdm(dataset, desc="Evaluating Text-to-SQL Pipelines", total=total_questions):
        question = item.get("question")
        gold_sql = item.get("SQL")

        # 1. Execute Gold SQL
        gold_res = execute_sql(gold_sql)

        # 2. Evaluate Baseline
        base_sql = baseline_text_to_sql(question)
        base_res = execute_sql(base_sql)
        base_is_valid = base_res.get("success", False)
        
        base_is_accurate = compare_execution_results(gold_res, base_res)

        if base_is_valid:
            base_va_count += 1
        else:
            cat = categorize_sqlite_error(base_res.get("error"))
            base_errors[cat] = base_errors.get(cat, 0) + 1
            
        if base_is_accurate:
            base_ex_count += 1
            
        time.sleep(2)  # Avoid rate limiting

        # 3. Evaluate SPTS
        _, mappings = ground_query(question)
        spts_sql = spts_text_to_sql(question, mappings=mappings)
        spts_res = execute_sql(spts_sql)
        
        # Self-correction check: if failed, try once more just like in app.py
        if not spts_res.get("success", False):
            # Attempt to import fix func if it exists, otherwise skip
            try:
                from backend.text_to_sql import fix_sql_with_llm
                error_msg = spts_res.get("error", "Unknown error")
                spts_sql = fix_sql_with_llm(question, spts_sql, error_msg, mappings=mappings)
                spts_res = execute_sql(spts_sql)
            except ImportError:
                pass

        spts_is_valid = spts_res.get("success", False)
        spts_is_accurate = compare_execution_results(gold_res, spts_res)

        if spts_is_valid:
            spts_va_count += 1
        else:
            cat = categorize_sqlite_error(spts_res.get("error"))
            spts_errors[cat] = spts_errors.get(cat, 0) + 1
            
        if spts_is_accurate:
            spts_ex_count += 1

        time.sleep(2)  # Avoid rate limiting
        
        # Log item
        results_log.append({
            "question": question,
            "gold_sql": gold_sql,
            "baseline": {
                "generated_sql": base_sql,
                "valid": base_is_valid,
                "accurate": base_is_accurate,
                "error": base_res.get("error") if not base_is_valid else None
            },
            "spts": {
                "generated_sql": spts_sql,
                "valid": spts_is_valid,
                "accurate": spts_is_accurate,
                "mappings": mappings,
                "error": spts_res.get("error") if not spts_is_valid else None
            }
        })

    # Metrics computation
    base_va_rate = (base_va_count / total_questions) * 100
    base_ex_rate = (base_ex_count / total_questions) * 100
    spts_va_rate = (spts_va_count / total_questions) * 100
    spts_ex_rate = (spts_ex_count / total_questions) * 100

    # Save detailed log map
    log_path = os.path.join(os.path.dirname(__file__), "evaluation_log.json")
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(results_log, f, indent=4)

    # Save thesis metrics
    thesis_path = os.path.join(os.path.dirname(__file__), "final_thesis_metrics.json")
    metrics_payload = {
        "dataset_size": total_questions,
        "metrics": {
            "VA": {
                "baseline": base_va_rate,
                "spts": spts_va_rate,
                "improvement": spts_va_rate - base_va_rate
            },
            "EX": {
                "baseline": base_ex_rate,
                "spts": spts_ex_rate,
                "improvement": spts_ex_rate - base_ex_rate
            }
        },
        "errors": {
            "baseline": base_errors,
            "spts": spts_errors
        }
    }
    with open(thesis_path, "w", encoding="utf-8") as f:
        json.dump(metrics_payload, f, indent=4)

    # Print Thesis-Ready Summary Table
    print("\n" + "="*80)
    print(f"SPTS THESIS EVALUATION METRICS (N={total_questions})")
    print("="*80)
    print(f"| {'Metric':<30} | {'Baseline':<12} | {'SPTS':<12} | {'Delta':<12} |")
    print("|" + "-"*32 + "|" + "-"*14 + "|" + "-"*14 + "|" + "-"*14 + "|")
    print(f"| {'Valid SQL (VA) Rate':<30} | {base_va_rate:>11.2f}% | {spts_va_rate:>11.2f}% | {spts_va_rate - base_va_rate:>11.2f}% |")
    print(f"| {'Execution Accuracy (EX) Rate':<30} | {base_ex_rate:>11.2f}% | {spts_ex_rate:>11.2f}% | {spts_ex_rate - base_ex_rate:>11.2f}% |")
    
    print("\n| ERROR BREAKDOWN VERIFICATION")
    print("|" + "-"*79)
    print(f"| {'Error Category':<30} | {'Baseline':<12} | {'SPTS':<12} | {'Reduction':<12} |")
    print("|" + "-"*32 + "|" + "-"*14 + "|" + "-"*14 + "|" + "-"*14 + "|")
    
    for error_cat in base_errors.keys():
        b_err = base_errors.get(error_cat, 0)
        s_err = spts_errors.get(error_cat, 0)
        reduction = b_err - s_err
        print(f"| {error_cat:<30} | {b_err:>12} | {s_err:>12} | {reduction:>12} |")
        
    print("="*80)
    print(f"Detailed logs saved to:\n- {log_path}\n- {thesis_path}")

if __name__ == "__main__":
    evaluate()
