import argparse
import json
import os
import sqlite3
import sys
import time

from metrics_calculator import compare_execution_results, evaluate_etm

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from backend.grounding import ground_query
    from backend.text_to_sql import baseline_text_to_sql, fix_sql_with_llm, spts_text_to_sql
except ImportError:
    print("Error: Could not import evaluator dependencies from backend.")
    print("Please ensure you are running this from the project root directory.")
    exit(1)

API_DELAY_SECONDS = 2.0
DEFAULT_DB_ID = "bird_mini_dev"
DEFAULT_DATASET = os.path.join("data", "bird_dev_sample.json")
DEFAULT_LOG = "evaluation_log.json"
DEFAULT_METRICS = "final_thesis_metrics.json"


def _candidate_db_paths(db_id: str):
    return (
        f"{db_id}.sqlite",
        os.path.join("data", f"{db_id}.sqlite"),
        os.path.join(".", "data", f"{db_id}.sqlite"),
        "bird_mini_dev.sqlite",
        os.path.join("data", "bird_mini_dev.sqlite"),
    )


def resolve_db_path(db_id: str) -> str | None:
    for path in _candidate_db_paths(db_id):
        if os.path.exists(path):
            return path
    return None


def execute_raw_sql(sql: str, db_id: str) -> dict:
    if not sql or not sql.strip():
        return {"success": False, "error": "Empty SQL query", "data": []}

    db_path = resolve_db_path(db_id)
    if not db_path:
        return {"success": False, "error": f"Database {db_id}.sqlite not found.", "data": []}

    try:
        uri = f"file:{os.path.abspath(db_path)}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            return {"success": True, "data": cursor.fetchall()}
        finally:
            conn.close()
    except sqlite3.Error as error:
        return {"success": False, "error": str(error), "data": []}


def _safe_sql_from_response(response) -> str:
    if isinstance(response, dict):
        return str(response.get("sql", "") or "")
    return str(response or "")


def _generate_baseline_sql(question: str) -> str:
    try:
        return _safe_sql_from_response(baseline_text_to_sql(question))
    except Exception as error:
        print(f"  -> Baseline generation failed: {error}")
        return ""


def _generate_spts_sql(question: str) -> tuple[str, list[dict]]:
    mappings = []
    try:
        _, mappings = ground_query(question)
        sql = _safe_sql_from_response(spts_text_to_sql(question, mappings=mappings))
        return sql, mappings
    except Exception as error:
        print(f"  -> SPTS generation failed: {error}")
        return "", mappings


def _auto_correct_spts(
    question: str,
    sql: str,
    result: dict,
    mappings: list[dict],
    db_id: str,
) -> tuple[str, dict]:
    if result.get("success"):
        return sql, result
    try:
        fixed_sql = fix_sql_with_llm(question, sql, result.get("error", "Unknown error"), mappings=mappings)
    except Exception as error:
        print(f"  -> SPTS auto-correction failed: {error}")
        return sql, result

    if not fixed_sql or "-- API Error:" in fixed_sql:
        return sql, result

    fixed_result = execute_raw_sql(fixed_sql, db_id)
    return fixed_sql, fixed_result


def _evaluate_prediction(gold_result: dict, gold_sql: str, predicted_sql: str, db_id: str) -> tuple[dict, dict]:
    pred_result = execute_raw_sql(predicted_sql, db_id)
    etm = evaluate_etm(gold_sql, predicted_sql)
    metrics = {
        "execution_accuracy": compare_execution_results(gold_result, pred_result),
        "etm_exact_match": etm["is_exact_match"],
        "etm_f1_score": round(etm["f1_score"], 4),
    }
    return pred_result, metrics


def _empty_summary() -> dict:
    return {
        "exe_correct": 0,
        "etm_exact": 0,
        "etm_f1_total": 0.0,
        "scored_queries": 0,
        "api_error_queries": 0,
    }


def _update_summary(summary: dict, metrics: dict):
    summary["exe_correct"] += int(metrics["execution_accuracy"])
    summary["etm_exact"] += int(metrics["etm_exact_match"])
    summary["etm_f1_total"] += metrics["etm_f1_score"]
    summary["scored_queries"] += 1


def _percentage(value: float, total: int) -> float:
    return round((value / total) * 100, 2) if total else 0.0


def _finalize_summary(summary: dict) -> dict:
    scored_queries = summary["scored_queries"]
    return {
        "Execution_Accuracy_EXE": _percentage(summary["exe_correct"], scored_queries),
        "Execution_Accuracy_EXE_Count": summary["exe_correct"],
        "Enhanced_Tree_Matching_ETM_Exact": _percentage(summary["etm_exact"], scored_queries),
        "Enhanced_Tree_Matching_ETM_Exact_Count": summary["etm_exact"],
        "Average_Structural_F1_Score": _percentage(summary["etm_f1_total"], scored_queries),
        "Average_Structural_F1_Score_Sum": round(summary["etm_f1_total"], 4),
        "Scored_Queries": scored_queries,
        "API_Error_Queries": summary["api_error_queries"],
    }


def _extract_api_error(sql_text: str) -> str | None:
    marker = "-- API Error:"
    if not isinstance(sql_text, str) or marker not in sql_text:
        return None
    return sql_text.split(marker, 1)[1].strip() or "LLM API unavailable"


def _improvement_block(baseline: dict, spts: dict) -> dict:
    return {
        "Execution_Accuracy_EXE": round(spts["Execution_Accuracy_EXE"] - baseline["Execution_Accuracy_EXE"], 2),
        "Enhanced_Tree_Matching_ETM_Exact": round(spts["Enhanced_Tree_Matching_ETM_Exact"] - baseline["Enhanced_Tree_Matching_ETM_Exact"], 2),
        "Average_Structural_F1_Score": round(spts["Average_Structural_F1_Score"] - baseline["Average_Structural_F1_Score"], 2),
    }


def _build_log_entry(
    query_id: int,
    question: str,
    db_id: str,
    gold_sql: str,
    baseline_sql: str,
    baseline_result: dict,
    baseline_metrics: dict,
    spts_sql: str,
    spts_result: dict,
    spts_metrics: dict,
    mappings: list[dict],
) -> dict:
    return {
        "query_id": query_id,
        "question": question,
        "db_id": db_id,
        "gold_sql": gold_sql,
        "baseline": {
            "predicted_sql": baseline_sql,
            "execution_result": {
                "pred_success": baseline_result.get("success"),
                "pred_error": baseline_result.get("error"),
            },
            "metrics": baseline_metrics,
        },
        "spts": {
            "predicted_sql": spts_sql,
            "execution_result": {
                "pred_success": spts_result.get("success"),
                "pred_error": spts_result.get("error"),
            },
            "metrics": spts_metrics,
            "mappings": mappings,
        },
    }


def load_dataset(test_data_path: str) -> list[dict]:
    with open(test_data_path, "r", encoding="utf-8") as file:
        return json.load(file)


def run_evaluation(
    test_data_path: str,
    output_log_path: str,
    final_metrics_path: str,
    delay_seconds: float = API_DELAY_SECONDS,
):
    print(f"Loading test dataset from {test_data_path}...")
    try:
        test_queries = load_dataset(test_data_path)
    except FileNotFoundError:
        print(f"Error: {test_data_path} not found. Please ensure the file exists.")
        return

    evaluation_logs = []
    total_queries = len(test_queries)
    baseline_summary = _empty_summary()
    spts_summary = _empty_summary()

    print(f"Starting evaluation of {total_queries} queries...\n")

    for index, item in enumerate(test_queries, start=1):
        question = item.get("question", "")
        gold_sql = item.get("SQL", "")
        db_id = item.get("db_id", DEFAULT_DB_ID)

        print(f"Processing [{index}/{total_queries}]: {question[:50]}...")

        gold_result = execute_raw_sql(gold_sql, db_id)

        baseline_sql = _generate_baseline_sql(question)
        baseline_api_error = _extract_api_error(baseline_sql)
        if baseline_api_error:
            baseline_result = {"success": False, "error": baseline_api_error}
            baseline_metrics = {
                "execution_accuracy": False,
                "etm_exact_match": False,
                "etm_f1_score": 0.0,
                "api_error": True,
            }
            baseline_summary["api_error_queries"] += 1
        else:
            baseline_result, baseline_metrics = _evaluate_prediction(gold_result, gold_sql, baseline_sql, db_id)
            baseline_metrics["api_error"] = False
            _update_summary(baseline_summary, baseline_metrics)

        if delay_seconds:
            time.sleep(delay_seconds)

        spts_sql, mappings = _generate_spts_sql(question)
        spts_result, spts_metrics = _evaluate_prediction(gold_result, gold_sql, spts_sql, db_id)
        spts_sql, spts_result = _auto_correct_spts(question, spts_sql, spts_result, mappings, db_id)

        spts_api_error = _extract_api_error(spts_sql)
        if spts_api_error:
            spts_result = {"success": False, "error": spts_api_error}
            spts_metrics = {
                "execution_accuracy": False,
                "etm_exact_match": False,
                "etm_f1_score": 0.0,
                "api_error": True,
            }
            spts_summary["api_error_queries"] += 1
        else:
            spts_metrics = _evaluate_prediction(gold_result, gold_sql, spts_sql, db_id)[1]
            spts_metrics["api_error"] = False
            _update_summary(spts_summary, spts_metrics)

        evaluation_logs.append(
            _build_log_entry(
                query_id=index,
                question=question,
                db_id=db_id,
                gold_sql=gold_sql,
                baseline_sql=baseline_sql,
                baseline_result=baseline_result,
                baseline_metrics=baseline_metrics,
                spts_sql=spts_sql,
                spts_result=spts_result,
                spts_metrics=spts_metrics,
                mappings=mappings,
            )
        )

        if index < total_queries and delay_seconds:
            time.sleep(delay_seconds)

    with open(output_log_path, "w", encoding="utf-8") as file:
        json.dump(evaluation_logs, file, indent=4)

    baseline_metrics = _finalize_summary(baseline_summary)
    spts_metrics = _finalize_summary(spts_summary)
    final_metrics = {
        "Total_Queries_Tested": total_queries,
        "Baseline": baseline_metrics,
        "SPTS": spts_metrics,
        "Improvement_SPTS_minus_Baseline": _improvement_block(baseline_metrics, spts_metrics),
    }

    with open(final_metrics_path, "w", encoding="utf-8") as file:
        json.dump(final_metrics, file, indent=4)

    print("\nEvaluation Complete!")
    print(f"  Baseline EXE: {final_metrics['Baseline']['Execution_Accuracy_EXE']}% ({final_metrics['Baseline']['Execution_Accuracy_EXE_Count']}/{final_metrics['Baseline']['Scored_Queries']})")
    print(f"  Baseline ETM: {final_metrics['Baseline']['Enhanced_Tree_Matching_ETM_Exact']}% ({final_metrics['Baseline']['Enhanced_Tree_Matching_ETM_Exact_Count']}/{final_metrics['Baseline']['Scored_Queries']})")
    print(f"  Baseline F1:  {final_metrics['Baseline']['Average_Structural_F1_Score']}%")
    print(f"  SPTS EXE:     {final_metrics['SPTS']['Execution_Accuracy_EXE']}% ({final_metrics['SPTS']['Execution_Accuracy_EXE_Count']}/{final_metrics['SPTS']['Scored_Queries']})")
    print(f"  SPTS ETM:     {final_metrics['SPTS']['Enhanced_Tree_Matching_ETM_Exact']}% ({final_metrics['SPTS']['Enhanced_Tree_Matching_ETM_Exact_Count']}/{final_metrics['SPTS']['Scored_Queries']})")
    print(f"  SPTS F1:      {final_metrics['SPTS']['Average_Structural_F1_Score']}%")
    print(f"  Delta EXE:    {final_metrics['Improvement_SPTS_minus_Baseline']['Execution_Accuracy_EXE']}%")
    print(f"  Delta ETM:    {final_metrics['Improvement_SPTS_minus_Baseline']['Enhanced_Tree_Matching_ETM_Exact']}%")
    print(f"  Delta F1:     {final_metrics['Improvement_SPTS_minus_Baseline']['Average_Structural_F1_Score']}%")
    print(f"  Baseline API errors: {final_metrics['Baseline']['API_Error_Queries']}")
    print(f"  SPTS API errors:     {final_metrics['SPTS']['API_Error_Queries']}")
    print(f"Logs saved to {output_log_path} and {final_metrics_path}")


def main():
    parser = argparse.ArgumentParser(description="Run baseline vs SPTS evaluation with EXE and ETM metrics.")
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="Path to the evaluation dataset JSON.")
    parser.add_argument("--log", default=DEFAULT_LOG, help="Path to write per-query evaluation logs.")
    parser.add_argument("--metrics", default=DEFAULT_METRICS, help="Path to write summary metrics JSON.")
    parser.add_argument("--delay", type=float, default=API_DELAY_SECONDS, help="Delay between LLM calls in seconds.")
    args = parser.parse_args()
    run_evaluation(args.dataset, args.log, args.metrics, args.delay)


if __name__ == "__main__":
    main()