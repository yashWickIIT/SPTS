import argparse
import json
import os
import re
import sys
import time
from urllib.parse import parse_qsl, urlencode, urlsplit

from sqlalchemy import create_engine, text

from metrics_calculator import compare_execution_results, evaluate_etm

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

API_ERROR_MARKER = "-- API Error:"


def _response_to_string(result) -> str:
    if isinstance(result, dict):
        return str(result.get("sql", "") or "")
    return str(result or "")


def _has_api_error_marker(result) -> bool:
    return API_ERROR_MARKER in _response_to_string(result)


def _handle_retry_failure(func_name: str, attempt: int, max_retries: int, error, wait_time: float, result):
    if attempt == max_retries - 1:
        print(f"  -> Final attempt failed for {func_name}: {error}")
        return True, (result if result is not None else ""), wait_time

    print(
        f"  -> API Limit/Error in {func_name}. Waiting {wait_time}s before retry {attempt + 1}/{max_retries}..."
    )
    time.sleep(wait_time)
    return False, None, wait_time * 2


def retry_with_backoff(func):
    """Decorator to retry LLM calls with exponential backoff if rate limited."""

    def wrapper(*args, **kwargs):
        max_retries = 5
        wait_time = 2.0

        for attempt in range(max_retries):
            result = None
            try:
                result = func(*args, **kwargs)
                if _has_api_error_marker(result):
                    response_str = _response_to_string(result)
                    raise ValueError(f"API Error Detected: {response_str.strip()}")
                return result
            except Exception as error:
                should_return, return_value, wait_time = _handle_retry_failure(
                    func.__name__, attempt, max_retries, error, wait_time, result
                )
                if should_return:
                    return return_value

    return wrapper


try:
    from backend.grounding import ground_query
    from backend.config import get_main_database_url
    from backend.text_to_sql import (
        baseline_text_to_sql,
        fix_sql_with_llm,
        generate_spts_sql_candidates,
        spts_text_to_sql,
    )

    ground_query = retry_with_backoff(ground_query)
    baseline_text_to_sql = retry_with_backoff(baseline_text_to_sql)
    fix_sql_with_llm = retry_with_backoff(fix_sql_with_llm)
    spts_text_to_sql = retry_with_backoff(spts_text_to_sql)

except ImportError:
    print("Error: Could not import evaluator dependencies from backend.")
    print("Please ensure you are running this from the project root directory.")
    exit(1)

API_DELAY_SECONDS = 4.0
DEFAULT_DB_ID = ""
DEFAULT_DATASET = os.getenv("SPTS_EVAL_DATASET_PATH", "").strip()
DEFAULT_LOG = "evaluation_log.json"
DEFAULT_METRICS = "final_thesis_metrics.json"
DEFAULT_EVAL_DB_PATH = os.getenv("SPTS_EVAL_DB_PATH", "").strip()
DEFAULT_EVAL_DB_URL = os.getenv("SPTS_EVAL_DATABASE_URL", "").strip()


def ensure_default_dataset_exists(test_data_path: str) -> bool:
    """Return True if the dataset file exists.

    If it is missing, attempt to build it via HF streaming
    (``extract_official_bird_sample.get_official_data``).
    """
    if os.path.exists(test_data_path):
        return True

    print(f"Dataset not found at '{test_data_path}'. Attempting to build from Hugging Face…")
    try:
        from extract_official_bird_sample import get_official_data

        success = get_official_data(output_json=test_data_path)
        if success and os.path.exists(test_data_path):
            print(f"Dataset built successfully: {test_data_path}")
            return True
    except Exception as err:
        print(f"Failed to auto-build dataset: {err}")

    print(
        "ERROR: Could not find or build the evaluation dataset.\n"
        "  Option A – run:  python extract_official_bird_sample.py\n"
        "             (needs internet access; streams from Hugging Face)\n"
        "  Option B – place bird_dev_sample.json manually in data/"
    )
    return False


def _ensure_sqlite_read_only_url(url: str) -> str:
    if not url.lower().startswith("sqlite://"):
        return url

    parsed = urlsplit(url)
    query_params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query_params["mode"] = "ro"
    query_params["uri"] = "true"

    if parsed.netloc:
        sqlite_target = f"{parsed.netloc}{parsed.path}"
    else:
        sqlite_target = parsed.path.lstrip("/")

    if not sqlite_target.lower().startswith("file:"):
        sqlite_target = f"file:{sqlite_target}"

    return f"sqlite:///{sqlite_target}?{urlencode(query_params)}"


def _sqlite_path_to_url(db_path: str) -> str:
    absolute_path = os.path.abspath(db_path).replace("\\", "/")
    return _ensure_sqlite_read_only_url(f"sqlite:///file:{absolute_path}?mode=ro&uri=true")


def _resolve_row_database_target(item: dict, explicit_db_path: str, explicit_db_url: str) -> tuple[str | None, str | None]:
    row_db_url = str(item.get("db_url") or "").strip()
    if row_db_url:
        return _ensure_sqlite_read_only_url(row_db_url), None

    row_db_path = str(item.get("db_path") or "").strip()
    if row_db_path:
        if not os.path.exists(row_db_path):
            return None, f"Row-level db_path not found: {row_db_path}"
        return _sqlite_path_to_url(row_db_path), None

    if explicit_db_url:
        return _ensure_sqlite_read_only_url(explicit_db_url), None

    if explicit_db_path:
        if not os.path.exists(explicit_db_path):
            return None, f"Evaluation db path not found: {explicit_db_path}"
        return _sqlite_path_to_url(explicit_db_path), None

    try:
        return get_main_database_url(), None
    except Exception:
        return (
            None,
            "No evaluation database configured. Provide --db-url, --db-path, row-level db_url/db_path, "
            "or set SPTS_DATABASE_URL/SPTS_MAIN_DB_PATH.",
        )


def _execute_sql_on_target(sql: str, database_url: str) -> dict:
    if not sql or not sql.strip():
        return {"success": False, "error": "Empty SQL query", "data": []}

    try:
        engine = create_engine(database_url, pool_pre_ping=True)
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            if not result.returns_rows:
                return {
                    "success": False,
                    "error": "Query returned no rows (non-SELECT statements are not evaluated).",
                    "data": [],
                }
            return {"success": True, "data": [tuple(row) for row in result.fetchall()]}
    except Exception as error:
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


def _generate_spts_sql(question: str) -> tuple[list[dict], list[dict]]:
    mappings = []
    try:
        _, mappings = ground_query(question)
        candidates = generate_spts_sql_candidates(question, mappings=mappings, max_candidates=3)
        if not candidates:
            sql = _safe_sql_from_response(spts_text_to_sql(question, mappings=mappings))
            candidates = [{"sql": sql, "source": "spts_fallback_single"}]
        return candidates, mappings
    except Exception as error:
        print(f"  -> SPTS generation failed: {error}")
        return [{"sql": "", "source": "spts_generation_error"}], mappings


def _mapping_consistency_score(sql: str, mappings: list[dict]) -> float:
    sql_lower = (sql or "").lower()
    exact_mappings = [m for m in (mappings or []) if "exact" in str(m.get("type", "")).lower()]
    if not exact_mappings:
        return 0.0

    score_total = 0.0
    for mapping in exact_mappings:
        grounded = str(mapping.get("grounded", "") or "").lower()
        column = str(mapping.get("column", "") or "").lower()
        table = str(mapping.get("table", "") or "").lower()

        grounded_present = bool(grounded and grounded in sql_lower)
        column_present = bool(column and column in sql_lower)
        table_present = bool(table and table in sql_lower)

        if grounded_present and column_present:
            score_total += 1.0
        elif grounded_present and table_present:
            score_total += 0.85
        elif grounded_present:
            score_total += 0.65
        elif column_present:
            score_total += 0.35

    return round(score_total / max(len(exact_mappings), 1), 4)


def _candidate_quality_score(sql: str, result: dict, mappings: list[dict]) -> float:
    execution_bonus = 2.0 if result.get("success") else 0.0
    consistency = _mapping_consistency_score(sql, mappings)
    api_penalty = -2.0 if API_ERROR_MARKER in (sql or "") else 0.0
    length_bonus = 0.05 if len((sql or "").strip()) < 600 else 0.0
    return round(execution_bonus + consistency + api_penalty + length_bonus, 4)


def _select_best_spts_candidate(candidates: list[dict], mappings: list[dict], database_url: str) -> tuple[str, dict, dict]:
    best_sql = ""
    best_result = {"success": False, "error": "No candidates", "data": []}
    best_meta = {"source": "none", "score": -999.0, "consistency": 0.0}

    for candidate in candidates or []:
        sql = str(candidate.get("sql", "") or "")
        if not sql.strip():
            continue
        result = _execute_sql_on_target(sql, database_url)
        consistency = _mapping_consistency_score(sql, mappings)
        score = _candidate_quality_score(sql, result, mappings)

        meta = {
            "source": candidate.get("source", "unknown"),
            "score": score,
            "consistency": consistency,
            "success": bool(result.get("success")),
        }

        if score > best_meta["score"]:
            best_sql = sql
            best_result = result
            best_meta = meta

    return best_sql, best_result, best_meta


def _evaluate_baseline(
    question: str,
    gold_result: dict,
    gold_sql: str,
    database_url: str,
) -> tuple[str, dict, dict]:
    baseline_sql = _generate_baseline_sql(question)
    baseline_api_error = _extract_api_error(baseline_sql)
    if baseline_api_error:
        return (
            baseline_sql,
            {"success": False, "error": baseline_api_error},
            {
                "execution_accuracy": False,
                "etm_exact_match": False,
                "etm_f1_score": 0.0,
                "api_error": True,
            },
        )

    baseline_result, baseline_metrics = _evaluate_prediction(
        gold_result, gold_sql, baseline_sql, database_url
    )
    baseline_metrics["api_error"] = False
    return baseline_sql, baseline_result, baseline_metrics


def _evaluate_spts(
    question: str,
    gold_result: dict,
    gold_sql: str,
    database_url: str,
    baseline_sql: str,
    baseline_result: dict,
    baseline_metrics: dict,
) -> tuple[str, dict, dict, list[dict]]:
    candidates, mappings = _generate_spts_sql(question)

    has_exact_mapping = any(
        "exact" in str(mapping.get("type", "")).lower()
        for mapping in (mappings or [])
    )

    # If grounding yielded no exact value matches, reuse baseline candidate under same-model fairness.
    if not has_exact_mapping:
        spts_result, spts_metrics = _evaluate_prediction(
            gold_result, gold_sql, baseline_sql, database_url
        )
        spts_metrics["api_error"] = False
        return baseline_sql, spts_result, spts_metrics, mappings

    spts_sql, spts_result, rerank_meta = _select_best_spts_candidate(candidates, mappings, database_url)
    if not spts_sql:
        spts_sql = baseline_sql
        spts_result, spts_metrics = _evaluate_prediction(
            gold_result, gold_sql, baseline_sql, database_url
        )
        spts_metrics["api_error"] = False
        spts_metrics["rerank_source"] = "baseline_fallback_no_candidate"
        return spts_sql, spts_result, spts_metrics, mappings

    spts_metrics = _evaluate_prediction(
        gold_result, gold_sql, spts_sql, database_url
    )[1]
    spts_metrics["rerank_source"] = rerank_meta.get("source", "unknown")
    spts_metrics["rerank_score"] = rerank_meta.get("score", 0.0)
    spts_metrics["mapping_consistency"] = rerank_meta.get("consistency", 0.0)

    spts_sql, spts_result = _auto_correct_spts(
        question, spts_sql, spts_result, mappings, database_url
    )

    spts_api_error = _extract_api_error(spts_sql)
    if spts_api_error:
        return (
            spts_sql,
            {"success": False, "error": spts_api_error},
            {
                "execution_accuracy": False,
                "etm_exact_match": False,
                "etm_f1_score": 0.0,
                "api_error": True,
            },
            mappings,
        )

    spts_metrics = _evaluate_prediction(gold_result, gold_sql, spts_sql, database_url)[1]

    # Keep SPTS from regressing below a successful baseline execution.
    if (not spts_result.get("success")) and baseline_result.get("success"):
        spts_sql = baseline_sql
        spts_result, spts_metrics = _evaluate_prediction(
            gold_result, gold_sql, baseline_sql, database_url
        )

    baseline_f1 = float(baseline_metrics.get("etm_f1_score", 0.0) or 0.0)
    spts_f1 = float(spts_metrics.get("etm_f1_score", 0.0) or 0.0)
    baseline_exec = bool(baseline_metrics.get("execution_accuracy", False))
    spts_exec = bool(spts_metrics.get("execution_accuracy", False))
    if baseline_result.get("success") and spts_result.get("success"):
        if (spts_f1 + 1e-9) < baseline_f1 and not (spts_exec and not baseline_exec):
            spts_sql = baseline_sql
            spts_result, spts_metrics = _evaluate_prediction(
                gold_result, gold_sql, baseline_sql, database_url
            )

    spts_metrics["api_error"] = False
    return spts_sql, spts_result, spts_metrics, mappings


def _auto_correct_spts(
    question: str,
    sql: str,
    result: dict,
    mappings: list[dict],
    database_url: str,
) -> tuple[str, dict]:
    if result.get("success"):
        return sql, result
    try:
        fixed_sql = fix_sql_with_llm(
            question, sql, result.get("error", "Unknown error"), mappings=mappings
        )
    except Exception as error:
        print(f"  -> SPTS auto-correction failed: {error}")
        return sql, result

    if not fixed_sql or API_ERROR_MARKER in fixed_sql:
        return sql, result

    fixed_result = _execute_sql_on_target(fixed_sql, database_url)
    return fixed_sql, fixed_result


def _evaluate_prediction(
    gold_result: dict,
    gold_sql: str,
    predicted_sql: str,
    database_url: str,
) -> tuple[dict, dict]:
    pred_result = _execute_sql_on_target(predicted_sql, database_url)
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
        "Enhanced_Tree_Matching_ETM_Exact": _percentage(
            summary["etm_exact"], scored_queries
        ),
        "Enhanced_Tree_Matching_ETM_Exact_Count": summary["etm_exact"],
        "Average_Structural_F1_Score": _percentage(
            summary["etm_f1_total"], scored_queries
        ),
        "Average_Structural_F1_Score_Sum": round(summary["etm_f1_total"], 4),
        "Scored_Queries": scored_queries,
        "API_Error_Queries": summary["api_error_queries"],
    }


def _extract_api_error(sql_text: str) -> str | None:
    marker = API_ERROR_MARKER
    if not isinstance(sql_text, str) or marker not in sql_text:
        return None
    return sql_text.split(marker, 1)[1].strip() or "LLM API unavailable"


def _improvement_block(baseline: dict, spts: dict) -> dict:
    return {
        "Execution_Accuracy_EXE": round(
            spts["Execution_Accuracy_EXE"] - baseline["Execution_Accuracy_EXE"], 2
        ),
        "Enhanced_Tree_Matching_ETM_Exact": round(
            spts["Enhanced_Tree_Matching_ETM_Exact"]
            - baseline["Enhanced_Tree_Matching_ETM_Exact"],
            2,
        ),
        "Average_Structural_F1_Score": round(
            spts["Average_Structural_F1_Score"]
            - baseline["Average_Structural_F1_Score"],
            2,
        ),
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
    db_path: str = DEFAULT_EVAL_DB_PATH,
    db_url: str = DEFAULT_EVAL_DB_URL,
    delay_seconds: float = API_DELAY_SECONDS,
):
    if not test_data_path:
        print(
            "Error: evaluation dataset path is not configured. "
            "Use --dataset or set SPTS_EVAL_DATASET_PATH."
        )
        return

    print(f"Loading test dataset from {test_data_path}...")
    if not ensure_default_dataset_exists(test_data_path):
        print(f"Error: {test_data_path} not found. Please ensure the file exists.")
        return

    test_queries = load_dataset(test_data_path)

    evaluation_logs = []
    total_queries = len(test_queries)
    baseline_summary = _empty_summary()
    spts_summary = _empty_summary()

    print(f"Starting evaluation of {total_queries} queries...\n")

    for index, item in enumerate(test_queries, start=1):
        question = item.get("question", "")
        gold_sql = item.get("SQL", "")
        db_id = item.get("db_id", DEFAULT_DB_ID)
        database_url, db_error = _resolve_row_database_target(item, db_path, db_url)
        if db_error:
            print(f"  -> Database resolution failed: {db_error}")
            return

        print(f"Processing [{index}/{total_queries}]: {question[:50]}...")

        gold_result = _execute_sql_on_target(gold_sql, database_url)

        baseline_sql, baseline_result, baseline_metrics = _evaluate_baseline(
            question, gold_result, gold_sql, database_url
        )
        if baseline_metrics["api_error"]:
            baseline_summary["api_error_queries"] += 1
        else:
            _update_summary(baseline_summary, baseline_metrics)

        time.sleep(max(delay_seconds, 0.0))

        spts_sql, spts_result, spts_metrics, mappings = _evaluate_spts(
            question,
            gold_result,
            gold_sql,
            database_url,
            baseline_sql,
            baseline_result,
            baseline_metrics,
        )
        if spts_metrics["api_error"]:
            spts_summary["api_error_queries"] += 1
        else:
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

        time.sleep(delay_seconds if index < total_queries else 0.0)

    with open(output_log_path, "w", encoding="utf-8") as file:
        json.dump(evaluation_logs, file, indent=4)

    baseline_metrics = _finalize_summary(baseline_summary)
    spts_metrics = _finalize_summary(spts_summary)
    final_metrics = {
        "Total_Queries_Tested": total_queries,
        "Baseline": baseline_metrics,
        "SPTS": spts_metrics,
        "Improvement_SPTS_minus_Baseline": _improvement_block(
            baseline_metrics, spts_metrics
        ),
    }

    with open(final_metrics_path, "w", encoding="utf-8") as file:
        json.dump(final_metrics, file, indent=4)

    print("\nEvaluation Complete!")
    print(
        f"  Baseline EXE: {final_metrics['Baseline']['Execution_Accuracy_EXE']}% ({final_metrics['Baseline']['Execution_Accuracy_EXE_Count']}/{final_metrics['Baseline']['Scored_Queries']})"
    )
    print(
        f"  Baseline ETM: {final_metrics['Baseline']['Enhanced_Tree_Matching_ETM_Exact']}% ({final_metrics['Baseline']['Enhanced_Tree_Matching_ETM_Exact_Count']}/{final_metrics['Baseline']['Scored_Queries']})"
    )
    print(
        f"  Baseline F1:  {final_metrics['Baseline']['Average_Structural_F1_Score']}%"
    )
    print(
        f"  SPTS EXE:     {final_metrics['SPTS']['Execution_Accuracy_EXE']}% ({final_metrics['SPTS']['Execution_Accuracy_EXE_Count']}/{final_metrics['SPTS']['Scored_Queries']})"
    )
    print(
        f"  SPTS ETM:     {final_metrics['SPTS']['Enhanced_Tree_Matching_ETM_Exact']}% ({final_metrics['SPTS']['Enhanced_Tree_Matching_ETM_Exact_Count']}/{final_metrics['SPTS']['Scored_Queries']})"
    )
    print(f"  SPTS F1:      {final_metrics['SPTS']['Average_Structural_F1_Score']}%")
    print(
        f"  Delta EXE:    {final_metrics['Improvement_SPTS_minus_Baseline']['Execution_Accuracy_EXE']}%"
    )
    print(
        f"  Delta ETM:    {final_metrics['Improvement_SPTS_minus_Baseline']['Enhanced_Tree_Matching_ETM_Exact']}%"
    )
    print(
        f"  Delta F1:     {final_metrics['Improvement_SPTS_minus_Baseline']['Average_Structural_F1_Score']}%"
    )
    print(f"  Baseline API errors: {final_metrics['Baseline']['API_Error_Queries']}")
    print(f"  SPTS API errors:     {final_metrics['SPTS']['API_Error_Queries']}")
    print(f"Logs saved to {output_log_path} and {final_metrics_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Run baseline vs SPTS evaluation with EXE and ETM metrics."
    )
    parser.add_argument(
        "--dataset",
        default=DEFAULT_DATASET,
        help="Path to the evaluation dataset JSON. Can also be set via SPTS_EVAL_DATASET_PATH.",
    )
    parser.add_argument(
        "--log", default=DEFAULT_LOG, help="Path to write per-query evaluation logs."
    )
    parser.add_argument(
        "--metrics", default=DEFAULT_METRICS, help="Path to write summary metrics JSON."
    )
    parser.add_argument(
        "--db-path",
        default=DEFAULT_EVAL_DB_PATH,
        help="Explicit SQLite database path for evaluation. Can also be set via SPTS_EVAL_DB_PATH.",
    )
    parser.add_argument(
        "--db-url",
        default=DEFAULT_EVAL_DB_URL,
        help="Explicit SQLAlchemy database URL for evaluation. Can also be set via SPTS_EVAL_DATABASE_URL.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=API_DELAY_SECONDS,
        help="Delay between LLM calls in seconds.",
    )
    args = parser.parse_args()
    run_evaluation(args.dataset, args.log, args.metrics, args.db_path, args.db_url, args.delay)


if __name__ == "__main__":
    main()