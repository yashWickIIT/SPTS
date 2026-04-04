import collections
import re

try:
    from sqlglot import exp, parse_one
except ImportError:
    exp = None
    parse_one = None

def categorize_sqlite_error(error_msg: str) -> str:
    if not error_msg:
        return "None"
    
    error_msg = str(error_msg).lower()
    if "no such table" in error_msg or "no such column" in error_msg:
        return "Schema Error (No such table/column)"
    elif "syntax error" in error_msg or "incomplete input" in error_msg or "near" in error_msg:
        return "Syntax Error"
    elif "ambiguous column" in error_msg:
        return "Ambiguous Column Reference"
    elif "no such function" in error_msg:
        return "Invalid Function"
    else:
        return "Execution Error (Other)"

def _normalize_value(val):
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        try:
            return float(val)
        except ValueError:
            return val.lower().strip()
    return val

def _normalize_row(row):
    return tuple(_normalize_value(val) for val in row)

def compare_execution_results(gold_result, pred_result):
    """Execution Accuracy (EXE)"""
    if not gold_result.get("success") or not pred_result.get("success"):
        return False
        
    gold_data = gold_result.get("data", [])
    pred_data = pred_result.get("data", [])
    
    if len(gold_data) == 0 and len(pred_data) == 0:
        return True
    if len(gold_data) != len(pred_data):
        return False
        
    gold_counter = collections.Counter(_normalize_row(row) for row in gold_data)
    pred_counter = collections.Counter(_normalize_row(row) for row in pred_data)
    
    return gold_counter == pred_counter

_JOIN_OPERATOR_TYPES = (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)
_WHERE_OPERATOR_TYPES = (
    exp.EQ,
    exp.NEQ,
    exp.GT,
    exp.GTE,
    exp.LT,
    exp.LTE,
    exp.In,
    exp.Like,
    exp.Between,
)


def _add_feature(features: collections.Counter, clause: str, feature_type: str, value):
    if not value:
        return
    clean_val = re.sub(r"\s+", " ", str(value).strip().lower())
    key = f"{clause}::{feature_type}::{clean_val}"
    features[key] += 1


def _collect_select_features(expression, features: collections.Counter):
    if not expression.args.get("expressions"):
        return
    for selected_expression in expression.expressions:
        for column in selected_expression.find_all(exp.Column):
            _add_feature(features, "SELECT", "COLUMN", column.name)
        for aggregate in selected_expression.find_all(exp.AggFunc):
            _add_feature(features, "SELECT", "AGGREGATE", aggregate.key)


def _collect_from_features(expression, features: collections.Counter):
    from_clause = expression.args.get("from")
    if not from_clause:
        return
    for table in from_clause.find_all(exp.Table):
        _add_feature(features, "FROM", "TABLE", table.name)


def _collect_join_features(expression, features: collections.Counter):
    for join in expression.args.get("joins") or []:
        if join.this and isinstance(join.this, exp.Table):
            _add_feature(features, "JOIN", "TABLE", join.this.name)
        on_clause = join.args.get("on")
        if not on_clause:
            continue
        for column in on_clause.find_all(exp.Column):
            _add_feature(features, "JOIN", "ON_COLUMN", column.name)
        for operator in on_clause.find_all(_JOIN_OPERATOR_TYPES):
            _add_feature(features, "JOIN", "ON_OPERATOR", operator.key)


def _collect_where_features(expression, features: collections.Counter):
    where_clause = expression.args.get("where")
    if not where_clause:
        return
    for column in where_clause.find_all(exp.Column):
        _add_feature(features, "WHERE", "COLUMN", column.name)
    for operator in where_clause.find_all(_WHERE_OPERATOR_TYPES):
        _add_feature(features, "WHERE", "OPERATOR", operator.key)


def _collect_group_features(expression, features: collections.Counter):
    group_clause = expression.args.get("group")
    if not group_clause:
        return
    for column in group_clause.find_all(exp.Column):
        _add_feature(features, "GROUP", "COLUMN", column.name)


def _collect_order_features(expression, features: collections.Counter):
    order_clause = expression.args.get("order")
    if not order_clause:
        return
    for column in order_clause.find_all(exp.Column):
        _add_feature(features, "ORDER", "COLUMN", column.name)


def _collect_limit_features(expression, features: collections.Counter):
    if expression.args.get("limit"):
        _add_feature(features, "LIMIT", "EXISTS", "true")


def _extract_hierarchical_features(expression) -> collections.Counter:
    """Extracts structural features hierarchically, ignoring literals."""
    features = collections.Counter()
    if not expression:
        return features

    collectors = (
        _collect_select_features,
        _collect_from_features,
        _collect_join_features,
        _collect_where_features,
        _collect_group_features,
        _collect_order_features,
        _collect_limit_features,
    )
    for collector in collectors:
        collector(expression, features)
    return features

def evaluate_etm(gold_sql: str, pred_sql: str, dialect: str = "sqlite") -> dict:
    """Calculates Enhanced Tree Matching (ETM) purely on structural equivalence.

    Args:
        gold_sql: The gold-standard SQL string.
        pred_sql: The predicted SQL string.
        dialect: sqlglot dialect used for parsing (e.g. ``"sqlite"``, ``"mysql"``,
            ``"postgres"``, ``"tsql"``). Defaults to ``"sqlite"`` for backward compat.
    """
    gold_sql = (gold_sql or "").strip()
    pred_sql = (pred_sql or "").strip()

    result = {"is_exact_match": False, "f1_score": 0.0}

    if not gold_sql or not pred_sql:
        return result

    if parse_one is None or exp is None:
        print("Warning: sqlglot not installed. ETM skipped.")
        return result

    try:
        gold_tree = parse_one(gold_sql, read=dialect)
        pred_tree = parse_one(pred_sql, read=dialect)
    except Exception:
        # Malformed SQL that cannot be parsed
        return result

    gold_features = _extract_hierarchical_features(gold_tree)
    pred_features = _extract_hierarchical_features(pred_tree)

    gold_total = sum(gold_features.values())
    pred_total = sum(pred_features.values())

    if gold_total == 0 or pred_total == 0:
        return result

    if gold_features == pred_features:
        result["is_exact_match"] = True

    overlap = sum(min(gold_features[key], pred_features[key]) for key in set(gold_features) | set(pred_features))
    precision = overlap / pred_total if pred_total > 0 else 0.0
    recall = overlap / gold_total if gold_total > 0 else 0.0

    if precision + recall > 0:
        result["f1_score"] = (2 * precision * recall) / (precision + recall)

    return result