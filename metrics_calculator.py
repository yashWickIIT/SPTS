import collections
import re

def categorize_sqlite_error(error_msg: str) -> str:
    """
    Categorizes the SQLite error string into specific buckets for analysis.
    """
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
    """
    Normalizes a single value for strict academic comparison.
    Handles type coercion (e.g., SQLite returning 1 vs 1.0) and string case.
    """
    if val is None:
        return None
    
    # Try converting to float if it's numeric
    if isinstance(val, (int, float)):
        return float(val)
        
    if isinstance(val, str):
        # Try to parse string as float if possible ("1" == 1.0)
        try:
            return float(val)
        except ValueError:
            # Fallback to lowercased string, stripped
            return val.lower().strip()
            
    return val

def _normalize_row(row):
    """
    Normalizes a row (tuple or list) by normalizing each value inside it.
    Returns a tuple so it can be hashed.
    """
    return tuple(_normalize_value(val) for val in row)

def compare_execution_results(gold_result, pred_result):
    """
    Compares the execution results of two queries using academic standards.
    Treats the results as unordered multisets and normalizes values.
    
    gold_result: dict from execute_sql -> {"success": bool, "data": [...]}
    pred_result: dict from execute_sql -> {"success": bool, "data": [...]}
    
    Returns: bool (True if strictly equal regardless of order, False otherwise)
    """
    # If either failed entirely, they don't match (unless gold failed too, 
    # but in Text-to-SQL gold shouldn't fail)
    if not gold_result.get("success") or not pred_result.get("success"):
        return False
        
    gold_data = gold_result.get("data", [])
    pred_data = pred_result.get("data", [])
    
    # Empty sets matching
    if len(gold_data) == 0 and len(pred_data) == 0:
        return True
    
    if len(gold_data) != len(pred_data):
        return False
        
    # Convert rows to normalized tuples and count frequencies to handle order invariance
    gold_counter = collections.Counter(_normalize_row(row) for row in gold_data)
    pred_counter = collections.Counter(_normalize_row(row) for row in pred_data)
    
    return gold_counter == pred_counter
