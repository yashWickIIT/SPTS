try:
    from .db_client import execute_raw_sql
except ImportError:
    from db_client import execute_raw_sql

def execute_sql(sql: str):
    try:
        result = execute_raw_sql(sql)
        return {"success": True, "data": result}
    except Exception as e:
        return {"success": False, "error": str(e)}