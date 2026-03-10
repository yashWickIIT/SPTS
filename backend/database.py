import sqlite3
import os

from config import get_env_path

DB_PATH = get_env_path("SPTS_MAIN_DB_PATH", os.path.join("data", "bird_mini_dev.sqlite"))

def execute_sql(sql: str):
    if not os.path.exists(DB_PATH):
        return {"success": False, "error": f"Error: Database not found at {DB_PATH}"}
        
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        conn.close()
        return {"success": True, "data": result}
    except Exception as e:
        return {"success": False, "error": str(e)}