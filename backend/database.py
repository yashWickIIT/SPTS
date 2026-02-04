import sqlite3
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "..", "data", "bird_mini_dev.sqlite")

def execute_sql(sql: str):
    if not os.path.exists(DB_PATH):
        return [("Error: Database not found at " + DB_PATH,)]
        
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        conn.close()
        return result
    except Exception as e:
        return [(str(e),)]