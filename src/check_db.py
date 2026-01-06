import sqlite3
import pandas as pd

DB_PATH = "../data/c.sqlite"
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()

print("📂 TABLES FOUND:")
for t in tables:
    print(f"- {t[0]}")
    try:
        df = pd.read_sql_query(f"SELECT * FROM {t[0]} LIMIT 5", conn)
        print(df.head())
        print("-" * 30)
    except:
        pass

conn.close()
