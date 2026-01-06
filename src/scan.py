import sqlite3
import pandas as pd

DB_PATH = "../data/c.sqlite"
TABLE_NAME = "schools"
COLUMN_NAME = "County"


def scan_database():
    conn = sqlite3.connect(DB_PATH)
    query = f"""
    SELECT {COLUMN_NAME}, COUNT(*) as frequency 
    FROM {TABLE_NAME} 
    GROUP BY {COLUMN_NAME} 
    ORDER BY frequency DESC 
    LIMIT 100
    """
    df = pd.read_sql_query(query, conn)
    df.to_csv("dirty_values.csv", index=False)
    print(
        f"✅ Scanned top 100 values from '{COLUMN_NAME}' and saved to dirty_values.csv"
    )
    conn.close()


if __name__ == "__main__":
    scan_database()
