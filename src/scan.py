import sqlite3
import pandas as pd
import os
from pathlib import Path

# --- CONFIGURATION ---
# Robust pathing: Finds 'data' folder relative to this script
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR.parent / "data"
DB_PATH = DATA_DIR / "c.sqlite"

TABLE_NAME = "schools"
COLUMN_NAME = "District"  # The column with "Long Formal Names" (e.g., Los Angeles Unified)

def scan_database():
    if not DB_PATH.exists():
        print(f"❌ Error: Database not found at {DB_PATH}")
        print("   Please create a 'data' folder in SPTS and paste the BIRD sqlite file there as 'c.sqlite'.")
        return

    conn = sqlite3.connect(str(DB_PATH))
    print(f"🔍 Scanning table '{TABLE_NAME}' for column '{COLUMN_NAME}'...")
    
    # 1. Extract Top Frequent Values (The "Ground Truth" for the Graph)
    query = f"""
    SELECT {COLUMN_NAME}, COUNT(*) as frequency 
    FROM {TABLE_NAME} 
    WHERE {COLUMN_NAME} IS NOT NULL 
    GROUP BY {COLUMN_NAME} 
    ORDER BY frequency DESC 
    LIMIT 50
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        
        # Save values for the Profiler
        output_csv = BASE_DIR / "db_values.csv"
        df.to_csv(output_csv, index=False)
        print(f"✅ Extracted {len(df)} unique values -> '{output_csv.name}'")
        
        # 2. Extract Schema (Crucial for the LLM to write valid SQL)
        schema_df = pd.read_sql_query(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", 
            conn, params=[TABLE_NAME]
        )
        if not schema_df.empty:
            schema_out = BASE_DIR / "bird_schema.txt"
            with open(schema_out, "w") as f:
                f.write(schema_df.iloc[0]["sql"])
            print(f"✅ Schema saved -> '{schema_out.name}'")
            
    except Exception as e:
        print(f"❌ Database Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    scan_database()