import sqlite3
import pandas as pd
import os

DB_PATH = "../data/c.sqlite" 
TABLE_NAME = "schools"
COLUMN_NAME = "District"  # We focus on Districts (e.g., "Los Angeles Unified" vs "LAUSD")

def scan_database():
    if not os.path.exists(DB_PATH):
        print(f"Error: Database '{DB_PATH}' not found. Please copy it from BIRD Mini Dev.")
        return

    conn = sqlite3.connect(DB_PATH)
    
    print(f"Scanning table '{TABLE_NAME}' for column '{COLUMN_NAME}'...")
    
    # Get top 50 most frequent districts (Real-world distribution)
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
        
        # Save as 'db_values.csv' (These are the GROUND TRUTH values)
        df.to_csv("db_values.csv", index=False)
        print(f"Success! Scanned {len(df)} unique values. Saved to 'db_values.csv'.")
        print("   (Example values found: " + ", ".join(df[COLUMN_NAME].head(3).tolist()) + "...)")
        
    except Exception as e:
        print(f"Database Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    scan_database()