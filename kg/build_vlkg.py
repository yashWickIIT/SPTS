import sqlite3
import json
import os
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "..", "data", "bird_mini_dev.sqlite")
OUTPUT_PATH = os.path.join(BASE_DIR, "vlkg.json")

API_KEY = os.getenv("API_KEY")
client = Groq(api_key=API_KEY)

def generate_synonyms(value, column_context):
    """
    Research Innovation: Context-Aware Synonym Generation.
    We pass the column name so the LLM knows if 'Apple' is a fruit or a tech company.
    """
    prompt = f"""
    Context: Database Column '{column_context}'.
    Value: "{value}".
    Task: Generate 3 likely user abbreviations, slang, or variations for this value.
    Example: "Los Angeles Unified" -> ["LAUSD", "LA Unified", "L.A. Schools"]
    Output ONLY JSON object with key 'synonyms'.
    """
    try:
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            response_format={"type": "json_object"}
        )
        return json.loads(resp.choices[0].message.content).get("synonyms", [])
    except:
        return []

def build_vlkg():
    if not os.path.exists(os.path.dirname(OUTPUT_PATH)):
        os.makedirs(os.path.dirname(OUTPUT_PATH))

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Dynamic schema scanning
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [r[0] for r in cursor.fetchall()]
    
    graph = {}
    print(f"SPTS Profiler: Scanning {len(tables)} tables for text columns...")

    for table in tables:
        # Find all TEXT columns in this table
        cursor.execute(f"PRAGMA table_info({table})")
        columns = cursor.fetchall()
        
        # Filter for TEXT columns
        text_cols = [c[1] for c in columns if 'TEXT' in c[2].upper()]
        
        for col in text_cols:
            print(f"   PLEASE WAIT: Profiling {table}.{col}...")
            
            cursor.execute(f"SELECT DISTINCT \"{col}\" FROM \"{table}\" WHERE \"{col}\" IS NOT NULL LIMIT 15")
            values = [row[0] for row in cursor.fetchall()]
            
            for canonical in values:
                # Generate synonyms
                aliases = generate_synonyms(canonical, col)
                
                # Adding initials rule
                initials = "".join(w[0] for w in canonical.split() if w.isalnum()).lower()
                if len(initials) > 1 and len(canonical.split()) > 1:
                    aliases.append(initials)

                # Build graph
                for alias in aliases:
                    clean_alias = alias.lower().strip()
                    if clean_alias not in graph:
                        graph[clean_alias] = []
                    
                    if not any(entry['canonical'] == canonical for entry in graph[clean_alias]):
                        graph[clean_alias].append({
                            "canonical": canonical,
                            "table": table,
                            "column": col
                        })

    conn.close()
    
    with open(OUTPUT_PATH, "w") as f:
        json.dump(graph, f, indent=2)

    print(f"Value-Level Knowledge Graph saved to {OUTPUT_PATH}")

if __name__ == "__main__":
    build_vlkg()